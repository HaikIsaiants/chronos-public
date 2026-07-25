package coordinator

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type ClusterConfig struct {
	Directory  string
	ClusterID  string
	Failpoints map[uint64]storage.Failpoint
}

type Cluster struct {
	config         ClusterConfig
	voters         []uint64
	nodes          map[uint64]*Coordinator
	links          map[[2]uint64]bool
	messages       []*pb.Message
	heartbeats     []*pb.Message
	holdHeartbeats bool
}

func NewCluster(config ClusterConfig) (*Cluster, error) {
	if config.Directory == "" || config.ClusterID == "" {
		return nil, fmt.Errorf("invalid cluster configuration")
	}
	if err := os.MkdirAll(config.Directory, 0o755); err != nil {
		return nil, err
	}
	cluster := &Cluster{
		config: config, voters: []uint64{1, 2, 3}, nodes: make(map[uint64]*Coordinator),
		links: make(map[[2]uint64]bool),
	}
	for _, from := range cluster.voters {
		for _, to := range cluster.voters {
			cluster.links[[2]uint64{from, to}] = true
		}
	}
	for _, id := range cluster.voters {
		if err := cluster.open(id); err != nil {
			cluster.Close()
			return nil, err
		}
	}
	return cluster, nil
}

func (c *Cluster) open(id uint64) error {
	node, err := Open(Config{
		ID: id, ClusterID: c.config.ClusterID, Directory: filepath.Join(c.config.Directory, fmt.Sprintf("node-%d", id)),
		Voters: c.voters, Failpoint: c.config.Failpoints[id],
		Transport: TransportFunc(func(message *pb.Message) error {
			c.messages = append(c.messages, message)
			return nil
		}),
	})
	if err != nil {
		return err
	}
	c.nodes[id] = node
	return nil
}

func (c *Cluster) Node(id uint64) (*Coordinator, bool) {
	node, exists := c.nodes[id]
	return node, exists
}

func (c *Cluster) Campaign(id uint64) error {
	node, exists := c.nodes[id]
	if !exists {
		return ErrUnavailable
	}
	if err := node.Campaign(); err != nil {
		return err
	}
	return c.Drive(10000)
}

func (c *Cluster) Elect(id uint64) error {
	if err := c.Campaign(id); err != nil {
		return err
	}
	node, exists := c.nodes[id]
	if !exists || node.Status().Role != raft.StateLeader {
		return fmt.Errorf("node %d did not become leader", id)
	}
	return nil
}

func (c *Cluster) Leader() (uint64, bool) {
	ids := c.activeIDs()
	for _, id := range ids {
		if c.nodes[id].Status().LeaderID == id {
			return id, true
		}
	}
	return 0, false
}

func (c *Cluster) Drive(limit int) error {
	for operations := 0; len(c.messages) > 0; operations++ {
		if operations >= limit {
			return ErrUnavailable
		}
		if err := c.deliverOne(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) deliverOne() error {
	message := c.messages[0]
	c.messages = c.messages[1:]
	if c.holdHeartbeats && (message.GetType() == pb.MsgHeartbeat || message.GetType() == pb.MsgHeartbeatResp) {
		c.heartbeats = append(c.heartbeats, message)
		return nil
	}
	source, sourceUp := c.nodes[message.GetFrom()]
	target, targetUp := c.nodes[message.GetTo()]
	delivered := sourceUp && targetUp && c.links[[2]uint64{message.GetFrom(), message.GetTo()}]
	if !delivered {
		if sourceUp && message.GetType() == pb.MsgSnap {
			return source.ReportSnapshot(message.GetTo(), false)
		}
		return nil
	}
	if err := target.Step(message); err != nil {
		return err
	}
	if message.GetType() == pb.MsgSnap {
		return source.ReportSnapshot(message.GetTo(), true)
	}
	return nil
}

func (c *Cluster) Tick(count int) error {
	for tick := 0; tick < count; tick++ {
		for _, id := range c.activeIDs() {
			if err := c.nodes[id].Tick(); err != nil {
				return err
			}
			if err := c.Drive(10000); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Cluster) WaitLeader(maxTicks int) (uint64, error) {
	for tick := 0; tick <= maxTicks; tick++ {
		if leader, exists := c.Leader(); exists {
			return leader, nil
		}
		if tick < maxTicks {
			if err := c.Tick(1); err != nil {
				return 0, err
			}
		}
	}
	return 0, ErrNoLeader
}

func (c *Cluster) Propose(targetID uint64, commands []core.Command) error {
	target, exists := c.nodes[targetID]
	if !exists {
		return ErrUnavailable
	}
	leaderID := target.Status().LeaderID
	if leaderID == 0 {
		return ErrNoLeader
	}
	leader, exists := c.nodes[leaderID]
	if !exists {
		return ErrNoLeader
	}
	_, err := leader.Propose(commands)
	return err
}

func (c *Cluster) Submit(targetID uint64, command core.Command, limit int) (core.Result, error) {
	hash, err := core.HashCommand(command)
	if err != nil {
		return core.Result{}, err
	}
	if result, found, err := c.lookup(command.RequestID, hash); err != nil || found {
		if found {
			result.Duplicate = true
		}
		return result, err
	}
	target, exists := c.nodes[targetID]
	if !exists {
		return core.Result{}, ErrUnavailable
	}
	leaderID := target.Status().LeaderID
	if leaderID == 0 {
		return core.Result{}, ErrNoLeader
	}
	leader, exists := c.nodes[leaderID]
	if !exists {
		return core.Result{}, ErrNoLeader
	}
	immediate, err := leader.Propose([]core.Command{command})
	if err != nil {
		return core.Result{}, err
	}
	if result, exists := immediate[command.RequestID]; exists {
		return result, nil
	}
	for operations := 0; operations < limit; operations++ {
		if result, found, err := c.lookup(command.RequestID, hash); err != nil || found {
			return result, err
		}
		if len(c.messages) == 0 {
			break
		}
		if err := c.deliverOne(); err != nil {
			return core.Result{}, err
		}
	}
	return core.Result{}, ErrUnavailable
}

func (c *Cluster) SubmitBatch(targetID uint64, commands []core.Command, limit int) ([]core.Result, error) {
	if len(commands) == 0 {
		return nil, fmt.Errorf("batch is empty")
	}
	target, exists := c.nodes[targetID]
	if !exists {
		return nil, ErrUnavailable
	}
	leaderID := target.Status().LeaderID
	if leaderID == 0 {
		return nil, ErrNoLeader
	}
	leader, exists := c.nodes[leaderID]
	if !exists {
		return nil, ErrNoLeader
	}
	immediate, err := leader.Propose(commands)
	if err != nil {
		return nil, err
	}
	results := make([]core.Result, len(commands))
	done := make([]bool, len(commands))
	for index, command := range commands {
		if result, exists := immediate[command.RequestID]; exists {
			results[index] = result
			done[index] = true
		}
	}
	for operations := 0; operations < limit; operations++ {
		complete := true
		for index, command := range commands {
			if done[index] {
				continue
			}
			hash, err := core.HashCommand(command)
			if err != nil {
				return nil, err
			}
			result, found, err := c.lookup(command.RequestID, hash)
			if err != nil {
				return nil, err
			}
			if found {
				results[index] = result
				done[index] = true
			} else {
				complete = false
			}
		}
		if complete {
			return results, nil
		}
		if len(c.messages) == 0 {
			break
		}
		if err := c.deliverOne(); err != nil {
			return nil, err
		}
	}
	return nil, ErrUnavailable
}

func (c *Cluster) lookup(requestID, requestHash string) (core.Result, bool, error) {
	for _, id := range c.activeIDs() {
		receipt, exists, err := c.nodes[id].store.Receipt(requestID)
		if err != nil {
			return core.Result{}, false, err
		}
		if exists {
			if receipt.RequestHash != requestHash {
				return core.Result{}, false, core.ErrRequestConflict
			}
			return receipt.Result, true, nil
		}
		outcome, exists, err := c.nodes[id].store.Outcome(requestID, requestHash)
		if err != nil {
			return core.Result{}, false, err
		}
		if exists && outcome.Code != "" {
			return core.Result{}, false, core.ErrRequestConflict
		}
	}
	return core.Result{}, false, nil
}

func (c *Cluster) SetLink(from, to uint64, enabled bool) {
	c.links[[2]uint64{from, to}] = enabled
}

func (c *Cluster) Isolate(id uint64) {
	for _, peer := range c.voters {
		if peer == id {
			continue
		}
		c.SetLink(id, peer, false)
		c.SetLink(peer, id, false)
	}
}

func (c *Cluster) Heal() {
	for _, from := range c.voters {
		for _, to := range c.voters {
			c.SetLink(from, to, true)
		}
	}
}

func (c *Cluster) HoldHeartbeats() {
	c.holdHeartbeats = true
}

func (c *Cluster) ReleaseHeartbeats() error {
	c.holdHeartbeats = false
	// Put held heartbeats ahead of newer traffic
	c.messages = append(c.heartbeats, c.messages...)
	c.heartbeats = nil
	return c.Drive(10000)
}

func (c *Cluster) Crash(id uint64) error {
	node, exists := c.nodes[id]
	if !exists {
		return nil
	}
	delete(c.nodes, id)
	return node.Close()
}

func (c *Cluster) Restart(id uint64) error {
	if _, exists := c.nodes[id]; exists {
		return fmt.Errorf("node %d is already running", id)
	}
	return c.open(id)
}

func (c *Cluster) Snapshot(id uint64) error {
	node, exists := c.nodes[id]
	if !exists {
		return ErrUnavailable
	}
	_, err := node.CreateSnapshot()
	return err
}

func (c *Cluster) Converged() (bool, error) {
	ids := c.activeIDs()
	if len(ids) < 2 {
		return true, nil
	}
	first, err := c.nodes[ids[0]].store.DurableSnapshot()
	if err != nil {
		return false, err
	}
	firstApplied := c.nodes[ids[0]].store.Applied()
	for _, id := range ids[1:] {
		current, err := c.nodes[id].store.DurableSnapshot()
		if err != nil {
			return false, err
		}
		applied := c.nodes[id].store.Applied()
		if !reflect.DeepEqual(current, first) || applied != firstApplied {
			return false, nil
		}
	}
	return true, nil
}

func (c *Cluster) Close() error {
	var joined error
	for _, id := range c.activeIDs() {
		joined = errors.Join(joined, c.nodes[id].Close())
		delete(c.nodes, id)
	}
	return joined
}

func (c *Cluster) activeIDs() []uint64 {
	ids := make([]uint64, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	// Stable node order keeps the simulation reproducible
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
