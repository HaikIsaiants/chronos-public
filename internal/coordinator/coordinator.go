package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/observability"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	ID            uint64
	ClusterID     string
	Directory     string
	Voters        []uint64
	Transport     Transport
	Failpoint     storage.Failpoint
	Observability *observability.Provider
}

type Coordinator struct {
	id            uint64
	store         *storage.Store
	raw           *raft.RawNode
	transport     Transport
	stopped       bool
	status        Status
	readStates    []raft.ReadState
	outcomes      []storage.Outcome
	proposed      []proposedBatch
	prepared      map[uint64]proposedBatch
	fatal         error
	observability *observability.Provider
}

const maxRaftMessageSize = 2 << 20

func Open(config Config) (*Coordinator, error) {
	if config.Transport == nil {
		return nil, fmt.Errorf("transport is required")
	}
	store, err := storage.Open(storage.Config{
		Directory: config.Directory, ClusterID: config.ClusterID, NodeID: config.ID,
		Voters: config.Voters, Failpoint: config.Failpoint,
	})
	if err != nil {
		return nil, err
	}
	applied := store.Applied()
	raw, err := raft.NewRawNode(&raft.Config{
		ID: config.ID, ElectionTick: 10, HeartbeatTick: 1, Storage: store, Applied: applied.Index,
		MaxSizePerMsg: maxRaftMessageSize, MaxCommittedSizePerReady: 4 << 20,
		MaxUncommittedEntriesSize: 64 << 20, MaxInflightMsgs: 256, MaxInflightBytes: 16 << 20,
		CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe,
		DisableProposalForwarding: true, AsyncStorageWrites: false,
	})
	if err != nil {
		store.Close()
		return nil, err
	}
	coordinator := &Coordinator{id: config.ID, store: store, raw: raw, transport: config.Transport, observability: config.Observability}
	coordinator.updateStatus()
	if err := coordinator.ProcessReady(); err != nil {
		store.Close()
		return nil, err
	}
	return coordinator, nil
}

func (c *Coordinator) ID() uint64 {
	return c.id
}

func (c *Coordinator) Status() Status {
	return c.status
}

func (c *Coordinator) Fatal() error {
	return c.fatal
}

func (c *Coordinator) Store() *storage.Store {
	return c.store
}

func (c *Coordinator) Campaign() error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	if err := c.raw.Campaign(); err != nil {
		return err
	}
	return c.ProcessReady()
}

func (c *Coordinator) Tick() error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	c.raw.Tick()
	return c.ProcessReady()
}

func (c *Coordinator) Step(message *pb.Message) error {
	return c.StepContext(context.Background(), message)
}

func (c *Coordinator) StepContext(ctx context.Context, message *pb.Message) error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	if err := c.raw.Step(message); err != nil {
		return err
	}
	return c.ProcessReadyContext(ctx)
}

func (c *Coordinator) Propose(commands []core.Command) (map[string]core.Result, error) {
	return c.ProposeContext(context.Background(), commands)
}

func (c *Coordinator) ProposeContext(ctx context.Context, commands []core.Command) (map[string]core.Result, error) {
	if err := c.checkProposal(commands); err != nil {
		return nil, err
	}
	batches, immediate, err := c.store.Prepare(commands)
	if err != nil {
		return nil, err
	}
	return c.proposeBatchesContext(ctx, batches, immediate)
}

func (c *Coordinator) checkProposal(commands []core.Command) error {
	if err := c.checkStagedProposal(commands); err != nil {
		return err
	}
	status := c.raw.BasicStatus()
	applied := c.store.Applied()
	lastIndex, err := c.store.LastIndex()
	if err != nil {
		return err
	}
	// Planning starts at the applied log tail
	if applied.Term != status.GetTerm() || applied.Index != lastIndex {
		return ErrUnavailable
	}
	return nil
}

func (c *Coordinator) checkStagedProposal(commands []core.Command) error {
	if err := c.checkStagedProposalState(); err != nil {
		return err
	}
	if len(commands) == 0 {
		return fmt.Errorf("proposal is empty")
	}
	seen := make(map[string]struct{}, len(commands))
	for _, command := range commands {
		if _, exists := seen[command.RequestID]; exists {
			return fmt.Errorf("%w: duplicate request id", core.ErrInvalidCommand)
		}
		seen[command.RequestID] = struct{}{}
	}
	return nil
}

func (c *Coordinator) checkStagedProposalState() error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	status := c.raw.BasicStatus()
	if status.RaftState != raft.StateLeader {
		return NotLeaderError{NodeID: c.id, LeaderID: status.Lead}
	}
	return nil
}

type stagedBatch struct {
	commands []core.Command
	batches  []core.CommandBatch
}

type proposedBatch struct {
	data  []byte
	batch LogBatch
}

func (c *Coordinator) stagePreparedGroup(group []stagedBatch) error {
	if len(group) == 0 {
		return nil
	}
	if err := c.checkStagedProposalState(); err != nil {
		return err
	}
	batches := make([]LogBatch, 0, len(group))
	for _, proposal := range group {
		if len(proposal.batches) == 0 {
			continue
		}
		batches = append(batches, LogBatch{Version: LogBatchVersion, Commands: proposal.batches})
	}
	if len(batches) == 0 {
		return nil
	}
	return c.stageLogBatches(batches)
}

func (c *Coordinator) stageLogBatches(batches []LogBatch) error {
	for index := range batches {
		canonicalizeLogBatch(&batches[index])
	}
	encoded, err := encodeLogBatches(batches)
	if err != nil {
		return err
	}
	entries := make([]*pb.Entry, len(encoded))
	for index, data := range encoded {
		entries[index] = &pb.Entry{Data: data}
	}
	// c.proposed keeps the entry order passed to raft
	id := c.id
	if err := c.raw.Step(&pb.Message{Type: pb.MsgProp.Enum(), From: &id, Entries: entries}); err != nil {
		return err
	}
	for index := range batches {
		c.proposed = append(c.proposed, proposedBatch{data: encoded[index], batch: batches[index]})
	}
	return nil
}

func encodeLogBatches(batches []LogBatch) ([][]byte, error) {
	encoded := make([]struct {
		data []byte
		err  error
	}, len(batches))
	var wait sync.WaitGroup
	wait.Add(len(batches))
	for index := range batches {
		go func(index int) {
			defer wait.Done()
			encoded[index].data, encoded[index].err = encodeLogBatch(batches[index])
		}(index)
	}
	wait.Wait()
	data := make([][]byte, len(encoded))
	for index, encoding := range encoded {
		if encoding.err != nil {
			return nil, encoding.err
		}
		data[index] = encoding.data
	}
	return data, nil
}

func (c *Coordinator) proposeBatchesContext(ctx context.Context, batches []core.CommandBatch, immediate map[string]core.Result) (map[string]core.Result, error) {
	if len(batches) == 0 {
		return immediate, nil
	}
	if err := c.stageLogBatches([]LogBatch{{Version: LogBatchVersion, Commands: batches}}); err != nil {
		return nil, err
	}
	if err := c.ProcessReadyContext(ctx); err != nil {
		return nil, err
	}
	return immediate, nil
}

func (c *Coordinator) ProcessReady() (err error) {
	return c.ProcessReadyContext(context.Background())
}

func (c *Coordinator) ProcessReadyContext(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			c.fatal = err
		}
	}()
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	// a Ready reaches disk before any of its messages leave
	// the node
	for c.raw.HasReady() {
		ready := c.raw.Ready()
		c.cacheReadyEntries(ready.Entries)
		snapshotStarted := time.Now()
		if err := c.store.SaveReady(ready); err != nil {
			return err
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			if err := c.store.InstallSnapshot(ready.Snapshot); err != nil {
				if c.observability != nil {
					c.observability.RecordSnapshot("install", len(ready.Snapshot.GetData()), time.Since(snapshotStarted), err)
				}
				return err
			}
			if c.observability != nil {
				c.observability.RecordSnapshot("install", len(ready.Snapshot.GetData()), time.Since(snapshotStarted), nil)
			}
		}
		for _, message := range ready.Messages {
			copy := proto.Clone(message).(*pb.Message)
			var err error
			if transport, ok := c.transport.(interface {
				SendContext(context.Context, *pb.Message) error
			}); ok {
				err = transport.SendContext(ctx, copy)
			} else {
				err = c.transport.Send(copy)
			}
			if err != nil {
				c.raw.ReportUnreachable(copy.GetTo())
				if copy.GetType() == pb.MsgSnap {
					c.raw.ReportSnapshot(copy.GetTo(), raft.SnapshotFailure)
				}
			}
		}
		entries, err := c.decodeCommitted(ready.CommittedEntries)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			outcomes, err := c.store.ApplyCommitted(entries)
			if err != nil {
				return err
			}
			if c.raw.BasicStatus().RaftState == raft.StateLeader {
				// Keep leader outcomes for proposal responses
				c.outcomes = append(c.outcomes, outcomes...)
				if len(c.outcomes) > 32768 {
					c.outcomes = append([]storage.Outcome(nil), c.outcomes[len(c.outcomes)-32768:]...)
				}
			}
			if c.observability != nil {
				var events []core.Event
				for _, outcome := range outcomes {
					if outcome.Code != "" || outcome.Result.Duplicate {
						continue
					}
					events = append(events, outcome.Result.Events...)
				}
				submitted := make(map[string]struct{})
				for _, event := range events {
					if event.Kind == core.EventWorkflowSubmitted {
						c.observability.SeedWorkflowStart(event.WorkflowID, event.At)
						submitted[event.WorkflowID] = struct{}{}
					}
				}
				for _, event := range events {
					if !terminalObservedWorkflow(event.Kind) {
						continue
					}
					if _, exists := submitted[event.WorkflowID]; exists {
						continue
					}
					first, exists, err := c.store.FirstEvent(event.WorkflowID)
					if err == nil && exists && first.Kind == core.EventWorkflowSubmitted {
						c.observability.SeedWorkflowStart(event.WorkflowID, first.At)
					}
				}
				c.observability.RecordCommit(len(entries), events)
			}
		}
		c.readStates = append(c.readStates, ready.ReadStates...)
		c.raw.Advance(ready)
		c.updateStatus()
		if c.status.Role != raft.StateLeader {
			c.proposed = nil
			c.prepared = nil
		}
	}
	c.updateStatus()
	return nil
}

func (c *Coordinator) cacheReadyEntries(entries []*pb.Entry) {
	// exact bytes let committed entries reuse their staged batches
	for _, entry := range entries {
		match := -1
		for index := range c.proposed {
			if bytes.Equal(entry.GetData(), c.proposed[index].data) {
				match = index
				break
			}
		}
		if match < 0 {
			continue
		}
		if c.prepared == nil {
			c.prepared = make(map[uint64]proposedBatch)
		}
		c.prepared[entry.GetIndex()] = c.proposed[match]
		c.proposed = c.proposed[match+1:]
	}
	if len(c.proposed) == 0 {
		c.proposed = nil
	}
}

func (c *Coordinator) decodeCommitted(entries []*pb.Entry) ([]storage.ApplicationEntry, error) {
	result := make([]storage.ApplicationEntry, 0, len(entries))
	for _, entry := range entries {
		application := storage.ApplicationEntry{
			Version: storage.ApplicationEntryVersion, Index: entry.GetIndex(), Term: entry.GetTerm(),
		}
		switch entry.GetType() {
		case pb.EntryNormal:
			if len(entry.GetData()) > 0 {
				cached, exists := c.prepared[entry.GetIndex()]
				if exists {
					delete(c.prepared, entry.GetIndex())
				}
				if exists && bytes.Equal(cached.data, entry.GetData()) {
					application.Batches = cached.batch.Commands
				} else {
					batch, err := decodeLogBatch(entry.GetData())
					if err != nil {
						return nil, err
					}
					application.Batches = batch.Commands
				}
			}
		default:
			return nil, fmt.Errorf("fixed cluster cannot apply entry type %s", entry.GetType())
		}
		result = append(result, application)
	}
	if len(c.prepared) == 0 {
		c.prepared = nil
	}
	return result, nil
}

func terminalObservedWorkflow(kind core.EventKind) bool {
	switch kind {
	case core.EventWorkflowCompleted, core.EventWorkflowFailed, core.EventWorkflowCancelled, core.EventWorkflowCompensated:
		return true
	default:
		return false
	}
}

func (c *Coordinator) TakeReadStates() []raft.ReadState {
	states := append([]raft.ReadState(nil), c.readStates...)
	c.readStates = c.readStates[:0]
	return states
}

func (c *Coordinator) TakeOutcomes() []storage.Outcome {
	outcomes := c.outcomes
	c.outcomes = nil
	return outcomes
}

func decodeCommitted(entries []*pb.Entry) ([]storage.ApplicationEntry, error) {
	result := make([]storage.ApplicationEntry, 0, len(entries))
	for _, entry := range entries {
		application := storage.ApplicationEntry{
			Version: storage.ApplicationEntryVersion, Index: entry.GetIndex(), Term: entry.GetTerm(),
		}
		switch entry.GetType() {
		case pb.EntryNormal:
			if len(entry.GetData()) > 0 {
				batch, err := decodeLogBatch(entry.GetData())
				if err != nil {
					return nil, err
				}
				application.Batches = batch.Commands
			}
		default:
			return nil, fmt.Errorf("fixed cluster cannot apply entry type %s", entry.GetType())
		}
		result = append(result, application)
	}
	return result, nil
}

func (c *Coordinator) ReadIndex(token []byte) error {
	return c.ReadIndexContext(context.Background(), token)
}

func (c *Coordinator) ReadIndexContext(ctx context.Context, token []byte) error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	if len(token) == 0 || len(token) > math.MaxUint16 {
		return fmt.Errorf("invalid read context")
	}
	c.raw.ReadIndex(token)
	return c.ProcessReadyContext(ctx)
}

func (c *Coordinator) ReportSnapshot(nodeID uint64, success bool) error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	status := raft.SnapshotFailure
	if success {
		status = raft.SnapshotFinish
	}
	c.raw.ReportSnapshot(nodeID, status)
	return c.ProcessReady()
}

func (c *Coordinator) ReportUnreachable(nodeID uint64) error {
	if c.stopped {
		return ErrStopped
	}
	if c.fatal != nil {
		return c.fatal
	}
	c.raw.ReportUnreachable(nodeID)
	return c.ProcessReady()
}

func (c *Coordinator) CreateSnapshot() (*pb.Snapshot, error) {
	if c.stopped {
		return nil, ErrStopped
	}
	started := time.Now()
	snapshot, err := c.store.CreateSnapshot()
	if c.observability != nil {
		size := 0
		if snapshot != nil {
			size = len(snapshot.GetData())
		}
		c.observability.RecordSnapshot("create", size, time.Since(started), err)
	}
	if err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
		c.fatal = err
	}
	return snapshot, err
}

func (c *Coordinator) Close() error {
	if c.stopped {
		return nil
	}
	c.stopped = true
	return c.store.Close()
}

func (c *Coordinator) updateStatus() {
	previous := c.status
	status := c.raw.BasicStatus()
	applied := c.store.Applied()
	c.status = Status{
		NodeID: c.id, LeaderID: status.Lead, Term: status.GetTerm(), CommitIndex: status.GetCommit(),
		AppliedIndex: applied.Index, Role: status.RaftState,
	}
	if c.observability != nil {
		leader := c.status.Role == raft.StateLeader
		if leader && (previous.Role != raft.StateLeader || previous.Term != c.status.Term) {
			c.observability.RecordElection()
		}
		c.observability.SetRaft(leader, c.status.Term, c.status.CommitIndex, c.status.AppliedIndex)
	}
}
