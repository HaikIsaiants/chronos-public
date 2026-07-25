package coordinator

import (
	"errors"
	"fmt"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

const LogBatchVersion uint32 = 1

var ErrNoLeader = errors.New("no raft leader")
var ErrUnavailable = errors.New("coordinator unavailable")
var ErrStopped = errors.New("coordinator stopped")

type LogBatch struct {
	Version  uint32              `json:"version"`
	Commands []core.CommandBatch `json:"commands"`
}

type Status struct {
	NodeID           uint64         `json:"node_id"`
	PID              int            `json:"pid"`
	LeaderID         uint64         `json:"leader_id"`
	Term             uint64         `json:"term"`
	CommitIndex      uint64         `json:"commit_index"`
	AppliedIndex     uint64         `json:"applied_index"`
	PendingProposals int            `json:"pending_proposals"`
	Role             raft.StateType `json:"role"`
}

type NotLeaderError struct {
	NodeID   uint64
	LeaderID uint64
}

func (e NotLeaderError) Error() string {
	if e.LeaderID == 0 {
		return fmt.Sprintf("coordinator %d has no leader", e.NodeID)
	}
	return fmt.Sprintf("coordinator %d is not leader; leader is %d", e.NodeID, e.LeaderID)
}

func (e NotLeaderError) Is(target error) bool {
	return target == ErrNoLeader
}

type Transport interface {
	Send(*pb.Message) error
}

type TransportFunc func(*pb.Message) error

func (function TransportFunc) Send(message *pb.Message) error {
	return function(message)
}
