package storage

import (
	"errors"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

const SchemaVersion uint32 = 6
const ApplicationEntryVersion uint32 = 1
const ReceiptVersion uint32 = 2
const OutcomeVersion uint32 = 2
const TaskResultVersion uint32 = 3
const SnapshotVersion uint32 = 4

var ErrStoreIdentity = errors.New("store identity mismatch")
var ErrStoreVersion = errors.New("unsupported store version")
var ErrRecordVersion = errors.New("unsupported record version")
var ErrAppliedGap = errors.New("applied index gap")
var ErrCorruptSnapshot = errors.New("corrupt snapshot")
var ErrInjectedCrash = errors.New("injected crash")

type Point string

const (
	RaftBeforeSync            Point = "raft_before_sync"
	RaftAfterSync             Point = "raft_after_sync"
	ApplyBeforeSync           Point = "app_before_sync"
	ApplyAfterSync            Point = "app_after_sync"
	SnapshotBeforeSync        Point = "snapshot_before_sync"
	SnapshotAfterSync         Point = "snapshot_after_sync"
	SnapshotInstallBeforeSync Point = "snapshot_install_before_sync"
	SnapshotInstallAfterSync  Point = "snapshot_install_after_sync"
	ActiveIndexBeforeSync     Point = "active_index_before_sync"
	ActiveIndexAfterSync      Point = "active_index_after_sync"
)

type Failpoint func(Point) error

type Config struct {
	Directory string
	ClusterID string
	NodeID    uint64
	Voters    []uint64
	Failpoint Failpoint
}

type ApplicationEntry struct {
	Version uint32              `json:"version"`
	Index   uint64              `json:"index"`
	Term    uint64              `json:"term"`
	Batches []core.CommandBatch `json:"batches"`
}

type Receipt struct {
	Version       uint32      `json:"version"`
	RequestID     string      `json:"request_id"`
	RequestHash   string      `json:"request_hash"`
	AppliedIndex  uint64      `json:"applied_index"`
	CommittedTerm uint64      `json:"committed_term,omitempty"`
	Result        core.Result `json:"result"`
}

type Outcome struct {
	Version       uint32      `json:"version"`
	RequestID     string      `json:"request_id"`
	RequestHash   string      `json:"request_hash"`
	AppliedIndex  uint64      `json:"applied_index"`
	CommittedTerm uint64      `json:"committed_term,omitempty"`
	Result        core.Result `json:"result"`
	Code          string      `json:"code,omitempty"`
	Message       string      `json:"message,omitempty"`
}

type TaskResult struct {
	Version      uint32            `json:"version"`
	WorkflowID   string            `json:"workflow_id"`
	TaskID       string            `json:"task_id"`
	EventKind    core.EventKind    `json:"event_kind"`
	TaskType     string            `json:"task_type"`
	Phase        string            `json:"phase"`
	AttemptID    string            `json:"attempt_id"`
	Attempt      uint32            `json:"attempt"`
	Status       core.TaskStatus   `json:"status"`
	Output       map[string]string `json:"output,omitempty"`
	Error        string            `json:"error,omitempty"`
	Sequence     uint64            `json:"sequence"`
	AppliedIndex uint64            `json:"applied_index"`
	At           int64             `json:"at"`
	WorkerID     string            `json:"worker_id"`
	Fence        uint64            `json:"fencing_token"`
	Idempotency  string            `json:"idempotency_key"`
}

type AppliedState struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

type SnapshotImage struct {
	Version      uint32              `json:"version"`
	ClusterID    string              `json:"cluster_id"`
	AppliedIndex uint64              `json:"applied_index"`
	Term         uint64              `json:"term"`
	Engine       core.EngineSnapshot `json:"engine"`
	Outcomes     []Outcome           `json:"outcomes"`
	TaskResults  []TaskResult        `json:"task_results"`
}
