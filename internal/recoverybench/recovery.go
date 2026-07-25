package recoverybench

import (
	"fmt"
	"time"

	"github.com/HaikIsaiants/chronos/internal/bench"
	"github.com/HaikIsaiants/chronos/internal/coordinator"
	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
	pb "go.etcd.io/raft/v3/raftpb"
)

const snapshotIndex uint64 = 2

type Options struct {
	Directory    string
	Workflows    uint64
	TailCommands uint64
}

type Report struct {
	Workflows              uint64 `json:"workflows"`
	StatesVerified         uint64 `json:"states_verified"`
	SnapshotIndex          uint64 `json:"snapshot_index"`
	TailEntries            uint64 `json:"tail_entries"`
	TailCommands           uint64 `json:"tail_commands"`
	TailEvents             uint64 `json:"tail_events"`
	AppliedBeforeRestart   uint64 `json:"applied_before_restart"`
	CommittedBeforeRestart uint64 `json:"committed_before_restart"`
	AppliedAfterRestart    uint64 `json:"applied_after_restart"`
	CommittedAfterRestart  uint64 `json:"committed_after_restart"`
	FixtureBuildNanos      int64  `json:"fixture_build_nanos"`
	PersistNanos           int64  `json:"persist_nanos"`
	RecoveryNanos          int64  `json:"recovery_nanos"`
	VerificationNanos      int64  `json:"verification_nanos"`
	IndexMismatches        uint64 `json:"index_mismatches"`
	StateHashMismatches    uint64 `json:"state_hash_mismatches"`
	VersionMismatches      uint64 `json:"version_mismatches"`
}

type fixture struct {
	snapshot core.EngineSnapshot
	expected []core.WorkflowState
	tail     []core.CommandBatch
}

func Run(options Options) (Report, error) {
	if options.Directory == "" || options.Workflows == 0 {
		return Report{}, fmt.Errorf("recovery directory and workflow count are required")
	}
	if options.TailCommands == 0 {
		options.TailCommands = options.Workflows
	}
	if options.TailCommands > options.Workflows && options.TailCommands-options.Workflows > options.Workflows {
		return Report{}, fmt.Errorf("tail command count must not exceed twice the workflow count")
	}
	started := time.Now()
	fixture, err := buildFixture(options.Workflows, options.TailCommands)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		Workflows: options.Workflows, SnapshotIndex: snapshotIndex,
		TailCommands: uint64(len(fixture.tail)), FixtureBuildNanos: time.Since(started).Nanoseconds(),
	}
	for _, batch := range fixture.tail {
		report.TailEvents += uint64(len(batch.Events))
	}
	config := storage.Config{Directory: options.Directory, ClusterID: "recovery-benchmark", NodeID: 1, Voters: []uint64{1, 2, 3}}
	started = time.Now()
	// Leave committed tail entries for startup replay
	report.TailEntries, err = storage.WriteRecoveryReplayFixture(config, snapshotIndex, fixture.snapshot, fixture.tail)
	if err != nil {
		return Report{}, err
	}
	report.PersistNanos = time.Since(started).Nanoseconds()
	persisted, err := storage.Open(config)
	if err != nil {
		return Report{}, err
	}
	before := persisted.Applied()
	hardBefore, _, err := persisted.InitialState()
	if err != nil {
		persisted.Close()
		return Report{}, err
	}
	report.AppliedBeforeRestart = before.Index
	report.CommittedBeforeRestart = hardBefore.GetCommit()
	if err := persisted.Close(); err != nil {
		return Report{}, err
	}
	started = time.Now()
	node, err := coordinator.Open(coordinator.Config{
		ID: 1, ClusterID: config.ClusterID, Directory: config.Directory, Voters: config.Voters,
		Transport: coordinator.TransportFunc(func(*pb.Message) error { return nil }),
	})
	if err != nil {
		return Report{}, err
	}
	defer node.Close()
	report.RecoveryNanos = time.Since(started).Nanoseconds()
	store := node.Store()
	after := store.Applied()
	hardAfter, _, err := store.InitialState()
	if err != nil {
		return Report{}, err
	}
	report.AppliedAfterRestart = after.Index
	report.CommittedAfterRestart = hardAfter.GetCommit()
	expectedCommit := report.SnapshotIndex + report.TailEntries
	if report.AppliedBeforeRestart != report.SnapshotIndex {
		report.IndexMismatches++
	}
	if report.CommittedBeforeRestart != expectedCommit {
		report.IndexMismatches++
	}
	if report.AppliedAfterRestart != expectedCommit {
		report.IndexMismatches++
	}
	if report.CommittedAfterRestart != expectedCommit {
		report.IndexMismatches++
	}
	started = time.Now()
	for index := range fixture.expected {
		expected := &fixture.expected[index]
		expectedHash, err := core.StateHash(expected)
		if err != nil {
			return Report{}, err
		}
		actual, exists := store.State(expected.ID)
		if !exists {
			report.StateHashMismatches++
			continue
		}
		report.StatesVerified++
		actualHash, err := core.StateHash(actual)
		if err != nil {
			return Report{}, err
		}
		if actualHash != expectedHash {
			report.StateHashMismatches++
		}
		if actual.Version != expected.Version {
			report.VersionMismatches++
		}
	}
	report.VerificationNanos = time.Since(started).Nanoseconds()
	if report.IndexMismatches != 0 || report.StateHashMismatches != 0 || report.VersionMismatches != 0 {
		return report, fmt.Errorf("recovery integrity mismatch")
	}
	return report, nil
}

func buildFixture(workflows, tailCommands uint64) (fixture, error) {
	engine := core.NewEngine()
	for index := uint64(0); index < workflows; index++ {
		_, commands := bench.WorkloadCommands(index)
		if _, _, duplicate, err := engine.PrepareAndApply(commands[0]); err != nil {
			return fixture{}, err
		} else if duplicate {
			return fixture{}, fmt.Errorf("duplicate workflow submission")
		}
	}
	value := fixture{snapshot: engine.Snapshot(), expected: make([]core.WorkflowState, 0, workflows), tail: make([]core.CommandBatch, 0, tailCommands)}
	for index := uint64(0); index < tailCommands; index++ {
		workflowIndex := index % workflows
		commandRound := index / workflows
		_, commands := bench.WorkloadCommands(workflowIndex)
		batch, _, duplicate, err := engine.PrepareAndApply(commands[commandRound+1])
		if err != nil {
			return fixture{}, err
		}
		if duplicate {
			return fixture{}, fmt.Errorf("duplicate recovery command")
		}
		value.tail = append(value.tail, batch)
	}
	for index := uint64(0); index < workflows; index++ {
		workflowID, _ := bench.WorkloadCommands(index)
		state, exists := engine.State(workflowID)
		if !exists {
			return fixture{}, fmt.Errorf("workflow %s is missing", workflowID)
		}
		value.expected = append(value.expected, *state)
	}
	return value, nil
}
