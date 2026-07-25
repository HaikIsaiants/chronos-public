package storage_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestTerminalRetentionPreservesDurableSemantics(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory, 1, nil)
	definition := definition()
	submitCommand := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	submitBatch, submitted := commitPrepared(t, store, 2, submitCommand)
	_, started := commitPrepared(t, store, 3, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
	}))
	commitPrepared(t, store, 4, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 2,
	}, started))
	assertTerminalRetainedDurably(t, store, submitted.WorkflowID, submitCommand, submitted)
	changed := definition
	changed.Name = "changed"
	if _, _, err := store.Prepare([]core.Command{{Kind: core.CommandSubmit, RequestID: "submit", Definition: &changed}}); !errors.Is(err, core.ErrRequestConflict) {
		t.Fatalf("durable request conflict returned %v", err)
	}
	if _, _, err := store.Prepare([]core.Command{lease(core.Command{
		Kind: core.CommandStart, RequestID: "late-start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 3,
	})}); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("terminal workflow accepted a new command: %v", err)
	}
	persistRaft(t, store, 5, 2)
	outcomes, err := store.Apply([]storage.ApplicationEntry{{
		Version: storage.ApplicationEntryVersion, Index: 5, Term: 2, Batches: []core.CommandBatch{submitBatch},
	}})
	if err != nil || len(outcomes) != 1 || !outcomes[0].Result.Duplicate || !reflect.DeepEqual(outcomes[0].Result.Events, submitted.Events) {
		t.Fatalf("replicated duplicate changed the original result: %+v %v", outcomes, err)
	}
	receipt, exists, err := store.Receipt("submit")
	if err != nil || !exists || receipt.AppliedIndex != 2 || receipt.CommittedTerm != 2 || !reflect.DeepEqual(receipt.Result, submitted) {
		t.Fatalf("replicated duplicate changed the durable proof: %+v %v", receipt, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, directory, 1, nil)
	defer store.Close()
	assertTerminalRetainedDurably(t, store, submitted.WorkflowID, submitCommand, submitted)
	snapshot, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	destination := openStore(t, t.TempDir(), 2, nil)
	defer destination.Close()
	hard := &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(5)}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	assertTerminalRetainedDurably(t, destination, submitted.WorkflowID, submitCommand, submitted)
}

func TestTerminalRetentionBoundsLargeBatch(t *testing.T) {
	store := openStore(t, t.TempDir(), 1, nil)
	defer store.Close()
	engine := core.NewEngine()
	batches := make([]core.CommandBatch, 0, 255)
	for index := 0; index < 85; index++ {
		definition := definition()
		definition.Name = fmt.Sprintf("retention-%d", index)
		submit, submitted, duplicate, err := engine.PrepareAndApply(core.Command{
			Kind: core.CommandSubmit, RequestID: fmt.Sprintf("submit-%d", index), Definition: &definition,
		})
		if err != nil || duplicate {
			t.Fatal(err)
		}
		start, started, duplicate, err := engine.PrepareAndApply(lease(core.Command{
			Kind: core.CommandStart, RequestID: fmt.Sprintf("start-%d", index), WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
		}))
		if err != nil || duplicate {
			t.Fatal(err)
		}
		complete, _, duplicate, err := engine.PrepareAndApply(fenced(core.Command{
			Kind: core.CommandComplete, RequestID: fmt.Sprintf("complete-%d", index), WorkflowID: submitted.WorkflowID, TaskID: "task", At: 2,
		}, started))
		if err != nil || duplicate {
			t.Fatal(err)
		}
		batches = append(batches, submit, start, complete)
	}
	persistRaft(t, store, 2, 2)
	if _, err := store.Apply([]storage.ApplicationEntry{{
		Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches,
	}}); err != nil {
		t.Fatal(err)
	}
	resident := store.Engine().Snapshot()
	if len(resident.Workflows) != 0 || len(resident.Requests) != 0 {
		t.Fatalf("terminal batch remained resident: %d workflows %d requests", len(resident.Workflows), len(resident.Requests))
	}
	if _, exists, err := store.Receipt("complete-84"); err != nil || !exists {
		t.Fatalf("terminal batch receipt is missing: %v", err)
	}
}

func commitPrepared(t *testing.T, store *storage.Store, index uint64, command core.Command) (core.CommandBatch, core.Result) {
	t.Helper()
	batches, immediate, err := store.Prepare([]core.Command{command})
	if err != nil || len(immediate) != 0 || len(batches) != 1 {
		t.Fatalf("prepare returned %d batches and %d immediate results: %v", len(batches), len(immediate), err)
	}
	persistRaft(t, store, index, 2)
	outcomes, err := store.Apply([]storage.ApplicationEntry{{
		Version: storage.ApplicationEntryVersion, Index: index, Term: 2, Batches: batches,
	}})
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("apply returned %d outcomes: %v", len(outcomes), err)
	}
	return batches[0], outcomes[0].Result
}

func assertTerminalRetainedDurably(t *testing.T, store *storage.Store, workflowID string, command core.Command, original core.Result) {
	t.Helper()
	resident := store.Engine()
	if len(resident.WorkflowIDs()) != 0 || len(resident.Snapshot().Requests) != 0 {
		t.Fatal("terminal workflow remained resident")
	}
	state, exists := store.State(workflowID)
	projection, projected := store.Projection(workflowID)
	if !exists || !projected || state.Status != core.WorkflowCompleted || projection.Status != core.WorkflowCompleted {
		t.Fatalf("durable terminal read failed: %+v %+v", state, projection)
	}
	batches, immediate, err := store.Prepare([]core.Command{command})
	result, duplicate := immediate[command.RequestID]
	if err != nil || len(batches) != 0 || !duplicate || !result.Duplicate || !reflect.DeepEqual(result.Events, original.Events) || result.WorkflowID != original.WorkflowID {
		t.Fatalf("durable duplicate changed the result: %+v %v", result, err)
	}
}
