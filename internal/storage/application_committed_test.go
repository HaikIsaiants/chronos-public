package storage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestStoreApplyRetainsCallerIsolation(t *testing.T) {
	store := openStore(t, t.TempDir(), 1, nil)
	defer store.Close()
	batches := plannedCommittedBatches(t, false)
	persistRaft(t, store, 2, 2)
	entry := storage.ApplicationEntry{Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches}
	outcomes, err := store.Apply([]storage.ApplicationEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	entry.Batches[0].Events[0].Definition.Tasks[0].Type = "input-mutated"
	outcomes[0].Result.Events[0].Definition.Tasks[0].Type = "result-mutated"
	assertSubmittedIsolation(t, store, batches[0], "test")
}

func TestApplyCommittedKeepsActiveResultsIsolated(t *testing.T) {
	store := openStore(t, t.TempDir(), 1, nil)
	defer store.Close()
	batches := plannedCommittedBatches(t, false)
	persistRaft(t, store, 2, 2)
	outcomes, err := store.ApplyCommitted([]storage.ApplicationEntry{{
		Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches,
	}})
	if err != nil {
		t.Fatal(err)
	}
	outcomes[0].Result.Events[0].Definition.Tasks[0].Type = "result-mutated"
	assertSubmittedIsolation(t, store, batches[0], "test")
}

func TestApplyCommittedTerminalDuplicateUsesDurableReceipt(t *testing.T) {
	directory := t.TempDir()
	batches := plannedCommittedBatches(t, true)
	var receipt storage.Receipt
	func() {
		store := openStore(t, directory, 1, nil)
		defer store.Close()
		persistRaft(t, store, 2, 2)
		if _, err := store.ApplyCommitted([]storage.ApplicationEntry{{
			Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches,
		}}); err != nil {
			t.Fatal(err)
		}
		engine := store.Engine()
		for _, batch := range batches {
			if _, exists := engine.ResidentRequestHash(batch.RequestID); exists {
				t.Fatalf("terminal request %s remained resident", batch.RequestID)
			}
		}
		if engine.ResidentWorkflow(batches[0].WorkflowID) {
			t.Fatal("terminal workflow remained resident")
		}
		var exists bool
		var err error
		receipt, exists, err = store.Receipt(batches[len(batches)-1].RequestID)
		if err != nil || !exists {
			t.Fatalf("durable receipt missing: %+v %t %v", receipt, exists, err)
		}
		persistRaft(t, store, 3, 2)
		outcomes, err := store.ApplyCommitted([]storage.ApplicationEntry{{
			Version: storage.ApplicationEntryVersion, Index: 3, Term: 2, Batches: []core.CommandBatch{batches[len(batches)-1]},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if len(outcomes) != 1 || !outcomes[0].Result.Duplicate || outcomes[0].Result.WorkflowID != batches[0].WorkflowID || outcomes[0].AppliedIndex != receipt.AppliedIndex || outcomes[0].CommittedTerm != receipt.CommittedTerm {
			t.Fatalf("unexpected durable duplicate: %+v", outcomes)
		}
		if _, exists := store.Engine().ResidentRequestHash(batches[len(batches)-1].RequestID); exists {
			t.Fatal("durable duplicate became resident")
		}
	}()
	reopened := openStore(t, directory, 1, nil)
	defer reopened.Close()
	persistRaft(t, reopened, 4, 2)
	outcomes, err := reopened.ApplyCommitted([]storage.ApplicationEntry{{
		Version: storage.ApplicationEntryVersion, Index: 4, Term: 2, Batches: []core.CommandBatch{batches[len(batches)-1]},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || !outcomes[0].Result.Duplicate || outcomes[0].AppliedIndex != receipt.AppliedIndex || outcomes[0].CommittedTerm != receipt.CommittedTerm {
		t.Fatalf("reopened duplicate proof changed: %+v", outcomes)
	}
}

func TestApplyCommittedDuplicateInSameGroupUsesOriginalProof(t *testing.T) {
	store := openStore(t, t.TempDir(), 1, nil)
	defer store.Close()
	batches := plannedCommittedBatches(t, false)
	persistRaft(t, store, 2, 2)
	persistRaft(t, store, 3, 2)
	outcomes, err := store.ApplyCommitted([]storage.ApplicationEntry{
		{Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches},
		{Version: storage.ApplicationEntryVersion, Index: 3, Term: 2, Batches: batches},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 || outcomes[0].Result.Duplicate || !outcomes[1].Result.Duplicate || outcomes[1].AppliedIndex != outcomes[0].AppliedIndex || outcomes[1].CommittedTerm != outcomes[0].CommittedTerm {
		t.Fatalf("same-group duplicate proof changed: %+v", outcomes)
	}
}

func TestApplyRejectsInvalidCommitProof(t *testing.T) {
	batches := plannedCommittedBatches(t, false)
	t.Run("uncommitted", func(t *testing.T) {
		store := openStore(t, t.TempDir(), 1, nil)
		defer store.Close()
		if err := store.SaveReady(raft.Ready{
			HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(1)},
			Entries:   []*pb.Entry{{Index: proto.Uint64(2), Term: proto.Uint64(2)}},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Apply([]storage.ApplicationEntry{{Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches}}); err == nil {
			t.Fatal("uncommitted application entry was accepted")
		}
		if store.Applied().Index != 1 {
			t.Fatal("uncommitted application entry advanced state")
		}
	})
	t.Run("term", func(t *testing.T) {
		store := openStore(t, t.TempDir(), 1, nil)
		defer store.Close()
		persistRaft(t, store, 2, 2)
		if _, err := store.Apply([]storage.ApplicationEntry{{Version: storage.ApplicationEntryVersion, Index: 2, Term: 3, Batches: batches}}); err == nil {
			t.Fatal("application entry with the wrong term was accepted")
		}
		if store.Applied().Index != 1 {
			t.Fatal("wrong-term application entry advanced state")
		}
	})
}

func TestApplyCommittedFailpointsRollBackLiveEngine(t *testing.T) {
	for _, point := range []storage.Point{storage.ApplyBeforeSync, storage.ApplyAfterSync} {
		t.Run(string(point), func(t *testing.T) {
			directory := t.TempDir()
			armed := false
			fired := false
			store := openStore(t, directory, 1, func(hit storage.Point) error {
				if armed && !fired && hit == point {
					fired = true
					return storage.ErrInjectedCrash
				}
				return nil
			})
			batches := plannedCommittedBatches(t, true)
			persistRaft(t, store, 2, 2)
			if _, err := store.ApplyCommitted([]storage.ApplicationEntry{{
				Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches[:2],
			}}); err != nil {
				t.Fatal(err)
			}
			before := store.Engine().Snapshot()
			persistRaft(t, store, 3, 2)
			entry := storage.ApplicationEntry{
				Version: storage.ApplicationEntryVersion, Index: 3, Term: 2, Batches: batches[2:],
			}
			armed = true
			if _, err := store.ApplyCommitted([]storage.ApplicationEntry{entry}); !errors.Is(err, storage.ErrInjectedCrash) {
				t.Fatalf("failpoint returned %v", err)
			}
			if store.Applied().Index != 2 || !reflect.DeepEqual(store.Engine().Snapshot(), before) {
				t.Fatal("failed committed apply changed live state")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openStore(t, directory, 1, nil)
			defer reopened.Close()
			expected := uint64(3)
			if point == storage.ApplyBeforeSync {
				expected = 2
			}
			if reopened.Applied().Index != expected {
				t.Fatalf("durable index is %d", reopened.Applied().Index)
			}
			if point == storage.ApplyBeforeSync {
				if _, err := reopened.ApplyCommitted([]storage.ApplicationEntry{entry}); err != nil {
					t.Fatal(err)
				}
			}
			if state, exists := reopened.State(batches[0].WorkflowID); !exists || state.Status != core.WorkflowCompleted {
				t.Fatalf("terminal state was not recovered: %+v %t", state, exists)
			}
		})
	}
}

func TestApplyCommittedMatchesApplyBytes(t *testing.T) {
	batches := plannedCommittedBatches(t, true)
	entry := storage.ApplicationEntry{Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: batches}
	regular := openStore(t, t.TempDir(), 1, nil)
	defer regular.Close()
	committed := openStore(t, t.TempDir(), 1, nil)
	defer committed.Close()
	persistRaft(t, regular, 2, 2)
	persistRaft(t, committed, 2, 2)
	expected, err := regular.Apply([]storage.ApplicationEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := committed.ApplyCommitted([]storage.ApplicationEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	actualBytes, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actualBytes, expectedBytes) {
		t.Fatal("committed outcomes differ")
	}
	expectedSnapshot, err := regular.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	actualSnapshot, err := committed.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actualSnapshot.GetData(), expectedSnapshot.GetData()) || !reflect.DeepEqual(actualSnapshot.GetMetadata(), expectedSnapshot.GetMetadata()) {
		t.Fatal("committed snapshot differs")
	}
}

func plannedCommittedBatches(t *testing.T, terminal bool) []core.CommandBatch {
	t.Helper()
	planner := core.NewEngine()
	definition := definition()
	batches := make([]core.CommandBatch, 0, 3)
	apply := func(command core.Command) core.Result {
		t.Helper()
		batch, result, duplicate, err := planner.PrepareAndApply(command)
		if err != nil || duplicate {
			t.Fatalf("planning failed: %v %t", err, duplicate)
		}
		batches = append(batches, batch)
		return result
	}
	submitted := apply(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if !terminal {
		return batches
	}
	started := apply(lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
	}))
	apply(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submitted.WorkflowID, TaskID: "task",
		At: 2, Output: map[string]string{"value": "done"},
	}, started))
	return batches
}

func assertSubmittedIsolation(t *testing.T, store *storage.Store, batch core.CommandBatch, taskType string) {
	t.Helper()
	state, exists := store.State(batch.WorkflowID)
	if !exists || state.Tasks["task"].Definition.Type != taskType {
		t.Fatalf("stored state aliases a result: %+v", state)
	}
	retry, err := store.Engine().ApplyBatch(batch)
	if err != nil || !retry.Duplicate || retry.Events[0].Definition.Tasks[0].Type != taskType {
		t.Fatalf("stored request aliases a result: %+v %v", retry, err)
	}
	receipt, exists, err := store.Receipt(batch.RequestID)
	if err != nil || !exists || receipt.Result.Events[0].Definition.Tasks[0].Type != taskType {
		t.Fatalf("durable receipt aliases a result: %+v %t %v", receipt, exists, err)
	}
}
