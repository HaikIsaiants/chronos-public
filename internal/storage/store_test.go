package storage_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestApplicationRecordsPersistAndReopen(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory, 1, nil)
	definition := definition()
	submit, submitEntry := applyCommand(t, store, 2, core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition,
	})
	start, _ := applyCommand(t, store, 3, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1,
	}))
	_, _ = applyCommand(t, store, 4, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "task",
		At: 2, Output: map[string]string{"value": "done"},
	}, start))
	if submitEntry.Index != 2 || store.Applied().Index != 4 {
		t.Fatalf("unexpected indexes: %+v %+v", submitEntry, store.Applied())
	}
	events, err := store.Events(submit.WorkflowID)
	if err != nil || len(events) != 5 {
		t.Fatalf("unexpected events: %d %v", len(events), err)
	}
	results, err := store.TaskResults()
	leaseEvent := start.Events[0]
	if err != nil || len(results) != 1 || results[0].Status != core.TaskCompleted || results[0].Output["value"] != "done" ||
		results[0].EventKind != core.EventTaskCompleted || results[0].TaskType != "test" || results[0].Phase != "normal" ||
		results[0].WorkerID != leaseEvent.WorkerID || results[0].Fence != leaseEvent.Fence ||
		results[0].Idempotency != leaseEvent.Idempotency {
		t.Fatalf("unexpected task results: %+v %v", results, err)
	}
	expected := store.Engine().Snapshot()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, directory, 1, nil)
	t.Cleanup(func() { reopened.Close() })
	if !reflect.DeepEqual(expected, reopened.Engine().Snapshot()) || reopened.Applied().Index != 4 {
		t.Fatal("reopened application state differs")
	}
	receipt, exists, err := reopened.Receipt("complete")
	if err != nil || !exists || receipt.Result.WorkflowID != submit.WorkflowID {
		t.Fatalf("receipt did not survive reopen: %+v %v", receipt, err)
	}
}

func TestFenceAndIdempotencySurviveReopen(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory, 1, nil)
	definition := definition()
	submit, _ := applyCommand(t, store, 2, core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition,
	})
	first, _ := applyCommand(t, store, 3, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start-1", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1,
	}))
	firstLease := first.Events[0]
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, directory, 1, nil)
	defer reopened.Close()
	applyCommand(t, reopened, 4, core.Command{
		Kind: core.CommandExpire, RequestID: "expire", WorkflowID: submit.WorkflowID, TaskID: "task",
		AttemptID: firstLease.AttemptID, WorkerID: firstLease.WorkerID, Fence: firstLease.Fence, At: firstLease.LeaseUntil,
	})
	second, _ := applyCommand(t, reopened, 5, core.Command{
		Kind: core.CommandStart, RequestID: "start-2", WorkflowID: submit.WorkflowID, TaskID: "task",
		WorkerID: "replacement", At: firstLease.LeaseUntil + 1, LeaseUntil: firstLease.LeaseUntil + 1001,
	})
	secondLease := second.Events[0]
	if secondLease.Fence != firstLease.Fence+1 || secondLease.Idempotency != firstLease.Idempotency ||
		secondLease.AttemptID == firstLease.AttemptID {
		t.Fatalf("lease identity did not survive reopen: %+v %+v", firstLease, secondLease)
	}
}

func TestApplicationCommitFailpoints(t *testing.T) {
	for _, test := range []struct {
		name  string
		point storage.Point
		index uint64
	}{
		{name: "before-sync", point: storage.ApplyBeforeSync, index: 1},
		{name: "after-sync", point: storage.ApplyAfterSync, index: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			armed := false
			fired := false
			store := openStore(t, directory, 1, func(point storage.Point) error {
				if armed && !fired && point == test.point {
					fired = true
					return storage.ErrInjectedCrash
				}
				return nil
			})
			definition := definition()
			batch, _, duplicate, err := store.Engine().Prepare(core.Command{
				Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition,
			})
			if err != nil || duplicate {
				t.Fatal(err)
			}
			persistRaft(t, store, 2, 2)
			entry := storage.ApplicationEntry{
				Version: storage.ApplicationEntryVersion, Index: 2, Term: 2, Batches: []core.CommandBatch{batch},
			}
			armed = true
			if _, err := store.Apply([]storage.ApplicationEntry{entry}); !errors.Is(err, storage.ErrInjectedCrash) {
				t.Fatalf("failpoint returned %v", err)
			}
			if store.Applied().Index != 1 {
				t.Fatal("live state advanced after injected crash")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openStore(t, directory, 1, nil)
			defer reopened.Close()
			if reopened.Applied().Index != test.index {
				t.Fatalf("durable index is %d", reopened.Applied().Index)
			}
			if test.point == storage.ApplyBeforeSync {
				if _, exists, err := reopened.Receipt("submit"); err != nil || exists {
					t.Fatal("pre-sync crash exposed a receipt")
				}
				if _, err := reopened.Apply([]storage.ApplicationEntry{entry}); err != nil {
					t.Fatal(err)
				}
			}
			if _, exists, err := reopened.Receipt("submit"); err != nil || !exists {
				t.Fatal("committed application record is missing")
			}
		})
	}
}

func TestSnapshotInstallAndTailMatchFullHistory(t *testing.T) {
	source := openStore(t, t.TempDir(), 1, nil)
	defer source.Close()
	definition := definition()
	submit, _ := applyCommand(t, source, 2, core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition,
	})
	start, _ := applyCommandAtTerm(t, source, 3, 3, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1,
	}))
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	completeBatch, _, duplicate, err := source.Engine().Prepare(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "task",
		At: 2, Output: map[string]string{"value": "done"},
	}, start))
	if err != nil || duplicate {
		t.Fatal(err)
	}
	persistRaft(t, source, 4, 3)
	tail := storage.ApplicationEntry{
		Version: storage.ApplicationEntryVersion, Index: 4, Term: 3, Batches: []core.CommandBatch{completeBatch},
	}
	if _, err := source.Apply([]storage.ApplicationEntry{tail}); err != nil {
		t.Fatal(err)
	}
	destination := openStore(t, t.TempDir(), 2, nil)
	defer destination.Close()
	hard := &pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(3)}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	receipt, exists, err := destination.Receipt("submit")
	if err != nil || !exists || receipt.AppliedIndex != 2 || receipt.CommittedTerm != 2 {
		t.Fatalf("snapshot receipt has invalid commit proof: %+v %v", receipt, err)
	}
	persistRaft(t, destination, 4, 3)
	if _, err := destination.Apply([]storage.ApplicationEntry{tail}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source.Engine().Snapshot(), destination.Engine().Snapshot()) {
		t.Fatal("snapshot plus tail differs from full history")
	}
	sourceEvents, _ := source.Events(submit.WorkflowID)
	destinationEvents, _ := destination.Events(submit.WorkflowID)
	if !reflect.DeepEqual(sourceEvents, destinationEvents) {
		t.Fatal("snapshot event history differs")
	}
	retry, _, duplicate, err := destination.Engine().Prepare(core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition,
	})
	if err != nil || !duplicate || retry.RequestID != "submit" {
		t.Fatal("snapshot did not preserve dedupe state")
	}
	corrupt := proto.Clone(snapshot).(*pb.Snapshot)
	corrupt.Data[len(corrupt.Data)-1] ^= 1
	third := openStore(t, t.TempDir(), 3, nil)
	defer third.Close()
	before := third.Applied()
	if err := third.SaveReady(raft.Ready{HardState: hard, Snapshot: corrupt}); !errors.Is(err, storage.ErrCorruptSnapshot) {
		t.Fatalf("corrupt snapshot returned %v", err)
	}
	if third.Applied() != before {
		t.Fatal("corrupt snapshot changed state")
	}
}

func TestSnapshotFailpointsRecover(t *testing.T) {
	for _, point := range []storage.Point{storage.SnapshotBeforeSync, storage.SnapshotAfterSync} {
		t.Run(string(point), func(t *testing.T) {
			directory := t.TempDir()
			armed := false
			store := openStore(t, directory, 1, func(hit storage.Point) error {
				if armed && hit == point {
					return storage.ErrInjectedCrash
				}
				return nil
			})
			definition := definition()
			applyCommand(t, store, 2, core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
			armed = true
			if _, err := store.CreateSnapshot(); !errors.Is(err, storage.ErrInjectedCrash) {
				t.Fatalf("failpoint returned %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openStore(t, directory, 1, nil)
			defer reopened.Close()
			if reopened.Applied().Index != 2 {
				t.Fatalf("applied index is %d", reopened.Applied().Index)
			}
			if _, exists, err := reopened.Receipt("submit"); err != nil || !exists {
				t.Fatal("snapshot creation lost application state")
			}
			_, _, snapshot, _, err := reopened.RaftState()
			if err != nil {
				t.Fatal(err)
			}
			if point == storage.SnapshotBeforeSync && snapshot.GetMetadata().GetIndex() != 1 {
				t.Fatal("pre-sync snapshot became durable")
			}
			if point == storage.SnapshotAfterSync && snapshot.GetMetadata().GetIndex() != 2 {
				t.Fatal("post-sync snapshot was lost")
			}
		})
	}

	source := openStore(t, t.TempDir(), 1, nil)
	definition := definition()
	applyCommand(t, source, 2, core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	expected := source.Engine().Snapshot()
	source.Close()
	for _, point := range []storage.Point{storage.SnapshotInstallBeforeSync, storage.SnapshotInstallAfterSync} {
		t.Run(string(point), func(t *testing.T) {
			directory := t.TempDir()
			armed := false
			store := openStore(t, directory, 2, func(hit storage.Point) error {
				if armed && hit == point {
					return storage.ErrInjectedCrash
				}
				return nil
			})
			hard := &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(2)}
			if err := store.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
				t.Fatal(err)
			}
			armed = true
			if err := store.InstallSnapshot(snapshot); !errors.Is(err, storage.ErrInjectedCrash) {
				t.Fatalf("failpoint returned %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openStore(t, directory, 2, nil)
			defer reopened.Close()
			if reopened.Applied().Index != 2 || !reflect.DeepEqual(expected, reopened.Engine().Snapshot()) {
				t.Fatal("snapshot installation did not recover")
			}
			if _, exists, err := reopened.Receipt("submit"); err != nil || !exists {
				t.Fatal("snapshot installation lost dedupe state")
			}
		})
	}
}

func TestRaftSuffixReplacementAndBounds(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory, 1, nil)
	entries := []*pb.Entry{
		{Index: proto.Uint64(2), Term: proto.Uint64(2)},
		{Index: proto.Uint64(3), Term: proto.Uint64(2)},
		{Index: proto.Uint64(4), Term: proto.Uint64(2)},
	}
	if err := store.SaveReady(raft.Ready{
		HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(2)}, Entries: entries,
	}); err != nil {
		t.Fatal(err)
	}
	replacement := &pb.Entry{Index: proto.Uint64(3), Term: proto.Uint64(3)}
	if err := store.SaveReady(raft.Ready{
		HardState: &pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(2)}, Entries: []*pb.Entry{replacement},
	}); err != nil {
		t.Fatal(err)
	}
	term, err := store.Term(3)
	if err != nil || term != 3 {
		t.Fatalf("replacement term is %d: %v", term, err)
	}
	last, err := store.LastIndex()
	if err != nil || last != 3 {
		t.Fatalf("replacement last index is %d: %v", last, err)
	}
	limited, err := store.Entries(2, 4, 0)
	if err != nil || len(limited) != 1 {
		t.Fatalf("size bound returned %d entries: %v", len(limited), err)
	}
	if _, err := store.Entries(1, 2, 100); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("compacted range returned %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, directory, 1, nil)
	defer reopened.Close()
	term, err = reopened.Term(3)
	if err != nil || term != 3 {
		t.Fatalf("replacement did not persist: %d %v", term, err)
	}
	last, err = reopened.LastIndex()
	if err != nil || last != 3 {
		t.Fatalf("replacement suffix persisted: %d %v", last, err)
	}
}

func openStore(t *testing.T, directory string, nodeID uint64, failpoint storage.Failpoint) *storage.Store {
	t.Helper()
	store, err := storage.Open(storage.Config{
		Directory: directory, ClusterID: "test", NodeID: nodeID, Voters: []uint64{1, 2, 3}, Failpoint: failpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func applyCommand(t *testing.T, store *storage.Store, index uint64, command core.Command) (core.Result, storage.ApplicationEntry) {
	return applyCommandAtTerm(t, store, index, 2, command)
}

func applyCommandAtTerm(t *testing.T, store *storage.Store, index, term uint64, command core.Command) (core.Result, storage.ApplicationEntry) {
	t.Helper()
	batch, expected, duplicate, err := store.Engine().Prepare(command)
	if err != nil || duplicate {
		t.Fatal(err)
	}
	persistRaft(t, store, index, term)
	entry := storage.ApplicationEntry{
		Version: storage.ApplicationEntryVersion, Index: index, Term: term, Batches: []core.CommandBatch{batch},
	}
	outcomes, err := store.Apply([]storage.ApplicationEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || !reflect.DeepEqual(outcomes[0].Result, expected) {
		t.Fatalf("unexpected outcome: %+v", outcomes)
	}
	return outcomes[0].Result, entry
}

func persistRaft(t *testing.T, store *storage.Store, index, term uint64) {
	t.Helper()
	entry := &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(term)}
	hard := &pb.HardState{Term: proto.Uint64(term), Commit: proto.Uint64(index)}
	if err := store.SaveReady(raft.Ready{HardState: hard, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
}

func definition() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name: "single", Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "test", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
}
