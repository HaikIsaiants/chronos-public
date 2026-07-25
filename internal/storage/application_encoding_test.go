package storage

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type persistedApplicationRecord struct {
	key   []byte
	value []byte
}

func TestApplicationEncodingWorkerCountsPersistIdentically(t *testing.T) {
	entries := applicationEncodingFixture(t)
	serialConfig := Config{Directory: t.TempDir(), ClusterID: "encoding", NodeID: 1, Voters: []uint64{1, 2, 3}}
	parallelConfig := Config{Directory: t.TempDir(), ClusterID: "encoding", NodeID: 1, Voters: []uint64{1, 2, 3}}
	serial, err := Open(serialConfig)
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := Open(parallelConfig)
	if err != nil {
		t.Fatal(err)
	}
	persistApplicationEncodingRaft(t, serial)
	persistApplicationEncodingRaft(t, parallel)
	serialOutcomes, err := serial.apply(entries, 1)
	if err != nil {
		t.Fatal(err)
	}
	parallelOutcomes, err := parallel.apply(entries, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(parallelOutcomes) != 6 || !parallelOutcomes[4].Result.Duplicate || parallelOutcomes[5].Code != "request_conflict" {
		t.Fatalf("unexpected outcomes: %+v", parallelOutcomes)
	}
	if !reflect.DeepEqual(serialOutcomes, parallelOutcomes) || !reflect.DeepEqual(applicationRecords(t, serial), applicationRecords(t, parallel)) {
		t.Fatal("worker counts produced different durable records")
	}
	if err := serial.Close(); err != nil {
		t.Fatal(err)
	}
	if err := parallel.Close(); err != nil {
		t.Fatal(err)
	}
	serial, err = Open(serialConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	parallel, err = Open(parallelConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer parallel.Close()
	if !reflect.DeepEqual(applicationRecords(t, serial), applicationRecords(t, parallel)) || !reflect.DeepEqual(serial.Engine().Snapshot(), parallel.Engine().Snapshot()) {
		t.Fatal("worker counts restored different durable state")
	}
}

func TestApplicationEncodingReturnsFirstCanonicalError(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "encoding-errors", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch := store.db.NewBatch()
	defer batch.Close()
	first := errors.New("first")
	err = stageEncodedSets(batch, []encodedSet{{key: []byte("key"), value: []byte("value")}, {err: first}, {err: errors.New("second")}})
	if !errors.Is(err, first) || batch.Count() != 1 {
		t.Fatalf("unexpected staged error or write count: %v %d", err, batch.Count())
	}
}

func TestApplicationRecordContainsOnlyCommitMetadata(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "application-record", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	persistApplicationEncodingRaft(t, store)
	if _, err := store.Apply(applicationEncodingFixture(t)); err != nil {
		t.Fatal(err)
	}
	data, err := store.get(numberKey(3, 2))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := decodeJSON(data, &record); err != nil {
		t.Fatal(err)
	}
	if len(record) != 3 || record["version"] != float64(ApplicationEntryVersion) || record["index"] != float64(2) || record["term"] != float64(2) {
		t.Fatalf("unexpected application record: %+v", record)
	}
}

func TestSuccessfulOutcomeUsesReceiptStorage(t *testing.T) {
	directory := t.TempDir()
	config := Config{Directory: directory, ClusterID: "outcome-storage", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	persistApplicationEncodingRaft(t, store)
	outcomes, err := store.Apply(applicationEncodingFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	var success Outcome
	var conflict Outcome
	for _, outcome := range outcomes {
		if outcome.Code == "" && !outcome.Result.Duplicate && success.RequestID == "" {
			success = outcome
		}
		if outcome.Code != "" {
			conflict = outcome
		}
	}
	if success.RequestID == "" || conflict.RequestID == "" {
		t.Fatalf("fixture outcomes are incomplete: %+v", outcomes)
	}
	if _, err := store.get(segmentedKey(7, success.RequestID, success.RequestHash)); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("successful outcome was persisted: %v", err)
	}
	if _, err := store.get(segmentedKey(7, conflict.RequestID, conflict.RequestHash)); err != nil {
		t.Fatalf("conflict outcome was not persisted: %v", err)
	}
	actual, exists, err := store.Outcome(success.RequestID, success.RequestHash)
	if err != nil || !exists || !reflect.DeepEqual(actual, success) {
		t.Fatalf("successful outcome differs: %+v %t %v", actual, exists, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertStoredOutcome(t, store, success)
	assertStoredOutcome(t, store, conflict)
	snapshot, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	destination, err := Open(Config{Directory: t.TempDir(), ClusterID: "outcome-storage", NodeID: 2, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	hard := &pb.HardState{Term: proto.Uint64(snapshot.GetMetadata().GetTerm()), Commit: proto.Uint64(snapshot.GetMetadata().GetIndex())}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	assertStoredOutcome(t, destination, success)
	assertStoredOutcome(t, destination, conflict)
}

func assertStoredOutcome(t *testing.T, store *Store, expected Outcome) {
	t.Helper()
	actual, exists, err := store.Outcome(expected.RequestID, expected.RequestHash)
	if err != nil || !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("outcome differs: %+v %t %v", actual, exists, err)
	}
}

func TestSnapshotRejectsMismatchedMaterializedKeys(t *testing.T) {
	for name, corrupt := range map[string]func(*Store) error{
		"workflow": func(store *Store) error {
			key := segmentedKey(6, "active")
			data, err := store.get(key)
			if err != nil {
				return err
			}
			var state core.WorkflowState
			if err := decodeJSON(data, &state); err != nil {
				return err
			}
			state.ID = "other"
			data, err = encodeJSON(state)
			if err != nil {
				return err
			}
			return store.db.Set(key, data, pebble.Sync)
		},
		"receipt": func(store *Store) error {
			key := segmentedKey(4, "active-submit")
			data, err := store.get(key)
			if err != nil {
				return err
			}
			receipt, err := decodeReceiptRecord(data)
			if err != nil {
				return err
			}
			receipt.RequestID = "other"
			data, err = encodeReceiptRecord(receipt)
			if err != nil {
				return err
			}
			return store.db.Set(key, data, pebble.Sync)
		},
		"outcome": func(store *Store) error {
			key := segmentedKey(7, "active-submit", "conflict")
			data, err := store.get(key)
			if err != nil {
				return err
			}
			var outcome Outcome
			if err := decodeJSON(data, &outcome); err != nil {
				return err
			}
			outcome.RequestHash = "other"
			data, err = encodeJSON(outcome)
			if err != nil {
				return err
			}
			return store.db.Set(key, data, pebble.Sync)
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, err := Open(Config{Directory: t.TempDir(), ClusterID: "snapshot-key", NodeID: 1, Voters: []uint64{1, 2, 3}})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			persistApplicationEncodingRaft(t, store)
			if _, err := store.Apply(applicationEncodingFixture(t)); err != nil {
				t.Fatal(err)
			}
			if err := corrupt(store); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CreateSnapshot(); !errors.Is(err, ErrRecordVersion) {
				t.Fatalf("mismatched key returned %v", err)
			}
		})
	}
}

func TestSnapshotRejectsMaterializedStateMismatch(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "snapshot-state", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	persistApplicationEncodingRaft(t, store)
	if _, err := store.Apply(applicationEncodingFixture(t)); err != nil {
		t.Fatal(err)
	}
	key := segmentedKey(6, "terminal")
	data, err := store.get(key)
	if err != nil {
		t.Fatal(err)
	}
	var state core.WorkflowState
	if err := decodeJSON(data, &state); err != nil {
		t.Fatal(err)
	}
	state.Status = core.WorkflowFailed
	data, err = encodeJSON(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Set(key, data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSnapshot(); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("mismatched state returned %v", err)
	}
}

func TestOpenRejectsMissingApplicationRecord(t *testing.T) {
	config := Config{Directory: t.TempDir(), ClusterID: "application-gap", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	entries := applicationEncodingFixture(t)
	persistApplicationEncodingRaft(t, store)
	if _, err := store.Apply(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Delete(numberKey(3, 2), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(config); !errors.Is(err, ErrAppliedGap) {
		if err == nil {
			reopened.Close()
		}
		t.Fatalf("missing application record returned %v", err)
	}
}

func TestOpenRejectsInvalidApplicationRecord(t *testing.T) {
	for name, mutate := range map[string]func(*Store) error{
		"corrupt": func(store *Store) error {
			return store.db.Set(numberKey(3, 2), []byte("{"), pebble.Sync)
		},
		"term": func(store *Store) error {
			data, err := store.get(numberKey(3, 2))
			if err != nil {
				return err
			}
			var entry ApplicationEntry
			if err := decodeJSON(data, &entry); err != nil {
				return err
			}
			entry.Term++
			data, err = encodeJSON(entry)
			if err != nil {
				return err
			}
			return store.db.Set(numberKey(3, 2), data, pebble.Sync)
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := Config{Directory: t.TempDir(), ClusterID: "invalid-application", NodeID: 1, Voters: []uint64{1, 2, 3}}
			store, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			persistApplicationEncodingRaft(t, store)
			if _, err := store.Apply(applicationEncodingFixture(t)); err != nil {
				t.Fatal(err)
			}
			if err := mutate(store); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(config); err == nil {
				reopened.Close()
				t.Fatal("invalid application record reopened")
			}
		})
	}
}

func applicationEncodingFixture(t *testing.T) []ApplicationEntry {
	t.Helper()
	definition := core.WorkflowDefinition{
		Name: "encoding", Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
	planner := core.NewEngine()
	prepare := func(command core.Command) core.CommandBatch {
		batch, _, duplicate, err := planner.PrepareAndApply(command)
		if err != nil || duplicate {
			t.Fatalf("prepare failed: %v %t", err, duplicate)
		}
		return batch
	}
	submitTerminal := prepare(core.Command{
		Kind: core.CommandSubmit, RequestID: "terminal-submit", WorkflowID: "terminal", Definition: &definition,
	})
	startTerminal := prepare(core.Command{
		Kind: core.CommandStart, RequestID: "terminal-start", WorkflowID: "terminal", TaskID: "task",
		At: 1, WorkerID: "worker", LeaseUntil: 100,
	})
	state, exists := planner.State("terminal")
	if !exists {
		t.Fatal("terminal workflow missing")
	}
	task := state.Tasks["task"]
	completeTerminal := prepare(core.Command{
		Kind: core.CommandComplete, RequestID: "terminal-complete", WorkflowID: "terminal", TaskID: "task",
		AttemptID: task.AttemptID, At: 2, WorkerID: task.WorkerID, Fence: task.Fence, Output: map[string]string{"value": "ok"},
	})
	submitActive := prepare(core.Command{
		Kind: core.CommandSubmit, RequestID: "active-submit", WorkflowID: "active", Definition: &definition,
	})
	conflict := submitActive
	conflict.RequestHash = "conflict"
	return []ApplicationEntry{
		{Version: ApplicationEntryVersion, Index: 2, Term: 2, Batches: []core.CommandBatch{submitTerminal, startTerminal}},
		{Version: ApplicationEntryVersion, Index: 3, Term: 2, Batches: []core.CommandBatch{completeTerminal, submitActive, submitActive, conflict}},
	}
}

func applicationRecords(t *testing.T, store *Store) []persistedApplicationRecord {
	t.Helper()
	iterator, err := store.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.Close()
	var records []persistedApplicationRecord
	for iterator.First(); iterator.Valid(); iterator.Next() {
		records = append(records, persistedApplicationRecord{
			key: append([]byte(nil), iterator.Key()...), value: append([]byte(nil), iterator.Value()...),
		})
	}
	if err := iterator.Error(); err != nil {
		t.Fatal(err)
	}
	return records
}

func persistApplicationEncodingRaft(t *testing.T, store *Store) {
	t.Helper()
	ready := raft.Ready{
		HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(3)},
		Entries: []*pb.Entry{
			{Index: proto.Uint64(2), Term: proto.Uint64(2)},
			{Index: proto.Uint64(3), Term: proto.Uint64(2)},
		},
	}
	if err := store.SaveReady(ready); err != nil {
		t.Fatal(err)
	}
}
