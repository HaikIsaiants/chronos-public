package storage

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/cockroachdb/pebble/v2"
)

func TestActiveIndexMigrationIsAtomic(t *testing.T) {
	for _, point := range []Point{ActiveIndexBeforeSync, ActiveIndexAfterSync} {
		t.Run(string(point), func(t *testing.T) {
			directory := t.TempDir()
			config := Config{Directory: directory, ClusterID: "migration", NodeID: 1, Voters: []uint64{1, 2, 3}}
			writeLegacyActiveIndexFixture(t, config)
			fired := false
			config.Failpoint = func(hit Point) error {
				if hit == point && !fired {
					fired = true
					return ErrInjectedCrash
				}
				return nil
			}
			if _, err := Open(config); !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("migration failpoint returned %v", err)
			}
			config.Failpoint = nil
			store, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if ids := store.engine.WorkflowIDs(); !reflect.DeepEqual(ids, []string{"active"}) {
				t.Fatalf("active migration restored %v", ids)
			}
			state, exists := store.State("terminal")
			if !exists || state.Status != core.WorkflowCompleted {
				t.Fatalf("terminal materialized state is missing: %+v", state)
			}
			marker, err := store.get(activeIndexKey)
			if err != nil || !reflect.DeepEqual(marker, activeIndexValue) {
				t.Fatalf("migration marker is invalid: %x %v", marker, err)
			}
		})
	}
}

func TestActiveIndexRejectsMalformedState(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*pebble.Batch) error
	}{
		{
			name: "marker",
			write: func(batch *pebble.Batch) error {
				return batch.Set(activeIndexKey, []byte{2}, nil)
			},
		},
		{
			name: "entry",
			write: func(batch *pebble.Batch) error {
				return batch.Set(segmentedKey(9, "missing"), activeWorkflowValue, nil)
			},
		},
		{
			name: "missing-receipt",
			write: func(batch *pebble.Batch) error {
				state, err := encodeJSON(core.WorkflowState{ID: "active", Status: core.WorkflowRunning})
				if err != nil {
					return err
				}
				if err := batch.Set(segmentedKey(6, "active"), state, nil); err != nil {
					return err
				}
				if err := batch.Set(segmentedKey(9, "active"), activeWorkflowValue, nil); err != nil {
					return err
				}
				return batch.Set(segmentedKey(10, "active", "missing"), activeWorkflowValue, nil)
			},
		},
		{
			name: "migration-state",
			write: func(batch *pebble.Batch) error {
				if err := batch.Delete(activeIndexKey, nil); err != nil {
					return err
				}
				data, err := encodeJSON(core.WorkflowState{ID: "different", Status: core.WorkflowRunning})
				if err != nil {
					return err
				}
				return batch.Set(segmentedKey(6, "workflow"), data, nil)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			config := Config{Directory: directory, ClusterID: "malformed", NodeID: 1, Voters: []uint64{1, 2, 3}}
			store, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			batch := store.db.NewBatch()
			if err := test.write(batch); err != nil {
				batch.Close()
				store.Close()
				t.Fatal(err)
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				batch.Close()
				store.Close()
				t.Fatal(err)
			}
			batch.Close()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(config); !errors.Is(err, ErrRecordVersion) {
				if err == nil {
					reopened.Close()
				}
				t.Fatalf("malformed active index returned %v", err)
			}
		})
	}
}

func writeLegacyActiveIndexFixture(t *testing.T, config Config) {
	t.Helper()
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	batch := store.db.NewBatch()
	definition := core.WorkflowDefinition{
		Name: "active", Namespace: "migration",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	engine := core.NewEngine()
	submitted, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "active-request", WorkflowID: "active", Definition: &definition})
	if err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	active, exists := engine.State("active")
	if !exists {
		batch.Close()
		store.Close()
		t.Fatal("active state is missing")
	}
	states := []core.WorkflowState{
		*active,
		{ID: "terminal", Status: core.WorkflowCompleted, Tasks: map[string]core.TaskState{}, Timers: map[string]core.TimerState{}},
	}
	for _, state := range states {
		data, err := encodeJSON(state)
		if err != nil {
			batch.Close()
			store.Close()
			t.Fatal(err)
		}
		if err := batch.Set(segmentedKey(6, state.ID), data, nil); err != nil {
			batch.Close()
			store.Close()
			t.Fatal(err)
		}
	}
	receipt := Receipt{
		Version: ReceiptVersion, RequestID: "active-request", RequestHash: submitted.Events[0].RequestHash,
		AppliedIndex: 1, CommittedTerm: 1, Result: submitted,
	}
	record, err := compactReceipt(receipt)
	if err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	data, err := encodeJSON(record)
	if err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	if err := batch.Set(segmentedKey(4, receipt.RequestID), data, nil); err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	segments, err := buildEventSegments([]eventSegmentSource{{receipt.Result.WorkflowID, receipt.AppliedIndex, receipt.Result.Events}}, true)
	if err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	data, err = encodeJSON(segments[0])
	if err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	if err := batch.Set(materializedSegmentKey(8, receipt.Result.WorkflowID, receipt.AppliedIndex), data, nil); err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	if err := batch.Delete(activeIndexKey, nil); err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		batch.Close()
		store.Close()
		t.Fatal(err)
	}
	batch.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
