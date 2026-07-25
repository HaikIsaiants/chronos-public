package storage_test

import (
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
)

func TestApplyBatchesMatchesSequentialWorkload(t *testing.T) {
	workload := bench.WorkloadBatch(0, bench.WorkloadBatchSize)
	planner := core.NewEngine()
	batches := make([]core.CommandBatch, 0, len(workload))
	for _, command := range workload {
		batch, _, duplicate, err := planner.PrepareAndApply(command)
		if err != nil || duplicate {
			t.Fatalf("prepare and apply failed: %v %t", err, duplicate)
		}
		batches = append(batches, batch)
	}
	sequential := core.NewEngine()
	expected := make([]core.Result, 0, len(batches))
	for _, batch := range batches {
		result, err := sequential.ApplyBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, result)
	}
	transaction := core.NewEngine()
	applications, err := transaction.ApplyBatches(batches)
	if err != nil {
		t.Fatal(err)
	}
	actual := make([]core.Result, len(applications))
	for index, application := range applications {
		if application.Conflict {
			t.Fatalf("application %d conflicted", index)
		}
		actual[index] = application.Result
	}
	expectedCommands := bench.WorkloadBatchSize * bench.WorkloadCommandsPerWorkflow
	expectedTransitions := bench.WorkloadBatchSize * bench.WorkloadTransitionsPerWorkflow
	if uint64(len(applications)) != expectedCommands || transaction.Metrics().Transitions != expectedTransitions {
		t.Fatalf("unexpected workload totals: applications=%d transitions=%d", len(applications), transaction.Metrics().Transitions)
	}
	if !reflect.DeepEqual(actual, expected) || !reflect.DeepEqual(transaction.Snapshot(), sequential.Snapshot()) || !reflect.DeepEqual(transaction.Journal(), sequential.Journal()) || !reflect.DeepEqual(transaction.Metrics(), sequential.Metrics()) {
		t.Fatal("workload transaction differs from sequential application")
	}
}

func TestStoreApplyContinuesAfterRequestConflict(t *testing.T) {
	store := openStore(t, t.TempDir(), 1, nil)
	defer store.Close()
	definition := definition()
	submitted, submittedEntry := applyCommand(t, store, 2, core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition,
	})
	planner := store.Engine()
	start, _, duplicate, err := planner.Prepare(lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
	}))
	if err != nil || duplicate {
		t.Fatalf("prepare start failed: %v %t", err, duplicate)
	}
	conflict := submittedEntry.Batches[0]
	conflict.RequestHash = "different"
	entry := storage.ApplicationEntry{
		Version: storage.ApplicationEntryVersion, Index: 3, Term: 2,
		Batches: []core.CommandBatch{submittedEntry.Batches[0], conflict, start, start},
	}
	persistRaft(t, store, 3, 2)
	outcomes, err := store.Apply([]storage.ApplicationEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 4 || !outcomes[0].Result.Duplicate || outcomes[0].Code != "" || outcomes[1].Code != "request_conflict" || outcomes[2].Result.Duplicate || !outcomes[3].Result.Duplicate {
		t.Fatalf("unexpected outcomes: %+v", outcomes)
	}
	state, exists := store.State(submitted.WorkflowID)
	if !exists || state.Tasks["task"].Status != core.TaskRunning || store.Applied().Index != 3 {
		t.Fatalf("valid command after conflict was not committed: %+v %+v", state, store.Applied())
	}
}
