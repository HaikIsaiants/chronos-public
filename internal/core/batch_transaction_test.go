package core_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestApplyBatchesRollsBackOnError(t *testing.T) {
	batches, _ := singleWorkflowBatches(t, "workflow", "request", "done")
	broken := batches[2]
	broken.Events = append([]core.Event(nil), batches[2].Events...)
	last := len(broken.Events) - 1
	broken.Events[last].Sequence++
	broken.Events[last].ID = core.EventID(broken.WorkflowID, broken.Events[last].Sequence, broken.Events[last].Kind)
	engine := core.NewEngine()
	if _, err := engine.ApplyBatch(batches[0]); err != nil {
		t.Fatal(err)
	}
	before := engine.Snapshot()
	metrics := engine.Metrics()
	journal := engine.Journal()
	if applications, err := engine.ApplyBatches([]core.CommandBatch{batches[1], broken}); !errors.Is(err, core.ErrSequence) || applications != nil {
		t.Fatalf("invalid transaction returned %+v %v", applications, err)
	}
	if !reflect.DeepEqual(engine.Snapshot(), before) || !reflect.DeepEqual(engine.Metrics(), metrics) || !reflect.DeepEqual(engine.Journal(), journal) || !reflect.DeepEqual(engine.ActiveWorkflowIDs(), []string{"workflow"}) {
		t.Fatal("invalid transaction changed the engine")
	}
	result, err := engine.ApplyBatch(batches[1])
	if err != nil || result.Duplicate {
		t.Fatalf("rolled back request remained visible: %+v %v", result, err)
	}
}

func TestApplyBatchesPreservesDuplicatesConflictsAndIsolation(t *testing.T) {
	batches, planner := singleWorkflowBatches(t, "workflow", "request", "done")
	engine := core.NewEngine()
	if _, err := engine.ApplyBatch(batches[0]); err != nil {
		t.Fatal(err)
	}
	conflict := batches[0]
	conflict.RequestHash = "different"
	applications, err := engine.ApplyBatches([]core.CommandBatch{batches[0], conflict, batches[1], batches[1], batches[2]})
	if err != nil {
		t.Fatal(err)
	}
	if len(applications) != 5 || !applications[0].Result.Duplicate || !applications[1].Conflict || applications[2].Result.Duplicate || !applications[3].Result.Duplicate || applications[4].Result.Duplicate {
		t.Fatalf("unexpected applications: %+v", applications)
	}
	if !reflect.DeepEqual(engine.Snapshot(), planner.Snapshot()) || !reflect.DeepEqual(engine.Journal(), planner.Journal()) || !reflect.DeepEqual(engine.Metrics(), planner.Metrics()) {
		t.Fatal("transaction result differs from sequential preparation")
	}
	batches[2].Events[0].Output["value"] = "input-mutated"
	applications[4].Result.Events[0].Output["value"] = "result-mutated"
	state, exists := engine.State("workflow")
	if !exists || state.Tasks["task"].Output["value"] != "done" {
		t.Fatalf("transaction aliases caller data: %+v", state)
	}
	retry, err := engine.ApplyBatch(batches[2])
	if err != nil || !retry.Duplicate || retry.Events[0].Output["value"] != "done" {
		t.Fatalf("stored result was mutated: %+v %v", retry, err)
	}
}

func TestApplyBatchesPreservesMixedWorkflowOrder(t *testing.T) {
	left, _ := singleWorkflowBatches(t, "left", "left", "L")
	right, _ := singleWorkflowBatches(t, "right", "right", "R")
	mixed := []core.CommandBatch{left[0], right[0], left[1], right[1], left[2], right[2]}
	sequential := core.NewEngine()
	expected := make([]core.Result, 0, len(mixed))
	for _, batch := range mixed {
		result, err := sequential.ApplyBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		expected = append(expected, result)
	}
	transaction := core.NewEngine()
	applications, err := transaction.ApplyBatches(mixed)
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
	if !reflect.DeepEqual(actual, expected) || !reflect.DeepEqual(transaction.Snapshot(), sequential.Snapshot()) || !reflect.DeepEqual(transaction.Journal(), sequential.Journal()) || !reflect.DeepEqual(transaction.Metrics(), sequential.Metrics()) {
		t.Fatal("mixed workflow transaction changed application order")
	}
	for workflowID, output := range map[string]string{"left": "L", "right": "R"} {
		state, exists := transaction.State(workflowID)
		if !exists || state.Status != core.WorkflowCompleted || state.Tasks["task"].Output["value"] != output {
			t.Fatalf("unexpected %s state: %+v", workflowID, state)
		}
	}
}

func singleWorkflowBatches(t *testing.T, workflowID, prefix, output string) ([]core.CommandBatch, *core.Engine) {
	t.Helper()
	planner := core.NewEngine()
	definition := singleDefinition()
	batches := make([]core.CommandBatch, 0, 3)
	apply := func(command core.Command) core.Result {
		t.Helper()
		batch, result, duplicate, err := planner.PrepareAndApply(command)
		if err != nil || duplicate {
			t.Fatalf("prepare and apply failed: %v %t", err, duplicate)
		}
		batches = append(batches, batch)
		return result
	}
	apply(core.Command{Kind: core.CommandSubmit, RequestID: prefix + "-submit", WorkflowID: workflowID, Definition: &definition})
	start := apply(lease(core.Command{Kind: core.CommandStart, RequestID: prefix + "-start", WorkflowID: workflowID, TaskID: "task", At: 1}))
	apply(fenced(core.Command{Kind: core.CommandComplete, RequestID: prefix + "-complete", WorkflowID: workflowID, TaskID: "task", At: 2, Output: map[string]string{"value": output}}, start))
	return batches, planner
}
