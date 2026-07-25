package core_test

import (
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

func TestStagingStageMatchesMaterializedPreparation(t *testing.T) {
	definition := singleDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition}
	expectedEngine := core.NewEngine()
	expectedBatch, expectedResult, duplicate, err := expectedEngine.PrepareAndApply(command)
	if err != nil || duplicate {
		t.Fatalf("materialized preparation failed: %v %t", err, duplicate)
	}
	engine := core.NewEngine()
	transaction := engine.BeginStaging()
	batch, result, duplicate, err := transaction.Stage(command)
	if err != nil || duplicate {
		t.Fatalf("staged preparation failed: %v %t", err, duplicate)
	}
	transaction.Commit()
	if result.WorkflowID != expectedResult.WorkflowID || result.Events != nil || result.Duplicate {
		t.Fatalf("accepted stage materialized a result: %+v", result)
	}
	if !reflect.DeepEqual(batch, expectedBatch) || !reflect.DeepEqual(engine.Snapshot(), expectedEngine.Snapshot()) || !reflect.DeepEqual(engine.Journal(), expectedEngine.Journal()) || !reflect.DeepEqual(engine.Metrics(), expectedEngine.Metrics()) {
		t.Fatal("staged preparation differs from materialized preparation")
	}
	definition.Tasks[0].ID = "input-mutated"
	batch.Events[0].Definition.Tasks[0].ID = "batch-mutated"
	state, exists := engine.State("workflow")
	if !exists || state.Tasks["task"].Definition.ID != "task" {
		t.Fatalf("returned batch aliases planner state: %+v", state)
	}
	fresh := singleDefinition()
	_, stored, duplicate, err := engine.PrepareAndApply(core.Command{Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &fresh})
	if err != nil || !duplicate || stored.Events[0].Definition.Tasks[0].ID != "task" {
		t.Fatalf("returned batch changed stored result: %+v %t %v", stored, duplicate, err)
	}
}

func TestStagingStageKeepsDuplicateOutputsIsolated(t *testing.T) {
	definition := singleDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition}
	engine := core.NewEngine()
	transaction := engine.BeginStaging()
	accepted, result, duplicate, err := transaction.Stage(command)
	if err != nil || duplicate || result.Events != nil {
		t.Fatalf("initial stage failed: %+v %t %v", result, duplicate, err)
	}
	batch, result, duplicate, err := transaction.Stage(command)
	if err != nil || !duplicate || len(batch.Events) == 0 || len(result.Events) == 0 {
		t.Fatalf("duplicate stage failed: %+v %+v %t %v", batch, result, duplicate, err)
	}
	accepted.Events[0].Definition.Tasks[0].ID = "accepted-mutated"
	batch.Events[0].Definition.Tasks[0].ID = "batch-mutated"
	result.Events[0].Definition.Tasks[0].ID = "result-mutated"
	batch, result, duplicate, err = transaction.Stage(command)
	if err != nil || !duplicate || batch.Events[0].Definition.Tasks[0].ID != "task" || result.Events[0].Definition.Tasks[0].ID != "task" {
		t.Fatalf("duplicate outputs alias planner state: %+v %+v %t %v", batch, result, duplicate, err)
	}
	transaction.Commit()
}

func TestStagingStageRollsBackAfterError(t *testing.T) {
	engine := core.NewEngine()
	before := engine.Snapshot()
	metrics := engine.Metrics()
	transaction := engine.BeginStaging()
	definition := singleDefinition()
	if _, _, duplicate, err := transaction.Stage(core.Command{Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition}); err != nil || duplicate {
		t.Fatalf("initial stage failed: %v %t", err, duplicate)
	}
	_, _, _, err := transaction.Stage(core.Command{
		Kind: core.CommandStart, RequestID: "broken", WorkflowID: "workflow", TaskID: "missing",
		WorkerID: "worker", At: 1, LeaseUntil: 100,
	})
	if err == nil {
		t.Fatal("invalid command was staged")
	}
	if !reflect.DeepEqual(engine.Snapshot(), before) || !reflect.DeepEqual(engine.Metrics(), metrics) || len(engine.Journal()) != 0 || len(engine.ActiveWorkflowIDs()) != 0 {
		t.Fatal("failed stage changed the engine")
	}
	if _, exists := engine.ResidentRequestHash("submit"); exists {
		t.Fatal("failed stage retained request state")
	}
}
