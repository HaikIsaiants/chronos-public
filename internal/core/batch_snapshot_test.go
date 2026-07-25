package core_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestPrepareDoesNotMutateEngine(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	before := engine.Snapshot()
	metrics := engine.Metrics()
	batch, result, duplicate, err := engine.Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate || result.WorkflowID == "" || batch.WorkflowID != result.WorkflowID {
		t.Fatalf("unexpected preparation: %+v %+v %v", batch, result, duplicate)
	}
	if !reflect.DeepEqual(before, engine.Snapshot()) || !reflect.DeepEqual(metrics, engine.Metrics()) || len(engine.Journal()) != 0 {
		t.Fatal("prepare mutated the engine")
	}
	applied, err := engine.ApplyBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(applied, result) {
		t.Fatalf("prepared result differs from applied result: %+v %+v", result, applied)
	}
}

func TestApplyBatchRetriesConflictsAndAtomicity(t *testing.T) {
	definition := singleDefinition()
	leader := core.NewEngine()
	batch, _, _, err := leader.Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	var persisted core.CommandBatch
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	replica := core.NewEngine()
	first, err := replica.ApplyBatch(persisted)
	if err != nil {
		t.Fatal(err)
	}
	historyLength := len(replica.Journal())
	retry, err := replica.ApplyBatch(batch)
	if err != nil || !retry.Duplicate || !reflect.DeepEqual(first.Events, retry.Events) {
		t.Fatalf("exact batch retry failed: %+v %v", retry, err)
	}
	if len(replica.Journal()) != historyLength || replica.Metrics().CommandsAccepted != 1 {
		t.Fatal("exact retry changed committed state")
	}
	conflict := batch
	conflict.Events = append([]core.Event(nil), batch.Events...)
	conflict.Events[0].At++
	stale, err := replica.ApplyBatch(conflict)
	if err != nil || !stale.Duplicate || !reflect.DeepEqual(stale.Events, first.Events) {
		t.Fatalf("stale exact retry did not return stored result: %+v %v", stale, err)
	}
	conflict.RequestHash = "different"
	if _, err := replica.ApplyBatch(conflict); !errors.Is(err, core.ErrRequestConflict) {
		t.Fatalf("conflicting request hash accepted: %v", err)
	}
	invalid := batch
	invalid.RequestID = "other"
	invalid.Events = append([]core.Event(nil), batch.Events...)
	invalid.Events[1].Sequence++
	invalid.Events[1].ID = core.EventID(invalid.WorkflowID, invalid.Events[1].Sequence, invalid.Events[1].Kind)
	invalid.Events[0].RequestID = invalid.RequestID
	invalid.Events[1].RequestID = invalid.RequestID
	empty := core.NewEngine()
	if _, err := empty.ApplyBatch(invalid); !errors.Is(err, core.ErrSequence) {
		t.Fatalf("invalid batch accepted: %v", err)
	}
	if len(empty.WorkflowIDs()) != 0 || len(empty.Journal()) != 0 || empty.Metrics().CommandsAccepted != 0 {
		t.Fatal("failed batch partially applied")
	}
}

func TestSnapshotRoundTripRestoresStateAndReceipts(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	submit, err := engine.Handle(command)
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Output: map[string]string{"value": "done"}}, start))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(engine.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var decoded core.EngineSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	restored, err := core.Restore(decoded)
	if err != nil {
		t.Fatal(err)
	}
	originalState, _ := engine.State(submit.WorkflowID)
	restoredState, _ := restored.State(submit.WorkflowID)
	if !reflect.DeepEqual(originalState, restoredState) {
		t.Fatalf("restored state differs: %+v %+v", originalState, restoredState)
	}
	if len(restored.Journal()) != 0 {
		t.Fatal("snapshot restored compacted event history")
	}
	metrics := restored.Metrics()
	if metrics.CommandsReceived != 0 || metrics.CommandsAccepted != 0 || metrics.Duplicates != 0 || metrics.Transitions != 0 {
		t.Fatalf("snapshot restored operational metrics: %+v", metrics)
	}
	retry, err := restored.Handle(command)
	if err != nil || !retry.Duplicate || len(restored.Journal()) != 0 {
		t.Fatalf("restored receipt did not deduplicate: %+v %v", retry, err)
	}
	clone := engine.Clone()
	if !reflect.DeepEqual(engine.Journal(), clone.Journal()) || !reflect.DeepEqual(engine.Metrics(), clone.Metrics()) {
		t.Fatal("clone lost engine history or metrics")
	}
}

func TestSnapshotEncodingIsCanonical(t *testing.T) {
	definition := singleDefinition()
	commands := []core.Command{
		{Kind: core.CommandSubmit, RequestID: "request-z", WorkflowID: "workflow-z", Definition: &definition},
		{Kind: core.CommandSubmit, RequestID: "request-a", WorkflowID: "workflow-a", Definition: &definition},
	}
	first := core.NewEngine()
	second := core.NewEngine()
	for _, command := range commands {
		if _, err := first.Handle(command); err != nil {
			t.Fatal(err)
		}
	}
	for index := len(commands) - 1; index >= 0; index-- {
		if _, err := second.Handle(commands[index]); err != nil {
			t.Fatal(err)
		}
	}
	firstData, err := json.Marshal(first.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	secondData, err := json.Marshal(second.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if string(firstData) != string(secondData) {
		t.Fatalf("snapshot encoding depends on application order:\n%s\n%s", firstData, secondData)
	}
	if first.Snapshot().Workflows[0].ID != "workflow-a" || first.Snapshot().Requests[0].RequestID != "request-a" {
		t.Fatal("snapshot records are not ordered")
	}
}

func TestCheckpointRollsBackAcceptedBatches(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	before := engine.Snapshot()
	metrics := engine.Metrics()
	batch, _, duplicate, err := engine.Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil || duplicate {
		t.Fatalf("prepare failed: %v %v", duplicate, err)
	}
	checkpoint := engine.Checkpoint([]core.CommandBatch{batch})
	if _, err := engine.ApplyBatch(batch); err != nil {
		t.Fatal(err)
	}
	engine.Rollback(checkpoint)
	if !reflect.DeepEqual(before, engine.Snapshot()) || !reflect.DeepEqual(metrics, engine.Metrics()) || len(engine.Journal()) != 0 || len(engine.ActiveWorkflowIDs()) != 0 {
		t.Fatal("rollback did not restore the engine")
	}
	result, err := engine.ApplyBatch(batch)
	if err != nil || result.Duplicate {
		t.Fatalf("rolled back request remained accepted: %+v %v", result, err)
	}
}
