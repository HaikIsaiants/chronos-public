package core

import "testing"

func TestApplyCommittedBatchesOwnsEventsAndReturnsChangedStates(t *testing.T) {
	definition := WorkflowDefinition{
		Name: "single", Namespace: "test",
		Tasks: []TaskDefinition{{
			ID: "task", Type: "test", Retry: RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
	planner := NewEngine()
	batch, _, duplicate, err := planner.PrepareAndApply(Command{
		Kind: CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition,
	})
	if err != nil || duplicate {
		t.Fatalf("planning failed: %v %t", err, duplicate)
	}
	engine := NewEngine()
	engine.DiscardJournal()
	applications, states, err := engine.ApplyCommittedBatches([]CommandBatch{batch})
	if err != nil {
		t.Fatal(err)
	}
	record := engine.requests[batch.RequestID]
	if states[batch.WorkflowID] != engine.states[batch.WorkflowID] {
		t.Fatal("changed state was copied")
	}
	if &record.result.Events[0] != &batch.Events[0] {
		t.Fatal("committed events were copied")
	}
	if &applications[0].Result.Events[0] == &record.result.Events[0] {
		t.Fatal("active application aliases retained request")
	}
	applications[0].Result.Events[0].Definition.Tasks[0].Type = "mutated"
	if record.result.Events[0].Definition.Tasks[0].Type != "test" || states[batch.WorkflowID].Tasks["task"].Definition.Type != "test" {
		t.Fatal("active application changed retained state")
	}
}
