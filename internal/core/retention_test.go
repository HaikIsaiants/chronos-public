package core_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

func TestTerminalRetentionUsesDurableLoaders(t *testing.T) {
	engine := core.NewEngine()
	definition := core.WorkflowDefinition{
		Name: "retention", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	submitCommand := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	_, submitted, duplicate, err := engine.PrepareAndApply(submitCommand)
	if err != nil || duplicate {
		t.Fatal(err)
	}
	_, started, duplicate, err := engine.PrepareAndApply(lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
	}))
	if err != nil || duplicate {
		t.Fatal(err)
	}
	_, _, duplicate, err = engine.PrepareAndApply(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 2,
	}, started))
	if err != nil || duplicate {
		t.Fatal(err)
	}
	state, exists := engine.State(submitted.WorkflowID)
	if !exists || state.Status != core.WorkflowCompleted {
		t.Fatalf("terminal state is missing: %+v", state)
	}
	if !engine.CompactTerminal(submitted.WorkflowID) {
		t.Fatal("terminal state was not compacted")
	}
	compacted, exists := engine.State(submitted.WorkflowID)
	if !exists || compacted.ID != state.ID || compacted.Status != state.Status || compacted.Version != state.Version || compacted.UpdatedAt != state.UpdatedAt || len(compacted.Tasks) != 0 || len(compacted.Timers) != 0 {
		t.Fatalf("terminal proof differs: %+v", compacted)
	}
	if hash, exists := engine.ResidentRequestHash(submitCommand.RequestID); !exists || hash != submitted.Events[0].RequestHash {
		t.Fatalf("request proof differs: %s", hash)
	}
	receipt := core.RequestReceipt{RequestID: submitCommand.RequestID, RequestHash: submitted.Events[0].RequestHash, Result: submitted}
	engine.SetLoaders(
		func(workflowID string) (*core.WorkflowState, bool, error) {
			if workflowID != state.ID {
				return nil, false, nil
			}
			return state, true, nil
		},
		func(requestID string) (core.RequestReceipt, bool, error) {
			if requestID != receipt.RequestID {
				return core.RequestReceipt{}, false, nil
			}
			return receipt, true, nil
		},
	)
	engine.RetainActive()
	if len(engine.WorkflowIDs()) != 0 || len(engine.Snapshot().Requests) != 0 {
		t.Fatal("terminal workflow remained resident")
	}
	loaded, exists := engine.State(state.ID)
	if !exists || !reflect.DeepEqual(loaded, state) {
		t.Fatal("durable state loader changed the state")
	}
	_, retried, duplicate, err := engine.Prepare(submitCommand)
	if err != nil || !duplicate || !reflect.DeepEqual(retried.Events, submitted.Events) {
		t.Fatalf("durable request loader changed the result: %+v %v", retried, err)
	}
	changed := definition
	changed.Name = "changed"
	_, _, _, err = engine.Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &changed})
	if !errors.Is(err, core.ErrRequestConflict) {
		t.Fatalf("durable request conflict returned %v", err)
	}
	clone := engine.Clone()
	if loaded, exists := clone.State(state.ID); !exists || !reflect.DeepEqual(loaded, state) {
		t.Fatal("clone lost durable state loader")
	}
}
