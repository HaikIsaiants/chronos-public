package core

import (
	"reflect"
	"testing"
)

func TestStateUpdateCloneIsolatesFanoutDependencies(t *testing.T) {
	retry := RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	dependencies := make([]string, 1, 4)
	dependencies[0] = "discover"
	state := &WorkflowState{
		ID: "workflow", Status: WorkflowRunning, Version: 1,
		Tasks: map[string]TaskState{
			"discover": {
				Definition: TaskDefinition{
					ID: "discover", Type: "discover", Retry: retry,
					Fanout: &FanoutDefinition{TaskType: "test", AggregateTaskID: "aggregate", MaxItems: 1, Retry: retry},
				},
				Status: TaskCompleted,
			},
			"aggregate": {
				Definition: TaskDefinition{ID: "aggregate", Type: "aggregate", Dependencies: dependencies, Retry: retry},
				Status:     TaskPending,
			},
		},
		Timers: map[string]TimerState{},
	}
	childID := FanoutTaskID(state.ID, "discover", "child")
	event := Event{
		Kind: EventFanoutExpanded, WorkflowID: state.ID, TaskID: "discover", Sequence: 2,
		RequestID: "request", RequestHash: "hash",
		ExpandedTasks: []ExpandedTask{{
			Key: "child", Definition: TaskDefinition{
				ID: childID, Type: "test", Dependencies: []string{"discover"}, Retry: retry,
			},
		}},
	}
	event.ID = EventID(event.WorkflowID, event.Sequence, event.Kind)
	next, err := applyEvent(cloneStateForUpdate(state), event, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.Tasks["aggregate"].Definition.Dependencies, []string{"discover"}) {
		t.Fatalf("source dependencies changed: %v", state.Tasks["aggregate"].Definition.Dependencies)
	}
	if !reflect.DeepEqual(next.Tasks["aggregate"].Definition.Dependencies, []string{"discover", childID}) {
		t.Fatalf("updated dependencies are wrong: %v", next.Tasks["aggregate"].Definition.Dependencies)
	}
	if _, exists := state.Tasks[childID]; exists {
		t.Fatal("source tasks changed")
	}
}
