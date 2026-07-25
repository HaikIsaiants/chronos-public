package storage

import (
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestTaskResultExecutionPhases(t *testing.T) {
	task := core.TaskState{Definition: core.TaskDefinition{
		Type: "deploy", Compensation: &core.CompensationDefinition{TaskType: "rollback"},
	}}
	tests := []struct {
		kind         core.EventKind
		compensation bool
		status       core.TaskStatus
		taskType     string
		phase        string
	}{
		{core.EventTaskCompleted, false, core.TaskCompleted, "deploy", "normal"},
		{core.EventTaskFailed, false, core.TaskFailed, "deploy", "normal"},
		{core.EventTaskTimedOut, false, core.TaskFailed, "deploy", "normal"},
		{core.EventTaskFailed, true, core.TaskFailed, "rollback", "compensation"},
		{core.EventTaskTimedOut, true, core.TaskFailed, "rollback", "compensation"},
		{core.EventTaskCompensated, true, core.TaskCompensated, "rollback", "compensation"},
	}
	for _, test := range tests {
		t.Run(string(test.kind)+test.phase, func(t *testing.T) {
			event := core.Event{
				Kind: test.kind, WorkflowID: "workflow", TaskID: "task", AttemptID: "attempt",
				Attempt: 2, Sequence: 9, At: 10, WorkerID: "worker", Fence: 3,
				Idempotency: "key", Output: map[string]string{"value": "done"}, Error: "failed",
				Compensation: test.compensation,
			}
			result, ok := taskResult(event, 7, task)
			if !ok || result.Version != TaskResultVersion || result.EventKind != test.kind ||
				result.Status != test.status || result.TaskType != test.taskType || result.Phase != test.phase {
				t.Fatalf("unexpected task result: %+v", result)
			}
		})
	}
}

func TestTaskResultIdentitySeparatesExecutionPhases(t *testing.T) {
	normal := resultIdentity(TaskResult{WorkflowID: "workflow", TaskID: "task", Phase: "normal", AttemptID: "attempt"})
	compensation := resultIdentity(TaskResult{WorkflowID: "workflow", TaskID: "task", Phase: "compensation", AttemptID: "attempt"})
	if normal == compensation {
		t.Fatal("normal and compensation results share an identity")
	}
}
