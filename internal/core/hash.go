package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type canonicalTaskState struct {
	ID                string                       `json:"id"`
	Definition        TaskDefinition               `json:"definition"`
	Status            TaskStatus                   `json:"status"`
	Attempt           uint32                       `json:"attempt"`
	AttemptID         string                       `json:"attempt_id,omitempty"`
	Inputs            map[string]map[string]string `json:"inputs,omitempty"`
	Output            map[string]string            `json:"output,omitempty"`
	Error             string                       `json:"error,omitempty"`
	StartedAt         int64                        `json:"started_at,omitempty"`
	FinishedAt        int64                        `json:"finished_at,omitempty"`
	WorkerID          string                       `json:"worker_id,omitempty"`
	LeaseUntil        int64                        `json:"lease_expires_at,omitempty"`
	Fence             uint64                       `json:"fencing_token,omitempty"`
	Idempotency       string                       `json:"idempotency_key,omitempty"`
	RetryCount        uint32                       `json:"retry_count,omitempty"`
	CompletedSequence uint64                       `json:"completed_sequence,omitempty"`
	Children          []string                     `json:"children,omitempty"`
	Payload           map[string]string            `json:"payload,omitempty"`
	FanoutExpanded    bool                         `json:"fanout_expanded,omitempty"`
	Compensation      bool                         `json:"compensation,omitempty"`
}

type canonicalState struct {
	ID        string               `json:"id"`
	Name      string               `json:"name"`
	Namespace string               `json:"namespace"`
	Status    WorkflowStatus       `json:"status"`
	Version   uint64               `json:"version"`
	UpdatedAt int64                `json:"updated_at"`
	Tasks     []canonicalTaskState `json:"tasks"`
	Timers    []TimerState         `json:"timers"`
}

type canonicalCommand struct {
	Kind       CommandKind         `json:"kind"`
	WorkflowID string              `json:"workflow_id,omitempty"`
	TaskID     string              `json:"task_id,omitempty"`
	AttemptID  string              `json:"attempt_id,omitempty"`
	Definition *WorkflowDefinition `json:"definition,omitempty"`
	Output     map[string]string   `json:"output,omitempty"`
	Error      string              `json:"error,omitempty"`
	WorkerID   string              `json:"worker_id,omitempty"`
	LeaseUntil int64               `json:"lease_expires_at,omitempty"`
	Fence      uint64              `json:"fencing_token,omitempty"`
	TimerID    string              `json:"timer_id,omitempty"`
	Fanout     []FanoutItem        `json:"fanout,omitempty"`
	Reason     string              `json:"reason,omitempty"`
}

func HashCommand(command Command) (string, error) {
	payload := canonicalCommand{Kind: command.Kind}
	switch command.Kind {
	case CommandSubmit:
		payload.WorkflowID = command.WorkflowID
		payload.Definition = canonicalDefinition(command.Definition)
		if payload.WorkflowID == "" && payload.Definition != nil {
			payload.WorkflowID = WorkflowID(payload.Definition.Namespace, command.RequestID)
		}
	case CommandStart:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.WorkerID = command.WorkerID
	case CommandComplete:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.WorkerID = command.WorkerID
		payload.Fence = command.Fence
		payload.Output = cloneMap(command.Output)
		payload.Fanout = canonicalFanoutItems(command.Fanout)
	case CommandFail:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.WorkerID = command.WorkerID
		payload.Fence = command.Fence
		payload.Error = command.Error
	case CommandRenew:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.WorkerID = command.WorkerID
		payload.LeaseUntil = command.LeaseUntil
		payload.Fence = command.Fence
	case CommandExpire:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.WorkerID = command.WorkerID
		payload.Fence = command.Fence
	case CommandFireTimer:
		payload.WorkflowID = command.WorkflowID
		payload.TimerID = command.TimerID
	case CommandCancel:
		payload.WorkflowID = command.WorkflowID
		payload.Reason = command.Reason
	default:
		payload.WorkflowID = command.WorkflowID
		payload.TaskID = command.TaskID
		payload.AttemptID = command.AttemptID
		payload.Definition = canonicalDefinition(command.Definition)
		payload.Output = cloneMap(command.Output)
		payload.Error = command.Error
		payload.WorkerID = command.WorkerID
		payload.LeaseUntil = command.LeaseUntil
		payload.Fence = command.Fence
		payload.TimerID = command.TimerID
		payload.Fanout = canonicalFanoutItems(command.Fanout)
		payload.Reason = command.Reason
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return hashBytes(data), nil
}

func StateHash(state *WorkflowState) (string, error) {
	data, err := CanonicalState(state)
	if err != nil {
		return "", err
	}
	return hashBytes(data), nil
}

func CanonicalState(state *WorkflowState) ([]byte, error) {
	if state == nil {
		return json.Marshal(nil)
	}
	view := canonicalState{
		ID: state.ID, Name: state.Name, Namespace: state.Namespace, Status: state.Status,
		Version: state.Version, UpdatedAt: state.UpdatedAt,
	}
	taskIDs := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		taskIDs = append(taskIDs, id)
	}
	sort.Strings(taskIDs)
	view.Tasks = make([]canonicalTaskState, 0, len(taskIDs))
	for _, id := range taskIDs {
		task := state.Tasks[id]
		view.Tasks = append(view.Tasks, canonicalTaskState{
			ID: id, Definition: cloneTaskDefinition(task.Definition), Status: task.Status,
			Attempt: task.Attempt, AttemptID: task.AttemptID, Inputs: cloneNestedMap(task.Inputs),
			Output: cloneMap(task.Output), Error: task.Error, StartedAt: task.StartedAt,
			FinishedAt: task.FinishedAt, WorkerID: task.WorkerID, LeaseUntil: task.LeaseUntil,
			Fence: task.Fence, Idempotency: task.Idempotency,
			RetryCount: task.RetryCount, CompletedSequence: task.CompletedSequence,
			Children: cloneStrings(task.Children), Payload: cloneMap(task.Payload), FanoutExpanded: task.FanoutExpanded,
			Compensation: task.Compensation,
		})
	}
	timerIDs := make([]string, 0, len(state.Timers))
	for id := range state.Timers {
		timerIDs = append(timerIDs, id)
	}
	sort.Strings(timerIDs)
	view.Timers = make([]TimerState, 0, len(timerIDs))
	for _, id := range timerIDs {
		view.Timers = append(view.Timers, state.Timers[id])
	}
	return json.Marshal(view)
}

func hashBytes(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
