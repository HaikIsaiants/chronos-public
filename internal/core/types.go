package core

type WorkflowStatus string

const (
	WorkflowScheduled    WorkflowStatus = "scheduled"
	WorkflowRunning      WorkflowStatus = "running"
	WorkflowCompleted    WorkflowStatus = "completed"
	WorkflowFailed       WorkflowStatus = "failed"
	WorkflowCancelled    WorkflowStatus = "cancelled"
	WorkflowCompensating WorkflowStatus = "compensating"
	WorkflowCompensated  WorkflowStatus = "compensated"
)

type TaskStatus string

const (
	TaskPending      TaskStatus = "pending"
	TaskReady        TaskStatus = "ready"
	TaskRunning      TaskStatus = "running"
	TaskCompleted    TaskStatus = "completed"
	TaskFailed       TaskStatus = "failed"
	TaskCancelled    TaskStatus = "cancelled"
	TaskCompensating TaskStatus = "compensating"
	TaskCompensated  TaskStatus = "compensated"
)

type TimerStatus string

const (
	TimerScheduled TimerStatus = "scheduled"
	TimerFired     TimerStatus = "fired"
	TimerCancelled TimerStatus = "cancelled"
)

type CommandKind string

const (
	CommandSubmit    CommandKind = "submit_workflow"
	CommandStart     CommandKind = "start_task"
	CommandComplete  CommandKind = "complete_task"
	CommandFail      CommandKind = "fail_task"
	CommandRenew     CommandKind = "renew_lease"
	CommandExpire    CommandKind = "expire_lease"
	CommandFireTimer CommandKind = "fire_timer"
	CommandCancel    CommandKind = "cancel_workflow"
)

type EventKind string

const (
	EventWorkflowSubmitted             EventKind = "workflow_submitted"
	EventTaskReady                     EventKind = "task_ready"
	EventTaskStarted                   EventKind = "task_started"
	EventTaskCompleted                 EventKind = "task_completed"
	EventTaskFailed                    EventKind = "task_failed"
	EventWorkflowCompleted             EventKind = "workflow_completed"
	EventWorkflowFailed                EventKind = "workflow_failed"
	EventLeaseRenewed                  EventKind = "task_lease_renewed"
	EventLeaseExpired                  EventKind = "task_lease_expired"
	EventWorkflowStarted               EventKind = "workflow_started"
	EventFanoutExpanded                EventKind = "fanout_expanded"
	EventTimerScheduled                EventKind = "timer_scheduled"
	EventTimerFired                    EventKind = "timer_fired"
	EventTimerCancelled                EventKind = "timer_cancelled"
	EventTaskRetryScheduled            EventKind = "task_retry_scheduled"
	EventTaskTimedOut                  EventKind = "task_timed_out"
	EventTaskCancelled                 EventKind = "task_cancelled"
	EventWorkflowCancellationRequested EventKind = "workflow_cancellation_requested"
	EventWorkflowCompensating          EventKind = "workflow_compensating"
	EventTaskCompensated               EventKind = "task_compensated"
	EventWorkflowCancelled             EventKind = "workflow_cancelled"
	EventWorkflowCompensated           EventKind = "workflow_compensated"
)

type TimerPurpose string

const (
	TimerWorkflowStart TimerPurpose = "workflow_start"
	TimerRetry         TimerPurpose = "retry"
	TimerTimeout       TimerPurpose = "timeout"
)

const MaxFanoutItems uint32 = 10000

type RetryPolicy struct {
	MaxAttempts          uint32 `json:"max_attempts"`
	InitialBackoffMillis int64  `json:"initial_backoff_millis"`
	BackoffMultiplier    uint32 `json:"backoff_multiplier"`
	MaxBackoffMillis     int64  `json:"max_backoff_millis"`
}

type CompensationDefinition struct {
	TaskType string `json:"task_type"`
}

type FanoutDefinition struct {
	TaskType        string                  `json:"task_type"`
	AggregateTaskID string                  `json:"aggregate_task_id"`
	MaxItems        uint32                  `json:"max_items"`
	Retry           RetryPolicy             `json:"retry"`
	TimeoutMillis   int64                   `json:"timeout_millis"`
	Compensation    *CompensationDefinition `json:"compensation,omitempty"`
}

type FanoutItem struct {
	Key     string            `json:"key"`
	Payload map[string]string `json:"payload,omitempty"`
}

type ExpandedTask struct {
	Key        string            `json:"key"`
	Definition TaskDefinition    `json:"definition"`
	Payload    map[string]string `json:"payload,omitempty"`
}

type TaskDefinition struct {
	ID            string                  `json:"id"`
	Type          string                  `json:"type"`
	Dependencies  []string                `json:"dependencies"`
	Retry         RetryPolicy             `json:"retry"`
	TimeoutMillis int64                   `json:"timeout_millis"`
	Compensation  *CompensationDefinition `json:"compensation,omitempty"`
	Fanout        *FanoutDefinition       `json:"fanout,omitempty"`
}

type WorkflowDefinition struct {
	Name      string           `json:"name"`
	Namespace string           `json:"namespace"`
	StartAt   int64            `json:"start_at"`
	Tasks     []TaskDefinition `json:"tasks"`
}

type Command struct {
	Kind       CommandKind         `json:"kind"`
	RequestID  string              `json:"request_id"`
	WorkflowID string              `json:"workflow_id,omitempty"`
	TaskID     string              `json:"task_id,omitempty"`
	AttemptID  string              `json:"attempt_id,omitempty"`
	At         int64               `json:"at"`
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

type Event struct {
	ID            string                       `json:"id"`
	Kind          EventKind                    `json:"kind"`
	Sequence      uint64                       `json:"sequence"`
	RequestID     string                       `json:"request_id"`
	RequestHash   string                       `json:"request_hash"`
	WorkflowID    string                       `json:"workflow_id"`
	TaskID        string                       `json:"task_id,omitempty"`
	AttemptID     string                       `json:"attempt_id,omitempty"`
	Attempt       uint32                       `json:"attempt,omitempty"`
	At            int64                        `json:"at"`
	Definition    *WorkflowDefinition          `json:"definition,omitempty"`
	Inputs        map[string]map[string]string `json:"inputs,omitempty"`
	Output        map[string]string            `json:"output,omitempty"`
	Error         string                       `json:"error,omitempty"`
	WorkerID      string                       `json:"worker_id,omitempty"`
	LeaseUntil    int64                        `json:"lease_expires_at,omitempty"`
	Fence         uint64                       `json:"fencing_token,omitempty"`
	Idempotency   string                       `json:"idempotency_key,omitempty"`
	Timer         *TimerState                  `json:"timer,omitempty"`
	ExpandedTasks []ExpandedTask               `json:"expanded_tasks,omitempty"`
	Reason        string                       `json:"reason,omitempty"`
	Compensation  bool                         `json:"compensation,omitempty"`
}

type TaskState struct {
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

type TimerState struct {
	ID        string       `json:"id"`
	TaskID    string       `json:"task_id"`
	Purpose   TimerPurpose `json:"purpose"`
	Deadline  int64        `json:"deadline"`
	AttemptID string       `json:"attempt_id,omitempty"`
	Attempt   uint32       `json:"attempt,omitempty"`
	Fence     uint64       `json:"fencing_token,omitempty"`
	Status    TimerStatus  `json:"status"`
}

type WorkflowState struct {
	ID        string                `json:"id"`
	Name      string                `json:"name"`
	Namespace string                `json:"namespace"`
	Status    WorkflowStatus        `json:"status"`
	Version   uint64                `json:"version"`
	UpdatedAt int64                 `json:"updated_at"`
	Tasks     map[string]TaskState  `json:"tasks"`
	Timers    map[string]TimerState `json:"timers"`
}

type AttemptProjection struct {
	TaskID      string `json:"task_id"`
	AttemptID   string `json:"attempt_id"`
	Attempt     uint32 `json:"attempt"`
	WorkerID    string `json:"worker_id"`
	LeaseUntil  int64  `json:"lease_expires_at"`
	Fence       uint64 `json:"fencing_token"`
	Idempotency string `json:"idempotency_key"`
}

type Summary struct {
	Total        int `json:"total"`
	Pending      int `json:"pending"`
	Ready        int `json:"ready"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	Cancelled    int `json:"cancelled"`
	Compensating int `json:"compensating"`
	Compensated  int `json:"compensated"`
}

type Projection struct {
	WorkflowID            string              `json:"workflow_id"`
	Status                WorkflowStatus      `json:"status"`
	ReadyTasks            []string            `json:"ready_tasks"`
	RemainingDependencies map[string]int      `json:"remaining_dependencies"`
	ActiveAttempts        []AttemptProjection `json:"active_attempts"`
	Timers                []TimerState        `json:"timers"`
	Summary               Summary             `json:"summary"`
}

type Result struct {
	WorkflowID string  `json:"workflow_id"`
	Events     []Event `json:"events"`
	Duplicate  bool    `json:"duplicate"`
}

const CommandBatchVersion uint32 = 1

type CommandBatch struct {
	Version     uint32  `json:"version"`
	RequestID   string  `json:"request_id"`
	RequestHash string  `json:"request_hash"`
	WorkflowID  string  `json:"workflow_id"`
	Events      []Event `json:"events"`
}

type RequestReceipt struct {
	RequestID   string `json:"request_id"`
	RequestHash string `json:"request_hash"`
	Result      Result `json:"result"`
}

const EngineSnapshotVersion uint32 = 3

type EngineSnapshot struct {
	Version   uint32           `json:"version"`
	Workflows []WorkflowState  `json:"workflows"`
	Requests  []RequestReceipt `json:"requests"`
}

type Metrics struct {
	CommandsReceived  uint64               `json:"commands_received"`
	CommandsAccepted  uint64               `json:"commands_accepted"`
	Duplicates        uint64               `json:"duplicates"`
	Transitions       uint64               `json:"transitions"`
	TransitionsByKind map[EventKind]uint64 `json:"transitions_by_kind"`
}
