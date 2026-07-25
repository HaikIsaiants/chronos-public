package core

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func Decide(state *WorkflowState, command Command) ([]Event, error) {
	plan, err := decidePlan(state, command)
	if err != nil {
		return nil, err
	}
	return plan.events, nil
}

func decidePlan(state *WorkflowState, command Command) (*eventPlan, error) {
	return decideCommand(state, command, newEventPlan)
}

func decideStagedPlan(state *WorkflowState, command Command) (*eventPlan, error) {
	return decideCommand(state, command, newStagedEventPlan)
}

func decideCommand(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	if strings.TrimSpace(command.RequestID) == "" {
		return nil, fmt.Errorf("%w: request id is required", ErrInvalidCommand)
	}
	if command.At < 0 {
		return nil, fmt.Errorf("%w: logical time cannot be negative", ErrInvalidCommand)
	}
	if state != nil && command.At < state.UpdatedAt {
		return nil, ErrTimeRegression
	}
	switch command.Kind {
	case CommandSubmit:
		return decideSubmit(state, command, newPlan)
	case CommandStart:
		return decideStart(state, command, newPlan)
	case CommandComplete:
		return decideComplete(state, command, newPlan)
	case CommandFail:
		return decideFail(state, command, newPlan)
	case CommandRenew:
		return decideRenew(state, command, newPlan)
	case CommandExpire:
		return decideExpire(state, command, newPlan)
	case CommandFireTimer:
		return decideFireTimer(state, command, newPlan)
	case CommandCancel:
		return decideCancel(state, command, newPlan)
	default:
		return nil, fmt.Errorf("%w: unknown command %s", ErrInvalidCommand, command.Kind)
	}
}

func decideSubmit(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	if state != nil {
		return nil, ErrWorkflowExists
	}
	if strings.TrimSpace(command.WorkflowID) == "" {
		return nil, fmt.Errorf("%w: workflow id is required", ErrInvalidCommand)
	}
	if err := ValidateDefinition(command.Definition); err != nil {
		return nil, err
	}
	definition := canonicalDefinition(command.Definition)
	plan := newPlan(nil)
	if err := plan.add(Event{Kind: EventWorkflowSubmitted, WorkflowID: command.WorkflowID, At: command.At, Definition: definition}); err != nil {
		return nil, err
	}
	if definition.StartAt > command.At {
		timer := TimerState{ID: TimerID(command.WorkflowID, "", string(TimerWorkflowStart), 0), Purpose: TimerWorkflowStart, Deadline: definition.StartAt, Status: TimerScheduled}
		if err := plan.add(timerEvent(EventTimerScheduled, command.WorkflowID, timer, command.At)); err != nil {
			return nil, err
		}
	} else if err := addReadyTasks(plan, command.At); err != nil {
		return nil, err
	}
	return plan, nil
}

func decideStart(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	task, err := activeTask(state, command.TaskID)
	if err != nil {
		return nil, err
	}
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID == "" && task.Compensation
	if task.Status != TaskReady && !compensation {
		return nil, fmt.Errorf("%w: task %s is %s", ErrInvalidTransition, command.TaskID, task.Status)
	}
	if strings.TrimSpace(command.AttemptID) == "" {
		return nil, fmt.Errorf("%w: attempt id is required", ErrInvalidCommand)
	}
	if strings.TrimSpace(command.WorkerID) == "" {
		return nil, fmt.Errorf("%w: worker id is required", ErrInvalidCommand)
	}
	if command.LeaseUntil <= command.At {
		return nil, fmt.Errorf("%w: lease must expire after start", ErrInvalidCommand)
	}
	if task.Attempt == math.MaxUint32 {
		return nil, ErrAttemptOverflow
	}
	if task.Fence == math.MaxUint64 {
		return nil, ErrFencingOverflow
	}
	attempt := task.Attempt + 1
	if command.AttemptID != AttemptID(state.ID, command.TaskID, attempt) {
		return nil, fmt.Errorf("%w: invalid attempt id", ErrInvalidCommand)
	}
	var inputs map[string]map[string]string
	if len(task.Definition.Dependencies) > 0 {
		inputs = make(map[string]map[string]string, len(task.Definition.Dependencies))
	}
	for _, dependency := range task.Definition.Dependencies {
		if !compensation {
			inputs[dependency] = cloneMap(state.Tasks[dependency].Output)
		}
	}
	idempotency := IdempotencyKey(state.ID, command.TaskID)
	if compensation {
		idempotency = CompensationIdempotencyKey(state.ID, command.TaskID)
	}
	plan := newPlan(state)
	if err := plan.add(Event{
		Kind: EventTaskStarted, WorkflowID: state.ID, TaskID: command.TaskID,
		AttemptID: command.AttemptID, Attempt: attempt, At: command.At, Inputs: inputs,
		WorkerID: command.WorkerID, LeaseUntil: command.LeaseUntil, Fence: task.Fence + 1,
		Idempotency: idempotency, Compensation: compensation,
	}); err != nil {
		return nil, err
	}
	if task.Definition.TimeoutMillis > 0 {
		if command.At > math.MaxInt64-task.Definition.TimeoutMillis {
			return nil, ErrTimeOverflow
		}
		deadline := command.At + task.Definition.TimeoutMillis
		if command.LeaseUntil > deadline {
			return nil, fmt.Errorf("%w: lease exceeds task timeout", ErrInvalidCommand)
		}
		started := plan.state.Tasks[command.TaskID]
		timer := TimerState{
			ID: TimerID(state.ID, command.TaskID, string(TimerTimeout), attempt), TaskID: command.TaskID,
			Purpose: TimerTimeout, Deadline: deadline, AttemptID: command.AttemptID, Attempt: attempt,
			Fence: started.Fence, Status: TimerScheduled,
		}
		if err := plan.add(timerEvent(EventTimerScheduled, state.ID, timer, command.At)); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func decideComplete(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	task, err := leasedTask(state, command)
	if err != nil {
		return nil, err
	}
	if command.At >= task.LeaseUntil {
		return nil, ErrLeaseExpired
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, command.TaskID, command.AttemptID); exists && command.At >= timer.Deadline {
		return nil, ErrLeaseExpired
	}
	plan := newPlan(state)
	if task.Compensation {
		if err := plan.add(Event{
			Kind: EventTaskCompensated, WorkflowID: state.ID, TaskID: command.TaskID, AttemptID: command.AttemptID,
			Attempt: task.Attempt, At: command.At, Output: cloneMap(command.Output), WorkerID: command.WorkerID,
			LeaseUntil: task.LeaseUntil, Fence: command.Fence, Idempotency: task.Idempotency, Compensation: true,
		}); err != nil {
			return nil, err
		}
		if err := addTimerCancellation(plan, TimerTimeout, command.TaskID, command.AttemptID, command.At); err != nil {
			return nil, err
		}
		if next := nextCompensation(plan.state); next != "" {
			if err := plan.add(Event{Kind: EventWorkflowCompensating, WorkflowID: state.ID, TaskID: next, At: command.At}); err != nil {
				return nil, err
			}
		} else if err := plan.add(Event{Kind: EventWorkflowCompensated, WorkflowID: state.ID, At: command.At}); err != nil {
			return nil, err
		}
		return plan, nil
	}
	if err := plan.add(Event{
		Kind: EventTaskCompleted, WorkflowID: state.ID, TaskID: command.TaskID,
		AttemptID: command.AttemptID, Attempt: task.Attempt, At: command.At, Output: cloneMap(command.Output),
		WorkerID: command.WorkerID, LeaseUntil: task.LeaseUntil, Fence: command.Fence, Idempotency: task.Idempotency,
	}); err != nil {
		return nil, err
	}
	if err := addTimerCancellation(plan, TimerTimeout, command.TaskID, command.AttemptID, command.At); err != nil {
		return nil, err
	}
	if task.Definition.Fanout != nil {
		expanded, err := materializeFanout(plan.state, task, command.Fanout)
		if err != nil {
			return nil, err
		}
		if err := plan.add(Event{Kind: EventFanoutExpanded, WorkflowID: state.ID, TaskID: command.TaskID, At: command.At, ExpandedTasks: expanded}); err != nil {
			return nil, err
		}
	} else if len(command.Fanout) != 0 {
		return nil, fmt.Errorf("%w: task %s does not support fanout", ErrInvalidCommand, command.TaskID)
	}
	if err := addReadyTasks(plan, command.At); err != nil {
		return nil, err
	}
	allCompleted := true
	for _, candidate := range plan.state.Tasks {
		if candidate.Status != TaskCompleted {
			allCompleted = false
			break
		}
	}
	if allCompleted {
		if err := plan.add(Event{Kind: EventWorkflowCompleted, WorkflowID: state.ID, At: command.At}); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func decideFail(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	task, err := leasedTask(state, command)
	if err != nil {
		return nil, err
	}
	if command.At >= task.LeaseUntil {
		return nil, ErrLeaseExpired
	}
	if strings.TrimSpace(command.Error) == "" {
		return nil, fmt.Errorf("%w: failure reason is required", ErrInvalidCommand)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, command.TaskID, command.AttemptID); exists && command.At >= timer.Deadline {
		return nil, ErrLeaseExpired
	}
	plan := newPlan(state)
	if err := plan.add(Event{
		Kind: EventTaskFailed, WorkflowID: state.ID, TaskID: command.TaskID, AttemptID: command.AttemptID,
		Attempt: task.Attempt, At: command.At, Error: command.Error, WorkerID: command.WorkerID,
		LeaseUntil: task.LeaseUntil, Fence: command.Fence, Idempotency: task.Idempotency, Compensation: task.Compensation,
	}); err != nil {
		return nil, err
	}
	if err := addTimerCancellation(plan, TimerTimeout, command.TaskID, command.AttemptID, command.At); err != nil {
		return nil, err
	}
	retried, err := addRetry(plan, command.TaskID, task.Compensation, command.At)
	if err != nil {
		return nil, err
	}
	if !retried {
		if task.Compensation {
			if err := plan.add(Event{Kind: EventWorkflowFailed, WorkflowID: state.ID, TaskID: command.TaskID, At: command.At, Error: command.Error}); err != nil {
				return nil, err
			}
		} else if err := addTerminalFailure(plan, command.TaskID, command.Error, command.At); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func decideRenew(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	task, err := leasedTask(state, command)
	if err != nil {
		return nil, err
	}
	if command.At >= task.LeaseUntil {
		return nil, ErrLeaseExpired
	}
	if command.LeaseUntil <= task.LeaseUntil {
		return nil, fmt.Errorf("%w: renewal must extend lease", ErrInvalidCommand)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, command.TaskID, command.AttemptID); exists && command.LeaseUntil > timer.Deadline {
		return nil, fmt.Errorf("%w: renewal exceeds task timeout", ErrInvalidCommand)
	}
	plan := newPlan(state)
	if err := plan.add(Event{
		Kind: EventLeaseRenewed, WorkflowID: state.ID, TaskID: command.TaskID,
		AttemptID: command.AttemptID, Attempt: task.Attempt, At: command.At,
		WorkerID: command.WorkerID, LeaseUntil: command.LeaseUntil, Fence: command.Fence,
		Idempotency: task.Idempotency, Compensation: task.Compensation,
	}); err != nil {
		return nil, err
	}
	return plan, nil
}

func decideExpire(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	task, err := leasedTask(state, command)
	if err != nil {
		return nil, err
	}
	if command.At < task.LeaseUntil {
		return nil, fmt.Errorf("%w: lease is active", ErrInvalidTransition)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, command.TaskID, command.AttemptID); exists && timer.Deadline <= task.LeaseUntil && command.At >= timer.Deadline {
		return nil, fmt.Errorf("%w: task timeout is due", ErrInvalidTransition)
	}
	plan := newPlan(state)
	if err := plan.add(Event{
		Kind: EventLeaseExpired, WorkflowID: state.ID, TaskID: command.TaskID,
		AttemptID: command.AttemptID, Attempt: task.Attempt, At: command.At,
		WorkerID: command.WorkerID, LeaseUntil: task.LeaseUntil, Fence: command.Fence,
		Idempotency: task.Idempotency, Compensation: task.Compensation,
	}); err != nil {
		return nil, err
	}
	if err := addTimerCancellation(plan, TimerTimeout, command.TaskID, command.AttemptID, command.At); err != nil {
		return nil, err
	}
	return plan, nil
}

func leasedTask(state *WorkflowState, command Command) (TaskState, error) {
	if state == nil {
		return TaskState{}, ErrWorkflowNotFound
	}
	task, exists := state.Tasks[command.TaskID]
	if !exists {
		return TaskState{}, fmt.Errorf("%w: task %s not found", ErrInvalidCommand, command.TaskID)
	}
	forward := state.Status == WorkflowRunning && task.Status == TaskRunning && !task.Compensation
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID != "" && task.Compensation
	if !forward && !compensation ||
		task.AttemptID != command.AttemptID || task.WorkerID != command.WorkerID || task.Fence != command.Fence {
		return TaskState{}, ErrFenced
	}
	return task, nil
}

func activeTask(state *WorkflowState, taskID string) (TaskState, error) {
	if state == nil {
		return TaskState{}, ErrWorkflowNotFound
	}
	if state.Status != WorkflowRunning && state.Status != WorkflowCompensating {
		return TaskState{}, fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	task, exists := state.Tasks[taskID]
	if !exists {
		return TaskState{}, fmt.Errorf("%w: task %s not found", ErrInvalidCommand, taskID)
	}
	return task, nil
}

func Apply(state *WorkflowState, event Event) (*WorkflowState, error) {
	return applyEvent(state, event, true)
}

func applyOwnedEvents(state *WorkflowState, events []Event) (*WorkflowState, error) {
	next := state
	for _, event := range events {
		var err error
		next, err = applyEvent(next, event, false)
		if err != nil {
			return nil, err
		}
	}
	return next, nil
}

func applyEvent(state *WorkflowState, event Event, isolated bool) (*WorkflowState, error) {
	if strings.TrimSpace(event.WorkflowID) == "" || strings.TrimSpace(event.RequestID) == "" || event.RequestHash == "" || event.At < 0 {
		return nil, fmt.Errorf("%w: event metadata is invalid", ErrInvalidTransition)
	}
	if event.Sequence == 0 || event.ID != EventID(event.WorkflowID, event.Sequence, event.Kind) {
		return nil, ErrSequence
	}
	if state == nil {
		if event.Kind != EventWorkflowSubmitted || event.Sequence != 1 {
			return nil, ErrSequence
		}
		return applySubmission(event)
	}
	if event.WorkflowID != state.ID || event.Sequence != state.Version+1 {
		return nil, ErrSequence
	}
	if event.At < state.UpdatedAt {
		return nil, ErrTimeRegression
	}
	next := state
	if isolated {
		next = cloneState(state)
	}
	switch event.Kind {
	case EventTaskReady:
		if err := applyReady(next, event); err != nil {
			return nil, err
		}
	case EventTaskStarted:
		if err := applyStarted(next, event); err != nil {
			return nil, err
		}
	case EventTaskCompleted:
		if err := applyCompleted(next, event); err != nil {
			return nil, err
		}
	case EventTaskFailed:
		if err := applyFailed(next, event); err != nil {
			return nil, err
		}
	case EventWorkflowCompleted:
		if err := applyWorkflowCompleted(next); err != nil {
			return nil, err
		}
	case EventWorkflowFailed:
		if err := applyWorkflowFailed(next, event); err != nil {
			return nil, err
		}
	case EventLeaseRenewed:
		if err := applyRenewed(next, event); err != nil {
			return nil, err
		}
	case EventLeaseExpired:
		if err := applyExpired(next, event); err != nil {
			return nil, err
		}
	case EventWorkflowStarted:
		if err := applyWorkflowStarted(next, event); err != nil {
			return nil, err
		}
	case EventFanoutExpanded:
		if err := applyFanoutExpanded(next, event); err != nil {
			return nil, err
		}
	case EventTimerScheduled:
		if err := applyTimerScheduled(next, event); err != nil {
			return nil, err
		}
	case EventTimerFired:
		if err := applyTimerTransition(next, event, TimerFired); err != nil {
			return nil, err
		}
	case EventTimerCancelled:
		if err := applyTimerTransition(next, event, TimerCancelled); err != nil {
			return nil, err
		}
	case EventTaskRetryScheduled:
		if err := applyTaskRetryScheduled(next, event); err != nil {
			return nil, err
		}
	case EventTaskTimedOut:
		if err := applyTaskTimedOut(next, event); err != nil {
			return nil, err
		}
	case EventTaskCancelled:
		if err := applyTaskCancelled(next, event); err != nil {
			return nil, err
		}
	case EventWorkflowCancellationRequested:
		if err := applyWorkflowCancellationRequested(next); err != nil {
			return nil, err
		}
	case EventWorkflowCompensating:
		if err := applyWorkflowCompensating(next, event); err != nil {
			return nil, err
		}
	case EventTaskCompensated:
		if err := applyTaskCompensated(next, event); err != nil {
			return nil, err
		}
	case EventWorkflowCancelled:
		if err := applyWorkflowCancelled(next); err != nil {
			return nil, err
		}
	case EventWorkflowCompensated:
		if err := applyWorkflowCompensated(next); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: unknown event %s", ErrInvalidTransition, event.Kind)
	}
	next.Version = event.Sequence
	next.UpdatedAt = event.At
	return next, nil
}

func applySubmission(event Event) (*WorkflowState, error) {
	if err := ValidateDefinition(event.Definition); err != nil {
		return nil, err
	}
	definition := canonicalDefinition(event.Definition)
	state := &WorkflowState{
		ID: event.WorkflowID, Name: definition.Name, Namespace: definition.Namespace,
		Status: WorkflowRunning, Version: event.Sequence, UpdatedAt: event.At,
		Tasks: make(map[string]TaskState, len(definition.Tasks)), Timers: make(map[string]TimerState),
	}
	if definition.StartAt > event.At {
		state.Status = WorkflowScheduled
	}
	for _, task := range definition.Tasks {
		state.Tasks[task.ID] = TaskState{Definition: cloneTaskDefinition(task), Status: TaskPending}
	}
	return state, nil
}

func applyReady(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	if !exists || task.Status != TaskPending {
		return fmt.Errorf("%w: task %s cannot become ready", ErrInvalidTransition, event.TaskID)
	}
	if task.Compensation {
		if state.Status != WorkflowCompensating {
			return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
		}
	} else {
		if state.Status != WorkflowRunning {
			return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
		}
		for _, dependency := range task.Definition.Dependencies {
			if state.Tasks[dependency].Status != TaskCompleted {
				return fmt.Errorf("%w: task %s dependency %s is incomplete", ErrInvalidTransition, event.TaskID, dependency)
			}
		}
	}
	if _, waiting := scheduledTimer(state, TimerRetry, event.TaskID, ""); waiting {
		return fmt.Errorf("%w: task %s retry is delayed", ErrInvalidTransition, event.TaskID)
	}
	if task.Compensation {
		task.Status = TaskCompensating
	} else {
		task.Status = TaskReady
	}
	state.Tasks[event.TaskID] = task
	return nil
}

func applyStarted(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	forward := state.Status == WorkflowRunning && task.Status == TaskReady && !task.Compensation && !event.Compensation
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID == "" && task.Compensation && event.Compensation
	idempotency := IdempotencyKey(state.ID, event.TaskID)
	if compensation {
		idempotency = CompensationIdempotencyKey(state.ID, event.TaskID)
	}
	if !exists || !forward && !compensation || task.Attempt == math.MaxUint32 ||
		task.Fence == math.MaxUint64 || event.Attempt != task.Attempt+1 ||
		event.AttemptID != AttemptID(state.ID, event.TaskID, event.Attempt) ||
		strings.TrimSpace(event.WorkerID) == "" || event.LeaseUntil <= event.At ||
		event.Fence != task.Fence+1 || event.Idempotency != idempotency {
		return fmt.Errorf("%w: task %s cannot start", ErrInvalidTransition, event.TaskID)
	}
	if task.Definition.TimeoutMillis > 0 && (event.At > math.MaxInt64-task.Definition.TimeoutMillis || event.LeaseUntil > event.At+task.Definition.TimeoutMillis) {
		return fmt.Errorf("%w: task %s lease exceeds timeout", ErrInvalidTransition, event.TaskID)
	}
	if forward {
		task.Status = TaskRunning
	}
	task.Attempt = event.Attempt
	task.AttemptID = event.AttemptID
	task.Inputs = cloneNestedMap(event.Inputs)
	task.StartedAt = event.At
	task.WorkerID = event.WorkerID
	task.LeaseUntil = event.LeaseUntil
	task.Fence = event.Fence
	task.Idempotency = event.Idempotency
	task.Error = ""
	state.Tasks[event.TaskID] = task
	return nil
}

func applyCompleted(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	if state.Status != WorkflowRunning || !exists || task.Status != TaskRunning || event.Compensation || task.Compensation ||
		task.AttemptID != event.AttemptID || task.Attempt != event.Attempt ||
		task.WorkerID != event.WorkerID || task.Fence != event.Fence ||
		task.Idempotency != event.Idempotency || event.At >= task.LeaseUntil {
		return fmt.Errorf("%w: task %s cannot complete", ErrInvalidTransition, event.TaskID)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, event.TaskID, event.AttemptID); exists && event.At >= timer.Deadline {
		return fmt.Errorf("%w: task %s timed out", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskCompleted
	task.Output = cloneMap(event.Output)
	task.FinishedAt = event.At
	task.CompletedSequence = event.Sequence
	state.Tasks[event.TaskID] = task
	return nil
}

func applyFailed(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	forward := state.Status == WorkflowRunning && task.Status == TaskRunning && !task.Compensation && !event.Compensation
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID != "" && task.Compensation && event.Compensation
	if !exists || !forward && !compensation ||
		task.AttemptID != event.AttemptID || task.Attempt != event.Attempt ||
		task.WorkerID != event.WorkerID || task.Fence != event.Fence ||
		task.Idempotency != event.Idempotency || event.At >= task.LeaseUntil {
		return fmt.Errorf("%w: task %s cannot fail", ErrInvalidTransition, event.TaskID)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, event.TaskID, event.AttemptID); exists && event.At >= timer.Deadline {
		return fmt.Errorf("%w: task %s timed out", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskFailed
	task.Error = event.Error
	task.FinishedAt = event.At
	task.Compensation = event.Compensation
	state.Tasks[event.TaskID] = task
	return nil
}

func applyRenewed(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	forward := state.Status == WorkflowRunning && task.Status == TaskRunning && !task.Compensation && !event.Compensation
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID != "" && task.Compensation && event.Compensation
	if !exists || !forward && !compensation ||
		task.AttemptID != event.AttemptID || task.Attempt != event.Attempt ||
		task.WorkerID != event.WorkerID || task.Fence != event.Fence ||
		task.Idempotency != event.Idempotency || event.At >= task.LeaseUntil ||
		event.LeaseUntil <= task.LeaseUntil {
		return fmt.Errorf("%w: task %s lease cannot renew", ErrInvalidTransition, event.TaskID)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, event.TaskID, event.AttemptID); exists && event.LeaseUntil > timer.Deadline {
		return fmt.Errorf("%w: task %s lease exceeds timeout", ErrInvalidTransition, event.TaskID)
	}
	task.LeaseUntil = event.LeaseUntil
	state.Tasks[event.TaskID] = task
	return nil
}

func applyExpired(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	forward := state.Status == WorkflowRunning && task.Status == TaskRunning && !task.Compensation && !event.Compensation
	compensation := state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID != "" && task.Compensation && event.Compensation
	if !exists || !forward && !compensation ||
		task.AttemptID != event.AttemptID || task.Attempt != event.Attempt ||
		task.WorkerID != event.WorkerID || task.Fence != event.Fence ||
		task.Idempotency != event.Idempotency || event.LeaseUntil != task.LeaseUntil ||
		event.At < task.LeaseUntil {
		return fmt.Errorf("%w: task %s lease cannot expire", ErrInvalidTransition, event.TaskID)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, event.TaskID, event.AttemptID); exists && timer.Deadline <= task.LeaseUntil {
		return fmt.Errorf("%w: task %s timeout is due", ErrInvalidTransition, event.TaskID)
	}
	if compensation {
		task.Status = TaskCompensating
	} else {
		task.Status = TaskReady
	}
	task.AttemptID = ""
	task.Inputs = nil
	task.StartedAt = 0
	task.WorkerID = ""
	task.LeaseUntil = 0
	state.Tasks[event.TaskID] = task
	return nil
}

func applyWorkflowCompleted(state *WorkflowState) error {
	if state.Status != WorkflowRunning {
		return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	for _, task := range state.Tasks {
		if task.Status != TaskCompleted {
			return fmt.Errorf("%w: workflow has incomplete tasks", ErrInvalidTransition)
		}
	}
	if hasScheduledTimers(state) {
		return fmt.Errorf("%w: workflow has scheduled timers", ErrInvalidTransition)
	}
	state.Status = WorkflowCompleted
	return nil
}

func applyWorkflowFailed(state *WorkflowState, event Event) error {
	if state.Status != WorkflowRunning && state.Status != WorkflowCompensating {
		return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	task, exists := state.Tasks[event.TaskID]
	if !exists || task.Status != TaskFailed {
		return fmt.Errorf("%w: workflow has no failed task", ErrInvalidTransition)
	}
	if hasActiveState(state) || hasScheduledTimers(state) {
		return fmt.Errorf("%w: workflow has active work", ErrInvalidTransition)
	}
	state.Status = WorkflowFailed
	return nil
}

func sortedTaskIDs(state *WorkflowState) []string {
	ids := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
