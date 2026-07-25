package core

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

type eventPlan struct {
	state  *WorkflowState
	events []Event
}

type eventPlanFactory func(*WorkflowState) *eventPlan

func newEventPlan(state *WorkflowState) *eventPlan {
	return &eventPlan{state: cloneStateForUpdate(state)}
}

func newStagedEventPlan(state *WorkflowState) *eventPlan {
	return &eventPlan{state: state}
}

func (p *eventPlan) add(event Event) error {
	validated := event
	validated.RequestID = "plan"
	validated.RequestHash = "plan"
	if p.state == nil {
		validated.Sequence = 1
	} else {
		validated.Sequence = p.state.Version + 1
	}
	validated.ID = EventID(validated.WorkflowID, validated.Sequence, validated.Kind)
	next, err := applyEvent(p.state, validated, false)
	if err != nil {
		return err
	}
	p.state = next
	p.events = append(p.events, cloneEvent(event))
	return nil
}

func timerEvent(kind EventKind, workflowID string, timer TimerState, at int64) Event {
	copy := timer
	switch kind {
	case EventTimerFired:
		copy.Status = TimerFired
	case EventTimerCancelled:
		copy.Status = TimerCancelled
	default:
		copy.Status = TimerScheduled
	}
	return Event{Kind: kind, WorkflowID: workflowID, TaskID: timer.TaskID, AttemptID: timer.AttemptID, Attempt: timer.Attempt, At: at, Fence: timer.Fence, Timer: &copy}
}

func scheduledTimer(state *WorkflowState, purpose TimerPurpose, taskID, attemptID string) (TimerState, bool) {
	for _, timer := range state.Timers {
		if timer.Status == TimerScheduled && timer.Purpose == purpose && timer.TaskID == taskID && (attemptID == "" || timer.AttemptID == attemptID) {
			return timer, true
		}
	}
	return TimerState{}, false
}

func addTimerCancellation(plan *eventPlan, purpose TimerPurpose, taskID, attemptID string, at int64) error {
	timer, exists := scheduledTimer(plan.state, purpose, taskID, attemptID)
	if !exists {
		return nil
	}
	return plan.add(timerEvent(EventTimerCancelled, plan.state.ID, timer, at))
}

func addReadyTasks(plan *eventPlan, at int64) error {
	if plan.state.Status != WorkflowRunning {
		return nil
	}
	for _, id := range sortedTaskIDs(plan.state) {
		task := plan.state.Tasks[id]
		if task.Status != TaskPending || task.Compensation {
			continue
		}
		if _, waiting := scheduledTimer(plan.state, TimerRetry, id, ""); waiting {
			continue
		}
		ready := true
		for _, dependency := range task.Definition.Dependencies {
			if plan.state.Tasks[dependency].Status != TaskCompleted {
				ready = false
				break
			}
		}
		if ready {
			if err := plan.add(Event{Kind: EventTaskReady, WorkflowID: plan.state.ID, TaskID: id, At: at}); err != nil {
				return err
			}
		}
	}
	return nil
}

func materializeFanout(state *WorkflowState, task TaskState, items []FanoutItem) ([]ExpandedTask, error) {
	definition := task.Definition.Fanout
	if definition == nil {
		if len(items) != 0 {
			return nil, fmt.Errorf("%w: task %s does not support fanout", ErrInvalidCommand, task.Definition.ID)
		}
		return nil, nil
	}
	if uint32(len(items)) > definition.MaxItems || len(items) > int(MaxFanoutItems) {
		return nil, fmt.Errorf("%w: task %s fanout exceeds maximum", ErrInvalidCommand, task.Definition.ID)
	}
	// sort fan-out keys before deriving child ids
	items = canonicalFanoutItems(items)
	result := make([]ExpandedTask, len(items))
	previous := ""
	for index, item := range items {
		if strings.TrimSpace(item.Key) == "" || index > 0 && item.Key == previous {
			return nil, fmt.Errorf("%w: fanout keys must be unique and non-empty", ErrInvalidCommand)
		}
		previous = item.Key
		id := FanoutTaskID(state.ID, task.Definition.ID, item.Key)
		if _, exists := state.Tasks[id]; exists {
			return nil, fmt.Errorf("%w: fanout task %s exists", ErrInvalidTransition, id)
		}
		result[index] = ExpandedTask{
			Key: item.Key,
			Definition: TaskDefinition{
				ID: id, Type: definition.TaskType, Dependencies: []string{task.Definition.ID}, Retry: definition.Retry,
				TimeoutMillis: definition.TimeoutMillis, Compensation: cloneCompensation(definition.Compensation),
			},
			Payload: cloneMap(item.Payload),
		}
	}
	return result, nil
}

func cloneCompensation(value *CompensationDefinition) *CompensationDefinition {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func nextCompensation(state *WorkflowState) string {
	selected := ""
	var sequence uint64
	// completion sequence defines compensation order
	for id, task := range state.Tasks {
		if task.Status != TaskCompleted || task.Definition.Compensation == nil {
			continue
		}
		if selected == "" || task.CompletedSequence > sequence || task.CompletedSequence == sequence && id > selected {
			selected = id
			sequence = task.CompletedSequence
		}
	}
	return selected
}

func addActiveCancellations(plan *eventPlan, at int64, reason string) error {
	for _, id := range sortedTaskIDs(plan.state) {
		task := plan.state.Tasks[id]
		switch task.Status {
		case TaskPending, TaskReady, TaskRunning:
			if err := plan.add(Event{Kind: EventTaskCancelled, WorkflowID: plan.state.ID, TaskID: id, AttemptID: task.AttemptID, Attempt: task.Attempt, WorkerID: task.WorkerID, Fence: task.Fence, At: at, Reason: reason, Compensation: task.Compensation}); err != nil {
				return err
			}
		}
	}
	return nil
}

func addScheduledTimerCancellations(plan *eventPlan, at int64) error {
	ids := make([]string, 0, len(plan.state.Timers))
	for id, timer := range plan.state.Timers {
		if timer.Status == TimerScheduled {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := plan.add(timerEvent(EventTimerCancelled, plan.state.ID, plan.state.Timers[id], at)); err != nil {
			return err
		}
	}
	return nil
}

func addTerminalFailure(plan *eventPlan, taskID, reason string, at int64) error {
	failedTaskID := taskID
	if err := addActiveCancellations(plan, at, reason); err != nil {
		return err
	}
	if err := addScheduledTimerCancellations(plan, at); err != nil {
		return err
	}
	compensationTaskID := nextCompensation(plan.state)
	if compensationTaskID != "" {
		return plan.add(Event{Kind: EventWorkflowCompensating, WorkflowID: plan.state.ID, TaskID: compensationTaskID, At: at, Reason: reason})
	}
	return plan.add(Event{Kind: EventWorkflowFailed, WorkflowID: plan.state.ID, TaskID: failedTaskID, At: at, Error: reason})
}

func retryDelay(policy RetryPolicy, retryCount uint32) int64 {
	delay := policy.InitialBackoffMillis
	for count := uint32(0); count < retryCount && delay > 0 && policy.BackoffMultiplier > 1; count++ {
		multiplier := int64(policy.BackoffMultiplier)
		if delay > math.MaxInt64/multiplier {
			delay = math.MaxInt64
			break
		}
		delay *= multiplier
		if policy.MaxBackoffMillis > 0 && delay >= policy.MaxBackoffMillis {
			delay = policy.MaxBackoffMillis
			break
		}
	}
	if policy.MaxBackoffMillis > 0 && delay > policy.MaxBackoffMillis {
		return policy.MaxBackoffMillis
	}
	return delay
}

func addRetry(plan *eventPlan, taskID string, compensation bool, at int64) (bool, error) {
	task := plan.state.Tasks[taskID]
	if task.RetryCount+1 >= task.Definition.Retry.MaxAttempts {
		return false, nil
	}
	delay := retryDelay(task.Definition.Retry, task.RetryCount)
	if at > math.MaxInt64-delay {
		return false, ErrTimeOverflow
	}
	timer := TimerState{
		ID: TimerID(plan.state.ID, taskID, string(TimerRetry), task.Attempt), TaskID: taskID,
		Purpose: TimerRetry, Deadline: at + delay, AttemptID: task.AttemptID, Attempt: task.Attempt,
		Fence: task.Fence, Status: TimerScheduled,
	}
	if err := plan.add(timerEvent(EventTimerScheduled, plan.state.ID, timer, at)); err != nil {
		return false, err
	}
	if err := plan.add(Event{Kind: EventTaskRetryScheduled, WorkflowID: plan.state.ID, TaskID: taskID, AttemptID: task.AttemptID, Attempt: task.Attempt, At: at, Fence: task.Fence, Timer: &timer, Compensation: compensation}); err != nil {
		return false, err
	}
	return true, nil
}

func validateExpandedTask(parent TaskState, expanded ExpandedTask, workflowID string) bool {
	definition := parent.Definition.Fanout
	if definition == nil || expanded.Key == "" {
		return false
	}
	expected := TaskDefinition{
		ID: FanoutTaskID(workflowID, parent.Definition.ID, expanded.Key), Type: definition.TaskType,
		Dependencies: []string{parent.Definition.ID}, Retry: definition.Retry, TimeoutMillis: definition.TimeoutMillis,
		Compensation: cloneCompensation(definition.Compensation),
	}
	return reflect.DeepEqual(expanded.Definition, expected)
}

func decideFireTimer(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	if state == nil {
		return nil, ErrWorkflowNotFound
	}
	timer, exists := state.Timers[command.TimerID]
	if !exists || timer.Status != TimerScheduled {
		return nil, fmt.Errorf("%w: timer is not scheduled", ErrInvalidTransition)
	}
	if command.At < timer.Deadline {
		return nil, fmt.Errorf("%w: timer deadline has not arrived", ErrInvalidTransition)
	}
	plan := newPlan(state)
	if err := plan.add(timerEvent(EventTimerFired, state.ID, timer, command.At)); err != nil {
		return nil, err
	}
	switch timer.Purpose {
	case TimerWorkflowStart:
		if err := plan.add(Event{Kind: EventWorkflowStarted, WorkflowID: state.ID, At: command.At, Timer: &timer}); err != nil {
			return nil, err
		}
		if err := addReadyTasks(plan, command.At); err != nil {
			return nil, err
		}
	case TimerRetry:
		if err := plan.add(Event{Kind: EventTaskReady, WorkflowID: state.ID, TaskID: timer.TaskID, At: command.At, Timer: &timer}); err != nil {
			return nil, err
		}
	case TimerTimeout:
		task, exists := plan.state.Tasks[timer.TaskID]
		if !exists || task.AttemptID != timer.AttemptID || task.Fence != timer.Fence {
			return nil, ErrFenced
		}
		if task.LeaseUntil < timer.Deadline {
			return nil, fmt.Errorf("%w: lease expiry is due", ErrInvalidTransition)
		}
		if err := plan.add(Event{
			Kind: EventTaskTimedOut, WorkflowID: state.ID, TaskID: timer.TaskID, AttemptID: timer.AttemptID,
			Attempt: timer.Attempt, At: command.At, WorkerID: task.WorkerID, LeaseUntil: task.LeaseUntil,
			Fence: timer.Fence, Idempotency: task.Idempotency, Error: "task timed out", Compensation: task.Compensation,
		}); err != nil {
			return nil, err
		}
		retried, err := addRetry(plan, timer.TaskID, task.Compensation, command.At)
		if err != nil {
			return nil, err
		}
		if !retried {
			if task.Compensation {
				if err := plan.add(Event{Kind: EventWorkflowFailed, WorkflowID: state.ID, TaskID: timer.TaskID, At: command.At, Error: "compensation timed out"}); err != nil {
					return nil, err
				}
			} else if err := addTerminalFailure(plan, timer.TaskID, "task timed out", command.At); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("%w: timer purpose is invalid", ErrInvalidTransition)
	}
	return plan, nil
}

func decideCancel(state *WorkflowState, command Command, newPlan eventPlanFactory) (*eventPlan, error) {
	if state == nil {
		return nil, ErrWorkflowNotFound
	}
	if state.Status != WorkflowScheduled && state.Status != WorkflowRunning {
		return nil, fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	reason := strings.TrimSpace(command.Reason)
	if reason == "" {
		return nil, fmt.Errorf("%w: cancellation reason is required", ErrInvalidCommand)
	}
	plan := newPlan(state)
	if err := plan.add(Event{Kind: EventWorkflowCancellationRequested, WorkflowID: state.ID, At: command.At, Reason: reason}); err != nil {
		return nil, err
	}
	if err := addActiveCancellations(plan, command.At, reason); err != nil {
		return nil, err
	}
	if err := addScheduledTimerCancellations(plan, command.At); err != nil {
		return nil, err
	}
	if taskID := nextCompensation(plan.state); taskID != "" {
		if err := plan.add(Event{Kind: EventWorkflowCompensating, WorkflowID: state.ID, TaskID: taskID, At: command.At, Reason: reason}); err != nil {
			return nil, err
		}
	} else if err := plan.add(Event{Kind: EventWorkflowCancelled, WorkflowID: state.ID, At: command.At, Reason: reason}); err != nil {
		return nil, err
	}
	return plan, nil
}

func applyTimerScheduled(state *WorkflowState, event Event) error {
	if event.Timer == nil || event.Timer.Status != TimerScheduled || event.Timer.ID == "" || event.Timer.Deadline < event.At || !timerEventMatches(event, *event.Timer) {
		return fmt.Errorf("%w: timer is invalid", ErrInvalidTransition)
	}
	timer := *event.Timer
	if _, exists := state.Timers[timer.ID]; exists {
		return fmt.Errorf("%w: timer exists", ErrInvalidTransition)
	}
	switch timer.Purpose {
	case TimerWorkflowStart:
		if state.Status != WorkflowScheduled || timer.TaskID != "" || timer.ID != TimerID(state.ID, "", string(TimerWorkflowStart), 0) {
			return fmt.Errorf("%w: workflow start timer is invalid", ErrInvalidTransition)
		}
	case TimerRetry:
		task, exists := state.Tasks[timer.TaskID]
		delay := retryDelay(task.Definition.Retry, task.RetryCount)
		if !exists || task.Status != TaskFailed || task.FinishedAt > math.MaxInt64-delay || timer.Deadline != task.FinishedAt+delay || timer.ID != TimerID(state.ID, timer.TaskID, string(TimerRetry), task.Attempt) || timer.AttemptID != task.AttemptID || timer.Attempt != task.Attempt || timer.Fence != task.Fence {
			return fmt.Errorf("%w: retry timer is invalid", ErrInvalidTransition)
		}
	case TimerTimeout:
		task, exists := state.Tasks[timer.TaskID]
		leased := exists && (task.Status == TaskRunning || task.Status == TaskCompensating && task.WorkerID != "")
		timeout := task.Definition.TimeoutMillis
		if !leased || timeout <= 0 || task.StartedAt > math.MaxInt64-timeout || timer.Deadline != task.StartedAt+timeout || task.LeaseUntil > timer.Deadline || timer.ID != TimerID(state.ID, timer.TaskID, string(TimerTimeout), task.Attempt) || timer.AttemptID != task.AttemptID || timer.Attempt != task.Attempt || timer.Fence != task.Fence {
			return fmt.Errorf("%w: timeout timer is invalid", ErrInvalidTransition)
		}
	default:
		return fmt.Errorf("%w: timer purpose is invalid", ErrInvalidTransition)
	}
	state.Timers[timer.ID] = timer
	return nil
}

func applyTimerTransition(state *WorkflowState, event Event, status TimerStatus) error {
	if event.Timer == nil || event.Timer.Status != status || !timerEventMatches(event, *event.Timer) {
		return fmt.Errorf("%w: timer transition is invalid", ErrInvalidTransition)
	}
	current, exists := state.Timers[event.Timer.ID]
	if !exists || current.Status != TimerScheduled {
		return fmt.Errorf("%w: timer is not scheduled", ErrInvalidTransition)
	}
	expected := current
	expected.Status = status
	if !reflect.DeepEqual(expected, *event.Timer) {
		return fmt.Errorf("%w: timer identity changed", ErrInvalidTransition)
	}
	if status == TimerFired && event.At < current.Deadline {
		return fmt.Errorf("%w: timer fired early", ErrInvalidTransition)
	}
	state.Timers[current.ID] = expected
	return nil
}

func timerEventMatches(event Event, timer TimerState) bool {
	return event.TaskID == timer.TaskID && event.AttemptID == timer.AttemptID && event.Attempt == timer.Attempt && event.Fence == timer.Fence
}

func applyWorkflowStarted(state *WorkflowState, event Event) error {
	if state.Status != WorkflowScheduled || event.Timer == nil || event.Timer.Purpose != TimerWorkflowStart {
		return fmt.Errorf("%w: workflow cannot start", ErrInvalidTransition)
	}
	timer, exists := state.Timers[event.Timer.ID]
	if !exists || timer.Status != TimerFired {
		return fmt.Errorf("%w: workflow start timer has not fired", ErrInvalidTransition)
	}
	state.Status = WorkflowRunning
	return nil
}

func applyFanoutExpanded(state *WorkflowState, event Event) error {
	if state.Status != WorkflowRunning {
		return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	parent, exists := state.Tasks[event.TaskID]
	if !exists || parent.Status != TaskCompleted || parent.Definition.Fanout == nil || parent.FanoutExpanded || uint32(len(event.ExpandedTasks)) > parent.Definition.Fanout.MaxItems {
		return fmt.Errorf("%w: task %s cannot expand", ErrInvalidTransition, event.TaskID)
	}
	previous := ""
	var children []string
	if len(event.ExpandedTasks) > 0 {
		children = make([]string, len(event.ExpandedTasks))
	}
	for index, expanded := range event.ExpandedTasks {
		if index > 0 && expanded.Key <= previous || !validateExpandedTask(parent, expanded, state.ID) {
			return fmt.Errorf("%w: fanout expansion is not canonical", ErrInvalidTransition)
		}
		previous = expanded.Key
		if _, exists := state.Tasks[expanded.Definition.ID]; exists {
			return fmt.Errorf("%w: fanout task exists", ErrInvalidTransition)
		}
		children[index] = expanded.Definition.ID
		state.Tasks[expanded.Definition.ID] = TaskState{Definition: cloneTaskDefinition(expanded.Definition), Status: TaskPending, Payload: cloneMap(expanded.Payload)}
	}
	aggregateID := parent.Definition.Fanout.AggregateTaskID
	aggregate, exists := state.Tasks[aggregateID]
	if !exists || aggregate.Status != TaskPending || !containsString(aggregate.Definition.Dependencies, event.TaskID) {
		return fmt.Errorf("%w: fanout aggregate is invalid", ErrInvalidTransition)
	}
	aggregate.Definition = cloneTaskDefinition(aggregate.Definition)
	aggregate.Definition.Dependencies = append(aggregate.Definition.Dependencies, children...)
	sort.Strings(aggregate.Definition.Dependencies)
	state.Tasks[aggregateID] = aggregate
	parent.Children = children
	parent.FanoutExpanded = true
	state.Tasks[event.TaskID] = parent
	return nil
}

func applyTaskRetryScheduled(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	if !exists || task.Status != TaskFailed || task.Compensation != event.Compensation || event.Timer == nil {
		return fmt.Errorf("%w: task %s cannot retry", ErrInvalidTransition, event.TaskID)
	}
	timer, exists := state.Timers[event.Timer.ID]
	if !exists || timer.Status != TimerScheduled || timer.Purpose != TimerRetry || timer.TaskID != event.TaskID || event.AttemptID != timer.AttemptID || event.Attempt != timer.Attempt || event.Fence != timer.Fence || task.RetryCount+1 >= task.Definition.Retry.MaxAttempts {
		return fmt.Errorf("%w: task %s retry timer is invalid", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskPending
	task.RetryCount++
	task.AttemptID = ""
	task.Inputs = nil
	task.StartedAt = 0
	task.WorkerID = ""
	task.LeaseUntil = 0
	state.Tasks[event.TaskID] = task
	return nil
}

func applyTaskTimedOut(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	leased := exists && (state.Status == WorkflowRunning && task.Status == TaskRunning && !event.Compensation || state.Status == WorkflowCompensating && task.Status == TaskCompensating && task.WorkerID != "" && event.Compensation)
	if !leased || task.AttemptID != event.AttemptID || task.Attempt != event.Attempt || task.WorkerID != event.WorkerID || task.Fence != event.Fence || task.Idempotency != event.Idempotency {
		return fmt.Errorf("%w: task %s cannot time out", ErrInvalidTransition, event.TaskID)
	}
	timer, exists := state.Timers[TimerID(state.ID, event.TaskID, string(TimerTimeout), event.Attempt)]
	if !exists || timer.Status != TimerFired || event.At < timer.Deadline || timer.Deadline > task.LeaseUntil || timer.AttemptID != event.AttemptID || timer.Fence != event.Fence {
		return fmt.Errorf("%w: task %s timeout timer is invalid", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskFailed
	task.Error = event.Error
	task.FinishedAt = event.At
	task.Compensation = event.Compensation
	state.Tasks[event.TaskID] = task
	return nil
}

func applyTaskCancelled(state *WorkflowState, event Event) error {
	if state.Status != WorkflowScheduled && state.Status != WorkflowRunning {
		return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	task, exists := state.Tasks[event.TaskID]
	if !exists {
		return fmt.Errorf("%w: task %s not found", ErrInvalidTransition, event.TaskID)
	}
	switch task.Status {
	case TaskPending, TaskReady:
	case TaskRunning:
		if task.AttemptID != event.AttemptID || task.WorkerID != event.WorkerID || task.Fence != event.Fence {
			return ErrFenced
		}
	default:
		return fmt.Errorf("%w: task %s cannot cancel", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskCancelled
	task.FinishedAt = event.At
	task.WorkerID = ""
	task.LeaseUntil = 0
	task.Compensation = false
	state.Tasks[event.TaskID] = task
	return nil
}

func applyWorkflowCancellationRequested(state *WorkflowState) error {
	if state.Status != WorkflowScheduled && state.Status != WorkflowRunning {
		return fmt.Errorf("%w: workflow is %s", ErrInvalidTransition, state.Status)
	}
	return nil
}

func applyWorkflowCompensating(state *WorkflowState, event Event) error {
	if state.Status != WorkflowRunning && state.Status != WorkflowCompensating || nextCompensation(state) != event.TaskID {
		return fmt.Errorf("%w: workflow cannot compensate task %s", ErrInvalidTransition, event.TaskID)
	}
	task := state.Tasks[event.TaskID]
	state.Status = WorkflowCompensating
	task.Status = TaskCompensating
	task.RetryCount = 0
	task.Payload = cloneMap(task.Output)
	task.Compensation = true
	task.AttemptID = ""
	task.Inputs = nil
	task.StartedAt = 0
	task.FinishedAt = 0
	task.WorkerID = ""
	task.LeaseUntil = 0
	task.Idempotency = ""
	state.Tasks[event.TaskID] = task
	return nil
}

func applyTaskCompensated(state *WorkflowState, event Event) error {
	task, exists := state.Tasks[event.TaskID]
	if state.Status != WorkflowCompensating || !exists || task.Status != TaskCompensating || task.WorkerID == "" || !task.Compensation || !event.Compensation || task.AttemptID != event.AttemptID || task.Attempt != event.Attempt || task.WorkerID != event.WorkerID || task.Fence != event.Fence || task.Idempotency != event.Idempotency || event.At >= task.LeaseUntil {
		return fmt.Errorf("%w: task %s cannot compensate", ErrInvalidTransition, event.TaskID)
	}
	if timer, exists := scheduledTimer(state, TimerTimeout, event.TaskID, event.AttemptID); exists && event.At >= timer.Deadline {
		return fmt.Errorf("%w: task %s compensation timed out", ErrInvalidTransition, event.TaskID)
	}
	task.Status = TaskCompensated
	task.FinishedAt = event.At
	task.WorkerID = ""
	task.LeaseUntil = 0
	task.Compensation = false
	state.Tasks[event.TaskID] = task
	return nil
}

func applyWorkflowCancelled(state *WorkflowState) error {
	if state.Status != WorkflowScheduled && state.Status != WorkflowRunning || nextCompensation(state) != "" || hasActiveState(state) || hasScheduledTimers(state) {
		return fmt.Errorf("%w: workflow cannot cancel", ErrInvalidTransition)
	}
	state.Status = WorkflowCancelled
	return nil
}

func applyWorkflowCompensated(state *WorkflowState) error {
	if state.Status != WorkflowCompensating || nextCompensation(state) != "" || hasActiveState(state) || hasScheduledTimers(state) {
		return fmt.Errorf("%w: workflow cannot finish compensation", ErrInvalidTransition)
	}
	state.Status = WorkflowCompensated
	return nil
}

func hasActiveState(state *WorkflowState) bool {
	for _, task := range state.Tasks {
		switch task.Status {
		case TaskPending, TaskReady, TaskRunning, TaskCompensating:
			return true
		}
	}
	return false
}

func hasScheduledTimers(state *WorkflowState) bool {
	for _, timer := range state.Timers {
		if timer.Status == TimerScheduled {
			return true
		}
	}
	return false
}
