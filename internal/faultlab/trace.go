package faultlab

import (
	"fmt"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

type Trace struct {
	Commands        []core.Command
	Results         []core.Result
	Expected        core.EngineSnapshot
	ReleaseCommands int
	EventKinds      map[core.EventKind]int
}

func ReleaseTrace() (Trace, error) {
	builder := traceBuilder{engine: core.NewEngine(), kinds: make(map[core.EventKind]int)}
	one := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	retry := core.RetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 3, BackoffMultiplier: 2, MaxBackoffMillis: 12}
	definition := core.WorkflowDefinition{
		Name: "release", Namespace: "fault-lab", StartAt: 5,
		Tasks: []core.TaskDefinition{
			{ID: "build-api", Type: "build", Retry: one},
			{ID: "build-web", Type: "build", Retry: one},
			{
				ID: "discover-tests", Type: "discover", Dependencies: []string{"build-api", "build-web"}, Retry: retry, TimeoutMillis: 10,
				Fanout: &core.FanoutDefinition{TaskType: "test", AggregateTaskID: "aggregate", MaxItems: 2, Retry: retry, TimeoutMillis: 6},
			},
			{ID: "aggregate", Type: "aggregate", Dependencies: []string{"discover-tests"}, Retry: one},
			{ID: "deploy", Type: "deploy", Dependencies: []string{"aggregate"}, Retry: retry, TimeoutMillis: 10, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
			{ID: "verify", Type: "verify", Dependencies: []string{"deploy"}, Retry: one},
		},
	}
	submit := builder.add(core.Command{Kind: core.CommandSubmit, RequestID: "release-submit", At: 0, Definition: &definition})
	workflowID := submit.WorkflowID
	builder.fire(workflowID, "", core.TimerWorkflowStart, 5)
	buildAPI := builder.start(workflowID, "build-api", "build-api-start", "build-worker-a", 6, 50)
	builder.attempt(core.CommandRenew, "build-api-renew", workflowID, "build-api", 7, 60, buildAPI, nil, "")
	builder.attempt(core.CommandComplete, "build-api-complete", workflowID, "build-api", 8, 0, buildAPI, map[string]string{"artifact": "api"}, "")
	buildWebFirst := builder.start(workflowID, "build-web", "build-web-start-1", "build-worker-b", 9, 12)
	builder.attempt(core.CommandExpire, "build-web-expire", workflowID, "build-web", 12, 0, buildWebFirst, nil, "")
	buildWebSecond := builder.start(workflowID, "build-web", "build-web-start-2", "build-worker-c", 13, 50)
	builder.attempt(core.CommandComplete, "build-web-complete", workflowID, "build-web", 14, 0, buildWebSecond, map[string]string{"artifact": "web"}, "")
	discoverFirst := builder.start(workflowID, "discover-tests", "discover-start-1", "discover-worker", 15, 24)
	builder.attempt(core.CommandFail, "discover-fail-1", workflowID, "discover-tests", 16, 0, discoverFirst, nil, "discovery retry")
	builder.fire(workflowID, "discover-tests", core.TimerRetry, 19)
	discoverSecond := builder.start(workflowID, "discover-tests", "discover-start-2", "discover-worker", 20, 29)
	builder.completeFanout(workflowID, "discover-tests", "discover-complete", 21, discoverSecond, []core.FanoutItem{
		{Key: "api", Payload: map[string]string{"suite": "api"}},
		{Key: "web", Payload: map[string]string{"suite": "web"}},
	})
	apiTask := core.FanoutTaskID(workflowID, "discover-tests", "api")
	webTask := core.FanoutTaskID(workflowID, "discover-tests", "web")
	apiTest := builder.start(workflowID, apiTask, "api-test-start", "test-worker-a", 22, 27)
	builder.attempt(core.CommandComplete, "api-test-complete", workflowID, apiTask, 23, 0, apiTest, map[string]string{"result": "pass"}, "")
	builder.start(workflowID, webTask, "web-test-start-1", "test-worker-b", 24, 30)
	builder.fire(workflowID, webTask, core.TimerTimeout, 30)
	builder.fire(workflowID, webTask, core.TimerRetry, 33)
	webTestSecond := builder.start(workflowID, webTask, "web-test-start-2", "test-worker-c", 34, 39)
	builder.attempt(core.CommandComplete, "web-test-complete", workflowID, webTask, 35, 0, webTestSecond, map[string]string{"result": "pass"}, "")
	aggregate := builder.start(workflowID, "aggregate", "aggregate-start", "aggregate-worker", 36, 60)
	builder.attempt(core.CommandComplete, "aggregate-complete", workflowID, "aggregate", 37, 0, aggregate, map[string]string{"result": "pass"}, "")
	deploy := builder.start(workflowID, "deploy", "deploy-start", "deploy-worker", 38, 47)
	builder.attempt(core.CommandComplete, "deploy-complete", workflowID, "deploy", 39, 0, deploy, map[string]string{"release": "deployed"}, "")
	verify := builder.start(workflowID, "verify", "verify-start", "verify-worker", 40, 60)
	builder.attempt(core.CommandFail, "verify-fail", workflowID, "verify", 41, 0, verify, nil, "forced verification failure")
	rollback := builder.start(workflowID, "deploy", "rollback-start", "rollback-worker", 42, 51)
	builder.attempt(core.CommandComplete, "rollback-complete", workflowID, "deploy", 43, 0, rollback, map[string]string{"release": "rolled-back"}, "")
	builder.releaseCommands = len(builder.commands)
	builder.completedWorkflow(one)
	builder.failedWorkflow(one)
	builder.cancelledWorkflow(one)
	if builder.err != nil {
		return Trace{}, builder.err
	}
	return Trace{
		Commands: builder.commands, Results: builder.results, Expected: builder.engine.Snapshot(),
		ReleaseCommands: builder.releaseCommands, EventKinds: builder.kinds,
	}, nil
}

func RequiredEventKinds() []core.EventKind {
	return []core.EventKind{
		core.EventWorkflowSubmitted,
		core.EventTaskReady,
		core.EventTaskStarted,
		core.EventTaskCompleted,
		core.EventTaskFailed,
		core.EventWorkflowCompleted,
		core.EventWorkflowFailed,
		core.EventLeaseRenewed,
		core.EventLeaseExpired,
		core.EventWorkflowStarted,
		core.EventFanoutExpanded,
		core.EventTimerScheduled,
		core.EventTimerFired,
		core.EventTimerCancelled,
		core.EventTaskRetryScheduled,
		core.EventTaskTimedOut,
		core.EventTaskCancelled,
		core.EventWorkflowCancellationRequested,
		core.EventWorkflowCompensating,
		core.EventTaskCompensated,
		core.EventWorkflowCancelled,
		core.EventWorkflowCompensated,
	}
}

type traceBuilder struct {
	engine          *core.Engine
	commands        []core.Command
	results         []core.Result
	kinds           map[core.EventKind]int
	releaseCommands int
	err             error
}

func (b *traceBuilder) add(command core.Command) core.Result {
	if b.err != nil {
		return core.Result{}
	}
	result, err := b.engine.Handle(command)
	if err != nil {
		b.err = fmt.Errorf("%s: %w", command.RequestID, err)
		return core.Result{}
	}
	b.commands = append(b.commands, command)
	b.results = append(b.results, result)
	for _, event := range result.Events {
		b.kinds[event.Kind]++
	}
	return result
}

func (b *traceBuilder) start(workflowID, taskID, requestID, workerID string, at, leaseUntil int64) core.Result {
	return b.add(core.Command{
		Kind: core.CommandStart, RequestID: requestID, WorkflowID: workflowID, TaskID: taskID,
		WorkerID: workerID, At: at, LeaseUntil: leaseUntil,
	})
}

func (b *traceBuilder) attempt(kind core.CommandKind, requestID, workflowID, taskID string, at, leaseUntil int64, started core.Result, output map[string]string, message string) core.Result {
	if b.err != nil {
		return core.Result{}
	}
	event, ok := eventOfKind(started, core.EventTaskStarted)
	if !ok {
		b.err = fmt.Errorf("%s has no started event", requestID)
		return core.Result{}
	}
	return b.add(core.Command{
		Kind: kind, RequestID: requestID, WorkflowID: workflowID, TaskID: taskID,
		AttemptID: event.AttemptID, WorkerID: event.WorkerID, Fence: event.Fence,
		At: at, LeaseUntil: leaseUntil, Output: output, Error: message,
	})
}

func (b *traceBuilder) completeFanout(workflowID, taskID, requestID string, at int64, started core.Result, fanout []core.FanoutItem) core.Result {
	if b.err != nil {
		return core.Result{}
	}
	event, ok := eventOfKind(started, core.EventTaskStarted)
	if !ok {
		b.err = fmt.Errorf("%s has no started event", requestID)
		return core.Result{}
	}
	return b.add(core.Command{
		Kind: core.CommandComplete, RequestID: requestID, WorkflowID: workflowID, TaskID: taskID,
		AttemptID: event.AttemptID, WorkerID: event.WorkerID, Fence: event.Fence, At: at,
		Output: map[string]string{"discovered": "2"}, Fanout: fanout,
	})
}

func (b *traceBuilder) fire(workflowID, taskID string, purpose core.TimerPurpose, at int64) core.Result {
	if b.err != nil {
		return core.Result{}
	}
	state, exists := b.engine.State(workflowID)
	if !exists {
		b.err = fmt.Errorf("workflow %s is missing", workflowID)
		return core.Result{}
	}
	selected := core.TimerState{}
	for _, timer := range state.Timers {
		if timer.TaskID == taskID && timer.Purpose == purpose && timer.Status == core.TimerScheduled {
			if selected.ID == "" || timer.Deadline < selected.Deadline || timer.Deadline == selected.Deadline && timer.ID < selected.ID {
				selected = timer
			}
		}
	}
	if selected.ID == "" {
		b.err = fmt.Errorf("timer %s/%s is missing", taskID, purpose)
		return core.Result{}
	}
	return b.add(core.Command{
		Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(selected.ID), WorkflowID: workflowID,
		TimerID: selected.ID, At: at,
	})
}

func (b *traceBuilder) completedWorkflow(retry core.RetryPolicy) {
	definition := core.WorkflowDefinition{Name: "completed", Namespace: "fault-lab", Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: retry}}}
	submit := b.add(core.Command{Kind: core.CommandSubmit, RequestID: "completed-submit", At: 100, Definition: &definition})
	start := b.start(submit.WorkflowID, "task", "completed-start", "worker", 101, 110)
	b.attempt(core.CommandComplete, "completed-complete", submit.WorkflowID, "task", 102, 0, start, map[string]string{"result": "done"}, "")
}

func (b *traceBuilder) failedWorkflow(retry core.RetryPolicy) {
	definition := core.WorkflowDefinition{Name: "failed", Namespace: "fault-lab", Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: retry}}}
	submit := b.add(core.Command{Kind: core.CommandSubmit, RequestID: "failed-submit", At: 200, Definition: &definition})
	start := b.start(submit.WorkflowID, "task", "failed-start", "worker", 201, 210)
	b.attempt(core.CommandFail, "failed-fail", submit.WorkflowID, "task", 202, 0, start, nil, "terminal failure")
}

func (b *traceBuilder) cancelledWorkflow(retry core.RetryPolicy) {
	definition := core.WorkflowDefinition{Name: "cancelled", Namespace: "fault-lab", StartAt: 310, Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: retry}}}
	submit := b.add(core.Command{Kind: core.CommandSubmit, RequestID: "cancelled-submit", At: 300, Definition: &definition})
	b.add(core.Command{Kind: core.CommandCancel, RequestID: "cancelled-cancel", WorkflowID: submit.WorkflowID, At: 301, Reason: "fault lab"})
}

func eventOfKind(result core.Result, kind core.EventKind) (core.Event, bool) {
	for _, event := range result.Events {
		if event.Kind == kind {
			return event, true
		}
	}
	return core.Event{}, false
}
