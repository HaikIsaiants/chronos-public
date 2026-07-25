package core_test

import (
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestScheduledWorkflowStart(t *testing.T) {
	definition := singleDefinition()
	definition.StartAt = 10
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", At: 5, Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Status != core.WorkflowScheduled || state.Tasks["task"].Status != core.TaskPending {
		t.Fatalf("unexpected scheduled state: %+v", state)
	}
	timer := timerFor(t, state, core.TimerWorkflowStart)
	history := engine.Journal()
	partial, err := core.Replay(history[:1])
	if err != nil {
		t.Fatal(err)
	}
	partialState, _ := partial.State(state.ID)
	tampered := history[1]
	tampered.TaskID = "wrong"
	if _, err := core.Apply(partialState, tampered); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("mismatched timer envelope accepted: %v", err)
	}
	if _, err := engine.Handle(core.Command{Kind: core.CommandFireTimer, RequestID: "early", WorkflowID: state.ID, TimerID: timer.ID, At: 9}); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("early timer accepted: %v", err)
	}
	if _, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "early-start", WorkflowID: state.ID, TaskID: "task", At: 9})); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("scheduled task started early: %v", err)
	}
	result, err := engine.Handle(core.Command{Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: state.ID, TimerID: timer.ID, At: 10})
	if err != nil {
		t.Fatal(err)
	}
	if kinds(result.Events)[0] != core.EventTimerFired || kinds(result.Events)[1] != core.EventWorkflowStarted {
		t.Fatalf("unexpected start events: %v", kinds(result.Events))
	}
	state, _ = engine.State(state.ID)
	if state.Status != core.WorkflowRunning || state.Tasks["task"].Status != core.TaskReady || state.Timers[timer.ID].Status != core.TimerFired {
		t.Fatalf("workflow did not start: %+v", state)
	}
}

func TestRetryBackoffAndExhaustion(t *testing.T) {
	definition := singleDefinition()
	definition.Tasks[0].Retry = core.RetryPolicy{MaxAttempts: 3, InitialBackoffMillis: 10, BackoffMultiplier: 2, MaxBackoffMillis: 15}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-1", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-1", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Error: "one"}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	first := timerFor(t, state, core.TimerRetry)
	if first.Deadline != 12 || state.Tasks["task"].RetryCount != 1 || state.Tasks["task"].Status != core.TaskPending {
		t.Fatalf("unexpected first retry: %+v %+v", first, state.Tasks["task"])
	}
	fireTimer(t, engine, state.ID, first, 12)
	start, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-2", WorkflowID: state.ID, TaskID: "task", At: 13}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-2", WorkflowID: state.ID, TaskID: "task", At: 14, Error: "two"}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	second := latestTimerFor(t, state, core.TimerRetry)
	if second.Deadline != 29 || state.Tasks["task"].RetryCount != 2 {
		t.Fatalf("unexpected second retry: %+v %+v", second, state.Tasks["task"])
	}
	fireTimer(t, engine, state.ID, second, 29)
	start, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-3", WorkflowID: state.ID, TaskID: "task", At: 30}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-3", WorkflowID: state.ID, TaskID: "task", At: 31, Error: "three"}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	if state.Status != core.WorkflowFailed || state.Tasks["task"].Status != core.TaskFailed {
		t.Fatalf("retry exhaustion did not fail: %+v", state)
	}
}

func TestTimeoutAndCompletionRace(t *testing.T) {
	newEngine := func(request string) (*core.Engine, string, core.Result, core.TimerState) {
		definition := singleDefinition()
		definition.Tasks[0].TimeoutMillis = 100
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: request, Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		start, err := engine.Handle(core.Command{Kind: core.CommandStart, RequestID: request + "-start", WorkflowID: submit.WorkflowID, TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 101})
		if err != nil {
			t.Fatal(err)
		}
		state, _ := engine.State(submit.WorkflowID)
		return engine, submit.WorkflowID, start, timerFor(t, state, core.TimerTimeout)
	}
	completed, workflowID, start, timer := newEngine("complete")
	if _, err := completed.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-result", WorkflowID: workflowID, TaskID: "task", At: 100}, start)); err != nil {
		t.Fatal(err)
	}
	if _, err := completed.Handle(core.Command{Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: workflowID, TimerID: timer.ID, At: 101}); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("cancelled timeout fired: %v", err)
	}
	timedOut, workflowID, start, timer := newEngine("timeout")
	if _, err := timedOut.Handle(fenced(core.Command{Kind: core.CommandExpire, RequestID: "expire-at-timeout", WorkflowID: workflowID, TaskID: "task", At: 101}, start)); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("lease expiry beat timeout: %v", err)
	}
	result := fireTimer(t, timedOut, workflowID, timer, 101)
	if !containsKind(result.Events, core.EventTaskTimedOut) {
		t.Fatalf("timeout event missing: %v", kinds(result.Events))
	}
	state, _ := timedOut.State(workflowID)
	if state.Status != core.WorkflowFailed || state.Tasks["task"].Status != core.TaskFailed {
		t.Fatalf("timeout did not fail workflow: %+v", state)
	}
	if _, err := timedOut.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "late", WorkflowID: workflowID, TaskID: "task", At: 101}, start)); !errors.Is(err, core.ErrFenced) {
		t.Fatalf("late completion was not fenced: %v", err)
	}
}

func TestTimeoutAndLeaseDeadlineOrdering(t *testing.T) {
	tests := []struct {
		name            string
		leaseUntil      int64
		timeoutDeadline int64
		winner          core.EventKind
	}{
		{name: "lease earlier", leaseUntil: 50, timeoutDeadline: 100, winner: core.EventLeaseExpired},
		{name: "equal", leaseUntil: 100, timeoutDeadline: 100, winner: core.EventTaskTimedOut},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, workflowID, task, timer := deadlineEngine(t, test.name, test.leaseUntil, test.timeoutDeadline)
			at := max(test.leaseUntil, test.timeoutDeadline) + 1
			expire := core.Command{
				Kind: core.CommandExpire, RequestID: "expire-" + test.name, WorkflowID: workflowID, TaskID: "task",
				AttemptID: task.AttemptID, WorkerID: task.WorkerID, Fence: task.Fence, At: at,
			}
			fire := core.Command{
				Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: workflowID,
				TimerID: timer.ID, At: at,
			}
			loser := fire
			winner := expire
			if test.winner == core.EventTaskTimedOut {
				loser = expire
				winner = fire
			}
			if _, err := engine.Handle(loser); !errors.Is(err, core.ErrInvalidTransition) {
				t.Fatalf("losing deadline was accepted: %v", err)
			}
			result, err := engine.Handle(winner)
			if err != nil {
				t.Fatal(err)
			}
			if !containsKind(result.Events, test.winner) {
				t.Fatalf("winner %s missing: %v", test.winner, kinds(result.Events))
			}
		})
	}
}

func TestReplayRejectsTamperedExecutionDeadlines(t *testing.T) {
	t.Run("lease exceeds timeout", func(t *testing.T) {
		definition := singleDefinition()
		definition.Tasks[0].TimeoutMillis = 100
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "lease-submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Handle(core.Command{Kind: core.CommandStart, RequestID: "lease-start", WorkflowID: submit.WorkflowID, TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 50}); err != nil {
			t.Fatal(err)
		}
		history := engine.Journal()
		for index := range history {
			if history[index].Kind == core.EventTaskStarted {
				history[index].LeaseUntil = 102
			}
		}
		if _, err := core.Replay(history); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("oversized lease was accepted: %v", err)
		}
	})
	t.Run("timeout deadline", func(t *testing.T) {
		definition := singleDefinition()
		definition.Tasks[0].TimeoutMillis = 100
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "timeout-submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Handle(core.Command{Kind: core.CommandStart, RequestID: "timeout-start", WorkflowID: submit.WorkflowID, TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 50}); err != nil {
			t.Fatal(err)
		}
		history := engine.Journal()
		for index := range history {
			if history[index].Kind == core.EventTimerScheduled && history[index].Timer.Purpose == core.TimerTimeout {
				history[index].Timer.Deadline++
			}
		}
		if _, err := core.Replay(history); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("altered timeout deadline was accepted: %v", err)
		}
	})
	t.Run("retry deadline", func(t *testing.T) {
		definition := singleDefinition()
		definition.Tasks[0].Retry = core.RetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 10, BackoffMultiplier: 1}
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "retry-submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "retry-start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "retry-fail", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Error: "failed"}, start)); err != nil {
			t.Fatal(err)
		}
		history := engine.Journal()
		for index := range history {
			if history[index].Kind == core.EventTimerScheduled && history[index].Timer.Purpose == core.TimerRetry {
				history[index].Timer.Deadline++
			}
		}
		if _, err := core.Replay(history); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("altered retry deadline was accepted: %v", err)
		}
	})
	t.Run("timeout overflow", func(t *testing.T) {
		definition := singleDefinition()
		definition.Tasks[0].TimeoutMillis = math.MaxInt64
		engine := core.NewEngine()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "overflow-submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		state, _ := engine.State(submit.WorkflowID)
		start, err := engine.Handle(core.Command{Kind: core.CommandStart, RequestID: "overflow-start", WorkflowID: submit.WorkflowID, TaskID: "task", WorkerID: "worker", LeaseUntil: 1})
		if err != nil {
			t.Fatal(err)
		}
		event := start.Events[0]
		event.At = 1
		event.LeaseUntil = 2
		if _, err := core.Apply(state, event); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("overflowing timeout was accepted: %v", err)
		}
	})
}

func TestCancellationIsExplicit(t *testing.T) {
	definition := singleDefinition()
	definition.Tasks[0].TimeoutMillis = 1000
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.Handle(core.Command{Kind: core.CommandCancel, RequestID: core.CancelRequestID(submit.WorkflowID), WorkflowID: submit.WorkflowID, At: 2, Reason: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []core.EventKind{core.EventWorkflowCancellationRequested, core.EventTaskCancelled, core.EventTimerCancelled, core.EventWorkflowCancelled} {
		if !containsKind(result.Events, kind) {
			t.Fatalf("missing cancellation event %s: %v", kind, kinds(result.Events))
		}
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Status != core.WorkflowCancelled || state.Tasks["task"].Status != core.TaskCancelled {
		t.Fatalf("unexpected cancellation state: %+v", state)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "late", WorkflowID: state.ID, TaskID: "task", At: 3}, start)); !errors.Is(err, core.ErrFenced) {
		t.Fatalf("cancelled completion accepted: %v", err)
	}
}

func TestFanoutAggregation(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{Name: "fanout", Namespace: "test", Tasks: []core.TaskDefinition{
		{ID: "discover", Type: "discover", Retry: retry, Fanout: &core.FanoutDefinition{TaskType: "test", AggregateTaskID: "aggregate", MaxItems: 3, Retry: retry}},
		{ID: "aggregate", Type: "aggregate", Dependencies: []string{"discover"}, Retry: retry},
	}}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-discover", WorkflowID: submit.WorkflowID, TaskID: "discover", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	items := []core.FanoutItem{{Key: "web", Payload: map[string]string{"suite": "web"}}, {Key: "api", Payload: map[string]string{"suite": "api"}}}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-discover", WorkflowID: submit.WorkflowID, TaskID: "discover", At: 2, Output: map[string]string{"manifest": "ok"}, Fanout: items}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	apiID := core.FanoutTaskID(state.ID, "discover", "api")
	webID := core.FanoutTaskID(state.ID, "discover", "web")
	if state.Tasks[apiID].Payload["suite"] != "api" || state.Tasks[webID].Payload["suite"] != "web" || state.Tasks["aggregate"].Status != core.TaskPending {
		t.Fatalf("fanout state is wrong: %+v", state)
	}
	expectedDependencies := []string{"discover", apiID, webID}
	sort.Strings(expectedDependencies)
	if !reflect.DeepEqual(state.Tasks["aggregate"].Definition.Dependencies, expectedDependencies) {
		t.Fatalf("aggregate dependencies are wrong: %v", state.Tasks["aggregate"].Definition.Dependencies)
	}
	for index, id := range []string{apiID, webID} {
		at := int64(index*2 + 3)
		childStart, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-" + id, WorkflowID: state.ID, TaskID: id, At: at}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-" + id, WorkflowID: state.ID, TaskID: id, At: at + 1, Output: map[string]string{"result": id}}, childStart)); err != nil {
			t.Fatal(err)
		}
	}
	state, _ = engine.State(state.ID)
	if state.Tasks["aggregate"].Status != core.TaskReady {
		t.Fatalf("aggregate was not readied: %+v", state.Tasks["aggregate"])
	}
	aggregateStart, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-aggregate", WorkflowID: state.ID, TaskID: "aggregate", At: 7}))
	if err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	if len(state.Tasks["aggregate"].Inputs) != 3 {
		t.Fatalf("aggregate inputs are incomplete: %v", state.Tasks["aggregate"].Inputs)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-aggregate", WorkflowID: state.ID, TaskID: "aggregate", At: 8}, aggregateStart)); err != nil {
		t.Fatal(err)
	}
	assertReplaySnapshotHash(t, engine, state.ID)
}

func TestZeroItemFanoutSnapshotRoundTrip(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{Name: "empty-fanout", Namespace: "test", Tasks: []core.TaskDefinition{
		{ID: "discover", Type: "discover", Retry: retry, Fanout: &core.FanoutDefinition{TaskType: "test", AggregateTaskID: "aggregate", MaxItems: 2, Retry: retry}},
		{ID: "aggregate", Type: "aggregate", Dependencies: []string{"discover"}, Retry: retry},
	}}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "discover", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "discover", At: 2}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Tasks["discover"].Children != nil || state.Tasks["aggregate"].Status != core.TaskReady {
		t.Fatalf("zero fanout was not normalized: %+v", state)
	}
	assertReplaySnapshotHash(t, engine, state.ID)
}

func TestReverseCompensationWithRetry(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 5, BackoffMultiplier: 2, MaxBackoffMillis: 10}
	one := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{Name: "compensation", Namespace: "test", Tasks: []core.TaskDefinition{
		{ID: "a", Type: "work", Retry: retry, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
		{ID: "b", Type: "work", Dependencies: []string{"a"}, Retry: one, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
		{ID: "verify", Type: "verify", Dependencies: []string{"b"}, Retry: one},
	}}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	at := int64(1)
	for _, id := range []string{"a", "b"} {
		start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-" + id, WorkflowID: submit.WorkflowID, TaskID: id, At: at}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-" + id, WorkflowID: submit.WorkflowID, TaskID: id, At: at + 1, Output: map[string]string{"value": id}}, start)); err != nil {
			t.Fatal(err)
		}
		at += 2
	}
	verify, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-verify", WorkflowID: submit.WorkflowID, TaskID: "verify", At: at}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-verify", WorkflowID: submit.WorkflowID, TaskID: "verify", At: at + 1, Error: "verification"}, verify)); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Status != core.WorkflowCompensating || state.Tasks["b"].Status != core.TaskCompensating || state.Tasks["a"].Status != core.TaskCompleted {
		t.Fatalf("reverse compensation did not start with b: %+v", state)
	}
	bStart, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "compensate-b", WorkflowID: state.ID, TaskID: "b", At: 7}))
	if err != nil {
		t.Fatal(err)
	}
	if bStart.Events[0].Idempotency != core.CompensationIdempotencyKey(state.ID, "b") || !bStart.Events[0].Compensation {
		t.Fatalf("compensation identity is wrong: %+v", bStart.Events[0])
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "compensated-b", WorkflowID: state.ID, TaskID: "b", At: 8}, bStart)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	if state.Tasks["b"].Status != core.TaskCompensated || state.Tasks["a"].Status != core.TaskCompensating || state.Tasks["a"].Payload["value"] != "a" {
		t.Fatalf("compensation did not advance to a: %+v", state)
	}
	aStart, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "compensate-a-1", WorkflowID: state.ID, TaskID: "a", At: 9}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-compensation-a", WorkflowID: state.ID, TaskID: "a", At: 10, Error: "rollback retry"}, aStart)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	retryTimer := timerFor(t, state, core.TimerRetry)
	if retryTimer.Deadline != 15 || !state.Tasks["a"].Compensation || state.Tasks["a"].Status != core.TaskPending {
		t.Fatalf("compensation retry is wrong: %+v %+v", retryTimer, state.Tasks["a"])
	}
	fireTimer(t, engine, state.ID, retryTimer, 15)
	aStart, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "compensate-a-2", WorkflowID: state.ID, TaskID: "a", At: 16}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "compensated-a", WorkflowID: state.ID, TaskID: "a", At: 17}, aStart)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	if state.Status != core.WorkflowCompensated || state.Tasks["a"].Status != core.TaskCompensated || state.Tasks["b"].Status != core.TaskCompensated {
		t.Fatalf("workflow did not compensate: %+v", state)
	}
	assertReplaySnapshotHash(t, engine, state.ID)
}

func TestRetryTimerIdentityAcrossForwardAndCompensation(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 1, BackoffMultiplier: 1}
	one := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{Name: "retry-phases", Namespace: "test", Tasks: []core.TaskDefinition{
		{ID: "task", Type: "work", Retry: retry, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
		{ID: "verify", Type: "verify", Dependencies: []string{"task"}, Retry: one},
	}}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-task-1", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-task-1", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Error: "retry"}, start)); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	forwardTimer := timerFor(t, state, core.TimerRetry)
	fireTimer(t, engine, state.ID, forwardTimer, 3)
	start, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-task-2", WorkflowID: state.ID, TaskID: "task", At: 4}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-task", WorkflowID: state.ID, TaskID: "task", At: 5}, start)); err != nil {
		t.Fatal(err)
	}
	verify, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-verify", WorkflowID: state.ID, TaskID: "verify", At: 6}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-verify", WorkflowID: state.ID, TaskID: "verify", At: 7, Error: "verification"}, verify)); err != nil {
		t.Fatal(err)
	}
	compensation, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "compensate-task-1", WorkflowID: state.ID, TaskID: "task", At: 8}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-compensation", WorkflowID: state.ID, TaskID: "task", At: 9, Error: "retry rollback"}, compensation)); err != nil {
		t.Fatal(err)
	}
	state, _ = engine.State(state.ID)
	compensationTimer := timerFor(t, state, core.TimerRetry)
	if compensationTimer.ID == forwardTimer.ID || state.Timers[forwardTimer.ID].Status != core.TimerFired || compensationTimer.Attempt == forwardTimer.Attempt {
		t.Fatalf("retry timers collided: %+v %+v", forwardTimer, compensationTimer)
	}
}

func timerFor(t *testing.T, state *core.WorkflowState, purpose core.TimerPurpose) core.TimerState {
	t.Helper()
	for _, timer := range state.Timers {
		if timer.Purpose == purpose && timer.Status == core.TimerScheduled {
			return timer
		}
	}
	t.Fatalf("scheduled timer %s not found", purpose)
	return core.TimerState{}
}

func deadlineEngine(t *testing.T, requestID string, leaseUntil, timeoutDeadline int64) (*core.Engine, string, core.TaskState, core.TimerState) {
	t.Helper()
	definition := singleDefinition()
	definition.Tasks[0].TimeoutMillis = timeoutDeadline
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: requestID, Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: requestID + "-start", WorkflowID: submit.WorkflowID,
		TaskID: "task", WorkerID: "worker", LeaseUntil: leaseUntil,
	}); err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	return engine, submit.WorkflowID, state.Tasks["task"], timerFor(t, state, core.TimerTimeout)
}

func latestTimerFor(t *testing.T, state *core.WorkflowState, purpose core.TimerPurpose) core.TimerState {
	t.Helper()
	var selected core.TimerState
	for _, timer := range state.Timers {
		if timer.Purpose == purpose && timer.Status == core.TimerScheduled && timer.Deadline >= selected.Deadline {
			selected = timer
		}
	}
	if selected.ID == "" {
		t.Fatalf("scheduled timer %s not found", purpose)
	}
	return selected
}

func fireTimer(t *testing.T, engine *core.Engine, workflowID string, timer core.TimerState, at int64) core.Result {
	t.Helper()
	result, err := engine.Handle(core.Command{Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: workflowID, TimerID: timer.ID, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func kinds(events []core.Event) []core.EventKind {
	result := make([]core.EventKind, len(events))
	for index, event := range events {
		result[index] = event.Kind
	}
	return result
}

func containsKind(events []core.Event, kind core.EventKind) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

func assertReplaySnapshotHash(t *testing.T, engine *core.Engine, workflowID string) {
	t.Helper()
	state, _ := engine.State(workflowID)
	live, err := core.StateHash(state)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := core.Replay(engine.Journal())
	if err != nil {
		t.Fatal(err)
	}
	replayState, _ := replayed.State(workflowID)
	replay, _ := core.StateHash(replayState)
	restored, err := core.Restore(engine.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restoredState, _ := restored.State(workflowID)
	restore, _ := core.StateHash(restoredState)
	if live != replay || live != restore {
		t.Fatalf("state hashes differ: %s %s %s", live, replay, restore)
	}
}
