package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	chronosv1 "github.com/HaikIsaiants/chronos-public/api/chronos/v1"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	"go.opentelemetry.io/otel/trace"
)

func TestStopSessionWithFullOutboundQueue(t *testing.T) {
	stopped := errors.New("stopped")
	current := &session{
		workerID: "worker", outbound: make(chan delivery, 1), stopped: make(chan error, 1),
	}
	current.outbound <- delivery{message: &chronosv1.CoordinatorMessage{}}
	manager := &Manager{sessions: map[string]*session{"worker": current}}
	manager.stopSession(current, stopped)
	if manager.sessions["worker"] != nil {
		t.Fatal("stopped session remained registered")
	}
	select {
	case err := <-current.stopped:
		if !errors.Is(err, stopped) {
			t.Fatal(err)
		}
	default:
		t.Fatal("full outbound queue hid session stop")
	}
}

func TestWorkerMessagePreservesTraceContext(t *testing.T) {
	engine := core.NewEngine()
	definition := core.WorkflowDefinition{
		Name: "trace", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	submitted, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID,
		TaskID: "task", WorkerID: "worker", LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submitted.WorkflowID)
	task := state.Tasks["task"]
	schedulerValue, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	captured := make(chan trace.SpanContext, 1)
	manager, err := New(Config{
		Scheduler: schedulerValue,
		View: func(context.Context) (View, error) {
			return View{Leader: true, LeaderID: 1, Engine: engine.Clone()}, nil
		},
		Submit: func(ctx context.Context, command core.Command) (core.Result, error) {
			captured <- trace.SpanContextFromContext(ctx)
			return engine.Handle(command)
		},
		TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.sessions["worker"] = &session{
		id: 1, workerID: "worker", capabilities: []string{"work"}, capability: map[string]struct{}{"work": {}},
		inflight: map[string]struct{}{task.AttemptID: {}}, outbound: make(chan delivery, 1), stopped: make(chan error, 1),
	}
	runContext, cancel := context.WithCancel(context.Background())
	go manager.Run(runContext)
	defer func() {
		cancel()
		<-manager.done
	}()
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	requestContext := trace.ContextWithSpanContext(context.Background(), spanContext)
	response := make(chan error, 1)
	manager.messages <- messageRequest{
		ctx: requestContext, sessionID: 1, workerID: "worker", response: response,
		message: &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Completion{Completion: &chronosv1.TaskCompletion{
			RequestId: "complete", WorkflowId: submitted.WorkflowID, TaskId: "task",
			AttemptId: task.AttemptID, FencingToken: task.Fence, Output: map[string]string{"value": "ok"},
		}}},
	}
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	if actual := <-captured; actual.TraceID() != spanContext.TraceID() || actual.SpanID() != spanContext.SpanID() {
		t.Fatalf("trace context differs: %s %s", actual.TraceID(), actual.SpanID())
	}
}
