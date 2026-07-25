package execution_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chronosv1 "github.com/HaikIsaiants/chronos/api/chronos/v1"
	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/execution"
	"github.com/HaikIsaiants/chronos/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type backend struct {
	mu     sync.Mutex
	engine *core.Engine
}

func (b *backend) view(context.Context) (execution.View, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return execution.View{Leader: true, LeaderID: 1, Engine: b.engine.Clone()}, nil
}

func (b *backend) submit(_ context.Context, command core.Command) (core.Result, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.engine.Handle(command)
}

func (b *backend) state(workflowID string) *core.WorkflowState {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, _ := b.engine.State(workflowID)
	return state
}

func (b *backend) journal() []core.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.engine.Journal()
}

func TestRealBidirectionalCreditsAndCommitOrdering(t *testing.T) {
	backend := newBackend(t, "one", "two")
	var now atomic.Int64
	manager, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	_ = manager
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	first := receiveAssignment(t, stream)
	state := backend.state(first.GetWorkflowId())
	task := state.Tasks[first.GetTaskId()]
	if task.Status != core.TaskRunning || task.AttemptID != first.GetAttemptId() || task.Fence != first.GetFencingToken() {
		t.Fatalf("assignment preceded commit: %+v %+v", first, task)
	}
	if running(backend) != 1 {
		t.Fatal("worker credit was exceeded")
	}
	if err := stream.Send(completion(first)); err != nil {
		t.Fatal(err)
	}
	ack := receiveAck(t, stream)
	if !ack.GetAccepted() || ack.GetDuplicate() {
		t.Fatalf("completion was not accepted: %+v", ack)
	}
	second := receiveAssignment(t, stream)
	if second.GetAttemptId() == first.GetAttemptId() || running(backend) != 1 {
		t.Fatalf("credit was not conserved: %+v %+v", first, second)
	}
}

func TestProtocolCapabilitiesAndReconnect(t *testing.T) {
	backend := newBackend(t, "one")
	var now atomic.Int64
	_, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	invalid, err := client.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := invalid.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Credit{
		Credit: &chronosv1.CreditGrant{Credits: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := invalid.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("non-hello first message returned %v", err)
	}
	incompatible := openWorkerCapabilities(t, ctx, client, "incompatible", 1, "other")
	if err := incompatible.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Heartbeat{
		Heartbeat: &chronosv1.LeaseHeartbeat{RequestId: "barrier", WorkflowId: "missing"},
	}}); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, incompatible); ack.GetAccepted() {
		t.Fatalf("invalid heartbeat was accepted: %+v", ack)
	}
	first := openWorker(t, ctx, client, "worker", 1)
	assignment := receiveAssignment(t, first)
	replacement := openWorker(t, ctx, client, "worker", 1)
	redelivery := receiveAssignment(t, replacement)
	if redelivery.GetAttemptId() != assignment.GetAttemptId() ||
		redelivery.GetFencingToken() != assignment.GetFencingToken() ||
		redelivery.GetIdempotencyKey() != assignment.GetIdempotencyKey() {
		t.Fatalf("active lease changed on reconnect: %+v %+v", assignment, redelivery)
	}
	if _, err := first.Recv(); status.Code(err) != codes.Aborted {
		t.Fatalf("replaced session returned %v", err)
	}
}

func TestWorkerDeathExpiryReassignmentAndStaleFence(t *testing.T) {
	backend := newBackend(t, "one")
	var now atomic.Int64
	_, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstStream := openWorker(t, ctx, client, "worker-a", 1)
	first := receiveAssignment(t, firstStream)
	if err := firstStream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := firstStream.Recv(); err != io.EOF {
		t.Fatalf("worker stream did not close cleanly: %v", err)
	}
	now.Store(first.GetLeaseExpiresAt())
	secondStream := openWorker(t, ctx, client, "worker-b", 1)
	second := receiveAssignment(t, secondStream)
	if second.GetFencingToken() <= first.GetFencingToken() || second.GetAttemptId() == first.GetAttemptId() ||
		second.GetIdempotencyKey() != first.GetIdempotencyKey() {
		t.Fatalf("invalid reassignment: %+v %+v", first, second)
	}
	staleStream := openWorker(t, ctx, client, "worker-a", 1)
	if err := staleStream.Send(completion(first)); err != nil {
		t.Fatal(err)
	}
	ack := receiveAck(t, staleStream)
	if ack.GetAccepted() || ack.GetCode() != "fenced" {
		t.Fatalf("stale completion was not fenced: %+v", ack)
	}
	if err := secondStream.Send(completion(second)); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, secondStream); !ack.GetAccepted() {
		t.Fatalf("current completion failed: %+v", ack)
	}
}

func TestLostCompletionAcknowledgementRetriesAsDuplicate(t *testing.T) {
	backend := newBackend(t, "one")
	var now atomic.Int64
	var failed atomic.Bool
	failpoint := func(kind string) error {
		if kind == "completion" && failed.CompareAndSwap(false, true) {
			return errors.New("lost acknowledgement")
		}
		return nil
	}
	_, client, cleanup := startManager(t, backend, &now, failpoint)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	assignment := receiveAssignment(t, stream)
	message := completion(assignment)
	if err := stream.Send(message); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) == 0 {
		t.Fatalf("completion acknowledgement was not lost: %v", err)
	}
	now.Store(1000)
	retry := openWorker(t, ctx, client, "worker", 1)
	if err := retry.Send(message); err != nil {
		t.Fatal(err)
	}
	ack := receiveAck(t, retry)
	if !ack.GetAccepted() || !ack.GetDuplicate() {
		t.Fatalf("retry was not deduplicated: %+v", ack)
	}
	completions := 0
	for _, event := range backend.journal() {
		if event.Kind == core.EventTaskCompleted {
			completions++
		}
	}
	if completions != 1 {
		t.Fatalf("accepted %d completion events", completions)
	}
}

func TestLostRenewalAcknowledgementRetriesAfterDeadline(t *testing.T) {
	backend := newBackend(t, "one")
	var now atomic.Int64
	var failed atomic.Bool
	failpoint := func(kind string) error {
		if kind == "renew" && failed.CompareAndSwap(false, true) {
			return errors.New("lost acknowledgement")
		}
		return nil
	}
	_, client, cleanup := startManager(t, backend, &now, failpoint)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	assignment := receiveAssignment(t, stream)
	now.Store(50)
	heartbeat := &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Heartbeat{
		Heartbeat: &chronosv1.LeaseHeartbeat{
			RequestId: "heartbeat-lost", WorkflowId: assignment.GetWorkflowId(), TaskId: assignment.GetTaskId(),
			AttemptId: assignment.GetAttemptId(), FencingToken: assignment.GetFencingToken(), LeaseExpiresAt: 150,
		},
	}}
	if err := stream.Send(heartbeat); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) == 0 {
		t.Fatalf("renewal acknowledgement was not lost: %v", err)
	}
	retry := openWorker(t, ctx, client, "worker", 1)
	redelivery := receiveAssignment(t, retry)
	if redelivery.GetAttemptId() != assignment.GetAttemptId() || redelivery.GetLeaseExpiresAt() != 150 {
		t.Fatalf("renewed lease was not redelivered: %+v", redelivery)
	}
	now.Store(200)
	if err := retry.Send(heartbeat); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, retry); !ack.GetAccepted() || !ack.GetDuplicate() {
		t.Fatalf("renewal retry was not deduplicated: %+v", ack)
	}
}

func TestLeaseRenewalAndCreditUpdates(t *testing.T) {
	backend := newBackend(t, "one", "two")
	var now atomic.Int64
	_, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	first := receiveAssignment(t, stream)
	now.Store(50)
	heartbeat := &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Heartbeat{
		Heartbeat: &chronosv1.LeaseHeartbeat{
			RequestId: "heartbeat", WorkflowId: first.GetWorkflowId(), TaskId: first.GetTaskId(),
			AttemptId: first.GetAttemptId(), FencingToken: first.GetFencingToken(), LeaseExpiresAt: 150,
		},
	}}
	if err := stream.Send(heartbeat); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, stream); !ack.GetAccepted() || ack.GetDuplicate() {
		t.Fatalf("renewal failed: %+v", ack)
	}
	state := backend.state(first.GetWorkflowId())
	if state.Tasks[first.GetTaskId()].LeaseUntil != 150 {
		t.Fatal("renewal was acknowledged before commit")
	}
	if err := stream.Send(heartbeat); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, stream); !ack.GetAccepted() || !ack.GetDuplicate() {
		t.Fatalf("renewal retry was not deduplicated: %+v", ack)
	}
	if err := stream.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Credit{
		Credit: &chronosv1.CreditGrant{Credits: 2},
	}}); err != nil {
		t.Fatal(err)
	}
	second := receiveAssignment(t, stream)
	if second.GetAttemptId() == first.GetAttemptId() || running(backend) != 2 {
		t.Fatal("credit increase did not release exactly one assignment")
	}
	if err := stream.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Credit{
		Credit: &chronosv1.CreditGrant{Credits: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("credit reduction below inflight was accepted: %v", err)
	}
}

func TestLeaseExpiryReturnsWorkerCredit(t *testing.T) {
	backend := newBackend(t, "one", "two")
	var now atomic.Int64
	manager, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	first := receiveAssignment(t, stream)
	now.Store(first.GetLeaseExpiresAt())
	if err := manager.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	second := receiveAssignment(t, stream)
	if second.GetAttemptId() == first.GetAttemptId() || running(backend) != 1 {
		t.Fatalf("expired lease did not return one credit: %+v %+v", first, second)
	}
}

func TestManagerFiresScheduledWorkflowTimer(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	backend := backendFor(t, core.WorkflowDefinition{
		Name: "scheduled", Namespace: "test", StartAt: 100,
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: retry}},
	})
	var now atomic.Int64
	manager, _, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now.Store(99)
	if err := manager.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	state := backend.state(core.WorkflowID("test", "scheduled"))
	if state.Status != core.WorkflowScheduled {
		t.Fatalf("workflow started early: %+v", state)
	}
	now.Store(100)
	if err := manager.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	state = backend.state(state.ID)
	if state.Status != core.WorkflowRunning || state.Tasks["task"].Status != core.TaskReady {
		t.Fatalf("workflow timer did not start work: %+v", state)
	}
	fired := 0
	for _, event := range backend.journal() {
		if event.Kind == core.EventTimerFired {
			fired++
		}
	}
	if fired != 1 {
		t.Fatalf("fired %d timers", fired)
	}
}

func TestTimeoutWinsLeaseExpiryTie(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	backend := backendFor(t, core.WorkflowDefinition{
		Name: "timeout", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: retry, TimeoutMillis: 25}},
	})
	var now atomic.Int64
	manager, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := openWorker(t, ctx, client, "worker", 1)
	assignment := receiveAssignment(t, stream)
	if assignment.GetLeaseExpiresAt() != 25 {
		t.Fatalf("lease was not capped by timeout: %+v", assignment)
	}
	now.Store(assignment.GetLeaseExpiresAt())
	if err := manager.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	timedOut, expired := 0, 0
	for _, event := range backend.journal() {
		switch event.Kind {
		case core.EventTaskTimedOut:
			timedOut++
		case core.EventLeaseExpired:
			expired++
		}
	}
	if timedOut != 1 || expired != 0 {
		t.Fatalf("timeout=%d expiry=%d", timedOut, expired)
	}
	if err := stream.Send(completion(assignment)); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, stream); ack.GetAccepted() || ack.GetCode() != "fenced" {
		t.Fatalf("timed-out completion was accepted: %+v", ack)
	}
}

func TestOverdueTimeoutAndLeaseOrdering(t *testing.T) {
	tests := []struct {
		name            string
		leaseUntil      int64
		timeoutDeadline int64
		winner          core.EventKind
		loser           core.EventKind
	}{
		{name: "lease earlier", leaseUntil: 50, timeoutDeadline: 100, winner: core.EventLeaseExpired, loser: core.EventTaskTimedOut},
		{name: "equal", leaseUntil: 100, timeoutDeadline: 100, winner: core.EventTaskTimedOut, loser: core.EventLeaseExpired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := deadlineBackend(t, test.name, test.leaseUntil, test.timeoutDeadline)
			var now atomic.Int64
			manager, _, cleanup := startManager(t, backend, &now, nil)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			now.Store(max(test.leaseUntil, test.timeoutDeadline) + 1)
			if err := manager.Trigger(ctx); err != nil {
				t.Fatal(err)
			}
			counts := map[core.EventKind]int{}
			for _, event := range backend.journal() {
				counts[event.Kind]++
			}
			if counts[test.winner] != 1 || counts[test.loser] != 0 {
				t.Fatalf("winner=%s count=%d loser=%s count=%d", test.winner, counts[test.winner], test.loser, counts[test.loser])
			}
		})
	}
}

func TestFanoutCompletionProducesPayloadAssignment(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	backend := backendFor(t, core.WorkflowDefinition{
		Name: "fanout", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "plan", Type: "plan", Retry: retry, Fanout: &core.FanoutDefinition{
				TaskType: "work", AggregateTaskID: "join", MaxItems: 2, Retry: retry,
			}},
			{ID: "join", Type: "join", Dependencies: []string{"plan"}, Retry: retry},
		},
	})
	var now atomic.Int64
	_, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	worker := openWorkerCapabilities(t, ctx, client, "work-worker", 1, "work")
	planner := openWorkerCapabilities(t, ctx, client, "plan-worker", 1, "plan")
	assignment := receiveAssignment(t, planner)
	message := completion(assignment)
	message.GetCompletion().Fanout = []*chronosv1.FanoutItem{{Key: "linux", Payload: map[string]string{"target": "linux"}}}
	if err := planner.Send(message); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, planner); !ack.GetAccepted() {
		t.Fatalf("fanout was rejected: %+v", ack)
	}
	child := receiveAssignment(t, worker)
	if child.GetTaskType() != "work" || child.GetPayload()["target"] != "linux" || child.GetCompensation() {
		t.Fatalf("fanout assignment mismatch: %+v", child)
	}
}

func TestCompensationCapabilityCreditAndRedelivery(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	backend := backendFor(t, core.WorkflowDefinition{
		Name: "compensation", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "deploy", Type: "deploy", Retry: retry, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
			{ID: "verify", Type: "verify", Dependencies: []string{"deploy"}, Retry: retry},
		},
	})
	var now atomic.Int64
	_, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deployer := openWorkerCapabilities(t, ctx, client, "deploy-worker", 1, "deploy")
	verifier := openWorkerCapabilities(t, ctx, client, "verify-worker", 1, "verify")
	rollback := openWorkerCapabilities(t, ctx, client, "rollback-worker", 1, "rollback")
	deploy := receiveAssignment(t, deployer)
	deployCompletion := completion(deploy)
	deployCompletion.GetCompletion().Output = map[string]string{"release": "v1"}
	if err := deployer.Send(deployCompletion); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, deployer); !ack.GetAccepted() {
		t.Fatalf("deploy failed: %+v", ack)
	}
	verify := receiveAssignment(t, verifier)
	failure := completion(verify)
	failure.GetCompletion().Output = nil
	failure.GetCompletion().Error = "verification failed"
	if err := verifier.Send(failure); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, verifier); !ack.GetAccepted() {
		t.Fatalf("verification failure was rejected: %+v", ack)
	}
	compensation := receiveAssignment(t, rollback)
	if !compensation.GetCompensation() || compensation.GetTaskType() != "rollback" ||
		compensation.GetPayload()["release"] != "v1" ||
		compensation.GetIdempotencyKey() != core.CompensationIdempotencyKey(compensation.GetWorkflowId(), "deploy") || running(backend) != 1 {
		t.Fatalf("compensation assignment mismatch: %+v", compensation)
	}
	replacement := openWorkerCapabilities(t, ctx, client, "rollback-worker", 1, "rollback")
	redelivery := receiveAssignment(t, replacement)
	if redelivery.GetAttemptId() != compensation.GetAttemptId() || redelivery.GetFencingToken() != compensation.GetFencingToken() || !redelivery.GetCompensation() {
		t.Fatalf("compensation lease changed on reconnect: %+v %+v", compensation, redelivery)
	}
}

func TestTriggerReturnsAfterManagerStops(t *testing.T) {
	backend := newBackend(t)
	schedulerValue, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := execution.New(execution.Config{
		Scheduler: schedulerValue, View: backend.view, Submit: backend.submit,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.Run(ctx)
	triggerContext, triggerCancel := context.WithTimeout(context.Background(), time.Second)
	defer triggerCancel()
	if err := manager.Trigger(triggerContext); status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped manager returned %v", err)
	}
}

func TestWorkflowFailureReturnsPeerWorkerCredit(t *testing.T) {
	backend := newParallelBackend(t)
	var now atomic.Int64
	manager, client, cleanup := startManager(t, backend, &now, nil)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failing := openWorker(t, ctx, client, "z-worker", 1)
	first := receiveAssignment(t, failing)
	peer := openWorker(t, ctx, client, "a-worker", 1)
	second := receiveAssignment(t, peer)
	if first.GetWorkflowId() != second.GetWorkflowId() || first.GetTaskId() == second.GetTaskId() {
		t.Fatalf("parallel tasks were not assigned: %+v %+v", first, second)
	}
	failure := completion(first)
	failure.GetCompletion().Output = nil
	failure.GetCompletion().Error = "failed"
	if err := failing.Send(failure); err != nil {
		t.Fatal(err)
	}
	if ack := receiveAck(t, failing); !ack.GetAccepted() {
		t.Fatalf("failure was not accepted: %+v", ack)
	}
	paused := openWorker(t, ctx, client, "z-worker", 0)
	if err := paused.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Heartbeat{
		Heartbeat: &chronosv1.LeaseHeartbeat{RequestId: "pause-barrier", WorkflowId: "missing"},
	}}); err != nil {
		t.Fatal(err)
	}
	receiveAck(t, paused)
	definition := core.WorkflowDefinition{
		Name: "spare", Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
	if _, err := backend.submit(ctx, core.Command{Kind: core.CommandSubmit, RequestID: "spare", Definition: &definition}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	if assignment := receiveAssignment(t, peer); assignment.GetWorkflowId() == second.GetWorkflowId() {
		t.Fatalf("peer credit remained attached to cancelled attempt: %+v", assignment)
	}
}

func startManager(t *testing.T, backend *backend, now *atomic.Int64, failpoint execution.AckFailpoint) (*execution.Manager, chronosv1.WorkerServiceClient, func()) {
	t.Helper()
	schedulerValue, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := execution.New(execution.Config{
		Scheduler: schedulerValue, View: backend.view, Submit: backend.submit, Now: now.Load,
		LeaseMillis: 100, TickInterval: time.Hour, MaxCredits: 8, AckFailpoint: failpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	manager.Register(server)
	ctx, cancel := context.WithCancel(context.Background())
	go manager.Run(ctx)
	go server.Serve(listener)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		cancel()
		server.Stop()
		connection.Close()
		listener.Close()
	}
	return manager, chronosv1.NewWorkerServiceClient(connection), cleanup
}

func newBackend(t *testing.T, requestIDs ...string) *backend {
	t.Helper()
	engine := core.NewEngine()
	for _, requestID := range requestIDs {
		definition := core.WorkflowDefinition{
			Name: requestID, Namespace: "test",
			Tasks: []core.TaskDefinition{{
				ID: "task", Type: "work",
				Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
			}},
		}
		if _, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: requestID, Definition: &definition}); err != nil {
			t.Fatal(err)
		}
	}
	return &backend{engine: engine}
}

func newParallelBackend(t *testing.T) *backend {
	t.Helper()
	engine := core.NewEngine()
	definition := core.WorkflowDefinition{
		Name: "parallel", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "one", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}},
			{ID: "two", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}},
		},
	}
	if _, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "parallel", Definition: &definition}); err != nil {
		t.Fatal(err)
	}
	return &backend{engine: engine}
}

func backendFor(t *testing.T, definition core.WorkflowDefinition) *backend {
	t.Helper()
	engine := core.NewEngine()
	if _, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: definition.Name, Definition: &definition}); err != nil {
		t.Fatal(err)
	}
	return &backend{engine: engine}
}

func deadlineBackend(t *testing.T, requestID string, leaseUntil, timeoutDeadline int64) *backend {
	t.Helper()
	definition := core.WorkflowDefinition{
		Name: requestID, Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "work", TimeoutMillis: timeoutDeadline,
			Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
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
	return &backend{engine: engine}
}

func openWorker(t *testing.T, ctx context.Context, client chronosv1.WorkerServiceClient, workerID string, credits uint32) grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage] {
	return openWorkerCapabilities(t, ctx, client, workerID, credits, "work")
}

func openWorkerCapabilities(t *testing.T, ctx context.Context, client chronosv1.WorkerServiceClient, workerID string, credits uint32, capabilities ...string) grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage] {
	t.Helper()
	stream, err := client.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Hello{
		Hello: &chronosv1.WorkerHello{WorkerId: workerID, Capabilities: capabilities, Credits: credits},
	}}); err != nil {
		t.Fatal(err)
	}
	return stream
}

func receiveAssignment(t *testing.T, stream grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage]) *chronosv1.TaskAssignment {
	t.Helper()
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if message.GetAssignment() == nil {
		t.Fatalf("expected assignment, received %+v", message)
	}
	return message.GetAssignment()
}

func receiveAck(t *testing.T, stream grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage]) *chronosv1.WorkerAck {
	t.Helper()
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if message.GetAck() == nil {
		t.Fatalf("expected acknowledgement, received %+v", message)
	}
	return message.GetAck()
}

func completion(assignment *chronosv1.TaskAssignment) *chronosv1.WorkerMessage {
	return &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Completion{
		Completion: &chronosv1.TaskCompletion{
			RequestId:  core.CompletionRequestID(assignment.GetAttemptId(), assignment.GetFencingToken()),
			WorkflowId: assignment.GetWorkflowId(), TaskId: assignment.GetTaskId(),
			AttemptId: assignment.GetAttemptId(), FencingToken: assignment.GetFencingToken(),
			Output: map[string]string{"value": "done"},
		},
	}}
}

func running(backend *backend) int {
	backend.mu.Lock()
	engine := backend.engine.Clone()
	backend.mu.Unlock()
	count := 0
	for _, workflowID := range engine.WorkflowIDs() {
		state, _ := engine.State(workflowID)
		for _, task := range state.Tasks {
			if task.Status == core.TaskRunning || task.Status == core.TaskCompensating && task.WorkerID != "" {
				count++
			}
		}
	}
	return count
}
