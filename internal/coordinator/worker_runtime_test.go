package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	chronosv1 "github.com/HaikIsaiants/chronos-public/api/chronos/v1"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestRuntimeWorkerFailoverExpiryFencingAndLostAck(t *testing.T) {
	httpListeners := map[uint64]net.Listener{}
	workerListeners := map[uint64]net.Listener{}
	addresses := map[uint64]string{}
	workerAddresses := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		httpListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		workerListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		httpListeners[id] = httpListener
		workerListeners[id] = workerListener
		addresses[id] = "http://" + httpListener.Addr().String()
		workerAddresses[id] = workerListener.Addr().String()
	}
	var now atomic.Int64
	var lost atomic.Bool
	failpoint := func(kind string) error {
		if kind == "completion" && lost.CompareAndSwap(false, true) {
			return errors.New("lost acknowledgement")
		}
		return nil
	}
	directory := t.TempDir()
	runtimes := map[uint64]*Runtime{}
	contexts := map[uint64]context.CancelFunc{}
	runErrors := map[uint64]chan error{}
	for id := uint64(1); id <= 3; id++ {
		runtime, err := NewRuntime(RuntimeConfig{
			ID: id, ClusterID: "worker-runtime", Directory: filepath.Join(directory, fmt.Sprintf("node-%d", id)),
			Listen: httpListeners[id].Addr().String(), WorkerListen: workerAddresses[id], Peers: addresses,
			TickInterval: time.Duration(id) * 25 * time.Millisecond, WorkerTick: time.Hour,
			LeaseMillis: 100, Now: now.Load, AckFailpoint: failpoint,
			Listener: httpListeners[id], WorkerListener: workerListeners[id],
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		runtimes[id] = runtime
		contexts[id] = cancel
		runErrors[id] = result
		go func() { result <- runtime.Run(ctx) }()
	}
	t.Cleanup(func() {
		for _, cancel := range contexts {
			cancel()
		}
		for id, result := range runErrors {
			select {
			case err := <-result:
				if err != nil {
					t.Errorf("node %d: %v", id, err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("node %d did not stop", id)
			}
		}
	})
	client := &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	for id, address := range addresses {
		waitHTTPHealthy(t, client, id, address, 10*time.Second)
	}
	leader := waitHTTPLeader(t, client, addresses, map[uint64]bool{}, 15*time.Second)
	follower := uint64(1)
	if follower == leader {
		follower = 2
	}
	testContext, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	followerConnection := dialWorker(t, workerAddresses[follower])
	defer followerConnection.Close()
	followerStream, err := chronosv1.NewWorkerServiceClient(followerConnection).Work(testContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendHello(followerStream, "follower-worker"); err != nil {
		t.Fatal(err)
	}
	if _, err := followerStream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("follower accepted worker stream: %v", err)
	}
	definition := testDefinition()
	submitted := postRuntimeCommand(t, client, addresses[leader], core.Command{
		Kind: core.CommandSubmit, RequestID: "worker-submit", Definition: &definition,
	})
	workflowID := submitted.Results[0].WorkflowID
	firstContext, stopFirst := context.WithCancel(testContext)
	firstConnection := dialWorker(t, workerAddresses[leader])
	defer firstConnection.Close()
	firstStream, err := chronosv1.NewWorkerServiceClient(firstConnection).Work(firstContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendHello(firstStream, "worker-a"); err != nil {
		t.Fatal(err)
	}
	first := receiveRuntimeAssignment(t, firstStream)
	state, _ := runtimes[leader].coordinator.Store().Engine().State(workflowID)
	if state.Tasks["task"].AttemptID != first.GetAttemptId() {
		t.Fatal("assignment was sent before durable application")
	}
	stopFirst()
	contexts[leader]()
	if err := <-runErrors[leader]; err != nil {
		t.Fatal(err)
	}
	delete(contexts, leader)
	delete(runErrors, leader)
	newLeader := waitHTTPLeader(t, client, addresses, map[uint64]bool{leader: true}, 15*time.Second)
	secondConnection := dialWorker(t, workerAddresses[newLeader])
	defer secondConnection.Close()
	secondStream, err := chronosv1.NewWorkerServiceClient(secondConnection).Work(testContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendHello(secondStream, "worker-b"); err != nil {
		t.Fatal(err)
	}
	now.Store(first.GetLeaseExpiresAt() - 1)
	if err := runtimes[newLeader].execution.Trigger(testContext); err != nil {
		t.Fatal(err)
	}
	state, _ = runtimes[newLeader].coordinator.Store().Engine().State(workflowID)
	if state.Tasks["task"].AttemptID != first.GetAttemptId() {
		t.Fatal("task was reassigned before lease expiry")
	}
	now.Store(first.GetLeaseExpiresAt())
	if err := runtimes[newLeader].execution.Trigger(testContext); err != nil {
		t.Fatal(err)
	}
	second := receiveRuntimeAssignment(t, secondStream)
	if second.GetFencingToken() <= first.GetFencingToken() ||
		// second.GetAttemptId() == first.GetAttemptId() ||
		second.GetIdempotencyKey() != first.GetIdempotencyKey() {
		t.Fatalf("replacement lease is invalid: %+v %+v", first, second)
	}
	staleConnection := dialWorker(t, workerAddresses[newLeader])
	defer staleConnection.Close()
	staleStream, err := chronosv1.NewWorkerServiceClient(staleConnection).Work(testContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendHello(staleStream, "worker-a"); err != nil {
		t.Fatal(err)
	}
	staleCompletion := runtimeCompletion(first)
	if err := staleStream.Send(staleCompletion); err != nil {
		t.Fatal(err)
	}
	if ack := receiveRuntimeAck(t, staleStream); ack.GetAccepted() || ack.GetCode() != "fenced" {
		t.Fatalf("stale result was not fenced: %+v", ack)
	}
	currentCompletion := runtimeCompletion(second)
	if err := secondStream.Send(currentCompletion); err != nil {
		t.Fatal(err)
	}
	if _, err := secondStream.Recv(); status.Code(err) == codes.OK {
		t.Fatal("completion acknowledgement was not lost")
	}
	state, _ = runtimes[newLeader].coordinator.Store().Engine().State(workflowID)
	if state.Status != core.WorkflowCompleted {
		t.Fatalf("lost acknowledgement preceded commit: %+v", state)
	}
	retryConnection := dialWorker(t, workerAddresses[newLeader])
	defer retryConnection.Close()
	retryStream, err := chronosv1.NewWorkerServiceClient(retryConnection).Work(testContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := sendHello(retryStream, "worker-b"); err != nil {
		t.Fatal(err)
	}
	if err := retryStream.Send(currentCompletion); err != nil {
		t.Fatal(err)
	}
	ack := receiveRuntimeAck(t, retryStream)
	if !ack.GetAccepted() || !ack.GetDuplicate() {
		t.Fatalf("lost acknowledgement retry was not idempotent: %+v", ack)
	}
	events, err := runtimes[newLeader].coordinator.Store().Events(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	starts, completions := 0, 0
	for _, event := range events {
		if event.Kind == core.EventTaskStarted {
			starts++
		}
		if event.Kind == core.EventTaskCompleted {
			completions++
		}
	}
	if starts != 2 || completions != 1 {
		t.Fatalf("unexpected accepted execution history: starts=%d completions=%d", starts, completions)
	}
}

func dialWorker(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func sendHello(stream grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage], workerID string) error {
	return stream.Send(&chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Hello{
		Hello: &chronosv1.WorkerHello{WorkerId: workerID, Capabilities: []string{"test"}, Credits: 1},
	}})
}

func receiveRuntimeAssignment(t *testing.T, stream grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage]) *chronosv1.TaskAssignment {
	t.Helper()
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if message.GetAssignment() == nil {
		t.Fatalf("expected assignment: %+v", message)
	}
	return message.GetAssignment()
}

func receiveRuntimeAck(t *testing.T, stream grpc.BidiStreamingClient[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage]) *chronosv1.WorkerAck {
	t.Helper()
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if message.GetAck() == nil {
		t.Fatalf("expected acknowledgement: %+v", message)
	}
	return message.GetAck()
}

func runtimeCompletion(assignment *chronosv1.TaskAssignment) *chronosv1.WorkerMessage {
	return &chronosv1.WorkerMessage{Value: &chronosv1.WorkerMessage_Completion{
		Completion: &chronosv1.TaskCompletion{
			RequestId:  core.CompletionRequestID(assignment.GetAttemptId(), assignment.GetFencingToken()),
			WorkflowId: assignment.GetWorkflowId(), TaskId: assignment.GetTaskId(),
			AttemptId: assignment.GetAttemptId(), FencingToken: assignment.GetFencingToken(),
			Output: map[string]string{"value": "done"},
		},
	}}
}

func postRuntimeCommand(t *testing.T, client *http.Client, address string, command core.Command) commandResponse {
	t.Helper()
	data, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Post(address+"/v1/commands", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result commandResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("command returned %s: %+v", response.Status, result)
	}
	return result
}
