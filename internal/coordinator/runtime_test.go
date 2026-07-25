package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/scheduler"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type httpStatus struct {
	NodeID       uint64 `json:"node_id"`
	LeaderID     uint64 `json:"leader_id"`
	AppliedIndex uint64 `json:"applied_index"`
	Role         string `json:"role"`
}

var benchmarkEventTail uint64
var benchmarkEventCount int

func BenchmarkEventCursorTail(b *testing.B) {
	const tail = 32
	events := make([]core.Event, 16_384)
	for i := range events {
		events[i].Sequence = uint64(i + 1)
	}
	after := uint64(len(events) - tail)
	// todo: compare persisted cursor reads once Store accepts a lower bound

	b.Run("scan", func(b *testing.B) {
		var first, last uint64
		var count int
		for b.Loop() {
			first, last, count = 0, 0, 0
			for _, event := range events {
				if event.Sequence <= after {
					continue
				}
				if count == 0 {
					first = event.Sequence
				}
				last = event.Sequence
				count++
			}
		}
		if first != after+1 || last != uint64(len(events)) || count != tail {
			b.Fatalf("unexpected tail: first=%d last=%d count=%d", first, last, count)
		}
		benchmarkEventTail = last
		benchmarkEventCount = count
	})

	b.Run("search", func(b *testing.B) {
		var first, last uint64
		var count int
		for b.Loop() {
			// start := int(after)
			start := sort.Search(len(events), func(i int) bool { return events[i].Sequence > after })
			first, last, count = 0, 0, 0
			for _, event := range events[start:] {
				if count == 0 {
					first = event.Sequence
				}
				last = event.Sequence
				count++
			}
		}
		if first != after+1 || last != uint64(len(events)) || count != tail {
			b.Fatalf("unexpected tail: first=%d last=%d count=%d", first, last, count)
		}
		benchmarkEventTail = last
		benchmarkEventCount = count
	})
}

func TestRaftHandlerAcknowledgesAdmission(t *testing.T) {
	runtime := &Runtime{
		config: RuntimeConfig{ID: 2, Peers: map[uint64]string{1: "http://1", 2: "http://2", 3: "http://3"}},
		steps:  make(chan stepRequest, 1),
	}
	from, to, kind := uint64(1), uint64(2), pb.MsgApp
	data, err := proto.Marshal(&pb.Message{From: &from, To: &to, Type: &kind})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(data)).WithContext(ctx)
	response := httptest.NewRecorder()
	runtime.handleRaft(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("raft admission returned %d", response.Code)
	}
	cancel()
	admitted := <-runtime.steps
	if admitted.message.GetFrom() != from || admitted.message.GetTo() != to || admitted.message.GetType() != kind || admitted.response != nil {
		t.Fatalf("unexpected admitted message: %+v", admitted.message)
	}
	if err := admitted.context.Err(); err != nil {
		t.Fatalf("admitted context was canceled: %v", err)
	}
}

func TestRaftHandlerCancellationUnblocksAdmission(t *testing.T) {
	steps := make(chan stepRequest, 1)
	steps <- stepRequest{}
	runtime := &Runtime{
		config: RuntimeConfig{ID: 2, Peers: map[uint64]string{1: "http://1", 2: "http://2", 3: "http://3"}},
		steps:  steps,
	}
	from, to, kind := uint64(1), uint64(2), pb.MsgApp
	data, err := proto.Marshal(&pb.Message{From: &from, To: &to, Type: &kind})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(data)).WithContext(ctx)
	finished := make(chan struct{})
	go func() {
		runtime.handleRaft(httptest.NewRecorder(), request)
		close(finished)
	}()
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("raft admission blocked after cancellation")
	}
}

func TestRaftHandlerRejectsInvalidMessagesWithoutStoppingRuntime(t *testing.T) {
	peers := map[uint64]string{1: "http://127.0.0.1:1", 2: "http://127.0.0.1:2", 3: "http://127.0.0.1:3"}
	runtime, err := NewRuntime(RuntimeConfig{
		ID: 1, ClusterID: "raft-ingress", Directory: t.TempDir(), Listen: "127.0.0.1:0", Peers: peers, TickInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.steps = make(chan stepRequest)
	defer runtime.observability.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	go runtime.loop(ctx)
	defer func() {
		cancel()
		<-runtime.done
	}()

	cases := []struct {
		name string
		from uint64
		kind pb.MessageType
	}{
		{name: "local", from: 1, kind: pb.MsgApp},
		{name: "unknown-peer", from: 4, kind: pb.MsgApp},
		{name: "local-only", from: 2, kind: pb.MsgHup},
		{name: "unsupported", from: 2, kind: pb.MsgProp},
		{name: "unknown-type", from: 2, kind: pb.MessageType(99)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			to := uint64(1)
			data, err := proto.Marshal(&pb.Message{From: &test.from, To: &to, Type: &test.kind})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			runtime.handleRaft(response, httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(data)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid raft message returned %d", response.Code)
			}
			statusContext, stop := context.WithTimeout(context.Background(), time.Second)
			status := runtime.status(statusContext)
			stop()
			if status.NodeID != 1 {
				t.Fatal("runtime stopped after invalid raft message")
			}
		})
	}

	from, to, term, kind := uint64(2), uint64(1), uint64(1), pb.MsgHeartbeat
	data, err := proto.Marshal(&pb.Message{From: &from, To: &to, Term: &term, Type: &kind})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	runtime.handleRaft(response, httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(data)))
	if response.Code != http.StatusNoContent {
		t.Fatalf("valid raft message returned %d", response.Code)
	}
	statusContext, stop := context.WithTimeout(context.Background(), time.Second)
	status := runtime.status(statusContext)
	stop()
	if status.NodeID != 1 {
		t.Fatal("runtime stopped after valid raft message")
	}
}

func TestRaftHandlerRejectsStoppedRuntime(t *testing.T) {
	for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgSnap} {
		t.Run(kind.String(), func(t *testing.T) {
			done := make(chan struct{})
			close(done)
			runtime := &Runtime{config: RuntimeConfig{ID: 2}, steps: make(chan stepRequest, 1), done: done}
			from, to := uint64(1), uint64(2)
			data, err := proto.Marshal(&pb.Message{From: &from, To: &to, Type: &kind})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			runtime.handleRaft(response, httptest.NewRequest(http.MethodPost, "/raft", bytes.NewReader(data)))
			if response.Code != http.StatusServiceUnavailable || len(runtime.steps) != 0 {
				t.Fatalf("stopped runtime returned %d with %d admissions", response.Code, len(runtime.steps))
			}
		})
	}
}

func TestAdmittedRaftErrorTerminatesRuntime(t *testing.T) {
	peers := map[uint64]string{1: "http://127.0.0.1:1", 2: "http://127.0.0.1:2", 3: "http://127.0.0.1:3"}
	runtime, err := NewRuntime(RuntimeConfig{
		ID: 1, ClusterID: "raft-error", Directory: t.TempDir(), Listen: "127.0.0.1:0", Peers: peers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cap(runtime.admissions) != 0 {
		t.Fatalf("proposal admissions have capacity %d", cap(runtime.admissions))
	}
	defer runtime.observability.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runtime.loop(ctx)
	from, to, kind := uint64(2), uint64(1), pb.MsgHup
	runtime.steps <- stepRequest{context: context.Background(), message: &pb.Message{From: &from, To: &to, Type: &kind}}
	select {
	case err := <-runtime.terminal:
		if !errors.Is(err, raft.ErrStepLocalMsg) {
			t.Fatalf("runtime terminated with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime ignored admitted raft error")
	}
	select {
	case <-runtime.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop after raft error")
	}
}

func TestBackpressureHTTPResponse(t *testing.T) {
	runtime := &Runtime{}
	request := httptest.NewRequest(http.MethodPost, "/v1/commands", nil)
	response := httptest.NewRecorder()
	runtime.writeError(response, request, scheduler.ErrBackpressure, 1)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("backpressure returned %d", response.Code)
	}
	var body commandResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.Code != "backpressure" {
		t.Fatalf("invalid backpressure response: %+v %v", body, err)
	}
}

func TestBoundedProposalReleasesOnRuntimeResponse(t *testing.T) {
	runtime := &Runtime{pending: make(chan struct{}, 1)}
	runtime.pending <- struct{}{}
	response := make(chan proposalResponse, 1)
	runtime.respond(proposalRequest{response: response, bounded: true}, proposalResponse{err: ErrStopped})
	if len(runtime.pending) != 0 {
		t.Fatal("bounded proposal token was not released")
	}
	if result := <-response; !errors.Is(result.err, ErrStopped) {
		t.Fatalf("unexpected proposal response: %+v", result)
	}
}

func TestRuntimeStatusReportsPendingProposals(t *testing.T) {
	pending := make(chan struct{}, 2)
	pending <- struct{}{}
	pending <- struct{}{}
	runtime := &Runtime{
		coordinator: &Coordinator{status: Status{NodeID: 1}}, pending: pending,
		queue: []proposalRequest{{bounded: true}}, active: []*activeProposal{{request: proposalRequest{bounded: true}}},
	}
	data, err := json.Marshal(runtime.statusSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		PendingProposals int `json:"pending_proposals"`
	}
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal(err)
	}
	if status.PendingProposals != 2 {
		t.Fatalf("unexpected pending proposal count %d", status.PendingProposals)
	}
}

func TestRuntimeStatusTracksExecutionProposalStates(t *testing.T) {
	runtime := &Runtime{
		coordinator: &Coordinator{status: Status{NodeID: 1}}, proposals: make(chan proposalRequest, 1), done: make(chan struct{}),
	}
	type executionResult struct {
		result core.Result
		err    error
	}
	finished := make(chan executionResult, 1)
	go func() {
		result, err := runtime.executionSubmit(context.Background(), core.Command{})
		finished <- executionResult{result: result, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for len(runtime.proposals) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(runtime.proposals) != 1 || runtime.statusSnapshot().PendingProposals != 1 {
		t.Fatalf("buffered execution proposal is not pending: buffered=%d status=%d", len(runtime.proposals), runtime.statusSnapshot().PendingProposals)
	}
	request := <-runtime.proposals
	if runtime.statusSnapshot().PendingProposals != 1 {
		t.Fatal("execution proposal disappeared during handoff")
	}
	runtime.enqueueProposal(request)
	if len(runtime.queue) != 1 || runtime.statusSnapshot().PendingProposals != 1 {
		t.Fatalf("queued execution proposal is not pending: queue=%d status=%d", len(runtime.queue), runtime.statusSnapshot().PendingProposals)
	}
	request = runtime.queue[0]
	runtime.queue = nil
	runtime.active = []*activeProposal{{request: request}}
	if runtime.statusSnapshot().PendingProposals != 1 {
		t.Fatal("active execution proposal is not pending")
	}
	runtime.active = nil
	runtime.respond(request, proposalResponse{results: []core.Result{{WorkflowID: "workflow"}}})
	select {
	case result := <-finished:
		if result.err != nil || result.result.WorkflowID != "workflow" {
			t.Fatalf("execution proposal returned %+v %v", result.result, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("execution proposal did not finish")
	}
	if runtime.statusSnapshot().PendingProposals != 0 {
		t.Fatal("completed execution proposal remained pending")
	}
}

func TestRuntimeStatusTracksBlockedSubmitters(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		runtime := &Runtime{
			coordinator: &Coordinator{status: Status{NodeID: 1}}, admissions: make(chan proposalRequest), done: make(chan struct{}),
		}
		requestContext, cancel := context.WithCancel(context.Background())
		finished := make(chan struct{})
		go func() {
			runtime.submitBounded(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/commands", nil).WithContext(requestContext), nil)
			close(finished)
		}()
		waitForPendingProposals(t, runtime, 1)
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("blocked HTTP submitter did not finish")
		}
		if runtime.statusSnapshot().PendingProposals != 0 {
			t.Fatal("canceled HTTP submitter remained pending")
		}
	})
	t.Run("execution", func(t *testing.T) {
		runtime := &Runtime{
			coordinator: &Coordinator{status: Status{NodeID: 1}}, proposals: make(chan proposalRequest), done: make(chan struct{}),
		}
		requestContext, cancel := context.WithCancel(context.Background())
		finished := make(chan error, 1)
		go func() {
			_, err := runtime.executionSubmit(requestContext, core.Command{})
			finished <- err
		}()
		waitForPendingProposals(t, runtime, 1)
		cancel()
		select {
		case err := <-finished:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("blocked execution submitter returned %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked execution submitter did not finish")
		}
		if runtime.statusSnapshot().PendingProposals != 0 {
			t.Fatal("canceled execution submitter remained pending")
		}
	})
}

func waitForPendingProposals(t *testing.T, runtime *Runtime, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if runtime.statusSnapshot().PendingProposals == expected {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending proposal count is %d, expected %d", runtime.statusSnapshot().PendingProposals, expected)
}

func TestProposalAdmissionRedirectsWithoutToken(t *testing.T) {
	response := make(chan proposalResponse, 1)
	runtime := &Runtime{
		coordinator: &Coordinator{status: Status{NodeID: 2, LeaderID: 1, Role: raft.StateFollower}},
		admissions:  make(chan proposalRequest),
		pending:     make(chan struct{}),
	}
	runtime.admitProposals(proposalRequest{context: context.Background(), response: response})
	result := <-response
	if !errors.Is(result.err, ErrNoLeader) || result.leader != 1 {
		t.Fatalf("follower admission returned %+v", result)
	}
	if len(runtime.pending) != 0 || len(runtime.queue) != 0 {
		t.Fatalf("follower admission retained work: pending=%d queue=%d", len(runtime.pending), len(runtime.queue))
	}
}

func TestProposalAdmissionRejectsFullLeaderWithoutLeak(t *testing.T) {
	pending := make(chan struct{}, 1)
	pending <- struct{}{}
	response := make(chan proposalResponse, 1)
	runtime := &Runtime{
		coordinator: &Coordinator{status: Status{NodeID: 1, LeaderID: 1, Role: raft.StateLeader}},
		admissions:  make(chan proposalRequest),
		pending:     pending,
	}
	runtime.admitProposals(proposalRequest{context: context.Background(), response: response})
	if result := <-response; !errors.Is(result.err, scheduler.ErrBackpressure) || result.leader != 1 {
		t.Fatalf("full leader admission returned %+v", result)
	}
	if len(runtime.pending) != 1 || len(runtime.queue) != 0 {
		t.Fatalf("full leader admission leaked work: pending=%d queue=%d", len(runtime.pending), len(runtime.queue))
	}
}

func TestProposalAdmissionDropsCanceledRequestWithoutToken(t *testing.T) {
	requestContext, cancel := context.WithCancel(context.Background())
	cancel()
	response := make(chan proposalResponse, 1)
	runtime := &Runtime{
		coordinator: &Coordinator{status: Status{NodeID: 1, LeaderID: 1, Role: raft.StateLeader}},
		admissions:  make(chan proposalRequest),
		pending:     make(chan struct{}, 1),
	}
	runtime.admitProposals(proposalRequest{context: requestContext, response: response})
	if result := <-response; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("canceled admission returned %+v", result)
	}
	if len(runtime.pending) != 0 || len(runtime.queue) != 0 {
		t.Fatalf("canceled admission retained work: pending=%d queue=%d", len(runtime.pending), len(runtime.queue))
	}
}

func TestProposalAdmissionRechecksLeadershipBeforeStaging(t *testing.T) {
	coordinator := &Coordinator{status: Status{NodeID: 1, LeaderID: 1, Role: raft.StateLeader}}
	response := make(chan proposalResponse, 1)
	runtime := &Runtime{
		coordinator: coordinator, admissions: make(chan proposalRequest), proposals: make(chan proposalRequest),
		pending: make(chan struct{}, 1),
	}
	runtime.admitProposals(proposalRequest{context: context.Background(), response: response})
	coordinator.status = Status{NodeID: 1, LeaderID: 2, Role: raft.StateFollower}
	runtime.startProposal()
	result := <-response
	if !errors.Is(result.err, ErrNoLeader) || result.leader != 2 {
		t.Fatalf("lost-leader admission returned %+v", result)
	}
	if len(runtime.pending) != 0 || len(runtime.queue) != 0 || len(runtime.active) != 0 {
		t.Fatalf("lost-leader admission retained work: pending=%d queue=%d active=%d", len(runtime.pending), len(runtime.queue), len(runtime.active))
	}
}

func TestBoundedSubmissionUnblocksOnShutdown(t *testing.T) {
	for _, receive := range []bool{false, true} {
		t.Run(strconv.FormatBool(receive), func(t *testing.T) {
			runtime := &Runtime{admissions: make(chan proposalRequest), done: make(chan struct{})}
			finished := make(chan struct{})
			go func() {
				runtime.submitBounded(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/commands", nil), nil)
				close(finished)
			}()
			if receive {
				<-runtime.admissions
			}
			close(runtime.done)
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("bounded submission blocked during shutdown")
			}
		})
	}
}

func TestStartProposalDropsCanceledRequests(t *testing.T) {
	requestContext, cancel := context.WithCancel(context.Background())
	cancel()
	pending := make(chan struct{}, 1)
	pending <- struct{}{}
	response := make(chan proposalResponse, 1)
	runtime := &Runtime{
		queue:       []proposalRequest{{context: requestContext, response: response, bounded: true}},
		pending:     pending,
		coordinator: &Coordinator{},
	}
	runtime.startProposal()
	if len(runtime.queue) != 0 || len(runtime.active) != 0 || len(runtime.pending) != 0 {
		t.Fatalf("canceled proposal was retained: queue=%d active=%d pending=%d", len(runtime.queue), len(runtime.active), len(runtime.pending))
	}
	if result := <-response; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("canceled proposal returned %v", result.err)
	}
}

func TestProposalLookupRequiresAppliedAdvance(t *testing.T) {
	proposal := &activeProposal{expectedIndex: 9, checkedAppliedIndex: 7}
	if proposal.shouldLookup(7, false) {
		t.Fatal("unchanged applied index triggered lookup")
	}
	if !proposal.shouldLookup(7, true) {
		t.Fatal("initial lookup was skipped")
	}
	if proposal.shouldLookup(8, false) {
		t.Fatal("lookup ran before expected index")
	}
	if !proposal.shouldLookup(9, false) {
		t.Fatal("advanced applied index skipped lookup")
	}
	if proposal.shouldLookup(9, false) {
		t.Fatal("repeated applied index triggered lookup")
	}
}

func TestSnapshotThreshold(t *testing.T) {
	for _, test := range []struct {
		applied   uint64
		snapshot  uint64
		threshold uint64
		expected  bool
	}{
		{100, 1, 0, false},
		{100, 1, 100, false},
		{101, 1, 100, true},
		{100, 101, 1, false},
	} {
		if actual := shouldSnapshot(test.applied, test.snapshot, test.threshold); actual != test.expected {
			t.Fatalf("snapshot decision mismatch: %+v %v", test, actual)
		}
	}
}

func TestLocalExecutionBatchRequiresOptIn(t *testing.T) {
	body, err := json.Marshal(batchRequest{Commands: []core.Command{{Kind: core.CommandStart, RequestID: "start"}}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
	response := httptest.NewRecorder()
	(&Runtime{}).handleBatch(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("local execution batch returned %d", response.Code)
	}
}

func TestLatestCommit(t *testing.T) {
	index, term := latestCommit([]commandCommit{
		{RequestID: "one", AppliedIndex: 3, CommittedTerm: 2},
		{RequestID: "two", AppliedIndex: 5, CommittedTerm: 4},
	})
	if index != 5 || term != 4 {
		t.Fatalf("unexpected latest commit: %d %d", index, term)
	}
}

func TestRuntimeHTTPFailoverAndLinearizableRead(t *testing.T) {
	addresses := map[uint64]string{}
	listens := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listens[id] = listener.Addr().String()
		addresses[id] = "http://" + listens[id]
		listener.Close()
	}
	directory := t.TempDir()
	contexts := map[uint64]context.CancelFunc{}
	errors := map[uint64]chan error{}
	tickIntervals := map[uint64]time.Duration{1: 25 * time.Millisecond, 2: 50 * time.Millisecond, 3: 100 * time.Millisecond}
	t.Cleanup(func() {
		for _, cancel := range contexts {
			cancel()
		}
		for id, result := range errors {
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
	start := func(id uint64) {
		t.Helper()
		runtime, err := NewRuntime(RuntimeConfig{
			ID: id, ClusterID: "runtime-test", Directory: filepath.Join(directory, fmt.Sprintf("node-%d", id)),
			Listen: listens[id], Peers: addresses, TickInterval: tickIntervals[id], LocalExecutionAPI: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		contexts[id] = cancel
		result := make(chan error, 1)
		errors[id] = result
		go func() { result <- runtime.Run(ctx) }()
	}
	for id := uint64(1); id <= 3; id++ {
		start(id)
	}
	client := &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	for id, address := range addresses {
		waitHTTPHealthy(t, client, id, address, 5*time.Second)
	}
	leader := waitHTTPLeader(t, client, addresses, map[uint64]bool{}, 15*time.Second)
	follower := uint64(1)
	if follower == leader {
		follower = 2
	}
	definition := testDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	body, _ := json.Marshal(command)
	response, err := client.Post(addresses[follower]+"/v1/commands", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != addresses[leader]+"/v1/commands" {
		t.Fatalf("unexpected redirect: %s %s", response.Status, response.Header.Get("Location"))
	}
	response, err = client.Post(addresses[leader]+"/v1/commands", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var submitted commandResponse
	if err := json.NewDecoder(response.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(submitted.Results) != 1 || len(submitted.Commits) != 1 || submitted.AppliedIndex == 0 || submitted.CommittedTerm == 0 || submitted.Commits[0].RequestID != "submit" || submitted.Commits[0].AppliedIndex != submitted.AppliedIndex || submitted.Commits[0].CommittedTerm != submitted.CommittedTerm {
		t.Fatalf("submit returned %s %+v", response.Status, submitted)
	}
	workflowID := submitted.Results[0].WorkflowID
	unsafeCommand := lease(core.Command{
		Kind: core.CommandStart, RequestID: "unsafe-start", WorkflowID: workflowID, TaskID: "task", At: 1,
	})
	unsafeBody, _ := json.Marshal(unsafeCommand)
	response, err = client.Post(addresses[leader]+"/v1/commands", "application/json", bytes.NewReader(unsafeBody))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("raw execution command returned %s", response.Status)
	}
	response, err = client.Get(addresses[leader] + "/v1/workflows/" + workflowID)
	if err != nil {
		t.Fatal(err)
	}
	var workflow workflowResponse
	if err := json.NewDecoder(response.Body).Decode(&workflow); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !workflow.Found || workflow.State.Version != 2 {
		t.Fatalf("read returned %s %+v", response.Status, workflow)
	}
	response, err = client.Get(addresses[leader] + "/v1/workflows/" + workflowID + "/events?after=1&follow=false")
	if err != nil {
		t.Fatal(err)
	}
	eventData, err := io.ReadAll(response.Body)
	response.Body.Close()
	// t.Log(string(eventData))
	if err != nil || response.StatusCode != http.StatusOK || strings.Contains(string(eventData), "id: 1\n") || !strings.Contains(string(eventData), "event: task_ready") {
		t.Fatalf("event stream returned %s %q %v", response.Status, eventData, err)
	}
	response, err = client.Get(addresses[leader] + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricData, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(metricData), "chronos_queue_ready_tasks") || !strings.Contains(string(metricData), "chronos_raft_commits_total") {
		t.Fatalf("metrics returned %s %v", response.Status, err)
	}
	workflowPath := "/v1/workflows/" + workflowID
	response, err = client.Get(addresses[follower] + workflowPath + "?trace=1")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != addresses[leader]+workflowPath+"?trace=1" {
		t.Fatalf("unexpected read redirect: %s %s", response.Status, response.Header.Get("Location"))
	}
	cancelPath := workflowPath + "/cancel"
	cancelBody, _ := json.Marshal(cancelRequest{RequestID: "cancel", Reason: "operator request"})
	response, err = client.Post(addresses[follower]+cancelPath, "application/json", bytes.NewReader(cancelBody))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != addresses[leader]+cancelPath {
		t.Fatalf("unexpected cancellation redirect: %s %s", response.Status, response.Header.Get("Location"))
	}
	response, err = client.Post(addresses[leader]+cancelPath, "application/json", bytes.NewReader(cancelBody))
	if err != nil {
		t.Fatal(err)
	}
	var cancelled commandResponse
	if err := json.NewDecoder(response.Body).Decode(&cancelled); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(cancelled.Results) != 1 {
		t.Fatalf("cancellation returned %s %+v", response.Status, cancelled)
	}
	response, err = client.Get(addresses[leader] + workflowPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(response.Body).Decode(&workflow); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if workflow.State.Status != core.WorkflowCancelled {
		t.Fatalf("cancellation state mismatch: %+v", workflow.State)
	}
	changed := definition
	changed.Name = "changed"
	conflict := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &changed}
	conflictBody, _ := json.Marshal(conflict)
	response, err = client.Post(addresses[leader]+"/v1/commands", "application/json", bytes.NewReader(conflictBody))
	if err != nil {
		t.Fatal(err)
	}
	var conflictResponse commandResponse
	if err := json.NewDecoder(response.Body).Decode(&conflictResponse); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict || conflictResponse.Code != "request_conflict" {
		t.Fatalf("conflict returned %s %+v", response.Status, conflictResponse)
	}
	response, err = client.Post(addresses[leader]+"/v1/commands", "application/json", bytes.NewReader(append(body, []byte("{}")...)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing JSON returned %s", response.Status)
	}
	batch := batchRequest{Commands: []core.Command{
		{Kind: core.CommandSubmit, RequestID: "batch-one", Definition: &definition},
		{Kind: core.CommandSubmit, RequestID: "batch-two", Definition: &definition},
	}}
	batchBody, _ := json.Marshal(batch)
	response, err = client.Post(addresses[leader]+"/v1/batches", "application/json", bytes.NewReader(batchBody))
	if err != nil {
		t.Fatal(err)
	}
	var batched commandResponse
	if err := json.NewDecoder(response.Body).Decode(&batched); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(batched.Results) != 2 || batched.Results[0].WorkflowID == batched.Results[1].WorkflowID {
		t.Fatalf("batch returned %s %+v", response.Status, batched)
	}
	response, err = client.Post(addresses[leader]+"/v1/batches", "application/json", bytes.NewReader(batchBody))
	if err != nil {
		t.Fatal(err)
	}
	var batchRetry commandResponse
	if err := json.NewDecoder(response.Body).Decode(&batchRetry); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(batchRetry.Results) != 2 || !batchRetry.Results[0].Duplicate || !batchRetry.Results[1].Duplicate || batchRetry.Results[0].WorkflowID != batched.Results[0].WorkflowID || batchRetry.Results[1].WorkflowID != batched.Results[1].WorkflowID {
		t.Fatalf("batch retry returned %s %+v", response.Status, batchRetry)
	}
	localExecution := testDefinition()
	localExecutionID := core.WorkflowID(localExecution.Namespace, "local-execution-submit")
	localExecutionBatch := batchRequest{Commands: []core.Command{
		{Kind: core.CommandSubmit, RequestID: "local-execution-submit", WorkflowID: localExecutionID, Definition: &localExecution},
		lease(core.Command{Kind: core.CommandStart, RequestID: "local-execution-start", WorkflowID: localExecutionID, TaskID: "task", At: 1}),
		{Kind: core.CommandComplete, RequestID: "local-execution-complete", WorkflowID: localExecutionID, TaskID: "task", AttemptID: core.AttemptID(localExecutionID, "task", 1), WorkerID: "test-worker", Fence: 1, At: 2},
	}}
	localExecutionBody, _ := json.Marshal(localExecutionBatch)
	response, err = client.Post(addresses[leader]+"/v1/batches", "application/json", bytes.NewReader(localExecutionBody))
	if err != nil {
		t.Fatal(err)
	}
	var executed commandResponse
	if err := json.NewDecoder(response.Body).Decode(&executed); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(executed.Results) != 3 || len(executed.Commits) != 3 || executed.CommittedTerm == 0 || executed.Results[2].Events[len(executed.Results[2].Events)-1].Kind != core.EventWorkflowCompleted {
		t.Fatalf("local execution batch returned %s %+v", response.Status, executed)
	}
	batch.Commands[1].RequestID = "batch-one"
	invalidBatch, _ := json.Marshal(batch)
	response, err = client.Post(addresses[leader]+"/v1/batches", "application/json", bytes.NewReader(invalidBatch))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("duplicate batch returned %s", response.Status)
	}
	request, _ := http.NewRequest(http.MethodPost, addresses[follower]+"/v1/snapshot", nil)
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != addresses[leader]+"/v1/snapshot" {
		t.Fatalf("unexpected snapshot redirect: %s %s", response.Status, response.Header.Get("Location"))
	}
	request, _ = http.NewRequest(http.MethodPost, addresses[leader]+"/v1/snapshot", nil)
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("snapshot returned %s", response.Status)
	}
	contexts[leader]()
	if err := <-errors[leader]; err != nil {
		t.Fatal(err)
	}
	delete(errors, leader)
	newLeader := waitHTTPLeader(t, client, addresses, map[uint64]bool{leader: true}, 15*time.Second)
	afterFailover := definition
	afterFailover.Name = "after-failover"
	afterFailoverCommand := core.Command{Kind: core.CommandSubmit, RequestID: "after-failover", Definition: &afterFailover}
	body, _ = json.Marshal(afterFailoverCommand)
	response, err = client.Post(addresses[newLeader]+"/v1/commands", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var started commandResponse
	if err := json.NewDecoder(response.Body).Decode(&started); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(started.Results) != 1 || len(started.Results[0].Events) < 2 || started.Results[0].Events[0].Kind != core.EventWorkflowSubmitted {
		t.Fatalf("failover submit returned %s %+v", response.Status, started)
	}
	start(leader)
	waitHTTPApplied(t, client, addresses[leader], started.AppliedIndex, 15*time.Second)
	retry, _ := json.Marshal(command)
	response, err = client.Post(addresses[newLeader]+"/v1/commands", "application/json", bytes.NewReader(retry))
	if err != nil {
		t.Fatal(err)
	}
	var duplicate commandResponse
	if err := json.NewDecoder(response.Body).Decode(&duplicate); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || len(duplicate.Results) != 1 || !duplicate.Results[0].Duplicate {
		t.Fatalf("retry returned %s %+v", response.Status, duplicate)
	}
}

func waitHTTPHealthy(t *testing.T, client *http.Client, id uint64, address string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := client.Get(address + "/healthz")
		if err == nil {
			var health struct {
				Status string `json:"status"`
				NodeID uint64 `json:"node_id"`
			}
			err = json.NewDecoder(response.Body).Decode(&health)
			response.Body.Close()
			if err == nil && response.StatusCode == http.StatusOK && health.Status == "ok" && health.NodeID == id {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %d did not become healthy", id)
}

func waitHTTPLeader(t *testing.T, client *http.Client, addresses map[uint64]string, excluded map[uint64]bool, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := map[uint64]string{}
	for time.Now().Before(deadline) {
		for id, address := range addresses {
			if excluded[id] {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, address+"/v1/status", nil)
			response, err := client.Do(request)
			if err != nil {
				cancel()
				last[id] = err.Error()
				continue
			}
			var status httpStatus
			err = json.NewDecoder(response.Body).Decode(&status)
			response.Body.Close()
			cancel()
			last[id] = fmt.Sprintf("%+v: %v", status, err)
			// t.Logf("%d: %s", id, last[id])
			if err == nil && status.LeaderID == id && status.Role == "StateLeader" {
				return id
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("leader was not elected: %v", last)
	return 0
}

func waitHTTPApplied(t *testing.T, client *http.Client, address string, index uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := client.Get(address + "/v1/status")
		if err == nil {
			var status httpStatus
			err = json.NewDecoder(response.Body).Decode(&status)
			response.Body.Close()
			if err == nil && status.AppliedIndex >= index {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("restarted node did not catch up")
}
