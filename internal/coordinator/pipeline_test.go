package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestProposalAdmissionsCoalesceReadyGroup(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	base, err := runtime.coordinator.Store().LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]proposalRequest, proposalReadyBatch)
	for index := range requests {
		requests[index] = pipelineSubmit(fmt.Sprintf("group-%02d", index))
	}
	runtime.pending = make(chan struct{}, proposalReadyBatch)
	runtime.admissions = make(chan proposalRequest, proposalReadyBatch-1)
	for _, request := range requests[1:] {
		runtime.admissions <- request
	}
	runtime.admitProposals(requests[0])
	if len(runtime.queue) != proposalReadyBatch || len(runtime.pending) != proposalReadyBatch {
		t.Fatalf("admissions were not coalesced: queue=%d pending=%d", len(runtime.queue), len(runtime.pending))
	}
	runtime.startProposal()
	replication := 0
	for _, message := range cluster.messages {
		if message.GetFrom() != 1 || message.GetType() != pb.MsgApp {
			continue
		}
		replication++
		if len(message.GetEntries()) != proposalReadyBatch {
			t.Fatalf("replication message has %d entries", len(message.GetEntries()))
		}
		for offset, entry := range message.GetEntries() {
			if entry.GetIndex() != base+uint64(offset)+1 {
				t.Fatalf("entry %d has index %d", offset, entry.GetIndex())
			}
		}
	}
	if replication != 2 {
		t.Fatalf("staged group produced %d replication messages", replication)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	for offset, request := range requests {
		response := <-request.response
		if response.err != nil || response.appliedIndex != base+uint64(offset)+1 {
			t.Fatalf("proposal %d returned %+v", offset, response)
		}
	}
	if len(runtime.pending) != 0 {
		t.Fatalf("coalesced admissions retained %d tokens", len(runtime.pending))
	}
}

func TestProposalAdmissionsRetainNextReadyGroup(t *testing.T) {
	_, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	requests := make([]proposalRequest, proposalReadyBatch+1)
	for index := range requests {
		requests[index] = pipelineSubmit(fmt.Sprintf("retained-%02d", index))
	}
	runtime.pending = make(chan struct{}, len(requests))
	runtime.admissions = make(chan proposalRequest, len(requests)-1)
	for _, request := range requests[1:] {
		runtime.admissions <- request
	}
	runtime.admitProposals(requests[0])
	if len(runtime.queue) != proposalReadyBatch || len(runtime.pending) != proposalReadyBatch || len(runtime.admissions) != 1 {
		t.Fatalf("admission crossed ready group: queue=%d pending=%d admissions=%d", len(runtime.queue), len(runtime.pending), len(runtime.admissions))
	}
	runtime.admitProposals(<-runtime.admissions)
	if len(runtime.queue) != len(requests) || len(runtime.pending) != len(requests) {
		t.Fatalf("retained admission was lost: queue=%d pending=%d", len(runtime.queue), len(runtime.pending))
	}
	for _, request := range runtime.queue {
		runtime.respond(request, proposalResponse{})
		<-request.response
	}
	if len(runtime.pending) != 0 {
		t.Fatalf("retained admissions leaked %d tokens", len(runtime.pending))
	}
}

func TestProposalPipelineCommitsDistinctEntries(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	base, err := runtime.coordinator.Store().LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	requests := []proposalRequest{
		pipelineSubmit("first"), pipelineSubmit("second"), pipelineSubmit("third"),
	}
	runtime.queue = append(runtime.queue, requests...)
	runtime.startProposal()
	if len(runtime.active) != 3 || runtime.pipelineLast != base+3 || runtime.planner == nil {
		t.Fatalf("pipeline was not staged: active=%d last=%d", len(runtime.active), runtime.pipelineLast)
	}
	for offset, proposal := range runtime.active {
		expected := base + uint64(offset) + 1
		if proposal.expectedIndex != expected || len(proposal.request.commands) != 0 || len(proposal.requestIDs) != 1 {
			t.Fatalf("proposal %d expects index %d, expected %d", offset, proposal.expectedIndex, expected)
		}
	}
	for _, request := range requests {
		select {
		case response := <-request.response:
			t.Fatalf("proposal completed before commit: %+v", response)
		default:
		}
		workflowID := core.WorkflowID("test", request.commands[0].RequestID)
		if _, exists := runtime.coordinator.Store().State(workflowID); exists {
			t.Fatalf("speculative workflow %s became visible", workflowID)
		}
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	indexes := map[uint64]struct{}{}
	for offset, request := range requests {
		response := <-request.response
		if response.err != nil || len(response.results) != 1 || len(response.commits) != 1 {
			t.Fatalf("proposal %d failed: %+v", offset, response)
		}
		expected := base + uint64(offset) + 1
		if response.appliedIndex != expected || response.commits[0].AppliedIndex != expected {
			t.Fatalf("proposal %d committed at %d, expected %d", offset, response.appliedIndex, expected)
		}
		indexes[response.appliedIndex] = struct{}{}
	}
	if len(indexes) != len(requests) || len(runtime.active) != 0 || runtime.planner != nil || len(runtime.coordinator.outcomes) != 0 || len(runtime.outcomes) != 0 {
		t.Fatalf("pipeline did not drain: indexes=%v active=%d", indexes, len(runtime.active))
	}
}

func TestProposalPipelineRejectsPendingDuplicateAndConflict(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	first := pipelineSubmit("request")
	runtime.queue = append(runtime.queue, first)
	runtime.startProposal()
	last := runtime.pipelineLast
	retry := pipelineSubmit("request")
	conflict := pipelineSubmit("request")
	conflict.commands[0].Definition.Name = "changed"
	runtime.queue = append(runtime.queue, retry, conflict)
	runtime.startProposal()
	if response := <-retry.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("pending duplicate returned %+v", response)
	}
	if response := <-conflict.response; !errors.Is(response.err, core.ErrRequestConflict) {
		t.Fatalf("pending conflict returned %+v", response)
	}
	if runtime.pipelineLast != last || len(runtime.active) != 1 {
		t.Fatalf("duplicate requests entered raft: last=%d active=%d", runtime.pipelineLast, len(runtime.active))
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	if response := <-first.response; response.err != nil {
		t.Fatalf("original proposal failed: %+v", response)
	}
}

func TestProposalPipelineRejectsMixedPendingDuplicateWithoutStaging(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	first := pipelineSubmit("pending")
	runtime.queue = append(runtime.queue, first)
	runtime.startProposal()
	last := runtime.pipelineLast
	mixed := pipelineSubmit("new")
	mixed.commands = append([]core.Command{first.commands[0]}, mixed.commands...)
	runtime.queue = append(runtime.queue, mixed)
	runtime.startProposal()
	if response := <-mixed.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("mixed pending duplicate returned %+v", response)
	}
	if runtime.pipelineLast != last || len(runtime.active) != 1 {
		t.Fatalf("mixed request entered raft: last=%d active=%d", runtime.pipelineLast, len(runtime.active))
	}
	if _, exists := runtime.planner.ResidentRequestHash("new"); exists {
		t.Fatal("rejected request remained staged")
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	if response := <-first.response; response.err != nil {
		t.Fatalf("original proposal failed: %+v", response)
	}
}

func TestProposalPipelineAdmissionIncludesUncommittedWork(t *testing.T) {
	config := scheduler.DefaultConfig()
	config.Global.Workflows = 1
	config.Default.Limits.Workflows = 1
	cluster, runtime := newPipelineRuntime(t, config)
	first := pipelineSubmit("first")
	second := pipelineSubmit("second")
	runtime.queue = append(runtime.queue, first, second)
	runtime.startProposal()
	if response := <-second.response; !errors.Is(response.err, scheduler.ErrBackpressure) {
		t.Fatalf("uncommitted admission returned %+v", response)
	}
	if len(runtime.active) != 1 {
		t.Fatalf("rejected proposal entered raft: %d", len(runtime.active))
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	if response := <-first.response; response.err != nil {
		t.Fatalf("admitted proposal failed: %+v", response)
	}
}

func TestProposalPipelineResetsAfterDuplicateLookupMiss(t *testing.T) {
	_, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	status := runtime.coordinator.Status()
	last, err := runtime.coordinator.Store().LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	loaded := pipelineSubmit("loaded")
	hash, err := core.HashCommand(loaded.commands[0])
	if err != nil {
		t.Fatal(err)
	}
	planner := core.NewEngine()
	planner.SetLoaders(nil, func(requestID string) (core.RequestReceipt, bool, error) {
		if requestID != "loaded" {
			return core.RequestReceipt{}, false, nil
		}
		return core.RequestReceipt{
			RequestID: requestID, RequestHash: hash,
			Result: core.Result{WorkflowID: core.WorkflowID("test", requestID)},
		}, true, nil
	})
	runtime.planner = planner
	runtime.pipelineTerm = status.Term
	runtime.pipelineLast = last
	first := pipelineSubmit("first")
	runtime.queue = append(runtime.queue, first, loaded)
	runtime.startProposal()
	if response := <-loaded.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("missing durable duplicate returned %+v", response)
	}
	if response := <-first.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("earlier speculative proposal returned %+v", response)
	}
	if runtime.planner != nil || len(runtime.active) != 0 {
		t.Fatal("failed duplicate lookup retained speculative state")
	}
	if current, err := runtime.coordinator.Store().LastIndex(); err != nil || current != last {
		t.Fatalf("failed proposal reached raft: last=%d err=%v", current, err)
	}
}

func TestProposalPipelineBoundsTerminalPlannerWithoutDraining(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	previous := make([]proposalRequest, proposalReadyBatch)
	for index := range previous {
		previous[index] = pipelineTerminal(fmt.Sprintf("terminal-00-%02d", index))
	}
	runtime.queue = append(runtime.queue, previous...)
	runtime.startProposal()
	if len(runtime.active) != proposalReadyBatch || runtime.planner == nil {
		t.Fatalf("initial window was not staged: %d", len(runtime.active))
	}
	for _, request := range previous {
		workflowID := request.commands[1].WorkflowID
		if !runtime.planner.ResidentWorkflow(workflowID) {
			t.Fatalf("uncommitted workflow %s was evicted", workflowID)
		}
		state, _ := runtime.planner.State(workflowID)
		if len(state.Tasks) != 0 || len(state.Timers) != 0 {
			t.Fatalf("uncommitted terminal workflow %s was not compacted", workflowID)
		}
	}
	for generation := 1; generation <= 20; generation++ {
		if err := cluster.Drive(100000); err != nil {
			t.Fatal(err)
		}
		next := make([]proposalRequest, proposalReadyBatch)
		for index := range next {
			next[index] = pipelineTerminal(fmt.Sprintf("terminal-%02d-%02d", generation, index))
		}
		runtime.queue = append(runtime.queue, next...)
		runtime.startProposal()
		if len(runtime.active) != proposalReadyBatch || runtime.planner == nil {
			t.Fatalf("generation %d drained the active window: %d", generation, len(runtime.active))
		}
		if resident := len(runtime.planner.WorkflowIDs()); resident != proposalReadyBatch {
			t.Fatalf("generation %d retained %d workflows", generation, resident)
		}
		for _, request := range previous {
			response := <-request.response
			if response.err != nil {
				t.Fatalf("generation %d failed: %+v", generation-1, response)
			}
			workflowID := request.commands[1].WorkflowID
			if runtime.planner.ResidentWorkflow(workflowID) {
				t.Fatalf("committed workflow %s was retained", workflowID)
			}
			for _, command := range request.commands {
				if _, exists := runtime.planner.ResidentRequestHash(command.RequestID); exists {
					t.Fatalf("committed request %s was retained", command.RequestID)
				}
			}
		}
		previous = next
	}
	if err := cluster.Drive(100000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	for _, request := range previous {
		if response := <-request.response; response.err != nil {
			t.Fatalf("final generation failed: %+v", response)
		}
	}
	if len(runtime.active) != 0 || runtime.planner != nil {
		t.Fatal("final window did not drain")
	}
}

func TestProposalPipelineRetainsLaterSameWorkflowProposal(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	started, workflowID := pipelineStarted("ordered")
	runtime.queue = append(runtime.queue, started)
	runtime.startProposal()
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	completed := pipelineCompleted("ordered", workflowID)
	runtime.queue = append(runtime.queue, completed)
	runtime.startProposal()
	if response := <-started.response; response.err != nil {
		t.Fatalf("start proposal failed: %+v", response)
	}
	if len(runtime.active) != 1 || !runtime.planner.ResidentWorkflow(workflowID) {
		t.Fatalf("later proposal lost its workflow: active=%d", len(runtime.active))
	}
	completeID := completed.commands[0].RequestID
	if _, exists := runtime.planner.ResidentRequestHash(completeID); !exists {
		t.Fatal("later proposal lost its request")
	}
	last := runtime.pipelineLast
	retry := pipelineCompleted("ordered", workflowID)
	runtime.queue = append(runtime.queue, retry)
	runtime.startProposal()
	if response := <-retry.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("pending same-workflow retry returned %+v", response)
	}
	if runtime.pipelineLast != last || len(runtime.active) != 1 {
		t.Fatalf("pending retry entered raft: last=%d active=%d", runtime.pipelineLast, len(runtime.active))
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	if response := <-completed.response; response.err != nil {
		t.Fatalf("completion proposal failed: %+v", response)
	}
	state, exists := runtime.coordinator.Store().State(workflowID)
	if !exists || state.Status != core.WorkflowCompleted {
		t.Fatalf("workflow did not complete: %+v", state)
	}
}

func TestProposalPipelineRebasesSameWorkflowAfterLeadershipLoss(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	started, workflowID := pipelineStarted("failover-ordered")
	runtime.queue = append(runtime.queue, started)
	runtime.startProposal()
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	completed := pipelineCompleted("failover-ordered", workflowID)
	runtime.queue = append(runtime.queue, completed)
	runtime.startProposal()
	if response := <-started.response; response.err != nil {
		t.Fatalf("start proposal failed: %+v", response)
	}
	cluster.Isolate(1)
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Tick(20); err != nil {
		t.Fatal(err)
	}
	newLeader, err := cluster.WaitLeader(20)
	if err != nil || newLeader == 1 {
		t.Fatalf("leadership did not move: leader=%d err=%v", newLeader, err)
	}
	runtime.checkProposal(false)
	if response := <-completed.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("lost-leader completion returned %+v", response)
	}
	cluster.Heal()
	if err := cluster.Tick(3); err != nil {
		t.Fatal(err)
	}
	leader, exists := cluster.Node(newLeader)
	if !exists {
		t.Fatal("new leader is missing")
	}
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	rebased := &Runtime{coordinator: leader, scheduler: value}
	retry := pipelineCompleted("failover-ordered", workflowID)
	rebased.queue = append(rebased.queue, retry)
	rebased.startProposal()
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	rebased.checkProposal(false)
	response := <-retry.response
	if response.err != nil || len(response.results) != 1 {
		t.Fatalf("rebased completion failed: %+v", response)
	}
	state, exists := leader.Store().State(workflowID)
	if !exists || state.Status != core.WorkflowCompleted {
		t.Fatalf("rebased workflow did not complete: %+v", state)
	}
}

func TestProposalPipelineFailsAndRebasesAfterLeadershipLoss(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	request := pipelineSubmit("rebase")
	runtime.queue = append(runtime.queue, request)
	runtime.startProposal()
	cluster.Isolate(1)
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Tick(20); err != nil {
		t.Fatal(err)
	}
	newLeader, err := cluster.WaitLeader(20)
	if err != nil || newLeader == 1 || runtime.coordinator.Status().Role == raft.StateLeader {
		t.Fatalf("leadership did not move: leader=%d old=%s err=%v", newLeader, runtime.coordinator.Status().Role, err)
	}
	runtime.checkProposal(false)
	if response := <-request.response; !errors.Is(response.err, ErrUnavailable) {
		t.Fatalf("lost-leader proposal returned %+v", response)
	}
	if len(runtime.active) != 0 || runtime.planner != nil {
		t.Fatal("lost-leader pipeline was retained")
	}
	cluster.Heal()
	if err := cluster.Tick(3); err != nil {
		t.Fatal(err)
	}
	leader, exists := cluster.Node(newLeader)
	if !exists {
		t.Fatal("new leader is missing")
	}
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	rebased := &Runtime{coordinator: leader, scheduler: value}
	retry := pipelineSubmit("rebase")
	rebased.queue = append(rebased.queue, retry)
	rebased.startProposal()
	if len(rebased.active) != 1 {
		t.Fatalf("retry was not staged: active=%d role=%s", len(rebased.active), leader.Status().Role)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	rebased.checkProposal(false)
	if response := <-retry.response; response.err != nil || response.appliedIndex == 0 {
		t.Fatalf("rebased proposal failed: %+v", response)
	}
}

func newPipelineRuntime(t *testing.T, config scheduler.Config) (*Cluster, *Runtime) {
	t.Helper()
	cluster, err := NewCluster(ClusterConfig{Directory: t.TempDir(), ClusterID: "pipeline"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cluster.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	leader, exists := cluster.Node(1)
	if !exists {
		t.Fatal("leader is missing")
	}
	return cluster, &Runtime{coordinator: leader, scheduler: value}
}

func pipelineSubmit(requestID string) proposalRequest {
	definition := testDefinition()
	return proposalRequest{
		context:  context.Background(),
		commands: []core.Command{{Kind: core.CommandSubmit, RequestID: requestID, Definition: &definition}},
		response: make(chan proposalResponse, 1),
	}
}

func pipelineStarted(requestID string) (proposalRequest, string) {
	definition := testDefinition()
	workflowID := core.WorkflowID(definition.Namespace, requestID+"-submit")
	return proposalRequest{
		context: context.Background(),
		commands: []core.Command{
			{Kind: core.CommandSubmit, RequestID: requestID + "-submit", Definition: &definition},
			{
				Kind: core.CommandStart, RequestID: requestID + "-start", WorkflowID: workflowID,
				TaskID: "task", AttemptID: core.AttemptID(workflowID, "task", 1), WorkerID: "worker", At: 1, LeaseUntil: 1000,
			},
		},
		response: make(chan proposalResponse, 1),
	}, workflowID
}

func pipelineCompleted(requestID, workflowID string) proposalRequest {
	return proposalRequest{
		context: context.Background(),
		commands: []core.Command{{
			Kind: core.CommandComplete, RequestID: requestID + "-complete", WorkflowID: workflowID,
			TaskID: "task", AttemptID: core.AttemptID(workflowID, "task", 1), WorkerID: "worker", Fence: 1, At: 2,
		}},
		response: make(chan proposalResponse, 1),
	}
}

func pipelineTerminal(requestID string) proposalRequest {
	request, workflowID := pipelineStarted(requestID)
	completed := pipelineCompleted(requestID, workflowID)
	request.commands = append(request.commands, completed.commands...)
	return request
}
