package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
)

func TestAsyncProposalPreparationOverlapsCommitAndBoundsWindow(t *testing.T) {
	cluster, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	stop := startPreparationWorker(t, runtime)
	defer stop()
	base, err := runtime.coordinator.Store().LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]proposalRequest, proposalWindow+1)
	for index := range requests {
		requests[index] = pipelineSubmit(fmt.Sprintf("async-%02d", index))
	}
	runtime.queue = append(runtime.queue, requests...)
	for group := 1; group <= proposalWindow/proposalReadyBatch; group++ {
		runtime.startProposal()
		expectedActive := (group - 1) * proposalReadyBatch
		if !runtime.prepareActive || len(runtime.preparing) != proposalReadyBatch || len(runtime.active) != expectedActive {
			t.Fatalf("group %d preparation is active=%d preparing=%d", group, len(runtime.active), len(runtime.preparing))
		}
		runtime.finishPreparation(waitPreparation(t, runtime))
		expectedActive += proposalReadyBatch
		if len(runtime.active) != expectedActive || runtime.pipelineLast != base+uint64(expectedActive) {
			t.Fatalf("group %d staged active=%d last=%d", group, len(runtime.active), runtime.pipelineLast)
		}
	}
	if len(runtime.active) != proposalWindow || len(runtime.queue) != 1 || runtime.prepareActive {
		t.Fatalf("full window is active=%d queued=%d preparing=%t", len(runtime.active), len(runtime.queue), runtime.prepareActive)
	}
	for offset, proposal := range runtime.active {
		expected := base + uint64(offset) + 1
		if proposal.expectedIndex != expected {
			t.Fatalf("proposal %d expects index %d, expected %d", offset, proposal.expectedIndex, expected)
		}
	}
	runtime.startProposal()
	last, err := runtime.coordinator.Store().LastIndex()
	if err != nil || last != base+proposalWindow || len(runtime.active) != proposalWindow || len(runtime.queue) != 1 || runtime.prepareActive {
		t.Fatalf("window overflowed: last=%d active=%d queued=%d preparing=%t err=%v", last, len(runtime.active), len(runtime.queue), runtime.prepareActive, err)
	}
	if err := cluster.Drive(100000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	runtime.startProposal()
	if !runtime.prepareActive || len(runtime.preparing) != 1 || len(runtime.active) != 0 || len(runtime.queue) != 0 {
		t.Fatalf("overflow preparation is active=%d preparing=%d queued=%d", len(runtime.active), len(runtime.preparing), len(runtime.queue))
	}
	runtime.finishPreparation(waitPreparation(t, runtime))
	if len(runtime.active) != 1 || runtime.pipelineLast != base+proposalWindow+1 {
		t.Fatalf("overflow proposal staged active=%d last=%d", len(runtime.active), runtime.pipelineLast)
	}
	if err := cluster.Drive(100000); err != nil {
		t.Fatal(err)
	}
	runtime.checkProposal(false)
	for offset, request := range requests {
		response := <-request.response
		expected := base + uint64(offset) + 1
		if response.err != nil || response.appliedIndex != expected || len(response.results) != 1 || len(response.commits) != 1 || response.commits[0].RequestID != request.commands[0].RequestID {
			t.Fatalf("proposal %d returned %+v", offset, response)
		}
		select {
		case duplicate := <-request.response:
			t.Fatalf("proposal %d returned twice: %+v", offset, duplicate)
		default:
		}
	}
	if runtime.prepareActive || len(runtime.preparing) != 0 || len(runtime.active) != 0 || runtime.planner != nil {
		t.Fatalf("pipeline retained active=%d preparing=%d", len(runtime.active), len(runtime.preparing))
	}
}

func TestAsyncProposalPreparationInvalidationRespondsOnce(t *testing.T) {
	_, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	stop := startPreparationWorker(t, runtime)
	defer stop()
	requests := []proposalRequest{pipelineSubmit("invalidate-first"), pipelineSubmit("invalidate-second")}
	runtime.queue = append(runtime.queue, requests...)
	runtime.startProposal()
	if !runtime.prepareActive || len(runtime.preparing) != len(requests) {
		t.Fatalf("preparation did not start: %t %d", runtime.prepareActive, len(runtime.preparing))
	}
	runtime.failPipeline(2)
	for _, request := range requests {
		if response := <-request.response; !errors.Is(response.err, ErrUnavailable) || response.leader != 2 {
			t.Fatalf("invalidation returned %+v", response)
		}
	}
	runtime.finishPreparation(waitPreparation(t, runtime))
	for _, request := range requests {
		select {
		case response := <-request.response:
			t.Fatalf("proposal received a second response: %+v", response)
		default:
		}
	}
	if runtime.prepareActive || runtime.prepareFailed || runtime.planner != nil || len(runtime.active) != 0 || len(runtime.preparing) != 0 {
		t.Fatal("invalidated preparation retained pipeline state")
	}
}

func TestAsyncProposalPreparationHonorsCancellation(t *testing.T) {
	_, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	runtime.prepare = make(chan proposalPreparation, 1)
	runtime.prepared = make(chan proposalPreparationResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	request := pipelineSubmit("canceled-preparation")
	request.context = ctx
	request.bounded = true
	runtime.pending = make(chan struct{}, 1)
	runtime.pending <- struct{}{}
	runtime.queue = append(runtime.queue, request)
	runtime.startProposal()
	cancel()
	preparation := <-runtime.prepare
	runtime.finishPreparation(runtime.prepareProposalGroup(preparation))
	response := <-request.response
	if !errors.Is(response.err, context.Canceled) {
		t.Fatalf("canceled preparation returned %+v", response)
	}
	last, err := runtime.coordinator.Store().LastIndex()
	if err != nil || last != runtime.coordinator.Status().AppliedIndex || len(runtime.pending) != 0 {
		t.Fatalf("canceled preparation changed state: last=%d pending=%d err=%v", last, len(runtime.pending), err)
	}
	if runtime.prepareActive || runtime.planner != nil || len(runtime.active) != 0 || len(runtime.preparing) != 0 {
		t.Fatal("canceled preparation retained pipeline state")
	}
}

func TestAsyncProposalPreparationRejectsDuplicateRequestIDs(t *testing.T) {
	_, runtime := newPipelineRuntime(t, scheduler.DefaultConfig())
	runtime.prepare = make(chan proposalPreparation, 1)
	runtime.prepared = make(chan proposalPreparationResult, 1)
	request := pipelineSubmit("duplicate-preparation")
	request.commands = append(request.commands, request.commands[0])
	runtime.queue = append(runtime.queue, request)
	before, err := runtime.coordinator.Store().LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	runtime.startProposal()
	response := <-request.response
	if !errors.Is(response.err, core.ErrInvalidCommand) {
		t.Fatalf("duplicate preparation returned %+v", response)
	}
	after, err := runtime.coordinator.Store().LastIndex()
	if err != nil || after != before || runtime.prepareActive || len(runtime.active) != 0 {
		t.Fatalf("duplicate preparation changed state: before=%d after=%d active=%d err=%v", before, after, len(runtime.active), err)
	}
}

func startPreparationWorker(t *testing.T, runtime *Runtime) func() {
	t.Helper()
	runtime.prepare = make(chan proposalPreparation, 1)
	runtime.prepared = make(chan proposalPreparationResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.prepareProposals(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func waitPreparation(t *testing.T, runtime *Runtime) proposalPreparationResult {
	t.Helper()
	select {
	case result := <-runtime.prepared:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("proposal preparation timed out")
		return proposalPreparationResult{}
	}
}
