package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/execution"
	"github.com/HaikIsaiants/chronos-public/internal/observability"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type RuntimeConfig struct {
	ID                uint64
	ClusterID         string
	Directory         string
	Listen            string
	Peers             map[uint64]string
	Failpoint         storage.Failpoint
	TickInterval      time.Duration
	WorkerListen      string
	Scheduler         scheduler.Config
	MaxPending        int
	LeaseMillis       int64
	WorkerTick        time.Duration
	MaxCredits        uint32
	WorkerBuffer      int
	MaxDispatch       int
	Now               func() int64
	AckFailpoint      execution.AckFailpoint
	Listener          net.Listener
	WorkerListener    net.Listener
	Observability     *observability.Provider
	SnapshotEntries   uint64
	LocalExecutionAPI bool
}

type Runtime struct {
	config        RuntimeConfig
	coordinator   *Coordinator
	transport     *httpTransport
	admissions    chan proposalRequest
	proposals     chan proposalRequest
	steps         chan stepRequest
	reads         chan readRequest
	snapshots     chan chan error
	statuses      chan chan Status
	reports       chan transportReport
	done          chan struct{}
	terminal      chan error
	queue         []proposalRequest
	pendingReads  map[string]*activeRead
	readCounter   uint64
	scheduler     *scheduler.Scheduler
	execution     *execution.Manager
	views         chan executionViewRequest
	pending       chan struct{}
	observability *observability.Provider
	eventMu       sync.Mutex
	eventSignal   chan struct{}
	eventIndex    uint64
	lastMetrics   time.Time
	snapshotIndex uint64
	planner       *core.Engine
	active        []*activeProposal
	pipelineTerm  uint64
	pipelineLast  uint64
	outcomes      map[string]storage.Outcome
	submitting    atomic.Int64
	proposalDelay time.Duration
	proposalTick  <-chan time.Time
	proposalFlush bool
	prepare       chan proposalPreparation
	prepared      chan proposalPreparationResult
	preparing     []proposalRequest
	prepareActive bool
	prepareFailed bool
	evictions     map[string]struct{}
}

type proposalRequest struct {
	context  context.Context
	commands []core.Command
	response chan proposalResponse
	bounded  bool
	handoff  bool
}

type proposalResponse struct {
	results       []core.Result
	commits       []commandCommit
	appliedIndex  uint64
	committedTerm uint64
	err           error
	leader        uint64
}

type activeProposal struct {
	request             proposalRequest
	hashes              []string
	requestIDs          []string
	workflows           []string
	expectedIndex       uint64
	results             []core.Result
	commits             []commandCommit
	duplicate           []bool
	done                []bool
	checkedAppliedIndex uint64
}

type proposalPreparation struct {
	planner  *core.Engine
	requests []proposalRequest
}

type preparedProposal struct {
	request   proposalRequest
	prepared  scheduler.PreparedBatch
	workflows []string
	err       error
}

type proposalPreparationResult struct {
	proposals []preparedProposal
}

const proposalReadyBatch = 16
const proposalWindow = 48
const proposalBatchDelay = 100 * time.Millisecond

type stepRequest struct {
	context  context.Context
	message  *pb.Message
	response chan error
}

type readRequest struct {
	context    context.Context
	workflowID string
	response   chan readResponse
}

type readResponse struct {
	state      *core.WorkflowState
	projection core.Projection
	found      bool
	err        error
	leader     uint64
}

type activeRead struct {
	request readRequest
	index   uint64
}

type executionViewRequest struct {
	response chan execution.View
}

type batchRequest struct {
	Commands []core.Command `json:"commands"`
}

type cancelRequest struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

type commandResponse struct {
	Results       []core.Result   `json:"results,omitempty"`
	Commits       []commandCommit `json:"commits,omitempty"`
	LeaderID      uint64          `json:"leader_id,omitempty"`
	AppliedIndex  uint64          `json:"applied_index,omitempty"`
	CommittedTerm uint64          `json:"committed_term,omitempty"`
	Code          string          `json:"code,omitempty"`
	Message       string          `json:"message,omitempty"`
}

type commandCommit struct {
	RequestID     string `json:"request_id"`
	AppliedIndex  uint64 `json:"applied_index"`
	CommittedTerm uint64 `json:"committed_term"`
}

type workflowResponse struct {
	State      *core.WorkflowState `json:"state,omitempty"`
	Projection core.Projection     `json:"projection"`
	Found      bool                `json:"found"`
	LeaderID   uint64              `json:"leader_id,omitempty"`
	Code       string              `json:"code,omitempty"`
	Message    string              `json:"message,omitempty"`
}

func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	if config.ID < 1 || config.ID > 3 || (config.Listen == "" && config.Listener == nil) || len(config.Peers) != 3 {
		return nil, fmt.Errorf("invalid runtime configuration")
	}
	for id := uint64(1); id <= 3; id++ {
		if config.Peers[id] == "" {
			return nil, fmt.Errorf("invalid runtime configuration")
		}
	}
	if config.TickInterval == 0 {
		config.TickInterval = 100 * time.Millisecond
	}
	if config.TickInterval < 0 {
		return nil, fmt.Errorf("invalid tick interval")
	}
	if config.Now == nil {
		config.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if config.Scheduler.Global.Workflows == 0 {
		config.Scheduler = scheduler.DefaultConfig()
	}
	scheduler, err := scheduler.New(config.Scheduler)
	if err != nil {
		return nil, err
	}
	if config.MaxPending <= 0 {
		config.MaxPending = 1024
	}
	if config.Observability == nil {
		provider, err := observability.New(context.Background(), observability.Config{InstanceID: strconv.FormatUint(config.ID, 10)})
		if err != nil {
			return nil, err
		}
		config.Observability = provider
	}
	reports := make(chan transportReport, 4096)
	transport, err := newHTTPTransport(config.ID, config.Peers, reports, config.Observability)
	if err != nil {
		config.Observability.Close(context.Background())
		return nil, err
	}
	coordinator, err := Open(Config{
		ID: config.ID, ClusterID: config.ClusterID, Directory: config.Directory,
		Voters: []uint64{1, 2, 3}, Transport: transport, Failpoint: config.Failpoint, Observability: config.Observability,
	})
	if err != nil {
		config.Observability.Close(context.Background())
		return nil, err
	}
	runtime := &Runtime{
		config: config, coordinator: coordinator, transport: transport,
		admissions: make(chan proposalRequest), proposals: make(chan proposalRequest, config.MaxPending+1), steps: make(chan stepRequest, 4096),
		reads: make(chan readRequest, 1024), snapshots: make(chan chan error), statuses: make(chan chan Status),
		reports: reports, done: make(chan struct{}), terminal: make(chan error, 1), pendingReads: make(map[string]*activeRead),
		scheduler: scheduler, views: make(chan executionViewRequest, 256), pending: make(chan struct{}, config.MaxPending),
		observability: config.Observability, eventSignal: make(chan struct{}),
		proposalDelay: proposalBatchDelay, prepare: make(chan proposalPreparation, 1), prepared: make(chan proposalPreparationResult, 1),
	}
	runtime.eventIndex = coordinator.Status().AppliedIndex
	if snapshot, err := coordinator.Store().Snapshot(); err == nil {
		runtime.snapshotIndex = snapshot.GetMetadata().GetIndex()
	}
	manager, err := execution.New(execution.Config{
		Scheduler: scheduler, View: runtime.executionView, Submit: runtime.executionSubmit,
		Now: config.Now, LeaseMillis: config.LeaseMillis, TickInterval: config.WorkerTick,
		MaxCredits: config.MaxCredits, OutboundBuffer: config.WorkerBuffer,
		MaxDispatch: config.MaxDispatch, AckFailpoint: config.AckFailpoint, Observability: config.Observability,
	})
	if err != nil {
		coordinator.Close()
		config.Observability.Close(context.Background())
		return nil, err
	}
	runtime.execution = manager
	return runtime, nil
}

func (r *Runtime) Run(parent context.Context) error {
	defer r.observability.Close(context.Background())
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	listener := r.config.Listener
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", r.config.Listen)
		if err != nil {
			r.coordinator.Close()
			return err
		}
	}
	r.transport.start(ctx)
	go r.loop(ctx)
	go r.execution.Run(ctx)
	server := &http.Server{
		Handler: r.handler(), ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.Serve(listener)
	}()
	var workerServer *grpc.Server
	var workerErrors <-chan error
	if r.config.WorkerListen != "" || r.config.WorkerListener != nil {
		workerListener := r.config.WorkerListener
		if workerListener == nil {
			var err error
			workerListener, err = net.Listen("tcp", r.config.WorkerListen)
			if err != nil {
				cancel()
				server.Close()
				<-r.done
				return err
			}
		}
		workerServer = grpc.NewServer(
			grpc.MaxRecvMsgSize(8<<20), grpc.MaxSendMsgSize(8<<20),
			grpc.StatsHandler(r.observability.GRPCServerHandler()),
		)
		r.execution.Register(workerServer)
		errors := make(chan error, 1)
		workerErrors = errors
		go func() {
			errors <- workerServer.Serve(workerListener)
		}()
	}
	var runError error
	select {
	case <-parent.Done():
	case runError = <-r.terminal:
	case serverErr := <-serverErrors:
		if !errors.Is(serverErr, http.ErrServerClosed) {
			runError = serverErr
		}
	case workerErr := <-workerErrors:
		runError = workerErr
	}
	cancel()
	server.Close()
	if workerServer != nil {
		workerServer.Stop()
	}
	<-r.done
	return runError
}

func (r *Runtime) loop(ctx context.Context) {
	ticker := time.NewTicker(r.config.TickInterval)
	defer ticker.Stop()
	defer close(r.done)
	defer r.coordinator.Close()
	if r.prepared != nil {
		prepareContext, cancel := context.WithCancel(ctx)
		finished := make(chan struct{})
		// Prepare proposals off-loop against a cloned planner
		go func() {
			defer close(finished)
			r.prepareProposals(prepareContext)
		}()
		defer func() {
			cancel()
			<-finished
		}()
	}
	for {
		select {
		case <-ctx.Done():
			r.failPending(ErrStopped)
			return
		case request := <-r.admissions:
			r.admitProposals(request)
		case request := <-r.proposals:
			r.enqueueProposal(request)
		case <-r.proposalTick:
			r.proposalTick = nil
			r.proposalFlush = true
		case result := <-r.prepared:
			r.finishPreparation(result)
		case request := <-r.steps:
			err := r.coordinator.StepContext(request.context, request.message)
			if request.response != nil {
				request.response <- err
			}
			if err != nil {
				r.terminate(err)
				return
			}
		case request := <-r.reads:
			r.startRead(request)
		case response := <-r.snapshots:
			response <- r.createSnapshot()
		case response := <-r.statuses:
			response <- r.statusSnapshot()
		case request := <-r.views:
			status := r.coordinator.Status()
			request.response <- execution.View{
				Leader: status.Role == raft.StateLeader, LeaderID: status.LeaderID,
				Engine: r.coordinator.Store().Engine(),
			}
		case report := <-r.reports:
			if report.snapshot {
				r.coordinator.ReportSnapshot(report.nodeID, report.success)
			} else if !report.success {
				r.coordinator.ReportUnreachable(report.nodeID)
			}
		case <-ticker.C:
			r.coordinator.Tick()
		}
		r.checkProposal(false)
		r.checkReads()
		r.startProposal()
		r.refreshObservability()
		if err := r.maybeSnapshot(); err != nil {
			r.terminate(err)
			return
		}
		if err := r.coordinator.Fatal(); err != nil {
			r.terminate(err)
			return
		}
	}
}

func (r *Runtime) terminate(err error) {
	r.failPending(err)
	select {
	case r.terminal <- err:
	default:
	}
}

func (r *Runtime) admitProposals(request proposalRequest) {
	status := r.coordinator.Status()
	for admitted := 0; admitted < proposalReadyBatch; admitted++ {
		if err := request.context.Err(); err != nil {
			r.respond(request, proposalResponse{err: err})
		} else if status.Role != raft.StateLeader {
			r.respond(request, proposalResponse{
				err: NotLeaderError{NodeID: status.NodeID, LeaderID: status.LeaderID}, leader: status.LeaderID,
			})
		} else {
			// Pending tokens bound admitted waiters
			select {
			case r.pending <- struct{}{}:
				request.bounded = true
				r.enqueueProposal(request)
			default:
				r.respond(request, proposalResponse{err: scheduler.ErrBackpressure, leader: status.LeaderID})
			}
		}
		if status.Role == raft.StateLeader && len(r.pending) == cap(r.pending) {
			return
		}
		if admitted+1 == proposalReadyBatch {
			return
		}
		select {
		case request = <-r.admissions:
		default:
			return
		}
	}
}

func (r *Runtime) startProposal() {
	if len(r.active) == 0 && !r.prepareActive {
		r.coordinator.TakeOutcomes()
		r.outcomes = nil
	}
	r.collectProposals()
	r.dropCanceledProposals()
	if r.prepareActive {
		return
	}
	if len(r.queue) == 0 || len(r.active)+len(r.preparing) >= proposalWindow {
		if len(r.queue) == 0 {
			r.proposalTick = nil
			r.proposalFlush = false
		}
		return
	}
	status := r.coordinator.Status()
	if status.Role != raft.StateLeader {
		request := r.queue[0]
		r.queue = r.queue[1:]
		r.respond(request, proposalResponse{err: NotLeaderError{NodeID: status.NodeID, LeaderID: status.LeaderID}, leader: status.LeaderID})
		return
	}
	limit := min(proposalReadyBatch, proposalWindow-len(r.active)-len(r.preparing))
	if r.proposalDelay > 0 && len(r.queue) < limit && !r.proposalFlush {
		if r.proposalTick == nil {
			r.proposalTick = time.After(r.proposalDelay)
		}
		return
	}
	r.proposalTick = nil
	r.proposalFlush = false
	if r.planner == nil {
		last, err := r.coordinator.Store().LastIndex()
		applied := r.coordinator.Store().Applied()
		if err != nil || applied.Term != status.Term || applied.Index != last {
			return
		}
		r.planner = r.coordinator.Store().Engine()
		r.pipelineTerm = status.Term
		r.pipelineLast = last
	} else if !r.pipelineValid(status) {
		r.failPipeline(status.LeaderID)
		return
	}
	if r.prepared != nil {
		r.startPreparation(status, limit)
		return
	}
	staged := make([]stagedBatch, 0, proposalReadyBatch)
	for len(staged) < proposalReadyBatch && len(r.active) < proposalWindow {
		r.collectProposals()
		r.dropCanceledProposals()
		if len(r.queue) == 0 {
			break
		}
		request := r.queue[0]
		if err := r.coordinator.checkStagedProposal(request.commands); err != nil {
			r.queue = r.queue[1:]
			r.respond(request, proposalResponse{err: err, leader: status.LeaderID})
			continue
		}
		if err := r.checkPipelineRequests(request.commands); err != nil {
			r.queue = r.queue[1:]
			r.respond(request, proposalResponse{err: err, leader: status.LeaderID})
			continue
		}
		prepared, err := r.scheduler.StageBatch(r.planner, request.commands)
		if err != nil {
			r.queue = r.queue[1:]
			r.respond(request, proposalResponse{err: err, leader: status.LeaderID})
			continue
		}
		current, err := r.newActiveProposal(request, prepared, nil, status.AppliedIndex)
		if err != nil {
			r.queue = r.queue[1:]
			r.respond(request, proposalResponse{err: err, leader: status.LeaderID})
			r.failPipeline(status.LeaderID)
			return
		}
		if len(prepared.Batches) == 0 {
			r.queue = r.queue[1:]
			r.finishProposal(current, status.LeaderID)
			continue
		}
		current.expectedIndex = r.pipelineLast + uint64(len(staged)) + 1
		r.queue = r.queue[1:]
		for _, workflowID := range current.workflows {
			r.planner.CompactTerminal(workflowID)
		}
		r.active = append(r.active, current)
		staged = append(staged, stagedBatch{commands: request.commands, batches: prepared.Batches})
	}
	if len(staged) > 0 {
		if err := r.coordinator.stagePreparedGroup(staged); err != nil {
			r.failPipeline(status.LeaderID)
			return
		}
		r.pipelineLast += uint64(len(staged))
		if err := r.coordinator.ProcessReady(); err != nil {
			r.failPipeline(status.LeaderID)
			return
		}
		status = r.coordinator.Status()
		if !r.pipelineValid(status) {
			r.failPipeline(status.LeaderID)
			return
		}
	}
	r.checkProposal(false)
	if len(r.active) == 0 {
		r.resetPipeline()
	}
}

func (r *Runtime) startPreparation(status Status, limit int) {
	r.preparing = r.preparing[:0]
	for len(r.preparing) < limit && len(r.active)+len(r.preparing) < proposalWindow {
		r.collectProposals()
		r.dropCanceledProposals()
		if len(r.queue) == 0 {
			break
		}
		request := r.queue[0]
		r.queue = r.queue[1:]
		if err := r.coordinator.checkStagedProposal(request.commands); err != nil {
			r.respond(request, proposalResponse{err: err, leader: status.LeaderID})
			continue
		}
		r.preparing = append(r.preparing, request)
	}
	if len(r.preparing) == 0 {
		if len(r.active) == 0 {
			r.resetPipeline()
		}
		return
	}
	r.prepareActive = true
	r.prepare <- proposalPreparation{planner: r.planner, requests: r.preparing}
}

func (r *Runtime) prepareProposals(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case preparation := <-r.prepare:
			result := r.prepareProposalGroup(preparation)
			select {
			case r.prepared <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *Runtime) prepareProposalGroup(preparation proposalPreparation) proposalPreparationResult {
	proposals := make([]preparedProposal, 0, len(preparation.requests))
	for _, request := range preparation.requests {
		proposal := preparedProposal{request: request}
		if err := request.context.Err(); err != nil {
			proposal.err = err
		} else if err := checkPreparedPipelineRequests(r.coordinator.Store(), preparation.planner, request.commands); err != nil {
			proposal.err = err
		} else {
			proposal.prepared, proposal.err = r.scheduler.StageBatch(preparation.planner, request.commands)
			if proposal.err == nil && len(proposal.prepared.Batches) > 0 {
				proposal.workflows = proposalWorkflowIDs(request.commands, proposal.prepared.Batches)
				for _, workflowID := range proposal.workflows {
					preparation.planner.CompactTerminal(workflowID)
				}
			}
		}
		proposals = append(proposals, proposal)
	}
	return proposalPreparationResult{proposals: proposals}
}

func checkPreparedPipelineRequests(store *storage.Store, planner *core.Engine, commands []core.Command) error {
	pending := false
	for _, command := range commands {
		storedHash, exists := planner.ResidentRequestHash(command.RequestID)
		if !exists {
			continue
		}
		hash, err := core.HashCommand(command)
		if err != nil {
			return err
		}
		if storedHash != hash {
			return core.ErrRequestConflict
		}
		receipt, found, err := store.Receipt(command.RequestID)
		if err != nil {
			return err
		}
		if found {
			if receipt.RequestHash != hash {
				return core.ErrRequestConflict
			}
			continue
		}
		outcome, found, err := store.Outcome(command.RequestID, hash)
		if err != nil {
			return err
		}
		if found && outcome.Code != "" {
			return core.ErrRequestConflict
		}
		pending = pending || !found
	}
	if pending {
		return ErrUnavailable
	}
	return nil
}

func (r *Runtime) finishPreparation(result proposalPreparationResult) {
	r.prepareActive = false
	if r.prepareFailed {
		r.prepareFailed = false
		r.preparing = nil
		r.resetPipeline()
		return
	}
	status := r.coordinator.Status()
	if !r.pipelineValid(status) {
		r.failPipeline(status.LeaderID)
		return
	}
	r.preparing = nil
	staged := make([]stagedBatch, 0, len(result.proposals))
	for index, proposal := range result.proposals {
		if proposal.err != nil {
			r.respond(proposal.request, proposalResponse{err: proposal.err, leader: status.LeaderID})
			continue
		}
		current, err := r.newActiveProposal(proposal.request, proposal.prepared, proposal.workflows, status.AppliedIndex)
		if err != nil {
			r.respond(proposal.request, proposalResponse{err: err, leader: status.LeaderID})
			remaining := make([]proposalRequest, 0, len(result.proposals)-index-1)
			for _, pending := range result.proposals[index+1:] {
				remaining = append(remaining, pending.request)
			}
			r.queue = append(remaining, r.queue...)
			r.failPipeline(status.LeaderID)
			return
		}
		if len(proposal.prepared.Batches) == 0 {
			r.finishProposal(current, status.LeaderID)
			continue
		}
		current.expectedIndex = r.pipelineLast + uint64(len(staged)) + 1
		r.active = append(r.active, current)
		staged = append(staged, stagedBatch{commands: proposal.request.commands, batches: proposal.prepared.Batches})
	}
	if len(staged) > 0 {
		if err := r.coordinator.stagePreparedGroup(staged); err != nil {
			r.failPipeline(status.LeaderID)
			return
		}
		r.pipelineLast += uint64(len(staged))
		if err := r.coordinator.ProcessReady(); err != nil {
			r.failPipeline(status.LeaderID)
			return
		}
		status = r.coordinator.Status()
		if !r.pipelineValid(status) {
			r.failPipeline(status.LeaderID)
			return
		}
	}
	r.evictCommittedPlannerWorkflows(nil)
	r.checkProposal(false)
	if len(r.active) == 0 {
		r.resetPipeline()
	}
}

func (r *Runtime) checkPipelineRequests(commands []core.Command) error {
	pending := false
	for _, command := range commands {
		storedHash, exists := r.planner.ResidentRequestHash(command.RequestID)
		if !exists {
			continue
		}
		hash, err := core.HashCommand(command)
		if err != nil {
			return err
		}
		if storedHash != hash {
			return core.ErrRequestConflict
		}
		_, _, found, err := r.lookup(command.RequestID, hash)
		if err != nil {
			return err
		}
		pending = pending || !found
	}
	if pending {
		return ErrUnavailable
	}
	return nil
}

func (r *Runtime) checkProposal(force bool) {
	r.captureOutcomes()
	if len(r.active) == 0 {
		r.outcomes = nil
		if r.prepareActive && !r.prepareFailed {
			status := r.coordinator.Status()
			if !r.pipelineValid(status) {
				r.failPipeline(status.LeaderID)
			}
		}
		return
	}
	status := r.coordinator.Status()
	if !r.pipelineValid(status) {
		r.failPipeline(status.LeaderID)
		return
	}
	remaining := r.active[:0]
	var committedWorkflows map[string]struct{}
	for proposalIndex, proposal := range r.active {
		if !proposal.shouldLookup(status.AppliedIndex, force) {
			remaining = append(remaining, proposal)
			continue
		}
		complete := true
		for index, requestID := range proposal.requestIDs {
			if proposal.done[index] {
				continue
			}
			result, commit, found, err := r.lookup(requestID, proposal.hashes[index])
			if err != nil {
				r.respond(proposal.request, proposalResponse{err: err, leader: status.LeaderID})
				r.active = append(remaining, r.active[proposalIndex+1:]...)
				r.failPipeline(status.LeaderID)
				return
			}
			if found {
				result.Duplicate = proposal.duplicate[index]
				proposal.results[index] = result
				proposal.commits[index] = commit
				proposal.done[index] = true
			} else {
				complete = false
			}
		}
		if complete {
			r.finishProposal(proposal, status.LeaderID)
			if committedWorkflows == nil {
				committedWorkflows = make(map[string]struct{})
			}
			for _, workflowID := range proposal.workflows {
				committedWorkflows[workflowID] = struct{}{}
			}
		} else {
			remaining = append(remaining, proposal)
		}
	}
	r.active = remaining
	r.evictCommittedPlannerWorkflows(committedWorkflows)
	if len(r.active) == 0 && !r.prepareActive {
		r.resetPipeline()
	}
}

func (r *Runtime) collectProposals() {
	for len(r.queue)+len(r.active)+len(r.preparing) < proposalWindow {
		select {
		case request := <-r.proposals:
			r.enqueueProposal(request)
		default:
			return
		}
	}
}

func (r *Runtime) enqueueProposal(request proposalRequest) {
	r.queue = append(r.queue, request)
	r.completeProposalHandoff(&r.queue[len(r.queue)-1])
}

func (r *Runtime) completeProposalHandoff(request *proposalRequest) {
	if request.handoff {
		request.handoff = false
		r.submitting.Add(-1)
	}
}

func (r *Runtime) dropCanceledProposals() {
	for len(r.queue) > 0 && r.queue[0].context.Err() != nil {
		request := r.queue[0]
		r.queue = r.queue[1:]
		r.respond(request, proposalResponse{err: request.context.Err()})
	}
}

func (r *Runtime) newActiveProposal(request proposalRequest, prepared scheduler.PreparedBatch, workflows []string, appliedIndex uint64) (*activeProposal, error) {
	requestIDs := make([]string, len(request.commands))
	for index, command := range request.commands {
		requestIDs[index] = command.RequestID
	}
	if workflows == nil {
		workflows = proposalWorkflowIDs(request.commands, prepared.Batches)
	}
	proposal := &activeProposal{
		request: request, hashes: prepared.Hashes, requestIDs: requestIDs, workflows: workflows,
		results: make([]core.Result, len(request.commands)),
		commits: make([]commandCommit, len(request.commands)), duplicate: make([]bool, len(request.commands)),
		done: make([]bool, len(request.commands)), checkedAppliedIndex: appliedIndex,
	}
	for index, command := range request.commands {
		if _, duplicate := prepared.Immediate[command.RequestID]; !duplicate {
			continue
		}
		result, commit, found, err := r.lookup(command.RequestID, prepared.Hashes[index])
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, ErrUnavailable
		}
		result.Duplicate = true
		proposal.results[index] = result
		proposal.commits[index] = commit
		proposal.duplicate[index] = true
		proposal.done[index] = true
	}
	proposal.request.commands = nil
	return proposal, nil
}

func proposalWorkflowIDs(commands []core.Command, batches []core.CommandBatch) []string {
	seen := make(map[string]struct{})
	workflows := make([]string, 0, len(commands))
	add := func(workflowID string) {
		if workflowID == "" {
			return
		}
		if _, exists := seen[workflowID]; exists {
			return
		}
		seen[workflowID] = struct{}{}
		workflows = append(workflows, workflowID)
	}
	for _, command := range commands {
		workflowID := command.WorkflowID
		if command.Kind == core.CommandSubmit && workflowID == "" && command.Definition != nil {
			workflowID = core.WorkflowID(command.Definition.Namespace, command.RequestID)
		}
		add(workflowID)
	}
	for _, batch := range batches {
		add(batch.WorkflowID)
	}
	return workflows
}

func (r *Runtime) evictCommittedPlannerWorkflows(workflows map[string]struct{}) {
	if len(workflows) > 0 {
		if r.evictions == nil {
			r.evictions = make(map[string]struct{})
		}
		for workflowID := range workflows {
			r.evictions[workflowID] = struct{}{}
		}
	}
	if r.planner == nil || r.prepareActive || len(r.evictions) == 0 {
		return
	}
	for _, proposal := range r.active {
		for _, workflowID := range proposal.workflows {
			delete(r.evictions, workflowID)
		}
	}
	for workflowID := range r.evictions {
		r.planner.EvictTerminal(workflowID)
	}
	r.evictions = nil
}

func (r *Runtime) finishProposal(proposal *activeProposal, leader uint64) {
	appliedIndex, committedTerm := latestCommit(proposal.commits)
	r.respond(proposal.request, proposalResponse{
		results: proposal.results, commits: proposal.commits, leader: leader,
		appliedIndex: appliedIndex, committedTerm: committedTerm,
	})
}

func (r *Runtime) pipelineValid(status Status) bool {
	// Prepared work belongs to this term and log tail.
	if status.Role != raft.StateLeader || status.Term != r.pipelineTerm {
		return false
	}
	last, err := r.coordinator.Store().LastIndex()
	return err == nil && last == r.pipelineLast
}

func (r *Runtime) failPipeline(leader uint64) {
	for _, proposal := range r.active {
		r.respond(proposal.request, proposalResponse{err: ErrUnavailable, leader: leader})
	}
	for _, request := range r.preparing {
		r.respond(request, proposalResponse{err: ErrUnavailable, leader: leader})
	}
	r.active = nil
	r.preparing = nil
	if r.prepareActive {
		r.prepareFailed = true
		r.pipelineTerm = 0
		r.pipelineLast = 0
		r.evictions = nil
		return
	}
	r.resetPipeline()
}

func (r *Runtime) resetPipeline() {
	r.planner = nil
	r.pipelineTerm = 0
	r.pipelineLast = 0
	r.evictions = nil
}

func (p *activeProposal) shouldLookup(appliedIndex uint64, force bool) bool {
	if !force && (appliedIndex < p.expectedIndex || appliedIndex <= p.checkedAppliedIndex) {
		return false
	}
	p.checkedAppliedIndex = appliedIndex
	return true
}

func (r *Runtime) lookup(requestID, requestHash string) (core.Result, commandCommit, bool, error) {
	if outcome, exists := r.outcomes[requestID]; exists {
		delete(r.outcomes, requestID)
		if outcome.RequestHash != requestHash || outcome.Code != "" {
			return core.Result{}, commandCommit{}, false, core.ErrRequestConflict
		}
		return outcome.Result, commandCommit{
			RequestID: requestID, AppliedIndex: outcome.AppliedIndex, CommittedTerm: outcome.CommittedTerm,
		}, true, nil
	}
	receipt, exists, err := r.coordinator.Store().Receipt(requestID)
	if err != nil {
		return core.Result{}, commandCommit{}, false, err
	}
	if exists {
		if receipt.RequestHash != requestHash {
			return core.Result{}, commandCommit{}, false, core.ErrRequestConflict
		}
		return receipt.Result, commandCommit{
			RequestID: requestID, AppliedIndex: receipt.AppliedIndex, CommittedTerm: receipt.CommittedTerm,
		}, true, nil
	}
	outcome, exists, err := r.coordinator.Store().Outcome(requestID, requestHash)
	if err != nil {
		return core.Result{}, commandCommit{}, false, err
	}
	if exists && outcome.Code != "" {
		return core.Result{}, commandCommit{}, false, core.ErrRequestConflict
	}
	return core.Result{}, commandCommit{}, false, nil
}

func (r *Runtime) captureOutcomes() {
	for _, outcome := range r.coordinator.TakeOutcomes() {
		if r.outcomes == nil {
			r.outcomes = make(map[string]storage.Outcome)
		}
		r.outcomes[outcome.RequestID] = outcome
	}
}

func latestCommit(commits []commandCommit) (uint64, uint64) {
	var index uint64
	var term uint64
	for _, commit := range commits {
		if commit.AppliedIndex >= index {
			index = commit.AppliedIndex
			term = commit.CommittedTerm
		}
	}
	return index, term
}

func (r *Runtime) startRead(request readRequest) {
	status := r.coordinator.Status()
	if status.Role != raft.StateLeader {
		request.response <- readResponse{err: NotLeaderError{NodeID: status.NodeID, LeaderID: status.LeaderID}, leader: status.LeaderID}
		return
	}
	r.readCounter++
	key := fmt.Sprintf("%d:%d", status.NodeID, r.readCounter)
	r.pendingReads[key] = &activeRead{request: request}
	if err := r.coordinator.ReadIndexContext(request.context, []byte(key)); err != nil {
		delete(r.pendingReads, key)
		request.response <- readResponse{err: err, leader: status.LeaderID}
	}
}

func (r *Runtime) checkReads() {
	for _, state := range r.coordinator.TakeReadStates() {
		if pending, exists := r.pendingReads[string(state.RequestCtx)]; exists {
			pending.index = state.Index
		}
	}
	status := r.coordinator.Status()
	for key, pending := range r.pendingReads {
		// Serve the read after applying its quorum index
		if pending.index > 0 && status.AppliedIndex >= pending.index {
			state, found := r.coordinator.Store().State(pending.request.workflowID)
			projection, _ := r.coordinator.Store().Projection(pending.request.workflowID)
			pending.request.response <- readResponse{state: state, projection: projection, found: found, leader: status.LeaderID}
			delete(r.pendingReads, key)
		} else if status.Role != raft.StateLeader {
			pending.request.response <- readResponse{err: ErrUnavailable, leader: status.LeaderID}
			delete(r.pendingReads, key)
		}
	}
}

func (r *Runtime) failPending(err error) {
	for _, proposal := range r.active {
		r.respond(proposal.request, proposalResponse{err: err})
	}
	for _, request := range r.preparing {
		r.respond(request, proposalResponse{err: err})
	}
	r.active = nil
	r.preparing = nil
	r.prepareFailed = r.prepareActive
	r.resetPipeline()
	for _, request := range r.queue {
		r.respond(request, proposalResponse{err: err})
	}
	r.queue = nil
	r.drainProposals(err)
	for key, pending := range r.pendingReads {
		pending.request.response <- readResponse{err: err}
		delete(r.pendingReads, key)
	}
}

func (r *Runtime) drainProposals(err error) {
	for {
		select {
		case request := <-r.proposals:
			r.respond(request, proposalResponse{err: err})
		default:
			return
		}
	}
}

func (r *Runtime) respond(request proposalRequest, response proposalResponse) {
	request.response <- response
	if request.bounded {
		<-r.pending
	}
	r.completeProposalHandoff(&request)
}

func (r *Runtime) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", r.handleRaft)
	mux.HandleFunc("/v1/commands", r.handleCommand)
	mux.HandleFunc("/v1/batches", r.handleBatch)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/cancel", r.handleCancel)
	mux.HandleFunc("GET /v1/workflows/{workflowID}/events", r.handleEvents)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", r.handleWorkflow)
	mux.HandleFunc("/v1/status", r.handleStatus)
	mux.HandleFunc("/v1/snapshot", r.handleSnapshot)
	mux.HandleFunc("/healthz", r.handleHealth)
	mux.Handle("GET /metrics", r.observability.MetricsHandler())
	return r.observability.HTTPHandler("chronos.http", mux)
}

func (r *Runtime) handleRaft(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	select {
	case <-r.done:
		http.Error(writer, ErrStopped.Error(), http.StatusServiceUnavailable)
		return
	default:
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 5<<30)
	var data []byte
	var err error
	if request.ContentLength >= 0 && request.ContentLength <= maxRaftMessageSize+(64<<10) {
		data = make([]byte, request.ContentLength)
		_, err = io.ReadFull(request.Body, data)
	} else {
		data, err = io.ReadAll(request.Body)
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	message := &pb.Message{}
	if err := proto.Unmarshal(data, message); err != nil || !r.validRemoteRaftMessage(message) {
		http.Error(writer, "invalid raft message", http.StatusBadRequest)
		return
	}
	var response chan error
	if message.GetType() == pb.MsgSnap {
		response = make(chan error, 1)
	}
	select {
	case r.steps <- stepRequest{context: context.WithoutCancel(request.Context()), message: message, response: response}:
	case <-request.Context().Done():
		return
	case <-r.done:
		http.Error(writer, ErrStopped.Error(), http.StatusServiceUnavailable)
		return
	}
	if response == nil {
		select {
		case <-r.done:
			http.Error(writer, ErrStopped.Error(), http.StatusServiceUnavailable)
		default:
			writer.WriteHeader(http.StatusNoContent)
		}
		return
	}
	select {
	case err := <-response:
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case <-request.Context().Done():
	case <-r.done:
		http.Error(writer, ErrStopped.Error(), http.StatusServiceUnavailable)
	}
}

func (r *Runtime) validRemoteRaftMessage(message *pb.Message) bool {
	if message.GetTo() != r.config.ID || message.GetFrom() == r.config.ID {
		return false
	}
	if address, exists := r.config.Peers[message.GetFrom()]; !exists || address == "" {
		return false
	}
	switch message.GetType() {
	case pb.MsgApp, pb.MsgAppResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgSnap, pb.MsgHeartbeat, pb.MsgHeartbeatResp,
		pb.MsgTransferLeader, pb.MsgTimeoutNow, pb.MsgReadIndex, pb.MsgReadIndexResp, pb.MsgPreVote, pb.MsgPreVoteResp:
		return true
	default:
		return false
	}
}

func (r *Runtime) handleCommand(writer http.ResponseWriter, request *http.Request) {
	var command core.Command
	if !r.decodeRequest(writer, request, &command) {
		return
	}
	r.submitHTTP(writer, request, []core.Command{command})
}

func (r *Runtime) handleBatch(writer http.ResponseWriter, request *http.Request) {
	var batch batchRequest
	if !r.decodeRequest(writer, request, &batch) {
		return
	}
	if len(batch.Commands) == 0 || len(batch.Commands) > 256 {
		http.Error(writer, "batch must contain 1 to 256 commands", http.StatusBadRequest)
		return
	}
	seen := make(map[string]struct{}, len(batch.Commands))
	for _, command := range batch.Commands {
		if _, exists := seen[command.RequestID]; exists {
			http.Error(writer, "batch request ids must be unique", http.StatusBadRequest)
			return
		}
		seen[command.RequestID] = struct{}{}
	}
	if !r.config.LocalExecutionAPI {
		r.submitHTTP(writer, request, batch.Commands)
		return
	}
	for _, command := range batch.Commands {
		switch command.Kind {
		case core.CommandSubmit, core.CommandStart, core.CommandComplete:
		default:
			r.writeError(writer, request, core.ErrInvalidCommand, 0)
			return
		}
	}
	r.submitBounded(writer, request, batch.Commands)
}

func (r *Runtime) decodeRequest(writer http.ResponseWriter, request *http.Request, value any) bool {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return false
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 8<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(writer, "request body must contain one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}

func (r *Runtime) submitHTTP(writer http.ResponseWriter, request *http.Request, commands []core.Command) {
	for _, command := range commands {
		if command.Kind != core.CommandSubmit {
			r.writeError(writer, request, core.ErrInvalidCommand, 0)
			return
		}
	}
	r.submitBounded(writer, request, commands)
}

func (r *Runtime) submitBounded(writer http.ResponseWriter, request *http.Request, commands []core.Command) {
	response := make(chan proposalResponse, 1)
	proposal := proposalRequest{context: request.Context(), commands: commands, response: response, handoff: true}
	r.submitting.Add(1)
	select {
	case r.admissions <- proposal:
	case <-request.Context().Done():
		r.completeProposalHandoff(&proposal)
		return
	case <-r.done:
		r.completeProposalHandoff(&proposal)
		return
	}
	select {
	case result := <-response:
		if result.err != nil {
			r.writeError(writer, request, result.err, result.leader)
			return
		}
		r.writeJSON(writer, http.StatusOK, commandResponse{
			Results: result.results, Commits: result.commits, LeaderID: result.leader,
			AppliedIndex: result.appliedIndex, CommittedTerm: result.committedTerm,
		})
	case <-request.Context().Done():
	case <-r.done:
	}
}

func (r *Runtime) handleCancel(writer http.ResponseWriter, request *http.Request) {
	var body cancelRequest
	if !r.decodeRequest(writer, request, &body) {
		return
	}
	workflowID := request.PathValue("workflowID")
	if workflowID == "" || strings.TrimSpace(body.RequestID) == "" || strings.TrimSpace(body.Reason) == "" {
		http.Error(writer, "workflow id, request id, and reason are required", http.StatusBadRequest)
		return
	}
	at := r.config.Now()
	view, err := r.executionView(request.Context())
	if err != nil {
		r.writeError(writer, request, err, 0)
		return
	}
	if workflow, exists := view.Engine.State(workflowID); exists {
		at = max(at, workflow.UpdatedAt)
	}
	r.submitBounded(writer, request, []core.Command{
		{Kind: core.CommandCancel, RequestID: body.RequestID, WorkflowID: workflowID, Reason: body.Reason, At: at},
	})
}

func (r *Runtime) handleWorkflow(writer http.ResponseWriter, request *http.Request) {
	workflowID, err := url.PathUnescape(request.PathValue("workflowID"))
	if err != nil || workflowID == "" {
		http.Error(writer, "workflow id is required", http.StatusBadRequest)
		return
	}
	result, ok := r.readWorkflow(request.Context(), workflowID)
	if !ok {
		return
	}
	if result.err != nil {
		r.writeError(writer, request, result.err, result.leader)
		return
	}
	status := http.StatusOK
	if !result.found {
		status = http.StatusNotFound
	}
	r.writeJSON(writer, status, workflowResponse{
		State: result.state, Projection: result.projection, Found: result.found, LeaderID: result.leader,
	})
}

func (r *Runtime) readWorkflow(ctx context.Context, workflowID string) (readResponse, bool) {
	response := make(chan readResponse, 1)
	select {
	case r.reads <- readRequest{context: ctx, workflowID: workflowID, response: response}:
	case <-ctx.Done():
		return readResponse{}, false
	}
	select {
	case result := <-response:
		return result, true
	case <-ctx.Done():
		return readResponse{}, false
	}
}

func eventCursor(request *http.Request) (uint64, error) {
	value := request.URL.Query().Get("after")
	if value == "" {
		value = request.Header.Get("Last-Event-ID")
	}
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid event cursor")
	}
	return cursor, nil
}

func terminalWorkflowEvent(kind core.EventKind) bool {
	switch kind {
	case core.EventWorkflowCompleted, core.EventWorkflowFailed, core.EventWorkflowCancelled, core.EventWorkflowCompensated:
		return true
	default:
		return false
	}
}

func terminalWorkflowStatus(status core.WorkflowStatus) bool {
	switch status {
	case core.WorkflowCompleted, core.WorkflowFailed, core.WorkflowCancelled, core.WorkflowCompensated:
		return true
	default:
		return false
	}
}

func (r *Runtime) currentEventSignal() <-chan struct{} {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	return r.eventSignal
}

func (r *Runtime) refreshObservability() {
	applied := r.coordinator.Status().AppliedIndex
	if applied != r.eventIndex {
		r.eventMu.Lock()
		// applied-index changes wake event waiters
		close(r.eventSignal)
		r.eventSignal = make(chan struct{})
		r.eventIndex = applied
		r.eventMu.Unlock()
	}
	now := time.Now()
	if !r.lastMetrics.IsZero() && now.Sub(r.lastMetrics) < time.Second {
		return
	}
	r.lastMetrics = now
	var stats scheduler.Stats
	if err := r.coordinator.Store().Check(func(engine *core.Engine) error {
		stats = r.scheduler.Stats(engine)
		return nil
	}); err != nil {
		return
	}
	ready := map[string]int{"_all": stats.Ready}
	for _, namespace := range stats.Namespaces {
		ready[namespace.Namespace] = namespace.Ready
	}
	r.observability.SetQueue(ready, len(r.pending))
}

func (r *Runtime) maybeSnapshot() error {
	applied := r.coordinator.Status().AppliedIndex
	if !shouldSnapshot(applied, r.snapshotIndex, r.config.SnapshotEntries) {
		return nil
	}
	return r.createSnapshot()
}

func (r *Runtime) createSnapshot() error {
	snapshot, err := r.coordinator.CreateSnapshot()
	if errors.Is(err, raft.ErrSnapOutOfDate) {
		r.snapshotIndex = r.coordinator.Store().SnapshotIndex()
		return nil
	}
	if err != nil {
		return err
	}
	r.snapshotIndex = snapshot.GetMetadata().GetIndex()
	return nil
}

func shouldSnapshot(applied, snapshot, threshold uint64) bool {
	return threshold > 0 && applied >= snapshot && applied-snapshot >= threshold
}

func (r *Runtime) handleEvents(writer http.ResponseWriter, request *http.Request) {
	status := r.status(request.Context())
	if status.Role != raft.StateLeader {
		r.writeError(writer, request, NotLeaderError{NodeID: status.NodeID, LeaderID: status.LeaderID}, status.LeaderID)
		return
	}
	workflowID, err := url.PathUnescape(request.PathValue("workflowID"))
	if err != nil || workflowID == "" {
		http.Error(writer, "workflow id is required", http.StatusBadRequest)
		return
	}
	after, err := eventCursor(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	follow, err := strconv.ParseBool(request.URL.Query().Get("follow"))
	if request.URL.Query().Get("follow") == "" {
		follow = false
		err = nil
	}
	if err != nil {
		http.Error(writer, "follow must be true or false", http.StatusBadRequest)
		return
	}
	read, ok := r.readWorkflow(request.Context(), workflowID)
	if !ok {
		return
	}
	if read.err != nil {
		r.writeError(writer, request, read.err, read.leader)
		return
	}
	if !read.found {
		http.Error(writer, "workflow not found", http.StatusNotFound)
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming is unavailable", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	for {
		if current := r.status(request.Context()); current.Role != raft.StateLeader {
			return
		}
		signal := r.currentEventSignal()
		events, err := r.coordinator.Store().Events(workflowID)
		if err != nil {
			return
		}
		terminal := false
		for _, event := range events {
			if event.Sequence <= after {
				continue
			}
			data, err := json.Marshal(event)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(writer, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Kind, data); err != nil {
				return
			}
			after = event.Sequence
			terminal = terminalWorkflowEvent(event.Kind)
		}
		flusher.Flush()
		if !follow || terminal || terminalWorkflowStatus(read.state.Status) {
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-signal:
		case <-time.After(15 * time.Second):
			if _, err := io.WriteString(writer, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (r *Runtime) handleStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	status := r.status(request.Context())
	status.PID = os.Getpid()
	r.writeJSON(writer, http.StatusOK, status)
}

func (r *Runtime) handleSnapshot(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	status := r.status(request.Context())
	if status.Role != raft.StateLeader {
		r.writeError(writer, request, NotLeaderError{NodeID: status.NodeID, LeaderID: status.LeaderID}, status.LeaderID)
		return
	}
	response := make(chan error, 1)
	select {
	case r.snapshots <- response:
	case <-request.Context().Done():
		return
	}
	select {
	case err := <-response:
		if err != nil {
			r.writeError(writer, request, err, status.LeaderID)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	case <-request.Context().Done():
	}
}

func (r *Runtime) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	status := r.status(request.Context())
	r.writeJSON(writer, http.StatusOK, map[string]any{"status": "ok", "node_id": status.NodeID})
}

func (r *Runtime) status(context context.Context) Status {
	response := make(chan Status, 1)
	select {
	case r.statuses <- response:
	case <-context.Done():
		return Status{}
	}
	select {
	case status := <-response:
		return status
	case <-context.Done():
		return Status{}
	}
}

func (r *Runtime) statusSnapshot() Status {
	status := r.coordinator.Status()
	status.PendingProposals = int(r.submitting.Load()) + len(r.pending)
	for _, request := range r.queue {
		if !request.bounded {
			status.PendingProposals++
		}
	}
	for _, proposal := range r.active {
		if !proposal.request.bounded {
			status.PendingProposals++
		}
	}
	for _, request := range r.preparing {
		if !request.bounded {
			status.PendingProposals++
		}
	}
	return status
}

func (r *Runtime) executionView(context context.Context) (execution.View, error) {
	response := make(chan execution.View, 1)
	select {
	case r.views <- executionViewRequest{response: response}:
	case <-context.Done():
		return execution.View{}, context.Err()
	case <-r.done:
		return execution.View{}, ErrStopped
	}
	select {
	case view := <-response:
		return view, nil
	case <-context.Done():
		return execution.View{}, context.Err()
	case <-r.done:
		return execution.View{}, ErrStopped
	}
}

func (r *Runtime) executionSubmit(context context.Context, command core.Command) (core.Result, error) {
	response := make(chan proposalResponse, 1)
	proposal := proposalRequest{context: context, commands: []core.Command{command}, response: response, handoff: true}
	r.submitting.Add(1)
	select {
	case r.proposals <- proposal:
	case <-context.Done():
		r.completeProposalHandoff(&proposal)
		return core.Result{}, context.Err()
	case <-r.done:
		r.completeProposalHandoff(&proposal)
		return core.Result{}, ErrStopped
	}
	select {
	case result := <-response:
		if result.err != nil {
			return core.Result{}, result.err
		}
		if len(result.results) != 1 {
			return core.Result{}, ErrUnavailable
		}
		return result.results[0], nil
	case <-context.Done():
		return core.Result{}, context.Err()
	case <-r.done:
		return core.Result{}, ErrStopped
	}
}

func (r *Runtime) writeError(writer http.ResponseWriter, request *http.Request, err error, leaderID uint64) {
	var notLeader NotLeaderError
	if errors.As(err, &notLeader) {
		if notLeader.LeaderID != 0 {
			writer.Header().Set("Location", strings.TrimRight(r.config.Peers[notLeader.LeaderID], "/")+request.URL.RequestURI())
			r.writeJSON(writer, http.StatusTemporaryRedirect, commandResponse{LeaderID: notLeader.LeaderID, Code: "not_leader", Message: err.Error()})
			return
		}
		r.writeJSON(writer, http.StatusServiceUnavailable, commandResponse{Code: "no_leader", Message: err.Error()})
		return
	}
	switch {
	case errors.Is(err, core.ErrRequestConflict), errors.Is(err, core.ErrWorkflowExists):
		r.writeJSON(writer, http.StatusConflict, commandResponse{LeaderID: leaderID, Code: "request_conflict", Message: err.Error()})
	case errors.Is(err, core.ErrInvalidCommand), errors.Is(err, core.ErrInvalidDefinition), errors.Is(err, core.ErrInvalidTransition):
		r.writeJSON(writer, http.StatusBadRequest, commandResponse{LeaderID: leaderID, Code: "invalid_command", Message: err.Error()})
	case errors.Is(err, core.ErrWorkflowNotFound), errors.Is(err, core.ErrTimeRegression), errors.Is(err, core.ErrTimeOverflow):
		r.writeJSON(writer, http.StatusBadRequest, commandResponse{LeaderID: leaderID, Code: "invalid_command", Message: err.Error()})
	case errors.Is(err, scheduler.ErrBackpressure):
		r.writeJSON(writer, http.StatusTooManyRequests, commandResponse{LeaderID: leaderID, Code: "backpressure", Message: err.Error()})
	default:
		r.writeJSON(writer, http.StatusServiceUnavailable, commandResponse{LeaderID: leaderID, Code: "unavailable", Message: err.Error()})
	}
}

func (r *Runtime) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	json.NewEncoder(writer).Encode(value)
}
