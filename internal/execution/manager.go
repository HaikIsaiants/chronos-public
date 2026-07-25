package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	chronosv1 "github.com/HaikIsaiants/chronos-public/api/chronos/v1"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/observability"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type View struct {
	Leader   bool
	LeaderID uint64
	Engine   *core.Engine
}

type ViewFunc func(context.Context) (View, error)
type SubmitFunc func(context.Context, core.Command) (core.Result, error)
type AckFailpoint func(string) error

type Config struct {
	Scheduler      *scheduler.Scheduler
	View           ViewFunc
	Submit         SubmitFunc
	Now            func() int64
	LeaseMillis    int64
	TickInterval   time.Duration
	MaxCredits     uint32
	OutboundBuffer int
	MaxDispatch    int
	AckFailpoint   AckFailpoint
	Observability  *observability.Provider
}

type Manager struct {
	chronosv1.UnimplementedWorkerServiceServer
	config   Config
	open     chan openRequest
	messages chan messageRequest
	closed   chan closeRequest
	trigger  chan chan struct{}
	done     chan struct{}
	sessions map[string]*session
	sequence uint64
}

type openRequest struct {
	ctx      context.Context
	hello    *chronosv1.WorkerHello
	response chan openResponse
}

type openResponse struct {
	session *session
	err     error
}

type messageRequest struct {
	ctx       context.Context
	sessionID uint64
	workerID  string
	message   *chronosv1.WorkerMessage
	response  chan error
}

type closeRequest struct {
	sessionID uint64
	workerID  string
	done      chan struct{}
}

type delivery struct {
	message *chronosv1.CoordinatorMessage
}

type session struct {
	id           uint64
	workerID     string
	capabilities []string
	capability   map[string]struct{}
	capacity     uint32
	inflight     map[string]struct{}
	outbound     chan delivery
	stopped      chan error
}

func New(config Config) (*Manager, error) {
	if config.Scheduler == nil || config.View == nil || config.Submit == nil {
		return nil, fmt.Errorf("scheduler, view, and submit are required")
	}
	if config.Now == nil {
		config.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if config.LeaseMillis <= 0 {
		config.LeaseMillis = 30000
	}
	if config.TickInterval <= 0 {
		config.TickInterval = 100 * time.Millisecond
	}
	if config.MaxCredits == 0 {
		config.MaxCredits = 1024
	}
	if config.OutboundBuffer <= 0 {
		config.OutboundBuffer = int(config.MaxCredits)
	}
	if config.OutboundBuffer < int(config.MaxCredits) {
		return nil, fmt.Errorf("outbound buffer must cover maximum credits")
	}
	if config.MaxDispatch <= 0 {
		config.MaxDispatch = 256
	}
	return &Manager{
		config: config, open: make(chan openRequest, 256), messages: make(chan messageRequest, 1024),
		closed: make(chan closeRequest, 256), trigger: make(chan chan struct{}), done: make(chan struct{}),
		sessions: make(map[string]*session),
	}, nil
}

func (m *Manager) Register(server *grpc.Server) {
	chronosv1.RegisterWorkerServiceServer(server, m)
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.config.TickInterval)
	defer ticker.Stop()
	defer close(m.done)
	defer m.stopAll(status.Error(codes.Unavailable, "worker service stopped"))
	for {
		var triggered chan struct{}
		select {
		case <-ctx.Done():
			return
		case request := <-m.open:
			m.handleOpen(request.ctx, request)
		case request := <-m.messages:
			request.response <- m.handleMessage(request.ctx, request)
		case request := <-m.closed:
			m.handleClose(request)
		case triggered = <-m.trigger:
		case <-ticker.C:
		}
		m.dispatch(ctx)
		if triggered != nil {
			close(triggered)
		}
	}
}

func (m *Manager) Trigger(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case m.trigger <- done:
	case <-m.done:
		return status.Error(codes.Unavailable, "worker service stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-m.done:
		return status.Error(codes.Unavailable, "worker service stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) handleOpen(ctx context.Context, request openRequest) {
	workerID := strings.TrimSpace(request.hello.GetWorkerId())
	capabilities, capability, err := normalizeCapabilities(request.hello.GetCapabilities())
	if err == nil && workerID == "" {
		err = fmt.Errorf("worker id is required")
	}
	if err == nil && request.hello.GetCredits() > m.config.MaxCredits {
		err = fmt.Errorf("credits exceed %d", m.config.MaxCredits)
	}
	var view View
	if err == nil {
		view, err = m.config.View(ctx)
	}
	if err == nil && !view.Leader {
		err = status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID)
	}
	var active []core.TaskState
	var activeIDs []string
	if err == nil {
		for _, workflowID := range view.Engine.WorkflowIDs() {
			workflow, _ := view.Engine.State(workflowID)
			for _, task := range workflow.Tasks {
				if !leased(task) || task.WorkerID != workerID {
					continue
				}
				taskType := capabilityFor(task)
				if _, supported := capability[taskType]; !supported {
					err = fmt.Errorf("active task capability %s is not supported", taskType)
					break
				}
				active = append(active, task)
				activeIDs = append(activeIDs, workflowID)
			}
			if err != nil {
				break
			}
		}
	}
	if err == nil && uint32(len(active)) > request.hello.GetCredits() {
		err = fmt.Errorf("credits are below %d active leases", len(active))
	}
	if err != nil {
		request.response <- openResponse{err: err}
		return
	}
	if previous := m.sessions[workerID]; previous != nil {
		m.stopSession(previous, status.Error(codes.Aborted, "worker session replaced"))
	}
	m.sequence++
	current := &session{
		id: m.sequence, workerID: workerID, capabilities: capabilities, capability: capability,
		capacity: request.hello.GetCredits(), inflight: make(map[string]struct{}),
		outbound: make(chan delivery, m.config.OutboundBuffer), stopped: make(chan error, 1),
	}
	m.sessions[workerID] = current
	order := make([]int, len(active))
	for index := range order {
		order[index] = index
	}
	sort.Slice(order, func(i, j int) bool {
		left, right := active[order[i]], active[order[j]]
		if activeIDs[order[i]] != activeIDs[order[j]] {
			return activeIDs[order[i]] < activeIDs[order[j]]
		}
		return left.Definition.ID < right.Definition.ID
	})
	for _, index := range order {
		task := active[index]
		current.inflight[task.AttemptID] = struct{}{}
		if !m.enqueue(current, assignment(activeIDs[index], task)) {
			err = status.Error(codes.ResourceExhausted, "worker outbound queue is full")
			break
		}
	}
	if err != nil {
		m.stopSession(current, err)
		request.response <- openResponse{err: err}
		return
	}
	if m.config.Observability != nil {
		m.config.Observability.SetWorker(workerID, current.capacity, len(current.inflight))
	}
	request.response <- openResponse{session: current}
}

func (m *Manager) handleMessage(ctx context.Context, request messageRequest) error {
	current := m.sessions[request.workerID]
	if current == nil || current.id != request.sessionID {
		return status.Error(codes.Aborted, "worker session is stale")
	}
	switch {
	case request.message.GetHello() != nil:
		return status.Error(codes.InvalidArgument, "hello must be the first message")
	case request.message.GetCredit() != nil:
		credits := request.message.GetCredit().GetCredits()
		if credits > m.config.MaxCredits || int(credits) < len(current.inflight) {
			return status.Error(codes.InvalidArgument, "invalid credit limit")
		}
		current.capacity = credits
		if m.config.Observability != nil {
			m.config.Observability.SetWorker(current.workerID, current.capacity, len(current.inflight))
		}
		return nil
	case request.message.GetHeartbeat() != nil:
		return m.renew(ctx, current, request.message.GetHeartbeat())
	case request.message.GetCompletion() != nil:
		return m.complete(ctx, current, request.message.GetCompletion())
	default:
		return status.Error(codes.InvalidArgument, "worker message is empty")
	}
}

func (m *Manager) renew(ctx context.Context, current *session, heartbeat *chronosv1.LeaseHeartbeat) error {
	view, err := m.config.View(ctx)
	if err != nil {
		return err
	}
	if !view.Leader {
		return status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID)
	}
	workflow, exists := view.Engine.State(heartbeat.GetWorkflowId())
	if !exists {
		return m.ackError(current, heartbeat.GetRequestId(), core.ErrWorkflowNotFound)
	}
	at := max(m.config.Now(), workflow.UpdatedAt)
	command := core.Command{
		Kind: core.CommandRenew, RequestID: heartbeat.GetRequestId(), WorkflowID: heartbeat.GetWorkflowId(),
		TaskID: heartbeat.GetTaskId(), AttemptID: heartbeat.GetAttemptId(), WorkerID: current.workerID,
		Fence: heartbeat.GetFencingToken(), LeaseUntil: heartbeat.GetLeaseExpiresAt(), At: at,
	}
	_, _, duplicate, err := view.Engine.Prepare(command)
	if err != nil {
		return m.ackError(current, heartbeat.GetRequestId(), err)
	}
	if !duplicate && (heartbeat.GetLeaseExpiresAt() <= at || heartbeat.GetLeaseExpiresAt()-at > m.config.LeaseMillis) {
		return m.ackError(current, heartbeat.GetRequestId(), fmt.Errorf("%w: invalid lease extension", core.ErrInvalidCommand))
	}
	result, err := m.config.Submit(ctx, command)
	if err != nil {
		return m.ackError(current, heartbeat.GetRequestId(), err)
	}
	return m.ack(current, "renew", &chronosv1.WorkerAck{
		RequestId: heartbeat.GetRequestId(), Accepted: true, Duplicate: result.Duplicate,
	})
}

func (m *Manager) complete(ctx context.Context, current *session, completion *chronosv1.TaskCompletion) error {
	if completion.GetRequestId() == "" || completion.GetWorkflowId() == "" || completion.GetTaskId() == "" ||
		completion.GetAttemptId() == "" || completion.GetFencingToken() == 0 {
		return status.Error(codes.InvalidArgument, "completion identity is incomplete")
	}
	if completion.GetError() != "" && (len(completion.GetOutput()) > 0 || len(completion.GetFanout()) > 0) {
		return status.Error(codes.InvalidArgument, "completion cannot contain output and error")
	}
	view, err := m.config.View(ctx)
	if err != nil {
		return err
	}
	if !view.Leader {
		return status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID)
	}
	workflow, exists := view.Engine.State(completion.GetWorkflowId())
	if !exists {
		return m.ackError(current, completion.GetRequestId(), core.ErrWorkflowNotFound)
	}
	kind := core.CommandComplete
	if completion.GetError() != "" {
		kind = core.CommandFail
	}
	result, err := m.config.Submit(ctx, core.Command{
		Kind: kind, RequestID: completion.GetRequestId(), WorkflowID: completion.GetWorkflowId(),
		TaskID: completion.GetTaskId(), AttemptID: completion.GetAttemptId(), WorkerID: current.workerID,
		Fence: completion.GetFencingToken(), At: max(m.config.Now(), workflow.UpdatedAt),
		Output: copyMap(completion.GetOutput()), Error: completion.GetError(), Fanout: fanoutItems(completion.GetFanout()),
	})
	if err != nil {
		return m.ackError(current, completion.GetRequestId(), err)
	}
	view, err = m.config.View(ctx)
	if err != nil {
		return err
	}
	m.reconcile(view.Engine)
	return m.ack(current, "completion", &chronosv1.WorkerAck{
		RequestId: completion.GetRequestId(), Accepted: true, Duplicate: result.Duplicate,
	})
}

func (m *Manager) ackError(current *session, requestID string, err error) error {
	ack := &chronosv1.WorkerAck{RequestId: requestID, Code: errorCode(err), Message: err.Error()}
	if !m.enqueue(current, response(ack)) {
		return status.Error(codes.ResourceExhausted, "worker outbound queue is full")
	}
	return nil
}

func (m *Manager) ack(current *session, kind string, ack *chronosv1.WorkerAck) error {
	if m.config.AckFailpoint != nil {
		if err := m.config.AckFailpoint(kind); err != nil {
			m.stopSession(current, err)
			return err
		}
	}
	if !m.enqueue(current, response(ack)) {
		return status.Error(codes.ResourceExhausted, "worker outbound queue is full")
	}
	return nil
}

func (m *Manager) handleClose(request closeRequest) {
	current := m.sessions[request.workerID]
	if current != nil && current.id == request.sessionID {
		delete(m.sessions, request.workerID)
		if m.config.Observability != nil {
			m.config.Observability.DeleteWorker(request.workerID)
		}
	}
	close(request.done)
}

func (m *Manager) dispatch(ctx context.Context) {
	view, err := m.config.View(ctx)
	if err != nil {
		return
	}
	if !view.Leader {
		m.stopAll(status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID))
		return
	}
	if m.fireTimers(ctx, &view) != nil {
		return
	}
	if !view.Leader {
		m.stopAll(status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID))
		return
	}
	if m.expire(ctx, &view) != nil {
		return
	}
	if !view.Leader {
		m.stopAll(status.Errorf(codes.Unavailable, "leader is %d", view.LeaderID))
		return
	}
	workerIDs := make([]string, 0, len(m.sessions))
	for workerID := range m.sessions {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	dispatched := 0
	for dispatched < m.config.MaxDispatch {
		progress := false
		for _, workerID := range workerIDs {
			current := m.sessions[workerID]
			if current == nil || uint32(len(current.inflight)) >= current.capacity {
				continue
			}
			plan, exists := m.config.Scheduler.Plan(view.Engine, current.capabilities)
			if !exists {
				continue
			}
			workflow, _ := view.Engine.State(plan.Task.WorkflowID)
			task := workflow.Tasks[plan.Task.TaskID]
			if task.Attempt == math.MaxUint32 || task.Fence == math.MaxUint64 {
				continue
			}
			at := max(m.config.Now(), workflow.UpdatedAt)
			leaseMillis := m.config.LeaseMillis
			if task.Definition.TimeoutMillis > 0 && task.Definition.TimeoutMillis < leaseMillis {
				leaseMillis = task.Definition.TimeoutMillis
			}
			if at > math.MaxInt64-leaseMillis {
				continue
			}
			command := core.Command{
				Kind: core.CommandStart, RequestID: core.LeaseRequestID(workflow.ID, plan.Task.TaskID, task.Attempt+1),
				WorkflowID: workflow.ID, TaskID: plan.Task.TaskID, WorkerID: current.workerID,
				At: at, LeaseUntil: at + leaseMillis,
			}
			dispatchContext := ctx
			var span trace.Span
			started := time.Now()
			if m.config.Observability != nil {
				dispatchContext, span = m.config.Observability.Tracer().Start(ctx, "chronos.task.dispatch", trace.WithAttributes(
					attribute.String("chronos.namespace", workflow.Namespace),
					attribute.String("chronos.task_type", plan.Task.Capability),
				))
			}
			result, err := m.config.Submit(dispatchContext, command)
			if span != nil {
				span.End()
			}
			if err != nil {
				return
			}
			event, exists := startedEvent(result)
			if !exists {
				return
			}
			if m.config.Observability != nil {
				m.config.Observability.ObserveDispatch(time.Since(started))
			}
			if m.config.Scheduler.Commit(plan) != nil {
				return
			}
			// send assignments after the lease commits
			current.inflight[event.AttemptID] = struct{}{}
			if m.config.Observability != nil {
				m.config.Observability.SetWorker(current.workerID, current.capacity, len(current.inflight))
			}
			if !m.enqueue(current, assignmentFromEvent(task.Definition, event, task.Compensation, task.Payload)) {
				m.stopSession(current, status.Error(codes.ResourceExhausted, "worker outbound queue is full"))
			}
			dispatched++
			progress = true
			view, err = m.config.View(ctx)
			if err != nil || !view.Leader || dispatched >= m.config.MaxDispatch {
				return
			}
		}
		if !progress {
			return
		}
	}
}

func (m *Manager) fireTimers(ctx context.Context, view *View) error {
	now := m.config.Now()
	type due struct {
		workflowID string
		timer      core.TimerState
	}
	var timers []due
	for _, workflowID := range view.Engine.WorkflowIDs() {
		workflow, _ := view.Engine.State(workflowID)
		for _, timer := range workflow.Timers {
			if timer.Status == core.TimerScheduled && timer.Deadline <= now && timerWins(workflow, timer) {
				timers = append(timers, due{workflowID, timer})
			}
		}
	}
	sort.Slice(timers, func(i, j int) bool {
		if timers[i].timer.Deadline != timers[j].timer.Deadline {
			return timers[i].timer.Deadline < timers[j].timer.Deadline
		}
		if timers[i].workflowID != timers[j].workflowID {
			return timers[i].workflowID < timers[j].workflowID
		}
		return timers[i].timer.ID < timers[j].timer.ID
	})
	if len(timers) > m.config.MaxDispatch {
		timers = timers[:m.config.MaxDispatch]
	}
	for _, item := range timers {
		workflow, exists := view.Engine.State(item.workflowID)
		if !exists {
			continue
		}
		timer, exists := workflow.Timers[item.timer.ID]
		if !exists || timer.Status != core.TimerScheduled || timer.Deadline > now || !timerWins(workflow, timer) {
			continue
		}
		_, err := m.config.Submit(ctx, core.Command{
			Kind: core.CommandFireTimer, RequestID: core.TimerRequestID(timer.ID), WorkflowID: item.workflowID,
			TimerID: timer.ID, At: max(now, max(workflow.UpdatedAt, timer.Deadline)),
		})
		if err != nil {
			return err
		}
		next, err := m.config.View(ctx)
		if err != nil {
			return err
		}
		*view = next
		m.reconcile(next.Engine)
	}
	return nil
}

func timerWins(workflow *core.WorkflowState, timer core.TimerState) bool {
	if timer.Purpose != core.TimerTimeout {
		return true
	}
	task, exists := workflow.Tasks[timer.TaskID]
	return !exists || task.AttemptID != timer.AttemptID || task.Fence != timer.Fence || timer.Deadline <= task.LeaseUntil
}

func (m *Manager) expire(ctx context.Context, view *View) error {
	now := m.config.Now()
	type expired struct {
		workflowID string
		taskID     string
		task       core.TaskState
		updatedAt  int64
	}
	var leases []expired
	for _, workflowID := range view.Engine.WorkflowIDs() {
		workflow, _ := view.Engine.State(workflowID)
		for taskID, task := range workflow.Tasks {
			if leased(task) && task.LeaseUntil <= now {
				leases = append(leases, expired{workflowID, taskID, task, workflow.UpdatedAt})
			}
		}
	}
	sort.Slice(leases, func(i, j int) bool {
		if leases[i].workflowID != leases[j].workflowID {
			return leases[i].workflowID < leases[j].workflowID
		}
		return leases[i].taskID < leases[j].taskID
	})
	for _, lease := range leases {
		_, err := m.config.Submit(ctx, core.Command{
			Kind: core.CommandExpire, RequestID: core.ExpiryRequestID(lease.task.AttemptID, lease.task.Fence),
			WorkflowID: lease.workflowID, TaskID: lease.taskID, AttemptID: lease.task.AttemptID,
			WorkerID: lease.task.WorkerID, Fence: lease.task.Fence,
			At: max(now, max(lease.updatedAt, lease.task.LeaseUntil)),
		})
		if err != nil && !errors.Is(err, core.ErrFenced) {
			return err
		}
	}
	if len(leases) > 0 {
		next, err := m.config.View(ctx)
		if err != nil {
			return err
		}
		*view = next
		m.reconcile(next.Engine)
	}
	return nil
}

func (m *Manager) reconcile(engine *core.Engine) {
	active := make(map[string]map[string]struct{})
	for _, workflowID := range engine.WorkflowIDs() {
		workflow, _ := engine.State(workflowID)
		for _, task := range workflow.Tasks {
			if !leased(task) {
				continue
			}
			attempts := active[task.WorkerID]
			if attempts == nil {
				attempts = make(map[string]struct{})
				active[task.WorkerID] = attempts
			}
			attempts[task.AttemptID] = struct{}{}
		}
	}
	for workerID, current := range m.sessions {
		for attemptID := range current.inflight {
			if _, exists := active[workerID][attemptID]; !exists {
				delete(current.inflight, attemptID)
			}
		}
		if m.config.Observability != nil {
			m.config.Observability.SetWorker(workerID, current.capacity, len(current.inflight))
		}
	}
}

func (m *Manager) enqueue(current *session, message *chronosv1.CoordinatorMessage) bool {
	select {
	case current.outbound <- delivery{message: message}:
		return true
	default:
		return false
	}
}

func (m *Manager) stopSession(current *session, err error) {
	if m.sessions[current.workerID] == current {
		delete(m.sessions, current.workerID)
		if m.config.Observability != nil {
			m.config.Observability.DeleteWorker(current.workerID)
		}
	}
	select {
	case current.stopped <- err:
	default:
	}
}

func (m *Manager) stopAll(err error) {
	for _, current := range m.sessions {
		m.stopSession(current, err)
	}
}

func normalizeCapabilities(values []string) ([]string, map[string]struct{}, error) {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, nil, fmt.Errorf("capability is required")
		}
		set[value] = struct{}{}
	}
	if len(set) == 0 {
		return nil, nil, fmt.Errorf("at least one capability is required")
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, set, nil
}

func assignment(workflowID string, task core.TaskState) *chronosv1.CoordinatorMessage {
	event := core.Event{
		WorkflowID: workflowID, TaskID: task.Definition.ID, AttemptID: task.AttemptID,
		Attempt: task.Attempt, WorkerID: task.WorkerID, LeaseUntil: task.LeaseUntil,
		Fence: task.Fence, Idempotency: task.Idempotency, Inputs: task.Inputs,
	}
	return assignmentFromEvent(task.Definition, event, task.Compensation, task.Payload)
}

func assignmentFromEvent(definition core.TaskDefinition, event core.Event, compensation bool, payload map[string]string) *chronosv1.CoordinatorMessage {
	inputs := make(map[string]*chronosv1.StringValues, len(event.Inputs))
	for key, values := range event.Inputs {
		inputs[key] = &chronosv1.StringValues{Values: copyMap(values)}
	}
	return &chronosv1.CoordinatorMessage{Value: &chronosv1.CoordinatorMessage_Assignment{
		Assignment: &chronosv1.TaskAssignment{
			WorkflowId: event.WorkflowID, TaskId: event.TaskID, TaskType: taskType(definition, compensation),
			AttemptId: event.AttemptID, Attempt: event.Attempt, WorkerId: event.WorkerID,
			LeaseExpiresAt: event.LeaseUntil, FencingToken: event.Fence,
			IdempotencyKey: event.Idempotency, Inputs: inputs, Compensation: compensation, Payload: copyMap(payload),
		},
	}}
}

func leased(task core.TaskState) bool {
	return task.Status == core.TaskRunning || task.Status == core.TaskCompensating && task.WorkerID != ""
}

func capabilityFor(task core.TaskState) string {
	return taskType(task.Definition, task.Compensation)
}

func taskType(definition core.TaskDefinition, compensation bool) string {
	if compensation {
		return definition.Compensation.TaskType
	}
	return definition.Type
}

func fanoutItems(values []*chronosv1.FanoutItem) []core.FanoutItem {
	if len(values) == 0 {
		return nil
	}
	items := make([]core.FanoutItem, len(values))
	for index, value := range values {
		items[index] = core.FanoutItem{Key: value.GetKey(), Payload: copyMap(value.GetPayload())}
	}
	return items
}

func response(ack *chronosv1.WorkerAck) *chronosv1.CoordinatorMessage {
	return &chronosv1.CoordinatorMessage{Value: &chronosv1.CoordinatorMessage_Ack{Ack: ack}}
}

func startedEvent(result core.Result) (core.Event, bool) {
	for _, event := range result.Events {
		if event.Kind == core.EventTaskStarted {
			return event, true
		}
	}
	return core.Event{}, false
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, core.ErrFenced):
		return "fenced"
	case errors.Is(err, core.ErrLeaseExpired):
		return "expired"
	case errors.Is(err, core.ErrRequestConflict):
		return "conflict"
	case errors.Is(err, core.ErrInvalidCommand), errors.Is(err, core.ErrInvalidTransition):
		return "invalid"
	default:
		return "unavailable"
	}
}

func copyMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func max(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func (m *Manager) Work(stream grpc.BidiStreamingServer[chronosv1.WorkerMessage, chronosv1.CoordinatorMessage]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello must be the first message")
	}
	responseChannel := make(chan openResponse, 1)
	request := openRequest{ctx: stream.Context(), hello: hello, response: responseChannel}
	select {
	case m.open <- request:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	var opened openResponse
	select {
	case opened = <-responseChannel:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if opened.err != nil {
		return opened.err
	}
	current := opened.session
	defer func() {
		done := make(chan struct{})
		select {
		case m.closed <- closeRequest{sessionID: current.id, workerID: current.workerID, done: done}:
		case <-m.done:
			return
		}
		select {
		case <-done:
		case <-m.done:
		}
	}()
	received := make(chan *chronosv1.WorkerMessage)
	receiveErrors := make(chan error, 1)
	go func() {
		defer close(received)
		for {
			message, err := stream.Recv()
			if err != nil {
				receiveErrors <- err
				return
			}
			select {
			case received <- message:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	sendErrors := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stream.Context().Done():
				return
			case item := <-current.outbound:
				if err := stream.Send(item.message); err != nil {
					sendErrors <- err
					return
				}
			}
		}
	}()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-receiveErrors:
			if err == io.EOF {
				return nil
			}
			return err
		case message, exists := <-received:
			if !exists {
				return nil
			}
			response := make(chan error, 1)
			request := messageRequest{
				ctx: stream.Context(), sessionID: current.id, workerID: current.workerID, message: message, response: response,
			}
			select {
			case m.messages <- request:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
			select {
			case err := <-response:
				if err != nil {
					return err
				}
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		case err := <-current.stopped:
			return err
		case err := <-sendErrors:
			return err
		}
	}
}
