package core

import (
	"fmt"
	"sort"
	"strings"
)

type requestRecord struct {
	hash   string
	result Result
}

type BatchApplication struct {
	Result   Result
	Conflict bool
}

type Engine struct {
	states             map[string]*WorkflowState
	active             map[string]struct{}
	journal            []Event
	requests           map[string]requestRecord
	requestsByWorkflow map[string]map[string]struct{}
	stateLoader        StateLoader
	requestLoader      RequestLoader
	metrics            Metrics
	journaled          bool
}

type stateCheckpoint struct {
	state  *WorkflowState
	exists bool
}

type requestCheckpoint struct {
	record requestRecord
	exists bool
}

type EngineCheckpoint struct {
	states   map[string]stateCheckpoint
	requests map[string]requestCheckpoint
	journal  int
	metrics  Metrics
}

func NewEngine() *Engine {
	return &Engine{
		states: make(map[string]*WorkflowState), active: make(map[string]struct{}), requests: make(map[string]requestRecord),
		requestsByWorkflow: make(map[string]map[string]struct{}),
		metrics:            Metrics{TransitionsByKind: make(map[EventKind]uint64)}, journaled: true,
	}
}

func (e *Engine) Handle(command Command) (Result, error) {
	e.metrics.CommandsReceived++
	_, result, duplicate, err := e.PrepareAndApply(command)
	if err != nil {
		return Result{}, err
	}
	if duplicate {
		e.metrics.Duplicates++
		return result, nil
	}
	return result, nil
}

func (e *Engine) Prepare(command Command) (CommandBatch, Result, bool, error) {
	batch, result, duplicate, _, err := e.prepare(command)
	return batch, result, duplicate, err
}

func (e *Engine) PrepareAndApply(command Command) (CommandBatch, Result, bool, error) {
	batch, result, duplicate, next, err := e.prepare(command)
	return e.applyPrepared(batch, result, duplicate, next, err)
}

func (e *Engine) applyPrepared(batch CommandBatch, result Result, duplicate bool, next *WorkflowState, err error) (CommandBatch, Result, bool, error) {
	if err != nil || duplicate {
		return batch, result, duplicate, err
	}
	stored := cloneEvents(batch.Events)
	storedResult := Result{WorkflowID: batch.WorkflowID, Events: stored}
	e.states[batch.WorkflowID] = next
	e.updateActive(batch.WorkflowID, next)
	if e.journaled {
		e.journal = append(e.journal, stored...)
	}
	e.storeRequest(batch.RequestID, requestRecord{hash: batch.RequestHash, result: storedResult})
	e.metrics.CommandsAccepted++
	e.metrics.Transitions += uint64(len(stored))
	for _, event := range stored {
		e.metrics.TransitionsByKind[event.Kind]++
	}
	return batch, result, false, nil
}

func (e *Engine) prepare(command Command) (CommandBatch, Result, bool, *WorkflowState, error) {
	return e.prepareWith(command, e.resolveState, decidePlan, true)
}

type prepareStateResolver func(string) (*WorkflowState, bool, error)
type preparePlanner func(*WorkflowState, Command) (*eventPlan, error)

func (e *Engine) prepareWith(command Command, resolveState prepareStateResolver, planCommand preparePlanner, materialize bool) (CommandBatch, Result, bool, *WorkflowState, error) {
	if strings.TrimSpace(command.RequestID) == "" {
		return CommandBatch{}, Result{}, false, nil, fmt.Errorf("%w: request id is required", ErrInvalidCommand)
	}
	hash, err := HashCommand(command)
	if err != nil {
		return CommandBatch{}, Result{}, false, nil, err
	}
	// Request ids are bound to the command hash
	record, exists, err := e.resolveRequest(command.RequestID)
	if err != nil {
		return CommandBatch{}, Result{}, false, nil, err
	}
	if exists {
		if record.hash != hash {
			return CommandBatch{}, Result{}, false, nil, ErrRequestConflict
		}
		result := cloneResult(record.result)
		result.Duplicate = true
		batch := CommandBatch{
			Version: CommandBatchVersion, RequestID: command.RequestID, RequestHash: hash,
			WorkflowID: result.WorkflowID, Events: cloneEvents(result.Events),
		}
		return batch, result, true, nil, nil
	}
	normalized := command
	if normalized.Kind == CommandSubmit && normalized.WorkflowID == "" && normalized.Definition != nil {
		normalized.WorkflowID = WorkflowID(normalized.Definition.Namespace, normalized.RequestID)
	}
	state, _, err := resolveState(normalized.WorkflowID)
	if err != nil {
		return CommandBatch{}, Result{}, false, nil, err
	}
	if normalized.Kind == CommandStart && state != nil {
		if task, exists := state.Tasks[normalized.TaskID]; exists {
			expected := AttemptID(normalized.WorkflowID, normalized.TaskID, task.Attempt+1)
			if normalized.AttemptID != "" && normalized.AttemptID != expected {
				return CommandBatch{}, Result{}, false, nil, fmt.Errorf("%w: attempt id must be %s", ErrInvalidCommand, expected)
			}
			normalized.AttemptID = expected
		}
	}
	sequence := uint64(0)
	if state != nil {
		sequence = state.Version
	}
	plan, err := planCommand(state, normalized)
	if err != nil {
		return CommandBatch{}, Result{}, false, nil, err
	}
	events := plan.events
	if len(events) == 0 {
		return CommandBatch{}, Result{}, false, nil, fmt.Errorf("%w: command produced no events", ErrInvalidTransition)
	}
	for index := range events {
		sequence++
		events[index].Sequence = sequence
		events[index].ID = EventID(events[index].WorkflowID, sequence, events[index].Kind)
		events[index].RequestID = normalized.RequestID
		events[index].RequestHash = hash
	}
	batch := CommandBatch{
		Version: CommandBatchVersion, RequestID: normalized.RequestID, RequestHash: hash,
		WorkflowID: normalized.WorkflowID, Events: events,
	}
	result := Result{WorkflowID: normalized.WorkflowID}
	if materialize {
		batch.Events = cloneEvents(events)
		result.Events = cloneEvents(events)
	}
	return batch, result, false, plan.state, nil
}

func (e *Engine) ApplyBatch(batch CommandBatch) (Result, error) {
	applications, err := e.ApplyBatches([]CommandBatch{batch})
	if err != nil {
		return Result{}, err
	}
	if applications[0].Conflict {
		return Result{}, ErrRequestConflict
	}
	return applications[0].Result, nil
}

func (e *Engine) ApplyBatches(batches []CommandBatch) ([]BatchApplication, error) {
	applications, _, err := e.applyBatches(batches, false)
	return applications, err
}

func (e *Engine) ApplyCommittedBatches(batches []CommandBatch) ([]BatchApplication, map[string]*WorkflowState, error) {
	return e.applyBatches(batches, true)
}

func (e *Engine) applyBatches(batches []CommandBatch, owned bool) ([]BatchApplication, map[string]*WorkflowState, error) {
	type acceptedBatch struct {
		requestID   string
		requestHash string
		workflowID  string
		result      Result
		application int
	}
	// Later batches see provisional workflow updates.
	states := make(map[string]*WorkflowState)
	requests := make(map[string]requestRecord, len(batches))
	accepted := make([]acceptedBatch, 0, len(batches))
	applications := make([]BatchApplication, 0, len(batches))
	for _, batch := range batches {
		if batch.Version != CommandBatchVersion {
			return nil, nil, ErrBatchVersion
		}
		if strings.TrimSpace(batch.RequestID) == "" || batch.RequestHash == "" {
			return nil, nil, fmt.Errorf("%w: command batch metadata is invalid", ErrInvalidTransition)
		}
		record, exists := requests[batch.RequestID]
		if !exists {
			var err error
			record, exists, err = e.resolveRequest(batch.RequestID)
			if err != nil {
				return nil, nil, err
			}
		}
		if exists {
			if record.hash != batch.RequestHash {
				applications = append(applications, BatchApplication{Conflict: true})
				continue
			}
			result := cloneResult(record.result)
			result.Duplicate = true
			applications = append(applications, BatchApplication{Result: result})
			continue
		}
		if strings.TrimSpace(batch.WorkflowID) == "" || len(batch.Events) == 0 {
			return nil, nil, fmt.Errorf("%w: command batch metadata is invalid", ErrInvalidTransition)
		}
		stored := batch.Events
		if !owned {
			stored = cloneEvents(batch.Events)
		}
		for _, event := range stored {
			if event.RequestID != batch.RequestID || event.RequestHash != batch.RequestHash || event.WorkflowID != batch.WorkflowID {
				return nil, nil, fmt.Errorf("%w: command batch event metadata is invalid", ErrInvalidTransition)
			}
		}
		state, loaded := states[batch.WorkflowID]
		if !loaded {
			var err error
			state, _, err = e.resolveState(batch.WorkflowID)
			if err != nil {
				return nil, nil, err
			}
			state = cloneStateForUpdate(state)
		}
		next, err := applyOwnedEvents(state, stored)
		if err != nil {
			return nil, nil, err
		}
		states[batch.WorkflowID] = next
		result := Result{WorkflowID: batch.WorkflowID, Events: stored}
		record = requestRecord{hash: batch.RequestHash, result: result}
		requests[batch.RequestID] = record
		application := result
		if !owned {
			application = cloneResult(result)
		}
		accepted = append(accepted, acceptedBatch{batch.RequestID, batch.RequestHash, batch.WorkflowID, result, len(applications)})
		applications = append(applications, BatchApplication{Result: application})
	}
	if owned {
		for _, batch := range accepted {
			if e.journaled || !terminalWorkflow(states[batch.workflowID].Status) {
				applications[batch.application].Result = cloneResult(batch.result)
			}
		}
	}
	for workflowID, state := range states {
		e.states[workflowID] = state
		e.updateActive(workflowID, state)
	}
	for _, batch := range accepted {
		e.storeRequest(batch.requestID, requestRecord{hash: batch.requestHash, result: batch.result})
		if e.journaled {
			e.journal = append(e.journal, batch.result.Events...)
		}
		e.metrics.CommandsAccepted++
		e.metrics.Transitions += uint64(len(batch.result.Events))
		for _, event := range batch.result.Events {
			e.metrics.TransitionsByKind[event.Kind]++
		}
	}
	return applications, states, nil
}

func Replay(events []Event) (*Engine, error) {
	engine := NewEngine()
	for _, original := range events {
		event := cloneEvent(original)
		if event.RequestID == "" || event.RequestHash == "" {
			return nil, fmt.Errorf("%w: event request metadata is required", ErrInvalidTransition)
		}
		state, err := Apply(engine.states[event.WorkflowID], event)
		if err != nil {
			return nil, err
		}
		engine.states[event.WorkflowID] = state
		engine.updateActive(event.WorkflowID, state)
		engine.journal = append(engine.journal, cloneEvent(event))
		record, exists := engine.requests[event.RequestID]
		if !exists {
			record = requestRecord{hash: event.RequestHash, result: Result{WorkflowID: event.WorkflowID}}
		} else if record.hash != event.RequestHash || record.result.WorkflowID != event.WorkflowID {
			return nil, ErrRequestConflict
		}
		record.result.Events = append(record.result.Events, cloneEvent(event))
		engine.storeRequest(event.RequestID, record)
	}
	return engine, nil
}

func (e *Engine) State(workflowID string) (*WorkflowState, bool) {
	state, exists, err := e.resolveState(workflowID)
	if err != nil {
		return nil, false
	}
	return cloneState(state), exists
}

func (e *Engine) Projection(workflowID string) (Projection, bool) {
	state, exists, err := e.resolveState(workflowID)
	if err != nil {
		return Projection{}, false
	}
	if !exists {
		return Projection{}, false
	}
	return Project(state), true
}

func (e *Engine) Journal() []Event {
	return cloneEvents(e.journal)
}

func (e *Engine) DiscardJournal() {
	e.journal = nil
	e.journaled = false
}

func (e *Engine) Metrics() Metrics {
	metrics := e.metrics
	metrics.TransitionsByKind = make(map[EventKind]uint64, len(e.metrics.TransitionsByKind))
	for kind, value := range e.metrics.TransitionsByKind {
		metrics.TransitionsByKind[kind] = value
	}
	return metrics
}

func (e *Engine) WorkflowIDs() []string {
	ids := make([]string, 0, len(e.states))
	for id := range e.states {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (e *Engine) ActiveWorkflowIDs() []string {
	ids := make([]string, 0, len(e.active))
	for id := range e.active {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (e *Engine) Checkpoint(batches []CommandBatch) EngineCheckpoint {
	checkpoint := EngineCheckpoint{
		states: make(map[string]stateCheckpoint), requests: make(map[string]requestCheckpoint, len(batches)),
		journal: len(e.journal), metrics: e.Metrics(),
	}
	for _, batch := range batches {
		e.ExtendCheckpoint(&checkpoint, batch)
	}
	return checkpoint
}

func (e *Engine) ExtendCheckpoint(checkpoint *EngineCheckpoint, batch CommandBatch) {
	if batch.WorkflowID != "" {
		if _, exists := checkpoint.states[batch.WorkflowID]; !exists {
			state, found := e.states[batch.WorkflowID]
			checkpoint.states[batch.WorkflowID] = stateCheckpoint{state: state, exists: found}
		}
	}
	if batch.RequestID != "" {
		if _, exists := checkpoint.requests[batch.RequestID]; !exists {
			record, found := e.requests[batch.RequestID]
			checkpoint.requests[batch.RequestID] = requestCheckpoint{record: record, exists: found}
		}
	}
}

func (e *Engine) Rollback(checkpoint EngineCheckpoint) {
	for id, value := range checkpoint.states {
		if value.exists {
			e.states[id] = value.state
			e.updateActive(id, value.state)
		} else {
			delete(e.states, id)
			delete(e.active, id)
		}
	}
	for id, value := range checkpoint.requests {
		if value.exists {
			e.storeRequest(id, value.record)
		} else {
			e.deleteRequest(id)
		}
	}
	if checkpoint.journal <= len(e.journal) {
		e.journal = e.journal[:checkpoint.journal]
	}
	e.metrics = checkpoint.metrics
}

func (e *Engine) updateActive(workflowID string, state *WorkflowState) {
	if state != nil && !terminalWorkflow(state.Status) {
		e.active[workflowID] = struct{}{}
		return
	}
	delete(e.active, workflowID)
}

func terminalWorkflow(status WorkflowStatus) bool {
	switch status {
	case WorkflowCompleted, WorkflowFailed, WorkflowCancelled, WorkflowCompensated:
		return true
	default:
		return false
	}
}

func cloneResult(result Result) Result {
	copy := result
	copy.Events = cloneEvents(result.Events)
	return copy
}
