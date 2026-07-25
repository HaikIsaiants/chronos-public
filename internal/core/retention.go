package core

type StateLoader func(string) (*WorkflowState, bool, error)

type RequestLoader func(string) (RequestReceipt, bool, error)

func (e *Engine) SetLoaders(state StateLoader, request RequestLoader) {
	e.stateLoader = state
	e.requestLoader = request
}

func (e *Engine) HydrateState(state *WorkflowState) error {
	if state == nil || state.ID == "" || terminalWorkflow(state.Status) {
		return ErrInvalidTransition
	}
	stored := cloneState(state)
	e.states[stored.ID] = stored
	e.updateActive(stored.ID, stored)
	return nil
}

func (e *Engine) HydrateRequest(receipt RequestReceipt) error {
	if receipt.RequestID == "" || receipt.RequestHash == "" || receipt.Result.WorkflowID == "" || receipt.Result.Duplicate || len(receipt.Result.Events) == 0 {
		return ErrInvalidTransition
	}
	if _, exists := e.states[receipt.Result.WorkflowID]; !exists {
		return ErrInvalidTransition
	}
	for _, event := range receipt.Result.Events {
		if event.RequestID != receipt.RequestID || event.RequestHash != receipt.RequestHash || event.WorkflowID != receipt.Result.WorkflowID {
			return ErrInvalidTransition
		}
	}
	e.storeRequest(receipt.RequestID, requestRecord{hash: receipt.RequestHash, result: cloneResult(receipt.Result)})
	return nil
}

func (e *Engine) resolveState(workflowID string) (*WorkflowState, bool, error) {
	state, exists := e.states[workflowID]
	if exists || e.stateLoader == nil || workflowID == "" {
		return state, exists, nil
	}
	return e.stateLoader(workflowID)
}

func (e *Engine) resolveRequest(requestID string) (requestRecord, bool, error) {
	record, exists := e.requests[requestID]
	if exists || e.requestLoader == nil || requestID == "" {
		return record, exists, nil
	}
	receipt, exists, err := e.requestLoader(requestID)
	if err != nil || !exists {
		return requestRecord{}, exists, err
	}
	return requestRecord{hash: receipt.RequestHash, result: cloneResult(receipt.Result)}, true, nil
}

func (e *Engine) storeRequest(requestID string, record requestRecord) {
	if previous, exists := e.requests[requestID]; exists {
		e.removeRequestIndex(requestID, previous.result.WorkflowID)
	}
	e.requests[requestID] = record
	requests := e.requestsByWorkflow[record.result.WorkflowID]
	if requests == nil {
		requests = make(map[string]struct{})
		e.requestsByWorkflow[record.result.WorkflowID] = requests
	}
	requests[requestID] = struct{}{}
}

func (e *Engine) deleteRequest(requestID string) {
	record, exists := e.requests[requestID]
	if !exists {
		return
	}
	delete(e.requests, requestID)
	e.removeRequestIndex(requestID, record.result.WorkflowID)
}

func (e *Engine) removeRequestIndex(requestID, workflowID string) {
	requests := e.requestsByWorkflow[workflowID]
	delete(requests, requestID)
	if len(requests) == 0 {
		delete(e.requestsByWorkflow, workflowID)
	}
}

func (e *Engine) EvictTerminal(workflowID string) bool {
	state, exists := e.states[workflowID]
	if !exists || !terminalWorkflow(state.Status) {
		return false
	}
	e.evictWorkflow(workflowID)
	return true
}

func (e *Engine) CompactTerminal(workflowID string) bool {
	state, exists := e.states[workflowID]
	if !exists || !terminalWorkflow(state.Status) {
		return false
	}
	e.states[workflowID] = &WorkflowState{
		ID: state.ID, Name: state.Name, Namespace: state.Namespace, Status: state.Status,
		Version: state.Version, UpdatedAt: state.UpdatedAt,
	}
	delete(e.active, workflowID)
	for requestID := range e.requestsByWorkflow[workflowID] {
		record := e.requests[requestID]
		record.result.Events = nil
		e.requests[requestID] = record
	}
	return true
}

func (e *Engine) RetainActive() {
	for workflowID, state := range e.states {
		if terminalWorkflow(state.Status) {
			e.evictWorkflow(workflowID)
		}
	}
}

func (e *Engine) evictWorkflow(workflowID string) {
	for requestID := range e.requestsByWorkflow[workflowID] {
		delete(e.requests, requestID)
	}
	delete(e.requestsByWorkflow, workflowID)
	delete(e.states, workflowID)
	delete(e.active, workflowID)
}
