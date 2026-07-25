package core

func (e *Engine) ResidentRequestHash(requestID string) (string, bool) {
	record, exists := e.requests[requestID]
	return record.hash, exists
}

func (e *Engine) ResidentWorkflow(workflowID string) bool {
	_, exists := e.states[workflowID]
	return exists
}
