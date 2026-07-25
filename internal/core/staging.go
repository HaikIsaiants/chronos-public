package core

import "fmt"

type StagingTransaction struct {
	engine     *Engine
	checkpoint EngineCheckpoint
	workflows  map[string]struct{}
	open       bool
}

func (e *Engine) BeginStaging() *StagingTransaction {
	return &StagingTransaction{
		engine: e, checkpoint: e.Checkpoint(nil), workflows: make(map[string]struct{}), open: true,
	}
}

func (transaction *StagingTransaction) PrepareAndApply(command Command) (CommandBatch, Result, bool, error) {
	return transaction.prepareAndApply(command, true)
}

func (transaction *StagingTransaction) Stage(command Command) (CommandBatch, Result, bool, error) {
	return transaction.prepareAndApply(command, false)
}

func (transaction *StagingTransaction) prepareAndApply(command Command, materialize bool) (CommandBatch, Result, bool, error) {
	if !transaction.open {
		return CommandBatch{}, Result{}, false, fmt.Errorf("%w: staging transaction is closed", ErrInvalidTransition)
	}
	transaction.engine.ExtendCheckpoint(&transaction.checkpoint, CommandBatch{RequestID: command.RequestID})
	batch, result, duplicate, next, err := transaction.engine.prepareWith(command, transaction.resolveState, decideStagedPlan, materialize)
	batch, result, duplicate, err = transaction.engine.applyPrepared(batch, result, duplicate, next, err)
	if err != nil {
		transaction.Rollback()
	}
	return batch, result, duplicate, err
}

func (transaction *StagingTransaction) Commit() {
	transaction.open = false
}

func (transaction *StagingTransaction) Rollback() {
	if !transaction.open {
		return
	}
	transaction.engine.Rollback(transaction.checkpoint)
	transaction.open = false
}

func (transaction *StagingTransaction) resolveState(workflowID string) (*WorkflowState, bool, error) {
	if workflowID == "" {
		return transaction.engine.resolveState(workflowID)
	}
	if _, exists := transaction.workflows[workflowID]; exists {
		state, found := transaction.engine.states[workflowID]
		return state, found, nil
	}
	transaction.engine.ExtendCheckpoint(&transaction.checkpoint, CommandBatch{WorkflowID: workflowID})
	state, exists, err := transaction.engine.resolveState(workflowID)
	if err != nil {
		return nil, false, err
	}
	if exists {
		state = cloneStateForUpdate(state)
		transaction.engine.states[workflowID] = state
		transaction.engine.updateActive(workflowID, state)
	}
	transaction.workflows[workflowID] = struct{}{}
	return state, exists, nil
}
