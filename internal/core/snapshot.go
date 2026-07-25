package core

import (
	"fmt"
	"reflect"
	"sort"
)

func (e *Engine) Snapshot() EngineSnapshot {
	snapshot := EngineSnapshot{Version: EngineSnapshotVersion}
	for _, id := range e.WorkflowIDs() {
		snapshot.Workflows = append(snapshot.Workflows, *cloneState(e.states[id]))
	}
	requestIDs := make([]string, 0, len(e.requests))
	for id := range e.requests {
		requestIDs = append(requestIDs, id)
	}
	sort.Strings(requestIDs)
	for _, id := range requestIDs {
		record := e.requests[id]
		snapshot.Requests = append(snapshot.Requests, RequestReceipt{
			RequestID: id, RequestHash: record.hash, Result: cloneResult(record.result),
		})
	}
	return snapshot
}

func Restore(snapshot EngineSnapshot) (*Engine, error) {
	if snapshot.Version != EngineSnapshotVersion {
		return nil, ErrSnapshotVersion
	}
	engine := NewEngine()
	workflowEvents := make(map[string][]Event, len(snapshot.Workflows))
	previousID := ""
	for index := range snapshot.Workflows {
		state := cloneState(&snapshot.Workflows[index])
		if state.ID == "" || index > 0 && state.ID <= previousID {
			return nil, fmt.Errorf("%w: workflows are not canonical", ErrInvalidTransition)
		}
		previousID = state.ID
		engine.states[state.ID] = state
		engine.updateActive(state.ID, state)
	}
	previousID = ""
	for index, receipt := range snapshot.Requests {
		if receipt.RequestID == "" || receipt.RequestHash == "" || receipt.Result.WorkflowID == "" || receipt.Result.Duplicate || len(receipt.Result.Events) == 0 {
			return nil, fmt.Errorf("%w: request receipt metadata is invalid", ErrInvalidTransition)
		}
		if index > 0 && receipt.RequestID <= previousID {
			return nil, fmt.Errorf("%w: request receipts are not canonical", ErrInvalidTransition)
		}
		previousID = receipt.RequestID
		for eventIndex, event := range receipt.Result.Events {
			if event.RequestID != receipt.RequestID || event.RequestHash != receipt.RequestHash || event.WorkflowID != receipt.Result.WorkflowID {
				return nil, fmt.Errorf("%w: request receipt event metadata is invalid", ErrInvalidTransition)
			}
			if eventIndex > 0 && event.Sequence != receipt.Result.Events[eventIndex-1].Sequence+1 {
				return nil, ErrSequence
			}
			workflowEvents[event.WorkflowID] = append(workflowEvents[event.WorkflowID], cloneEvent(event))
		}
		engine.storeRequest(receipt.RequestID, requestRecord{
			hash: receipt.RequestHash, result: cloneResult(receipt.Result),
		})
	}
	if len(engine.states) != len(workflowEvents) {
		return nil, fmt.Errorf("%w: snapshot state and receipts differ", ErrInvalidTransition)
	}
	for workflowID, expected := range engine.states {
		events, exists := workflowEvents[workflowID]
		if !exists {
			return nil, fmt.Errorf("%w: workflow has no events", ErrInvalidTransition)
		}
		sort.Slice(events, func(i, j int) bool {
			return events[i].Sequence < events[j].Sequence
		})
		var actual *WorkflowState
		var err error
		for _, event := range events {
			actual, err = Apply(actual, event)
			if err != nil {
				return nil, err
			}
		}
		if !reflect.DeepEqual(actual, expected) {
			return nil, fmt.Errorf("%w: workflow state does not match receipts", ErrInvalidTransition)
		}
	}
	return engine, nil
}

func (e *Engine) Clone() *Engine {
	copy := NewEngine()
	copy.journaled = e.journaled
	copy.stateLoader = e.stateLoader
	copy.requestLoader = e.requestLoader
	for id, state := range e.states {
		stateCopy := cloneState(state)
		copy.states[id] = stateCopy
		copy.updateActive(id, stateCopy)
	}
	copy.journal = cloneEvents(e.journal)
	for id, record := range e.requests {
		copy.storeRequest(id, requestRecord{hash: record.hash, result: cloneResult(record.result)})
	}
	copy.metrics = e.Metrics()
	return copy
}
