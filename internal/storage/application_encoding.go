package storage

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/HaikIsaiants/chronos/internal/core"
)

const applicationEncodingWorkers = 8

type acceptedApplication struct {
	entry  ApplicationEntry
	batch  core.CommandBatch
	result core.Result
}

type encodedSet struct {
	key   []byte
	value []byte
	err   error
}

func encodeApplicationEntries(entries []ApplicationEntry, workers int) []encodedSet {
	records := make([]encodedSet, len(entries))
	parallelEncode(len(entries), workers, func(index int) {
		entry := entries[index]
		records[index] = encodeSet(numberKey(3, entry.Index), applicationRecord{
			Version: entry.Version, Index: entry.Index, Term: entry.Term,
		})
	})
	return records
}

func encodeAcceptedReceipts(items []acceptedApplication, states map[string]*core.WorkflowState, workers int) []encodedSet {
	records := make([]encodedSet, len(items))
	parallelEncode(len(items), workers, func(index int) {
		item := items[index]
		state := states[item.batch.WorkflowID]
		if state == nil {
			records[index].err = fmt.Errorf("workflow %s missing after apply", item.batch.WorkflowID)
			return
		}
		result := item.result
		// Store the original result for duplicate replay
		result.Duplicate = false
		receipt := Receipt{
			Version: ReceiptVersion, RequestID: item.batch.RequestID, RequestHash: item.batch.RequestHash,
			AppliedIndex: item.entry.Index, CommittedTerm: item.entry.Term, Result: result,
		}
		record, err := compactReceipt(receipt)
		if err != nil {
			records[index].err = err
			return
		}
		data, err := encodeReceiptRecord(record)
		records[index] = encodedSet{key: segmentedKey(4, item.batch.RequestID), value: data, err: err}
	})
	return records
}

func buildAcceptedSegments(items []acceptedApplication, states map[string]*core.WorkflowState) ([]eventSegment, []taskResultSegment, error) {
	sources := make([]eventSegmentSource, 0, len(items))
	results := make([]TaskResult, 0)
	for _, item := range items {
		state := states[item.batch.WorkflowID]
		if state == nil {
			return nil, nil, fmt.Errorf("workflow %s missing after apply", item.batch.WorkflowID)
		}
		sources = append(sources, eventSegmentSource{item.batch.WorkflowID, item.entry.Index, item.result.Events})
		for _, event := range item.result.Events {
			task, exists := state.Tasks[event.TaskID]
			metadata, ok := taskResult(event, item.entry.Index, task)
			if ok && !exists {
				return nil, nil, fmt.Errorf("task %s missing after apply", event.TaskID)
			}
			if ok {
				results = append(results, metadata)
			}
		}
	}
	events, err := buildEventSegments(sources, false)
	if err != nil {
		return nil, nil, err
	}
	tasks, err := buildTaskResultSegments(results)
	if err != nil {
		return nil, nil, err
	}
	return events, tasks, nil
}

func encodeEventSegments(segments []eventSegment, workers int) []encodedSet {
	records := make([]encodedSet, len(segments))
	parallelEncode(len(segments), workers, func(index int) {
		segment := segments[index]
		records[index] = encodeSet(materializedSegmentKey(8, segment.WorkflowID, segment.AppliedIndex), segment)
	})
	return records
}

func encodeTaskResultSegments(segments []taskResultSegment, workers int) []encodedSet {
	records := make([]encodedSet, len(segments))
	parallelEncode(len(segments), workers, func(index int) {
		segment := segments[index]
		records[index] = encodeSet(materializedSegmentKey(5, segment.WorkflowID, segment.AppliedIndex), segment)
	})
	return records
}

func encodeWorkflowStates(workflowIDs []string, states map[string]*core.WorkflowState, workers int) []encodedSet {
	records := make([]encodedSet, len(workflowIDs))
	parallelEncode(len(workflowIDs), workers, func(index int) {
		workflowID := workflowIDs[index]
		records[index] = encodeSet(segmentedKey(6, workflowID), states[workflowID])
	})
	return records
}

func encodeApplicationOutcomes(outcomes []Outcome, workers int) []encodedSet {
	count := 0
	for _, outcome := range outcomes {
		if !outcome.Result.Duplicate && outcome.Code != "" {
			count++
		}
	}
	if count == 0 {
		return nil
	}
	values := make([]Outcome, 0, count)
	for _, outcome := range outcomes {
		if !outcome.Result.Duplicate && outcome.Code != "" {
			values = append(values, outcome)
		}
	}
	records := make([]encodedSet, len(values))
	parallelEncode(len(values), workers, func(index int) {
		outcome := values[index]
		records[index] = encodeSet(segmentedKey(7, outcome.RequestID, outcome.RequestHash), outcome)
	})
	return records
}

func encodeAppliedState(entry ApplicationEntry) encodedSet {
	return encodeSet(appliedKey, AppliedState{Index: entry.Index, Term: entry.Term})
}

func encodeSet(key []byte, value any) encodedSet {
	data, err := encodeJSON(value)
	return encodedSet{key: key, value: data, err: err}
}

func parallelEncode(count, workers int, encode func(int)) {
	// fixed output slots keep parallel encoding deterministic
	workers = min(count, runtime.GOMAXPROCS(0), workers)
	if workers < 2 {
		for index := 0; index < count; index++ {
			encode(index)
		}
		return
	}
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wait.Done()
			for index := worker; index < count; index += workers {
				encode(index)
			}
		}(worker)
	}
	wait.Wait()
}
