package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
)

type applicationRecord struct {
	Version uint32 `json:"version"`
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
}

func (s *Store) loadApplication() error {
	data, err := s.get(appliedKey)
	if err != nil {
		return err
	}
	var applied AppliedState
	if err := decodeJSON(data, &applied); err != nil {
		return err
	}
	if s.snapshot.GetMetadata().GetIndex() > applied.Index {
		s.engine = core.NewEngine()
		s.applied = applied.Index
		s.appliedTerm = applied.Term
		return s.installSnapshot(s.snapshot, false)
	}
	if err := s.validateApplicationLog(applied); err != nil {
		return err
	}
	if err := s.ensureActiveIndex(); err != nil {
		return err
	}
	s.applied = applied.Index
	s.appliedTerm = applied.Term
	engine, err := s.loadActiveEngine()
	if err != nil {
		return err
	}
	s.engine = engine
	s.attachEngine()
	return nil
}

func (s *Store) validateApplicationLog(applied AppliedState) error {
	appliedTerm, err := s.term(applied.Index)
	if err != nil {
		return err
	}
	if appliedTerm != applied.Term {
		return fmt.Errorf("applied term %d does not match raft term %d", applied.Term, appliedTerm)
	}
	lower, upper := prefixBounds(3)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iterator.Close()
	expected := s.snapshot.GetMetadata().GetIndex() + 1
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if len(iterator.Key()) != 9 {
			return fmt.Errorf("invalid application entry key")
		}
		index := binary.BigEndian.Uint64(iterator.Key()[1:])
		if index != expected {
			return fmt.Errorf("%w: expected %d, found %d", ErrAppliedGap, expected, index)
		}
		var entry applicationRecord
		if err := decodeJSON(iterator.Value(), &entry); err != nil {
			return err
		}
		if entry.Version != ApplicationEntryVersion || entry.Index != index || entry.Term == 0 {
			return ErrRecordVersion
		}
		term, err := s.term(index)
		if err != nil {
			return err
		}
		if entry.Term != term {
			return fmt.Errorf("application term %d at index %d does not match raft term %d", entry.Term, index, term)
		}
		expected++
	}
	if err := iterator.Error(); err != nil {
		return err
	}
	if expected != applied.Index+1 {
		return fmt.Errorf("%w: expected %d, applied %d", ErrAppliedGap, expected, applied.Index)
	}
	return nil
}

func (s *Store) Apply(entries []ApplicationEntry) ([]Outcome, error) {
	return s.apply(entries, applicationEncodingWorkers)
}

func (s *Store) ApplyCommitted(entries []ApplicationEntry) ([]Outcome, error) {
	return s.applyEntries(entries, applicationEncodingWorkers, true)
}

func (s *Store) apply(entries []ApplicationEntry, encodingWorkers int) ([]Outcome, error) {
	return s.applyEntries(entries, encodingWorkers, false)
}

func (s *Store) applyEntries(entries []ApplicationEntry, encodingWorkers int, owned bool) ([]Outcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil, nil
	}
	commandCount := 0
	for _, entry := range entries {
		commandCount += len(entry.Batches)
	}
	expected := s.applied + 1
	snapshotIndex := s.snapshot.GetMetadata().GetIndex()
	committedIndex := s.hard.GetCommit()
	commands := make([]core.CommandBatch, 0, commandCount)
	resident := make(map[string]bool)
	for _, entry := range entries {
		if entry.Version != ApplicationEntryVersion || entry.Term == 0 {
			return nil, ErrRecordVersion
		}
		if entry.Index != expected {
			return nil, fmt.Errorf("%w: expected %d, found %d", ErrAppliedGap, expected, entry.Index)
		}
		if entry.Index > committedIndex {
			return nil, fmt.Errorf("application entry %d exceeds committed index %d", entry.Index, committedIndex)
		}
		if entry.Index <= snapshotIndex {
			return nil, fmt.Errorf("application entry %d has no persisted raft term", entry.Index)
		}
		offset := entry.Index - snapshotIndex - 1
		if offset >= uint64(len(s.entryTerms)) {
			return nil, fmt.Errorf("application entry %d has no persisted raft term", entry.Index)
		}
		term := s.entryTerms[offset]
		if entry.Term != term {
			return nil, fmt.Errorf("application entry %d term %d does not match raft term %d", entry.Index, entry.Term, term)
		}
		commands = append(commands, entry.Batches...)
		for _, command := range entry.Batches {
			if _, exists := resident[command.WorkflowID]; !exists {
				resident[command.WorkflowID] = s.engine.ResidentWorkflow(command.WorkflowID)
			}
		}
		expected++
	}
	checkpoint := s.engine.Checkpoint(commands)
	committed := false
	defer func() {
		if !committed {
			s.engine.Rollback(checkpoint)
		}
	}()
	expected = s.applied + 1
	outcomes := make([]Outcome, 0, commandCount)
	accepted := make([]acceptedApplication, 0, commandCount)
	var applications []core.BatchApplication
	var states map[string]*core.WorkflowState
	var err error
	if owned {
		applications, states, err = s.engine.ApplyCommittedBatches(commands)
	} else {
		applications, err = s.engine.ApplyBatches(commands)
	}
	if err != nil {
		return nil, err
	}
	position := 0
	for _, entry := range entries {
		for _, command := range entry.Batches {
			application := applications[position]
			position++
			outcome := Outcome{
				Version: OutcomeVersion, RequestID: command.RequestID, RequestHash: command.RequestHash,
				AppliedIndex: entry.Index, CommittedTerm: entry.Term, Result: application.Result,
			}
			if application.Conflict {
				outcome.Code = "request_conflict"
				outcome.Message = core.ErrRequestConflict.Error()
				outcomes = append(outcomes, outcome)
				continue
			}
			if application.Result.Duplicate {
				var origin AppliedState
				for index := len(accepted) - 1; index >= 0; index-- {
					item := accepted[index]
					if item.batch.RequestID == command.RequestID && item.batch.RequestHash == command.RequestHash {
						origin = AppliedState{Index: item.entry.Index, Term: item.entry.Term}
						break
					}
				}
				if origin.Index == 0 {
					receipt, exists, err := s.loadReceipt(command.RequestID)
					if err != nil {
						return nil, err
					}
					if !exists || receipt.RequestHash != command.RequestHash {
						return nil, ErrRecordVersion
					}
					origin = AppliedState{Index: receipt.AppliedIndex, Term: receipt.CommittedTerm}
				}
				outcome.AppliedIndex = origin.Index
				outcome.CommittedTerm = origin.Term
			} else {
				accepted = append(accepted, acceptedApplication{entry, command, application.Result})
			}
			outcomes = append(outcomes, outcome)
		}
		expected++
	}
	batch := s.db.NewBatchWithSize((len(entries)+1)*4096 + len(accepted)*768)
	defer batch.Close()
	{
		records := encodeApplicationEntries(entries, encodingWorkers)
		if err := stageEncodedSets(batch, records); err != nil {
			return nil, err
		}
	}
	if states == nil {
		states = make(map[string]*core.WorkflowState)
	}
	var loaded map[string]bool
	if !owned {
		loaded = make(map[string]bool, len(states))
	}
	requests := make(map[string][]string, len(states))
	for _, item := range accepted {
		if !owned && !loaded[item.batch.WorkflowID] {
			loaded[item.batch.WorkflowID] = true
			state, exists := s.engine.State(item.batch.WorkflowID)
			if exists {
				states[item.batch.WorkflowID] = state
			}
		}
		if state := states[item.batch.WorkflowID]; state != nil && activeWorkflow(state.Status) {
			requests[item.batch.WorkflowID] = append(requests[item.batch.WorkflowID], item.batch.RequestID)
		}
	}
	{
		records := encodeAcceptedReceipts(accepted, states, encodingWorkers)
		if err := stageEncodedSets(batch, records); err != nil {
			return nil, err
		}
	}
	eventSegments, taskSegments, err := buildAcceptedSegments(accepted, states)
	if err != nil {
		return nil, err
	}
	if err := stageEncodedSets(batch, encodeEventSegments(eventSegments, encodingWorkers)); err != nil {
		return nil, err
	}
	if err := stageEncodedSets(batch, encodeTaskResultSegments(taskSegments, encodingWorkers)); err != nil {
		return nil, err
	}
	workflowIDs := make([]string, 0, len(states))
	for workflowID := range states {
		workflowIDs = append(workflowIDs, workflowID)
	}
	sort.Strings(workflowIDs)
	{
		records := encodeWorkflowStates(workflowIDs, states, encodingWorkers)
		for index, workflowID := range workflowIDs {
			if err := stageEncodedSets(batch, records[index:index+1]); err != nil {
				return nil, err
			}
			if activeWorkflow(states[workflowID].Status) {
				if err := batch.Set(segmentedKey(9, workflowID), activeWorkflowValue, nil); err != nil {
					return nil, err
				}
				for _, requestID := range requests[workflowID] {
					if err := batch.Set(segmentedKey(10, workflowID, requestID), activeWorkflowValue, nil); err != nil {
						return nil, err
					}
				}
			} else if resident[workflowID] {
				if err := batch.Delete(segmentedKey(9, workflowID), nil); err != nil {
					return nil, err
				}
				prefix := segmentedKey(10, workflowID)
				if err := batch.DeleteRange(prefix, prefixUpper(prefix), nil); err != nil {
					return nil, err
				}
			}
		}
	}
	{
		records := encodeApplicationOutcomes(outcomes, encodingWorkers)
		if err := stageEncodedSets(batch, records); err != nil {
			return nil, err
		}
	}
	last := entries[len(entries)-1]
	if err := stageEncodedSets(batch, []encodedSet{encodeAppliedState(last)}); err != nil {
		return nil, err
	}
	if err := s.hit(ApplyBeforeSync); err != nil {
		return nil, err
	}
	membership := s.membership.current.Swap(nil)
	if err := batch.Commit(pebble.Sync); err != nil {
		s.membership.current.Store(membership)
		return nil, err
	}
	if membership == nil {
		membership = newDurableMembership(nil, nil)
	}
	membership.addAccepted(accepted)
	s.membership.current.Store(membership)
	if err := s.hit(ApplyAfterSync); err != nil {
		return nil, err
	}
	s.applied = last.Index
	s.appliedTerm = last.Term
	for workflowID := range states {
		s.engine.EvictTerminal(workflowID)
	}
	committed = true
	return outcomes, nil
}

func (s *Store) Prepare(commands []core.Command) ([]core.CommandBatch, map[string]core.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint := s.engine.Checkpoint(nil)
	defer s.engine.Rollback(checkpoint)
	batches := make([]core.CommandBatch, 0, len(commands))
	immediate := make(map[string]core.Result)
	for _, command := range commands {
		batch, result, duplicate, err := s.engine.Prepare(command)
		if err != nil {
			return nil, nil, err
		}
		if duplicate {
			immediate[command.RequestID] = result
			continue
		}
		s.engine.ExtendCheckpoint(&checkpoint, batch)
		if _, err := s.engine.ApplyBatch(batch); err != nil {
			return nil, nil, err
		}
		batches = append(batches, batch)
	}
	return batches, immediate, nil
}

func (s *Store) Check(check func(*core.Engine) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return check(s.engine)
}

func stageEncodedSets(batch *pebble.Batch, records []encodedSet) error {
	for _, record := range records {
		if record.err != nil {
			return record.err
		}
		if err := batch.Set(record.key, record.value, nil); err != nil {
			return err
		}
	}
	return nil
}

func taskResult(event core.Event, index uint64, task core.TaskState) (TaskResult, bool) {
	phase := "normal"
	taskType := task.Definition.Type
	if event.Compensation {
		phase = "compensation"
		if task.Definition.Compensation != nil {
			taskType = task.Definition.Compensation.TaskType
		}
	}
	result := TaskResult{
		Version: TaskResultVersion, WorkflowID: event.WorkflowID, TaskID: event.TaskID,
		EventKind: event.Kind, TaskType: taskType, Phase: phase,
		AttemptID: event.AttemptID, Attempt: event.Attempt, Sequence: event.Sequence,
		AppliedIndex: index, At: event.At, WorkerID: event.WorkerID, Fence: event.Fence,
		Idempotency: event.Idempotency,
	}
	switch event.Kind {
	case core.EventTaskCompleted:
		result.Status = core.TaskCompleted
		result.Output = event.Output
	case core.EventTaskFailed:
		result.Status = core.TaskFailed
		result.Error = event.Error
	case core.EventTaskTimedOut:
		result.Status = core.TaskFailed
		result.Error = event.Error
	case core.EventTaskCompensated:
		result.Status = core.TaskCompensated
		result.Output = event.Output
	default:
		return TaskResult{}, false
	}
	return result, true
}

func (s *Store) Applied() AppliedState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return AppliedState{Index: s.applied, Term: s.appliedTerm}
}

func (s *Store) Engine() *core.Engine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engine.Clone()
}

func (s *Store) State(workflowID string) (*core.WorkflowState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, exists, err := s.loadWorkflow(workflowID)
	return state, exists && err == nil
}

func (s *Store) Projection(workflowID string) (core.Projection, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, exists, err := s.loadWorkflow(workflowID)
	if err != nil || !exists {
		return core.Projection{}, false
	}
	return core.Project(state), true
}

func (s *Store) Receipt(requestID string) (Receipt, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadReceipt(requestID)
}

func (s *Store) Outcome(requestID, requestHash string) (Outcome, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, err := s.get(segmentedKey(7, requestID, requestHash))
	if errors.Is(err, pebble.ErrNotFound) {
		receipt, exists, err := s.loadReceipt(requestID)
		if err != nil || !exists || receipt.RequestHash != requestHash {
			return Outcome{}, false, err
		}
		return outcomeFromReceipt(receipt), true, nil
	}
	if err != nil {
		return Outcome{}, false, err
	}
	var outcome Outcome
	if err := decodeJSON(data, &outcome); err != nil {
		return Outcome{}, false, err
	}
	if outcome.Version != OutcomeVersion || outcome.CommittedTerm == 0 || outcome.RequestID != requestID || outcome.RequestHash != requestHash || outcome.Code == "" {
		return Outcome{}, false, ErrRecordVersion
	}
	return outcome, true, nil
}

func outcomeFromReceipt(receipt Receipt) Outcome {
	return Outcome{
		Version: OutcomeVersion, RequestID: receipt.RequestID, RequestHash: receipt.RequestHash,
		AppliedIndex: receipt.AppliedIndex, CommittedTerm: receipt.CommittedTerm, Result: receipt.Result,
	}
}

func (s *Store) Events(workflowID string) ([]core.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := segmentedKey(8, workflowID)
	upper := prefixUpper(prefix)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var events []core.Event
	var previousLast uint64
	for iterator.First(); iterator.Valid(); iterator.Next() {
		segment, err := decodeEventSegment(iterator.Key(), iterator.Value())
		if err != nil {
			return nil, err
		}
		if segment.AppliedIndex > s.applied {
			return nil, ErrRecordVersion
		}
		if len(events) == 0 {
			if segment.FirstSequence != 1 {
				return nil, ErrRecordVersion
			}
		} else if previousLast == ^uint64(0) || segment.FirstSequence != previousLast+1 {
			return nil, ErrRecordVersion
		}
		events = append(events, segment.Events...)
		previousLast = segment.LastSequence
	}
	return events, iterator.Error()
}

func (s *Store) FirstEvent(workflowID string) (core.Event, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix := segmentedKey(8, workflowID)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpper(prefix)})
	if err != nil {
		return core.Event{}, false, err
	}
	defer iterator.Close()
	if !iterator.First() {
		return core.Event{}, false, iterator.Error()
	}
	segment, err := decodeEventSegment(iterator.Key(), iterator.Value())
	if err != nil {
		return core.Event{}, false, err
	}
	if segment.AppliedIndex > s.applied {
		return core.Event{}, false, ErrRecordVersion
	}
	if segment.FirstSequence != 1 {
		return core.Event{}, false, ErrRecordVersion
	}
	return segment.Events[0], true, nil
}

func (s *Store) TaskResults() ([]TaskResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadTaskResults()
}

func (s *Store) scan(prefix byte, visit func([]byte, []byte) error) error {
	lower, upper := prefixBounds(prefix)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iterator.Close()
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if err := visit(iterator.Key(), iterator.Value()); err != nil {
			return err
		}
	}
	return iterator.Error()
}
