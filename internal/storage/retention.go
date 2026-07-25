package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/cockroachdb/pebble/v2"
)

var activeIndexKey = []byte{1, 8}
var activeIndexValue = []byte{0, 0, 0, 1}
var activeWorkflowValue = []byte{1}

func (s *Store) ensureActiveIndex() error {
	data, err := s.get(activeIndexKey)
	if err == nil {
		if !bytes.Equal(data, activeIndexValue) {
			return ErrRecordVersion
		}
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	// Marker makes index reconstruction a one-time migration
	batch := s.db.NewBatch()
	defer batch.Close()
	lower, upper := prefixBounds(9)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	lower, upper = prefixBounds(10)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	active := make(map[string]uint64)
	lower, upper = prefixBounds(6)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var state core.WorkflowState
		if err := decodeJSON(iterator.Value(), &state); err != nil {
			iterator.Close()
			return err
		}
		if state.ID == "" || !bytes.Equal(iterator.Key(), segmentedKey(6, state.ID)) {
			iterator.Close()
			return ErrRecordVersion
		}
		if activeWorkflow(state.Status) {
			if err := batch.Set(segmentedKey(9, state.ID), activeWorkflowValue, nil); err != nil {
				iterator.Close()
				return err
			}
			active[state.ID] = 0
		}
	}
	if err := iterator.Error(); err != nil {
		iterator.Close()
		return err
	}
	if err := iterator.Close(); err != nil {
		return err
	}
	lower, upper = prefixBounds(4)
	iterator, err = s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		record, err := decodeReceiptRecord(iterator.Value())
		if err != nil {
			iterator.Close()
			return err
		}
		if !bytes.Equal(iterator.Key(), segmentedKey(4, record.RequestID)) {
			iterator.Close()
			return ErrRecordVersion
		}
		if _, exists := active[record.WorkflowID]; exists {
			if err := batch.Set(segmentedKey(10, record.WorkflowID, record.RequestID), activeWorkflowValue, nil); err != nil {
				iterator.Close()
				return err
			}
			active[record.WorkflowID]++
		}
	}
	if err := iterator.Error(); err != nil {
		iterator.Close()
		return err
	}
	if err := iterator.Close(); err != nil {
		return err
	}
	for _, requests := range active {
		if requests == 0 {
			return ErrRecordVersion
		}
	}
	if err := batch.Set(activeIndexKey, activeIndexValue, nil); err != nil {
		return err
	}
	if err := s.hit(ActiveIndexBeforeSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	return s.hit(ActiveIndexAfterSync)
}

func (s *Store) loadActiveEngine() (*core.Engine, error) {
	engine := core.NewEngine()
	engine.DiscardJournal()
	active := make(map[string]uint64)
	lower, upper := prefixBounds(9)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		workflowID, ok := activeWorkflowID(iterator.Key())
		if !ok || !bytes.Equal(iterator.Value(), activeWorkflowValue) {
			iterator.Close()
			return nil, ErrRecordVersion
		}
		state, exists, err := s.loadWorkflow(workflowID)
		if err != nil {
			iterator.Close()
			return nil, err
		}
		if !exists || !activeWorkflow(state.Status) {
			iterator.Close()
			return nil, ErrRecordVersion
		}
		if err := engine.HydrateState(state); err != nil {
			iterator.Close()
			return nil, err
		}
		active[workflowID] = 0
	}
	if err := iterator.Error(); err != nil {
		iterator.Close()
		return nil, err
	}
	if err := iterator.Close(); err != nil {
		return nil, err
	}
	lower, upper = prefixBounds(10)
	iterator, err = s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	for iterator.First(); iterator.Valid(); iterator.Next() {
		workflowID, requestID, ok := activeRequestIDs(iterator.Key())
		if !ok || !bytes.Equal(iterator.Value(), activeWorkflowValue) {
			iterator.Close()
			return nil, ErrRecordVersion
		}
		if _, exists := active[workflowID]; !exists {
			iterator.Close()
			return nil, ErrRecordVersion
		}
		receipt, exists, err := s.loadReceipt(requestID)
		if err != nil {
			iterator.Close()
			return nil, err
		}
		if !exists || receipt.Result.WorkflowID != workflowID {
			iterator.Close()
			return nil, ErrRecordVersion
		}
		if err := engine.HydrateRequest(core.RequestReceipt{RequestID: receipt.RequestID, RequestHash: receipt.RequestHash, Result: receipt.Result}); err != nil {
			iterator.Close()
			return nil, err
		}
		active[workflowID]++
	}
	if err := iterator.Error(); err != nil {
		iterator.Close()
		return nil, err
	}
	if err := iterator.Close(); err != nil {
		return nil, err
	}
	for _, requests := range active {
		if requests == 0 {
			return nil, ErrRecordVersion
		}
	}
	return engine, nil
}

func activeWorkflowID(key []byte) (string, bool) {
	values, ok := segmentedValues(key, 9, 1)
	if !ok {
		return "", false
	}
	return values[0], true
}

func activeRequestIDs(key []byte) (string, string, bool) {
	values, ok := segmentedValues(key, 10, 2)
	if !ok {
		return "", "", false
	}
	return values[0], values[1], true
}

func segmentedValues(key []byte, prefix byte, count int) ([]string, bool) {
	if len(key) < 6 || key[0] != prefix {
		return nil, false
	}
	values := make([]string, 0, count)
	position := 1
	for len(values) < count {
		if position+4 > len(key) {
			return nil, false
		}
		length := int(binary.BigEndian.Uint32(key[position : position+4]))
		position += 4
		if length == 0 || position+length > len(key) {
			return nil, false
		}
		values = append(values, string(key[position:position+length]))
		position += length
	}
	if position != len(key) {
		return nil, false
	}
	return values, true
}

func activeWorkflow(status core.WorkflowStatus) bool {
	switch status {
	case core.WorkflowCompleted, core.WorkflowFailed, core.WorkflowCancelled, core.WorkflowCompensated:
		return false
	default:
		return true
	}
}

func (s *Store) attachEngine() {
	s.engine.SetLoaders(s.loadEngineWorkflow, s.loadEngineRequest)
}

func (s *Store) loadWorkflow(workflowID string) (*core.WorkflowState, bool, error) {
	data, err := s.get(segmentedKey(6, workflowID))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var state core.WorkflowState
	if err := decodeJSON(data, &state); err != nil {
		return nil, false, err
	}
	if state.ID == "" || state.ID != workflowID {
		return nil, false, ErrRecordVersion
	}
	return &state, true, nil
}

func (s *Store) loadReceipt(requestID string) (Receipt, bool, error) {
	data, err := s.get(segmentedKey(4, requestID))
	if errors.Is(err, pebble.ErrNotFound) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, err
	}
	record, err := decodeReceiptRecord(data)
	if err != nil {
		return Receipt{}, false, err
	}
	if record.RequestID != requestID {
		return Receipt{}, false, ErrRecordVersion
	}
	receipt, err := s.expandReceipt(record)
	return receipt, err == nil, err
}

func (s *Store) loadRequest(requestID string) (core.RequestReceipt, bool, error) {
	receipt, exists, err := s.loadReceipt(requestID)
	if err != nil || !exists {
		return core.RequestReceipt{}, exists, err
	}
	return core.RequestReceipt{RequestID: receipt.RequestID, RequestHash: receipt.RequestHash, Result: receipt.Result}, true, nil
}

func (s *Store) loadEngineSnapshot() (core.EngineSnapshot, error) {
	receipts, err := s.loadReceipts()
	if err != nil {
		return core.EngineSnapshot{}, err
	}
	return s.loadEngineSnapshotWithReceipts(receipts)
}

func (s *Store) loadEngineSnapshotWithReceipts(receipts []Receipt) (core.EngineSnapshot, error) {
	snapshot := core.EngineSnapshot{Version: core.EngineSnapshotVersion}
	if err := s.scan(6, func(key, data []byte) error {
		var state core.WorkflowState
		if err := decodeJSON(data, &state); err != nil {
			return err
		}
		if state.ID == "" || !bytes.Equal(key, segmentedKey(6, state.ID)) {
			return ErrRecordVersion
		}
		snapshot.Workflows = append(snapshot.Workflows, state)
		return nil
	}); err != nil {
		return core.EngineSnapshot{}, err
	}
	for _, receipt := range receipts {
		snapshot.Requests = append(snapshot.Requests, core.RequestReceipt{
			RequestID: receipt.RequestID, RequestHash: receipt.RequestHash, Result: receipt.Result,
		})
	}
	sort.Slice(snapshot.Workflows, func(i, j int) bool { return snapshot.Workflows[i].ID < snapshot.Workflows[j].ID })
	sort.Slice(snapshot.Requests, func(i, j int) bool { return snapshot.Requests[i].RequestID < snapshot.Requests[j].RequestID })
	return snapshot, nil
}

func (s *Store) loadReceipts() ([]Receipt, error) {
	var receipts []Receipt
	cache := make(map[string]eventSegment)
	if err := s.scan(4, func(key, data []byte) error {
		record, err := decodeReceiptRecord(data)
		if err != nil {
			return err
		}
		if !bytes.Equal(key, segmentedKey(4, record.RequestID)) {
			return ErrRecordVersion
		}
		receipt, err := s.expandReceiptCached(record, cache)
		if err != nil {
			return err
		}
		receipts = append(receipts, receipt)
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].RequestID < receipts[j].RequestID })
	return receipts, nil
}

func (s *Store) DurableSnapshot() (core.EngineSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadEngineSnapshot()
}
