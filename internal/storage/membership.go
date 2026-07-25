package storage

import (
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/cockroachdb/pebble/v2"
)

const membershipShardBits = 12
const membershipShardCount = 1 << membershipShardBits
const membershipShardInitialCapacity = 1024
const membershipBitsPerID = 16
const membershipHashCount = 7
const membershipChunkSize = 1024
const membershipReceiptPrefix = 4
const membershipWorkflowPrefix = 6

type membershipLayer struct {
	words    []atomic.Uint64
	capacity uint64
	count    uint64
	mask     uint64
}

type membershipLayers struct {
	values []*membershipLayer
}

type membershipShard struct {
	mu     sync.Mutex
	layers atomic.Pointer[membershipLayers]
}

type membershipFilter struct {
	seed   maphash.Seed
	shards [membershipShardCount]membershipShard
}

type durableMembership struct {
	receipts  *membershipFilter
	workflows *membershipFilter
}

type durableMembershipIndex struct {
	current       atomic.Pointer[durableMembership]
	receiptSkips  atomic.Uint64
	workflowSkips atomic.Uint64
}

func newMembershipLayer(capacity uint64) *membershipLayer {
	bits := capacity * membershipBitsPerID
	return &membershipLayer{words: make([]atomic.Uint64, bits/64), capacity: capacity, mask: bits - 1}
}

func newMembershipFilter(values []string) *membershipFilter {
	filter := &membershipFilter{seed: maphash.MakeSeed()}
	filter.addAll(values)
	return filter
}

func newDurableMembership(receipts, workflows []string) *durableMembership {
	return &durableMembership{receipts: newMembershipFilter(receipts), workflows: newMembershipFilter(workflows)}
}

func membershipFromSnapshot(snapshot core.EngineSnapshot) *durableMembership {
	membership := newDurableMembership(nil, nil)
	values := make([]string, 0, membershipChunkSize)
	for index := range snapshot.Requests {
		values = append(values, snapshot.Requests[index].RequestID)
		if len(values) == cap(values) {
			membership.receipts.addAll(values)
			values = values[:0]
		}
	}
	membership.receipts.addAll(values)
	values = values[:0]
	for index := range snapshot.Workflows {
		values = append(values, snapshot.Workflows[index].ID)
		if len(values) == cap(values) {
			membership.workflows.addAll(values)
			values = values[:0]
		}
	}
	membership.workflows.addAll(values)
	return membership
}

func (f *membershipFilter) hash(value string) uint64 {
	return maphash.String(f.seed, value)
}

func membershipShardIndex(hash uint64) uint64 {
	return hash >> (64 - membershipShardBits)
}

func membershipDelta(hash uint64) uint64 {
	value := hash + 0x9e3779b97f4a7c15
	value = (value ^ value>>30) * 0xbf58476d1ce4e5b9
	value = (value ^ value>>27) * 0x94d049bb133111eb
	return (value ^ value>>31) | 1
}

func (l *membershipLayer) add(hash uint64) {
	delta := membershipDelta(hash)
	for index := uint64(0); index < membershipHashCount; index++ {
		bit := (hash + index*delta) & l.mask
		l.words[bit>>6].Or(uint64(1) << (bit & 63))
	}
}

func (l *membershipLayer) contains(hash uint64) bool {
	delta := membershipDelta(hash)
	for index := uint64(0); index < membershipHashCount; index++ {
		bit := (hash + index*delta) & l.mask
		if l.words[bit>>6].Load()&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func (f *membershipFilter) contains(value string) bool {
	hash := f.hash(value)
	layers := f.shards[membershipShardIndex(hash)].layers.Load()
	if layers == nil {
		return false
	}
	for index := len(layers.values) - 1; index >= 0; index-- {
		if layers.values[index].contains(hash) {
			return true
		}
	}
	return false
}

func (f *membershipFilter) addAll(values []string) {
	for _, value := range values {
		f.addValue(value)
	}
}

func (f *membershipFilter) addValue(value string) {
	hash := f.hash(value)
	f.shards[membershipShardIndex(hash)].add(hash)
}

func (s *membershipShard) add(hash uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	layers := s.layers.Load()
	if layers == nil {
		layer := newMembershipLayer(membershipShardInitialCapacity)
		layer.add(hash)
		layer.count = 1
		s.layers.Store(&membershipLayers{values: []*membershipLayer{layer}})
		return
	}
	layer := layers.values[len(layers.values)-1]
	if layer.count < layer.capacity {
		layer.add(hash)
		layer.count++
		return
	}
	layer = newMembershipLayer(layer.capacity * 2)
	layer.add(hash)
	layer.count = 1
	next := make([]*membershipLayer, len(layers.values)+1)
	copy(next, layers.values)
	next[len(layers.values)] = layer
	s.layers.Store(&membershipLayers{values: next})
}

func (m *durableMembership) add(receipts, workflows []string) {
	m.receipts.addAll(receipts)
	m.workflows.addAll(workflows)
}

func (m *durableMembership) addAccepted(accepted []acceptedApplication) {
	for _, item := range accepted {
		m.receipts.addValue(item.batch.RequestID)
		if len(item.batch.Events) > 0 && item.batch.Events[0].Kind == core.EventWorkflowSubmitted {
			m.workflows.addValue(item.batch.WorkflowID)
		}
	}
}

func (s *Store) rebuildDurableMembership() error {
	membership := newDurableMembership(nil, nil)
	if err := s.streamMembershipIDs(membershipReceiptPrefix, membership.receipts); err != nil {
		return err
	}
	if err := s.streamMembershipIDs(membershipWorkflowPrefix, membership.workflows); err != nil {
		return err
	}
	s.membership.current.Store(membership)
	return nil
}

func (s *Store) streamMembershipIDs(prefix byte, filter *membershipFilter) error {
	lower, upper := prefixBounds(prefix)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iterator.Close()
	ids := make([]string, 0, membershipChunkSize)
	for iterator.First(); iterator.Valid(); iterator.Next() {
		values, ok := segmentedValues(iterator.Key(), prefix, 1)
		if !ok {
			return ErrRecordVersion
		}
		ids = append(ids, values[0])
		if len(ids) == cap(ids) {
			filter.addAll(ids)
			ids = ids[:0]
		}
	}
	if err := iterator.Error(); err != nil {
		return err
	}
	filter.addAll(ids)
	return nil
}

func (s *Store) loadEngineWorkflow(workflowID string) (*core.WorkflowState, bool, error) {
	membership := s.membership.current.Load()
	if membership != nil && !membership.workflows.contains(workflowID) {
		s.membership.workflowSkips.Add(1)
		return nil, false, nil
	}
	return s.loadWorkflow(workflowID)
}

func (s *Store) loadEngineRequest(requestID string) (core.RequestReceipt, bool, error) {
	membership := s.membership.current.Load()
	if membership != nil && !membership.receipts.contains(requestID) {
		s.membership.receiptSkips.Add(1)
		return core.RequestReceipt{}, false, nil
	}
	return s.loadRequest(requestID)
}
