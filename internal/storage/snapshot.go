package storage

import (
	"bytes"
	"fmt"
	"slices"
	"sort"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func (s *Store) CreateSnapshot() (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied <= s.snapshot.GetMetadata().GetIndex() {
		return nil, raft.ErrSnapOutOfDate
	}
	term, err := s.term(s.applied)
	if err != nil {
		return nil, err
	}
	receipts, err := s.loadReceipts()
	if err != nil {
		return nil, err
	}
	outcomes, err := s.loadOutcomes(receipts)
	if err != nil {
		return nil, err
	}
	results, err := s.loadTaskResults()
	if err != nil {
		return nil, err
	}
	engine, err := s.loadEngineSnapshotWithReceipts(receipts)
	if err != nil {
		return nil, err
	}
	if _, err := core.Restore(engine); err != nil {
		return nil, err
	}
	image := SnapshotImage{
		Version: SnapshotVersion, ClusterID: s.clusterID, AppliedIndex: s.applied,
		Term: term, Engine: engine, Outcomes: outcomes, TaskResults: results,
	}
	data, err := encodeSnapshotImage(image)
	if err != nil {
		return nil, err
	}
	snapshot := &pb.Snapshot{
		Data: data,
		Metadata: &pb.SnapshotMetadata{
			Index: proto.Uint64(s.applied), Term: proto.Uint64(term),
			ConfState: proto.Clone(s.conf).(*pb.ConfState),
		},
	}
	encoded, err := encodeProto(snapshot)
	if err != nil {
		return nil, err
	}
	conf, err := encodeProto(s.conf)
	if err != nil {
		return nil, err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(snapshotKey, encoded, nil); err != nil {
		return nil, err
	}
	if err := batch.Set(confStateKey, conf, nil); err != nil {
		return nil, err
	}
	// Compact raft and application logs through this index
	lower, _ := prefixBounds(2)
	if err := batch.DeleteRange(lower, numberKey(2, s.applied+1), nil); err != nil {
		return nil, err
	}
	lower, _ = prefixBounds(3)
	if err := batch.DeleteRange(lower, numberKey(3, s.applied+1), nil); err != nil {
		return nil, err
	}
	if err := s.hit(SnapshotBeforeSync); err != nil {
		return nil, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, err
	}
	if err := s.hit(SnapshotAfterSync); err != nil {
		return nil, err
	}
	offset := s.applied - s.snapshot.GetMetadata().GetIndex()
	if offset > uint64(len(s.entryTerms)) {
		return nil, fmt.Errorf("snapshot index exceeds raft log")
	}
	s.entryTerms = append([]uint64(nil), s.entryTerms[offset:]...)
	s.snapshot = proto.Clone(snapshot).(*pb.Snapshot)
	return proto.Clone(snapshot).(*pb.Snapshot), nil
}

func (s *Store) InstallSnapshot(snapshot *pb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.installSnapshot(snapshot, true)
}

func (s *Store) installSnapshot(snapshot *pb.Snapshot, hooks bool) error {
	if snapshot.GetMetadata().GetIndex() <= s.applied {
		return nil
	}
	if snapshot.GetMetadata().GetIndex() != s.snapshot.GetMetadata().GetIndex() || snapshot.GetMetadata().GetTerm() != s.snapshot.GetMetadata().GetTerm() {
		return fmt.Errorf("snapshot is not durable raft state")
	}
	image, err := s.snapshotImage(snapshot)
	if err != nil {
		return err
	}
	engine, err := core.Restore(image.Engine)
	if err != nil {
		return err
	}
	engine.DiscardJournal()
	membership := membershipFromSnapshot(image.Engine)
	batch := s.db.NewBatch()
	defer batch.Close()
	for prefix := byte(3); prefix <= 10; prefix++ {
		lower, upper := prefixBounds(prefix)
		if err := batch.DeleteRange(lower, upper, nil); err != nil {
			return err
		}
	}
	type commitOrigin struct {
		index uint64
		term  uint64
	}
	active := make(map[string]struct{})
	for _, workflow := range image.Engine.Workflows {
		if activeWorkflow(workflow.Status) {
			active[workflow.ID] = struct{}{}
		}
	}
	origins := make(map[string]commitOrigin, len(image.Outcomes))
	for _, outcome := range image.Outcomes {
		if outcome.Version != OutcomeVersion || outcome.AppliedIndex == 0 || outcome.AppliedIndex > image.AppliedIndex || outcome.CommittedTerm == 0 || outcome.RequestID == "" || outcome.RequestHash == "" {
			return ErrRecordVersion
		}
		if outcome.Code == "" {
			key := outcome.RequestID + "\x00" + outcome.RequestHash
			if _, exists := origins[key]; exists {
				return ErrCorruptSnapshot
			}
			origins[key] = commitOrigin{outcome.AppliedIndex, outcome.CommittedTerm}
			continue
		}
		data, err := encodeJSON(outcome)
		if err != nil {
			return err
		}
		if err := batch.Set(segmentedKey(7, outcome.RequestID, outcome.RequestHash), data, nil); err != nil {
			return err
		}
	}
	eventSources := make([]eventSegmentSource, 0, len(image.Engine.Requests))
	for _, request := range image.Engine.Requests {
		origin, exists := origins[request.RequestID+"\x00"+request.RequestHash]
		if !exists {
			return ErrCorruptSnapshot
		}
		receipt := Receipt{
			Version: ReceiptVersion, RequestID: request.RequestID, RequestHash: request.RequestHash,
			AppliedIndex: origin.index, CommittedTerm: origin.term, Result: request.Result,
		}
		record, err := compactReceipt(receipt)
		if err != nil {
			return err
		}
		data, err := encodeReceiptRecord(record)
		if err != nil {
			return err
		}
		if err := batch.Set(segmentedKey(4, request.RequestID), data, nil); err != nil {
			return err
		}
		if _, exists := active[request.Result.WorkflowID]; exists {
			if err := batch.Set(segmentedKey(10, request.Result.WorkflowID, request.RequestID), activeWorkflowValue, nil); err != nil {
				return err
			}
		}
		eventSources = append(eventSources, eventSegmentSource{request.Result.WorkflowID, origin.index, request.Result.Events})
	}
	eventSegments, err := buildEventSegments(eventSources, true)
	if err != nil {
		return err
	}
	if err := stageEncodedSets(batch, encodeEventSegments(eventSegments, applicationEncodingWorkers)); err != nil {
		return err
	}
	for _, workflow := range image.Engine.Workflows {
		data, err := encodeJSON(workflow)
		if err != nil {
			return err
		}
		if err := batch.Set(segmentedKey(6, workflow.ID), data, nil); err != nil {
			return err
		}
		if activeWorkflow(workflow.Status) {
			if err := batch.Set(segmentedKey(9, workflow.ID), activeWorkflowValue, nil); err != nil {
				return err
			}
		}
	}
	for _, result := range image.TaskResults {
		if result.AppliedIndex > image.AppliedIndex {
			return ErrRecordVersion
		}
	}
	taskSegments, err := buildTaskResultSegments(image.TaskResults)
	if err != nil {
		return err
	}
	if err := stageEncodedSets(batch, encodeTaskResultSegments(taskSegments, applicationEncodingWorkers)); err != nil {
		return err
	}
	applied, err := encodeJSON(AppliedState{Index: image.AppliedIndex, Term: image.Term})
	if err != nil {
		return err
	}
	if err := batch.Set(appliedKey, applied, nil); err != nil {
		return err
	}
	if err := batch.Set(activeIndexKey, activeIndexValue, nil); err != nil {
		return err
	}
	if hooks {
		if err := s.hit(SnapshotInstallBeforeSync); err != nil {
			return err
		}
	}
	// Hide membership while the snapshot installs.
	previousMembership := s.membership.current.Swap(nil)
	if err := batch.Commit(pebble.Sync); err != nil {
		s.membership.current.Store(previousMembership)
		return err
	}
	s.membership.current.Store(membership)
	if hooks {
		if err := s.hit(SnapshotInstallAfterSync); err != nil {
			return err
		}
	}
	engine.RetainActive()
	s.engine = engine
	s.attachEngine()
	s.applied = image.AppliedIndex
	s.appliedTerm = image.Term
	return nil
}

func (s *Store) validateSnapshot(snapshot *pb.Snapshot) error {
	_, err := s.snapshotImage(snapshot)
	return err
}

func (s *Store) snapshotImage(snapshot *pb.Snapshot) (SnapshotImage, error) {
	image, err := decodeSnapshotImage(snapshot.GetData())
	if err != nil {
		return SnapshotImage{}, err
	}
	metadata := snapshot.GetMetadata()
	if image.ClusterID != s.clusterID || image.AppliedIndex != metadata.GetIndex() || image.Term != metadata.GetTerm() {
		return SnapshotImage{}, ErrCorruptSnapshot
	}
	if !equalConfState(metadata.GetConfState(), s.conf) {
		return SnapshotImage{}, fmt.Errorf("%w: snapshot conf %v store conf %v", ErrStoreIdentity, metadata.GetConfState(), s.conf)
	}
	if _, err := core.Restore(image.Engine); err != nil {
		return SnapshotImage{}, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	return image, nil
}

func equalConfState(first, second *pb.ConfState) bool {
	return slices.Equal(first.GetVoters(), second.GetVoters()) &&
		slices.Equal(first.GetLearners(), second.GetLearners()) &&
		slices.Equal(first.GetVotersOutgoing(), second.GetVotersOutgoing()) &&
		slices.Equal(first.GetLearnersNext(), second.GetLearnersNext()) &&
		first.GetAutoLeave() == second.GetAutoLeave()
}

func (s *Store) loadOutcomes(receipts []Receipt) ([]Outcome, error) {
	var outcomes []Outcome
	if err := s.scan(7, func(key, data []byte) error {
		var outcome Outcome
		if err := decodeJSON(data, &outcome); err != nil {
			return err
		}
		if outcome.Version != OutcomeVersion || outcome.CommittedTerm == 0 || outcome.RequestID == "" || outcome.RequestHash == "" || outcome.Code == "" || !bytes.Equal(key, segmentedKey(7, outcome.RequestID, outcome.RequestHash)) {
			return ErrRecordVersion
		}
		outcomes = append(outcomes, outcome)
		return nil
	}); err != nil {
		return nil, err
	}
	for _, receipt := range receipts {
		outcomes = append(outcomes, outcomeFromReceipt(receipt))
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].RequestID != outcomes[j].RequestID {
			return outcomes[i].RequestID < outcomes[j].RequestID
		}
		return outcomes[i].RequestHash < outcomes[j].RequestHash
	})
	return outcomes, nil
}

func (s *Store) loadTaskResults() ([]TaskResult, error) {
	var results []TaskResult
	seen := make(map[taskResultIdentity]struct{})
	err := s.scan(5, func(key, data []byte) error {
		segment, err := decodeTaskResultSegment(key, data)
		if err != nil {
			return err
		}
		if segment.AppliedIndex > s.applied {
			return ErrRecordVersion
		}
		for _, result := range segment.Results {
			identity := resultIdentity(result)
			if _, exists := seen[identity]; exists {
				return ErrRecordVersion
			}
			seen[identity] = struct{}{}
			results = append(results, result)
		}
		return nil
	})
	sortTaskResults(results)
	return results, err
}
