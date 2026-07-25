package storage

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func (s *Store) SaveReady(ready raft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if raft.IsEmptyHardState(ready.HardState) && len(ready.Entries) == 0 && raft.IsEmptySnap(ready.Snapshot) {
		return nil
	}
	nextHard := proto.Clone(s.hard).(*pb.HardState)
	nextConf := proto.Clone(s.conf).(*pb.ConfState)
	nextSnapshot := proto.Clone(s.snapshot).(*pb.Snapshot)
	nextLength := len(s.entryTerms)
	previousLast := nextSnapshot.GetMetadata().GetIndex() + uint64(nextLength)
	snapshotChanged := !raft.IsEmptySnap(ready.Snapshot)
	if snapshotChanged {
		if err := s.validateSnapshot(ready.Snapshot); err != nil {
			return err
		}
		if ready.Snapshot.GetMetadata().GetIndex() <= nextSnapshot.GetMetadata().GetIndex() {
			return raft.ErrSnapOutOfDate
		}
		nextSnapshot = proto.Clone(ready.Snapshot).(*pb.Snapshot)
		nextConf = proto.Clone(ready.Snapshot.GetMetadata().GetConfState()).(*pb.ConfState)
		nextLength = 0
	}
	base := nextSnapshot.GetMetadata().GetIndex() + 1
	kept := make([]*pb.Entry, 0, len(ready.Entries))
	for _, entry := range ready.Entries {
		if entry.GetIndex() < base {
			continue
		}
		offset := entry.GetIndex() - base
		if offset > uint64(nextLength) {
			return fmt.Errorf("raft entry gap at %d", entry.GetIndex())
		}
		nextLength = int(offset) + 1
		kept = append(kept, entry)
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		if ready.HardState.GetCommit() < nextHard.GetCommit() || ready.HardState.GetTerm() < nextHard.GetTerm() {
			return fmt.Errorf("raft hard state regressed")
		}
		nextHard = proto.Clone(ready.HardState).(*pb.HardState)
	}
	snapshotIndex := nextSnapshot.GetMetadata().GetIndex()
	last := snapshotIndex + uint64(nextLength)
	if nextHard.GetCommit() < snapshotIndex || nextHard.GetCommit() > last {
		return fmt.Errorf("raft commit %d is outside persisted range [%d,%d]", nextHard.GetCommit(), snapshotIndex, last)
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if snapshotChanged {
		data, err := encodeProto(nextSnapshot)
		if err != nil {
			return err
		}
		conf, err := encodeProto(nextConf)
		if err != nil {
			return err
		}
		lower, upper := prefixBounds(2)
		if err := batch.DeleteRange(lower, upper, nil); err != nil {
			return err
		}
		if err := batch.Set(snapshotKey, data, nil); err != nil {
			return err
		}
		if err := batch.Set(confStateKey, conf, nil); err != nil {
			return err
		}
	} else if len(kept) > 0 && kept[0].GetIndex() <= previousLast {
		// Replace conflicting log suffixes atomically.
		_, upper := prefixBounds(2)
		if err := batch.DeleteRange(numberKey(2, kept[0].GetIndex()), upper, nil); err != nil {
			return err
		}
	}
	for _, entry := range kept {
		data, err := encodeProto(entry)
		if err != nil {
			return err
		}
		if err := batch.Set(numberKey(2, entry.GetIndex()), data, nil); err != nil {
			return err
		}
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		data, err := encodeProto(nextHard)
		if err != nil {
			return err
		}
		if err := batch.Set(hardStateKey, data, nil); err != nil {
			return err
		}
	}
	// sync log, vote, term, and snapshot changes.
	writeOptions := raftWriteOptions(ready, snapshotChanged, len(kept), s.hard, nextHard)
	if writeOptions == pebble.Sync {
		if err := s.hit(RaftBeforeSync); err != nil {
			return err
		}
	}
	if err := batch.Commit(writeOptions); err != nil {
		return err
	}
	if writeOptions == pebble.Sync {
		if err := s.hit(RaftAfterSync); err != nil {
			return err
		}
	}
	s.hard = nextHard
	s.conf = nextConf
	s.snapshot = nextSnapshot
	if snapshotChanged {
		s.entryTerms = nil
	}
	for _, entry := range kept {
		offset := entry.GetIndex() - s.snapshot.GetMetadata().GetIndex() - 1
		s.entryTerms = s.entryTerms[:offset]
		s.entryTerms = append(s.entryTerms, entry.GetTerm())
	}
	return nil
}

func raftWriteOptions(ready raft.Ready, snapshotChanged bool, entries int, previous, next *pb.HardState) *pebble.WriteOptions {
	if ready.MustSync || snapshotChanged || entries > 0 || previous.GetTerm() != next.GetTerm() || previous.GetVote() != next.GetVote() {
		return pebble.Sync
	}
	return pebble.NoSync
}

func (s *Store) SaveConfState(conf *pb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := encodeProto(conf)
	if err != nil {
		return err
	}
	if err := s.db.Set(confStateKey, data, pebble.Sync); err != nil {
		return err
	}
	s.conf = proto.Clone(conf).(*pb.ConfState)
	return nil
}

func (s *Store) RaftState() (*pb.HardState, *pb.ConfState, *pb.Snapshot, []*pb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first := s.snapshot.GetMetadata().GetIndex() + 1
	last := first + uint64(len(s.entryTerms))
	entries, err := s.loadEntries(first, last, ^uint64(0))
	return proto.Clone(s.hard).(*pb.HardState), proto.Clone(s.conf).(*pb.ConfState), proto.Clone(s.snapshot).(*pb.Snapshot), entries, err
}
