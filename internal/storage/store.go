package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type Store struct {
	mu          sync.RWMutex
	db          *pebble.DB
	clusterID   string
	nodeID      uint64
	failpoint   Failpoint
	hard        *pb.HardState
	conf        *pb.ConfState
	snapshot    *pb.Snapshot
	entryTerms  []uint64
	engine      *core.Engine
	applied     uint64
	appliedTerm uint64
	membership  durableMembershipIndex
}

func Open(config Config) (*Store, error) {
	if config.Directory == "" || config.ClusterID == "" || config.NodeID == 0 || len(config.Voters) != 3 {
		return nil, fmt.Errorf("invalid store configuration")
	}
	db, err := pebble.Open(config.Directory, storageOptions())
	if err != nil {
		return nil, err
	}
	store := &Store{db: db, clusterID: config.ClusterID, nodeID: config.NodeID, failpoint: config.Failpoint}
	if err := store.load(config.Voters); err != nil {
		db.Close()
		return nil, err
	}
	store.attachEngine()
	return store, nil
}

func storageOptions() *pebble.Options {
	options := &pebble.Options{
		BytesPerSync:                64 << 20,
		CompactionConcurrencyRange:  func() (int, int) { return 2, 4 },
		FlushSplitBytes:             256 << 20,
		L0CompactionThreshold:       512,
		L0StopWritesThreshold:       1024,
		LBaseMaxBytes:               4 << 30,
		MemTableSize:                256 << 20,
		MemTableStopWritesThreshold: 4,
	}
	options.TargetFileSizes[0] = 128 << 20
	for index := range options.Levels {
		options.Levels[index].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
		options.Levels[index].FilterPolicy = bloom.FilterPolicy(10)
		options.Levels[index].FilterType = pebble.TableFilter
	}
	return options
}

func (s *Store) load(voters []uint64) error {
	data, err := s.get(schemaKey)
	if errors.Is(err, pebble.ErrNotFound) {
		if err := s.initialize(voters); err != nil {
			return err
		}
		data, err = s.get(schemaKey)
	}
	if err != nil {
		return err
	}
	if len(data) != 4 || binary.BigEndian.Uint32(data) != SchemaVersion {
		return ErrStoreVersion
	}
	cluster, err := s.get(clusterKey)
	if err != nil {
		return err
	}
	node, err := s.get(nodeKey)
	if err != nil {
		return err
	}
	if string(cluster) != s.clusterID || len(node) != 8 || binary.BigEndian.Uint64(node) != s.nodeID {
		return ErrStoreIdentity
	}
	if err := s.loadRaft(); err != nil {
		return err
	}
	if err := s.loadApplication(); err != nil {
		return err
	}
	if s.snapshot.GetMetadata().GetIndex() > s.applied {
		if err := s.installSnapshot(s.snapshot, false); err != nil {
			return err
		}
	}
	if s.applied > s.hard.GetCommit() {
		return fmt.Errorf("applied index %d exceeds commit %d", s.applied, s.hard.GetCommit())
	}
	return s.rebuildDurableMembership()
}

func (s *Store) initialize(voters []uint64) error {
	ids := append([]uint64(nil), voters...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	found := false
	for index, id := range ids {
		if id == 0 || index > 0 && id == ids[index-1] {
			return fmt.Errorf("invalid voter configuration")
		}
		found = found || id == s.nodeID
	}
	if !found {
		return fmt.Errorf("node %d is not a voter", s.nodeID)
	}
	conf := &pb.ConfState{Voters: ids}
	image := SnapshotImage{
		Version: SnapshotVersion, ClusterID: s.clusterID, AppliedIndex: 1, Term: 1,
		Engine: core.EngineSnapshot{Version: core.EngineSnapshotVersion},
	}
	payload, err := encodeSnapshotImage(image)
	if err != nil {
		return err
	}
	snapshot := &pb.Snapshot{Data: payload, Metadata: &pb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: conf}}
	hard := &pb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)}
	applied, err := encodeJSON(AppliedState{Index: 1, Term: 1})
	if err != nil {
		return err
	}
	hardData, err := encodeProto(hard)
	if err != nil {
		return err
	}
	confData, err := encodeProto(conf)
	if err != nil {
		return err
	}
	snapshotData, err := encodeProto(snapshot)
	if err != nil {
		return err
	}
	var schema [4]byte
	binary.BigEndian.PutUint32(schema[:], SchemaVersion)
	var node [8]byte
	binary.BigEndian.PutUint64(node[:], s.nodeID)
	batch := s.db.NewBatch()
	defer batch.Close()
	sets := []struct{ key, value []byte }{
		{schemaKey, schema[:]}, {clusterKey, []byte(s.clusterID)}, {nodeKey, node[:]},
		{hardStateKey, hardData}, {confStateKey, confData}, {snapshotKey, snapshotData}, {appliedKey, applied},
		{activeIndexKey, activeIndexValue},
	}
	for _, item := range sets {
		if err := batch.Set(item.key, item.value, nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) loadRaft() error {
	hardData, err := s.get(hardStateKey)
	if err != nil {
		return err
	}
	confData, err := s.get(confStateKey)
	if err != nil {
		return err
	}
	snapshotData, err := s.get(snapshotKey)
	if err != nil {
		return err
	}
	s.hard = &pb.HardState{}
	s.conf = &pb.ConfState{}
	s.snapshot = &pb.Snapshot{}
	if err := proto.Unmarshal(hardData, s.hard); err != nil {
		return err
	}
	if err := proto.Unmarshal(confData, s.conf); err != nil {
		return err
	}
	if err := proto.Unmarshal(snapshotData, s.snapshot); err != nil {
		return err
	}
	s.entryTerms = nil
	lower, upper := prefixBounds(2)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iterator.Close()
	expected := s.snapshot.GetMetadata().GetIndex() + 1
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if len(iterator.Key()) != 9 || binary.BigEndian.Uint64(iterator.Key()[1:]) != expected {
			return fmt.Errorf("raft log gap at %d", expected)
		}
		entry := &pb.Entry{}
		if err := proto.Unmarshal(iterator.Value(), entry); err != nil {
			return err
		}
		if entry.GetIndex() != expected {
			return fmt.Errorf("raft entry index mismatch at %d", expected)
		}
		s.entryTerms = append(s.entryTerms, entry.GetTerm())
		expected++
	}
	if err := iterator.Error(); err != nil {
		return err
	}
	snapshotIndex := s.snapshot.GetMetadata().GetIndex()
	lastIndex := expected - 1
	if s.hard.GetCommit() < snapshotIndex || s.hard.GetCommit() > lastIndex {
		return fmt.Errorf("raft commit %d is outside persisted range [%d,%d]", s.hard.GetCommit(), snapshotIndex, lastIndex)
	}
	return nil
}

func (s *Store) get(key []byte) ([]byte, error) {
	data, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	// Pebble owns data until the closer is released,
	// so copy it here
	copy := append([]byte(nil), data...)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return copy, nil
}

func (s *Store) hit(point Point) error {
	if s.failpoint == nil {
		return nil
	}
	return s.failpoint(point)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *Store) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.hard).(*pb.HardState), proto.Clone(s.conf).(*pb.ConfState), nil
}

func (s *Store) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	first := s.snapshot.GetMetadata().GetIndex() + 1
	last := first + uint64(len(s.entryTerms))
	if lo < first {
		return nil, raft.ErrCompacted
	}
	if hi > last || lo > hi {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	return s.loadEntries(lo, hi, maxSize)
}

func (s *Store) loadEntries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	if lo == hi {
		return nil, nil
	}
	lower := numberKey(2, lo)
	upper := numberKey(2, hi)
	iterator, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	var result []*pb.Entry
	expected := lo
	var size uint64
	limited := false
	for iterator.First(); iterator.Valid(); iterator.Next() {
		if len(iterator.Key()) != 9 || binary.BigEndian.Uint64(iterator.Key()[1:]) != expected {
			return nil, fmt.Errorf("raft log gap at %d", expected)
		}
		entry := &pb.Entry{}
		if err := proto.Unmarshal(iterator.Value(), entry); err != nil {
			return nil, err
		}
		if entry.GetIndex() != expected || entry.GetTerm() != s.entryTerms[expected-s.snapshot.GetMetadata().GetIndex()-1] {
			return nil, fmt.Errorf("raft entry metadata mismatch at %d", expected)
		}
		entrySize := uint64(proto.Size(entry))
		if len(result) > 0 && maxSize != math.MaxUint64 && size+entrySize > maxSize {
			limited = true
			break
		}
		result = append(result, entry)
		size += entrySize
		expected++
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	if expected != hi && !limited {
		return nil, fmt.Errorf("raft log gap at %d", expected)
	}
	return result, nil
}

func (s *Store) Term(index uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.term(index)
}

func (s *Store) term(index uint64) (uint64, error) {
	snapshotIndex := s.snapshot.GetMetadata().GetIndex()
	if index < snapshotIndex {
		return 0, raft.ErrCompacted
	}
	if index == snapshotIndex {
		return s.snapshot.GetMetadata().GetTerm(), nil
	}
	offset := index - snapshotIndex - 1
	if offset >= uint64(len(s.entryTerms)) {
		return 0, raft.ErrUnavailable
	}
	return s.entryTerms[offset], nil
}

func (s *Store) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.GetMetadata().GetIndex() + 1, nil
}

func (s *Store) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.GetMetadata().GetIndex() + uint64(len(s.entryTerms)), nil
}

func (s *Store) Snapshot() (*pb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.snapshot).(*pb.Snapshot), nil
}

func (s *Store) SnapshotIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.GetMetadata().GetIndex()
}
