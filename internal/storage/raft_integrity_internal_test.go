package storage

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRaftWriteOptions(t *testing.T) {
	previous := &pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(1)}
	commit := &pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(1), Commit: proto.Uint64(2)}
	term := &pb.HardState{Term: proto.Uint64(3), Vote: proto.Uint64(1), Commit: proto.Uint64(1)}
	vote := &pb.HardState{Term: proto.Uint64(2), Vote: proto.Uint64(2), Commit: proto.Uint64(1)}
	tests := []struct {
		name     string
		ready    raft.Ready
		snapshot bool
		entries  int
		next     *pb.HardState
		expected *pebble.WriteOptions
	}{
		{name: "commit", next: commit, expected: pebble.NoSync},
		{name: "must-sync", ready: raft.Ready{MustSync: true}, next: commit, expected: pebble.Sync},
		{name: "snapshot", snapshot: true, next: commit, expected: pebble.Sync},
		{name: "entries", entries: 1, next: commit, expected: pebble.Sync},
		{name: "term", next: term, expected: pebble.Sync},
		{name: "vote", next: vote, expected: pebble.Sync},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := raftWriteOptions(test.ready, test.snapshot, test.entries, previous, test.next); actual != test.expected {
				t.Fatalf("unexpected write options: %+v", actual)
			}
		})
	}
}

func TestRaftAppendDoesNotCreateTombstones(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "raft-append", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.db.Flush(); err != nil {
		t.Fatal(err)
	}
	before := store.db.Metrics().Keys.TombstoneCount
	if err := store.SaveReady(raft.Ready{
		HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(1)},
		Entries: []*pb.Entry{
			{Index: proto.Uint64(2), Term: proto.Uint64(2)},
			{Index: proto.Uint64(3), Term: proto.Uint64(2)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if after := store.db.Metrics().Keys.TombstoneCount; after != before {
		t.Fatalf("tombstones changed from %d to %d", before, after)
	}
}

func TestSaveReadyRejectsSnapshotBeyondCommit(t *testing.T) {
	clusterID := "snapshot-commit"
	source, err := Open(Config{Directory: t.TempDir(), ClusterID: clusterID, NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	entries := applicationEncodingFixture(t)
	persistApplicationEncodingRaft(t, source)
	if _, err := source.Apply(entries); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	config := Config{Directory: directory, ClusterID: clusterID, NodeID: 2, Voters: []uint64{1, 2, 3}}
	func() {
		destination, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		defer destination.Close()
		if err := destination.SaveReady(raft.Ready{Snapshot: snapshot}); err == nil {
			t.Fatal("snapshot beyond hard-state commit was accepted")
		}
		hard, _, persisted, _, err := destination.RaftState()
		if err != nil || hard.GetCommit() != 1 || persisted.GetMetadata().GetIndex() != 1 {
			t.Fatalf("rejected snapshot changed raft state: %+v %+v %v", hard, persisted.GetMetadata(), err)
		}
	}()
	reopened, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
}

func TestCommitOnlyReadyIsFlushedByApplicationSync(t *testing.T) {
	directory := t.TempDir()
	armed := false
	hits := 0
	config := Config{
		Directory: directory, ClusterID: "commit-only", NodeID: 1, Voters: []uint64{1, 2, 3},
		Failpoint: func(point Point) error {
			if armed && (point == RaftBeforeSync || point == RaftAfterSync) {
				hits++
			}
			return nil
		},
	}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	entries := applicationEncodingFixture(t)
	raftEntries := make([]*pb.Entry, len(entries))
	for index, entry := range entries {
		raftEntries[index] = &pb.Entry{Index: proto.Uint64(entry.Index), Term: proto.Uint64(entry.Term)}
	}
	if err := store.SaveReady(raft.Ready{
		MustSync: true, HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(1)}, Entries: raftEntries,
	}); err != nil {
		t.Fatal(err)
	}
	armed = true
	if err := store.SaveReady(raft.Ready{HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(3)}}); err != nil {
		t.Fatal(err)
	}
	if hits != 0 {
		t.Fatalf("commit-only ready hit sync boundaries %d times", hits)
	}
	if _, err := store.Apply(entries); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	hard, _, _, _, err := reopened.RaftState()
	if err != nil || hard.GetCommit() != 3 || reopened.Applied().Index != 3 {
		t.Fatalf("reopened state differs: %+v %+v %v", hard, reopened.Applied(), err)
	}
}

func TestRaftLogRetentionIsDiskBackedAndCallerIsolated(t *testing.T) {
	directory := t.TempDir()
	config := Config{Directory: directory, ClusterID: "retained", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	const count = 256
	const payloadSize = 32 << 10
	entries := make([]*pb.Entry, count)
	for index := range entries {
		data := make([]byte, payloadSize)
		data[0] = byte(index)
		entries[index] = &pb.Entry{Index: proto.Uint64(uint64(index + 2)), Term: proto.Uint64(2), Type: pb.EntryNormal.Enum(), Data: data}
	}
	if err := store.SaveReady(raft.Ready{
		HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(count + 1)},
		Entries:   entries,
	}); err != nil {
		t.Fatal(err)
	}
	if len(store.entryTerms) != count || store.entryTerms[0] != 2 || store.entryTerms[count-1] != 2 {
		t.Fatal("raft metadata index does not match the persisted log")
	}
	storeType := reflect.TypeOf(store).Elem()
	entryPointer := reflect.TypeOf((*pb.Entry)(nil))
	for index := range storeType.NumField() {
		field := storeType.Field(index)
		if field.Type.Kind() == reflect.Slice && field.Type.Elem() == entryPointer {
			t.Fatalf("store retains raft payloads in %s", field.Name)
		}
	}
	if cap(store.entryTerms)*8 >= count*payloadSize/100 {
		t.Fatal("raft metadata retention grew with payload size")
	}
	retained := &store.entryTerms[0]
	entries[0].Data[0] = 255
	if err := store.SaveReady(raft.Ready{HardState: &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(count + 1)}}); err != nil {
		t.Fatal(err)
	}
	if &store.entryTerms[0] != retained {
		t.Fatal("raft metadata was copied during hard state persistence")
	}
	loaded, err := store.Entries(2, 3, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if loaded[0].Data[0] != 0 {
		t.Fatal("ready entry was retained by reference")
	}
	loaded[0].Data[0] = 254
	again, err := store.Entries(2, 3, ^uint64(0))
	if err != nil || again[0].Data[0] != 0 {
		t.Fatal("storage entry escaped through Entries")
	}
	_, _, _, stateEntries, err := store.RaftState()
	if err != nil {
		t.Fatal(err)
	}
	stateEntries[0].Data[0] = 253
	again, err = store.Entries(2, 3, ^uint64(0))
	if err != nil || again[0].Data[0] != 0 {
		t.Fatal("storage entry escaped through RaftState")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.entryTerms) != count || store.entryTerms[0] != 2 || store.entryTerms[count-1] != 2 {
		t.Fatal("raft metadata index was not recovered")
	}
	last, err := store.Entries(count+1, count+2, ^uint64(0))
	if err != nil || len(last) != 1 || last[0].Data[0] != 255 {
		t.Fatal("raft payload was not recovered from disk")
	}
}

func TestOpenRejectsCommitOutsidePersistedRaftRange(t *testing.T) {
	for _, test := range []struct {
		name   string
		commit uint64
	}{{"below-snapshot", 0}, {"above-last-entry", 2}} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := Open(Config{Directory: directory, ClusterID: "integrity", NodeID: 1, Voters: []uint64{1, 2, 3}})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := pebble.Open(directory, &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			data, closer, err := db.Get(hardStateKey)
			if err != nil {
				t.Fatal(err)
			}
			hard := &pb.HardState{}
			if err := proto.Unmarshal(data, hard); err != nil {
				t.Fatal(err)
			}
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
			hard.Commit = proto.Uint64(test.commit)
			data, err = encodeProto(hard)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Set(hardStateKey, data, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(Config{Directory: directory, ClusterID: "integrity", NodeID: 1, Voters: []uint64{1, 2, 3}}); err == nil {
				reopened.Close()
				t.Fatal("corrupt commit was accepted")
			}
		})
	}
}

func BenchmarkRaftRetainedLog(b *testing.B) {
	store, err := Open(Config{Directory: b.TempDir(), ClusterID: "benchmark", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	const count = 100000
	entries := make([]*pb.Entry, count)
	for index := range entries {
		entries[index] = &pb.Entry{Index: proto.Uint64(uint64(index + 2)), Term: proto.Uint64(2)}
	}
	hard := &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(count + 1)}
	if err := store.SaveReady(raft.Ready{HardState: hard, Entries: entries}); err != nil {
		b.Fatal(err)
	}
	retainedBytes := float64(cap(store.entryTerms) * 8)
	b.Run("hard-state", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(retainedBytes, "retained-B")
		for range b.N {
			if err := store.SaveReady(raft.Ready{HardState: hard}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("range-256", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(retainedBytes, "retained-B")
		for range b.N {
			entries, err := store.Entries(count-254, count+2, ^uint64(0))
			if err != nil || len(entries) != 256 {
				b.Fatal(err)
			}
		}
	})
}
