package storage

import (
	"errors"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestCreateSnapshotCompactsApplicationHistory(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "snapshot-compaction", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	definition := core.WorkflowDefinition{
		Name: "snapshot", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	command, _, duplicate, err := store.Engine().Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil || duplicate {
		t.Fatal(err)
	}
	entry := &pb.Entry{Index: proto.Uint64(2), Term: proto.Uint64(2)}
	hard := &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(2)}
	if err := store.SaveReady(raft.Ready{HardState: hard, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Apply([]ApplicationEntry{{Version: ApplicationEntryVersion, Index: 2, Term: 2, Batches: []core.CommandBatch{command}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.get(numberKey(3, 2)); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("compacted application entry returned %v", err)
	}
	if _, err := store.get(numberKey(2, 2)); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("compacted raft entry returned %v", err)
	}
	if len(store.entryTerms) != 0 {
		t.Fatal("compacted raft metadata was retained")
	}
}
