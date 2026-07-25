package storage_test

import (
	"fmt"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func BenchmarkStoreApplyReadyGroup(b *testing.B) {
	for _, entryCount := range []int{1, 8, 12, 16, 20, 24, 28, 32} {
		b.Run(fmt.Sprintf("entries_%d", entryCount), func(b *testing.B) {
			benchmarkStoreApplyReadyGroup(b, entryCount, (*storage.Store).Apply)
		})
	}
}

func BenchmarkStoreApplyReadyGroup16(b *testing.B) {
	for _, test := range []struct {
		name  string
		apply func(*storage.Store, []storage.ApplicationEntry) ([]storage.Outcome, error)
	}{
		{name: "apply", apply: (*storage.Store).Apply},
		{name: "apply_committed", apply: (*storage.Store).ApplyCommitted},
	} {
		b.Run(test.name, func(b *testing.B) {
			benchmarkStoreApplyReadyGroup(b, 16, test.apply)
		})
	}
}

func BenchmarkRetentionLoaderMisses(b *testing.B) {
	store, err := storage.Open(storage.Config{
		Directory: b.TempDir(), ClusterID: "loader-misses", NodeID: 1, Voters: []uint64{1, 2, 3},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	for _, test := range []struct {
		name   string
		engine func() *core.Engine
	}{
		{name: "durable_loaders", engine: store.Engine},
		{name: "empty_engine", engine: core.NewEngine},
	} {
		b.Run(test.name, func(b *testing.B) {
			var workflow uint64
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				commands := bench.WorkloadBatch(workflow, bench.WorkloadBatchSize)
				workflow += bench.WorkloadBatchSize
				engine := test.engine()
				b.StartTimer()
				for _, command := range commands {
					if _, _, duplicate, err := engine.PrepareAndApply(command); err != nil || duplicate {
						b.Fatalf("prepare and apply failed: %v", err)
					}
				}
			}
		})
	}
}

func retentionBenchmarkBatches(b *testing.B, commands []core.Command) []core.CommandBatch {
	b.Helper()
	engine := core.NewEngine()
	batches := make([]core.CommandBatch, 0, len(commands))
	for _, command := range commands {
		batch, _, duplicate, err := engine.PrepareAndApply(command)
		if err != nil || duplicate {
			b.Fatalf("prepare and apply failed: %v", err)
		}
		batches = append(batches, batch)
	}
	return batches
}

func benchmarkStoreApplyReadyGroup(b *testing.B, entryCount int, apply func(*storage.Store, []storage.ApplicationEntry) ([]storage.Outcome, error)) {
	store, err := storage.Open(storage.Config{
		Directory: b.TempDir(), ClusterID: fmt.Sprintf("apply-%d", entryCount), NodeID: 1, Voters: []uint64{1, 2, 3},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	var workflow uint64
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		entries := make([]storage.ApplicationEntry, entryCount)
		base := store.Applied().Index
		for index := range entries {
			commands := bench.WorkloadBatch(workflow, bench.WorkloadBatchSize)
			workflow += bench.WorkloadBatchSize
			entries[index] = storage.ApplicationEntry{
				Version: storage.ApplicationEntryVersion, Index: base + uint64(index) + 1,
				Term: 1, Batches: retentionBenchmarkBatches(b, commands),
			}
		}
		raftEntries := make([]*pb.Entry, len(entries))
		for index, entry := range entries {
			raftEntries[index] = &pb.Entry{Index: proto.Uint64(entry.Index), Term: proto.Uint64(entry.Term)}
		}
		last := entries[len(entries)-1]
		if err := store.SaveReady(raft.Ready{
			HardState: &pb.HardState{Term: proto.Uint64(last.Term), Commit: proto.Uint64(last.Index)},
			Entries:   raftEntries,
		}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := apply(store, entries); err != nil {
			b.Fatal(err)
		}
	}
}
