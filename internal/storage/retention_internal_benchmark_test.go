package storage

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func BenchmarkReopenedRetentionLoaderMisses(b *testing.B) {
	const residentRecords = 50_000
	const missingRequests = 256
	const missingWorkflows = 16
	directory := b.TempDir()
	config := Config{Directory: directory, ClusterID: "loader-sstable", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		b.Fatal(err)
	}
	batch := store.db.NewBatchWithSize(8 << 20)
	for index := 0; index < residentRecords; index++ {
		id := fmt.Sprintf("resident-%08d", index)
		if err := batch.Set(segmentedKey(4, id), []byte{1}, nil); err != nil {
			b.Fatal(err)
		}
		if err := batch.Set(segmentedKey(6, id), []byte{1}, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := store.db.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := store.Close(); err != nil {
		b.Fatal(err)
	}
	store, err = Open(config)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	requests := make([]string, missingRequests)
	workflows := make([]string, missingWorkflows)
	for index := range requests {
		requests[index] = fmt.Sprintf("missing-request-%03d", index)
	}
	for index := range workflows {
		workflows[index] = fmt.Sprintf("missing-workflow-%02d", index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, requestID := range requests {
			if _, exists, err := store.loadEngineRequest(requestID); err != nil || exists {
				b.Fatalf("request miss returned %t %v", exists, err)
			}
		}
		for _, workflowID := range workflows {
			if _, exists, err := store.loadEngineWorkflow(workflowID); err != nil || exists {
				b.Fatalf("workflow miss returned %t %v", exists, err)
			}
		}
	}
}
