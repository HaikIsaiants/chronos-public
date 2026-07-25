package storage_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/bench"
	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
	"github.com/golang/snappy"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var parallelEncodingBytes int

type parallelEncodingCategory struct {
	name   string
	values []any
}

type parallelApplicationRecord struct {
	Version uint32 `json:"version"`
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
}

type parallelReceiptRecord struct {
	Version       uint32 `json:"version"`
	RequestID     string `json:"request_id"`
	RequestHash   string `json:"request_hash"`
	AppliedIndex  uint64 `json:"applied_index"`
	CommittedTerm uint64 `json:"committed_term"`
	WorkflowID    string `json:"workflow_id"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
}

type parallelSegmentID struct {
	workflowID   string
	appliedIndex uint64
}

type parallelEventSegment struct {
	Version       uint32       `json:"version"`
	WorkflowID    string       `json:"workflow_id"`
	AppliedIndex  uint64       `json:"applied_index"`
	FirstSequence uint64       `json:"first_sequence"`
	LastSequence  uint64       `json:"last_sequence"`
	Events        []core.Event `json:"events"`
}

type parallelTaskResultSegment struct {
	Version      uint32               `json:"version"`
	WorkflowID   string               `json:"workflow_id"`
	AppliedIndex uint64               `json:"applied_index"`
	Results      []storage.TaskResult `json:"results"`
}

type parallelEncodingWorkload struct {
	categories []parallelEncodingCategory
}

func BenchmarkStoreApplyEncoding(b *testing.B) {
	workload := buildParallelEncodingWorkload(b, 8)
	values := workload.values()
	encodedBytes, err := parallelEncode(values, 1)
	if err != nil {
		b.Fatal(err)
	}
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(encodedBytes))
			for b.Loop() {
				total, err := parallelEncode(values, workers)
				if err != nil {
					b.Fatal(err)
				}
				parallelEncodingBytes = total
			}
			b.ReportMetric(float64(encodedBytes), "encoded_B/op")
			b.ReportMetric(float64(len(values)), "records/op")
		})
	}
}

func BenchmarkStoreApplyEncodingCategories(b *testing.B) {
	workload := buildParallelEncodingWorkload(b, 8)
	for _, category := range workload.categories {
		encodedBytes, err := parallelEncode(category.values, 1)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(category.name, func(b *testing.B) {
			for _, workers := range []int{1, 8} {
				b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(encodedBytes))
					for b.Loop() {
						total, err := parallelEncode(category.values, workers)
						if err != nil {
							b.Fatal(err)
						}
						parallelEncodingBytes = total
					}
					b.ReportMetric(float64(encodedBytes), "encoded_B/op")
					b.ReportMetric(float64(len(category.values)), "records/op")
				})
			}
		})
	}
}

func BenchmarkStoreApplyCompressionCategories(b *testing.B) {
	workload := buildParallelEncodingWorkload(b, 8)
	for _, category := range workload.categories {
		values := make([][]byte, len(category.values))
		rawBytes := 0
		compressedBytes := 0
		for index, value := range category.values {
			data, err := json.Marshal(value)
			if err != nil {
				b.Fatal(err)
			}
			values[index] = data
			rawBytes += len(data)
			compressedBytes += len(snappy.Encode(nil, data))
		}
		b.Run(category.name, func(b *testing.B) {
			b.SetBytes(int64(rawBytes))
			b.ReportAllocs()
			for b.Loop() {
				total := 0
				for _, value := range values {
					total += len(snappy.Encode(nil, value))
				}
				parallelEncodingBytes = total
			}
			b.ReportMetric(float64(compressedBytes), "compressed_B/op")
			b.ReportMetric(float64(rawBytes), "raw_B/op")
		})
	}
}

func buildParallelEncodingWorkload(b *testing.B, entryCount int) parallelEncodingWorkload {
	b.Helper()
	store, err := storage.Open(storage.Config{
		Directory: b.TempDir(), ClusterID: "parallel-encoding", NodeID: 1, Voters: []uint64{1, 2, 3},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})
	entries := make([]storage.ApplicationEntry, entryCount)
	batches := make([]core.CommandBatch, 0, entryCount*int(bench.WorkloadBatchSize*bench.WorkloadCommandsPerWorkflow))
	workflowSet := make(map[string]struct{}, entryCount*int(bench.WorkloadBatchSize))
	base := store.Applied().Index
	for index := range entries {
		commands := bench.WorkloadBatch(uint64(index)*bench.WorkloadBatchSize, bench.WorkloadBatchSize)
		entryBatches := parallelEncodingBatches(b, commands)
		entries[index] = storage.ApplicationEntry{
			Version: storage.ApplicationEntryVersion, Index: base + uint64(index) + 1, Term: 1, Batches: entryBatches,
		}
		batches = append(batches, entryBatches...)
		for _, batch := range entryBatches {
			workflowSet[batch.WorkflowID] = struct{}{}
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
	if _, err := store.Apply(entries); err != nil {
		b.Fatal(err)
	}
	entryValues := make([]any, 0, len(entries))
	for _, entry := range entries {
		entryValues = append(entryValues, parallelApplicationRecord{
			Version: entry.Version, Index: entry.Index, Term: entry.Term,
		})
	}
	receiptValues := make([]any, 0, len(batches))
	eventGroups := make(map[parallelSegmentID][]core.Event)
	for _, batch := range batches {
		receipt, exists, err := store.Receipt(batch.RequestID)
		if err != nil || !exists {
			b.Fatalf("receipt %s: %t %v", batch.RequestID, exists, err)
		}
		receiptValues = append(receiptValues, parallelReceiptRecord{
			Version: receipt.Version, RequestID: receipt.RequestID, RequestHash: receipt.RequestHash,
			AppliedIndex: receipt.AppliedIndex, CommittedTerm: receipt.CommittedTerm, WorkflowID: receipt.Result.WorkflowID,
			FirstSequence: receipt.Result.Events[0].Sequence, LastSequence: receipt.Result.Events[len(receipt.Result.Events)-1].Sequence,
		})
		id := parallelSegmentID{receipt.Result.WorkflowID, receipt.AppliedIndex}
		eventGroups[id] = append(eventGroups[id], receipt.Result.Events...)
	}
	eventValues := make([]any, 0, len(eventGroups))
	for id, events := range eventGroups {
		sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
		eventValues = append(eventValues, parallelEventSegment{
			Version: 1, WorkflowID: id.workflowID, AppliedIndex: id.appliedIndex,
			FirstSequence: events[0].Sequence, LastSequence: events[len(events)-1].Sequence, Events: events,
		})
	}
	sort.Slice(eventValues, func(i, j int) bool {
		left := eventValues[i].(parallelEventSegment)
		right := eventValues[j].(parallelEventSegment)
		if left.WorkflowID != right.WorkflowID {
			return left.WorkflowID < right.WorkflowID
		}
		return left.AppliedIndex < right.AppliedIndex
	})
	taskResults, err := store.TaskResults()
	if err != nil {
		b.Fatal(err)
	}
	taskGroups := make(map[parallelSegmentID][]storage.TaskResult)
	for _, result := range taskResults {
		id := parallelSegmentID{result.WorkflowID, result.AppliedIndex}
		taskGroups[id] = append(taskGroups[id], result)
	}
	taskResultValues := make([]any, 0, len(taskGroups))
	for id, results := range taskGroups {
		sort.Slice(results, func(i, j int) bool { return results[i].Sequence < results[j].Sequence })
		taskResultValues = append(taskResultValues, parallelTaskResultSegment{
			Version: 1, WorkflowID: id.workflowID, AppliedIndex: id.appliedIndex, Results: results,
		})
	}
	sort.Slice(taskResultValues, func(i, j int) bool {
		left := taskResultValues[i].(parallelTaskResultSegment)
		right := taskResultValues[j].(parallelTaskResultSegment)
		if left.WorkflowID != right.WorkflowID {
			return left.WorkflowID < right.WorkflowID
		}
		return left.AppliedIndex < right.AppliedIndex
	})
	workflowIDs := make([]string, 0, len(workflowSet))
	for workflowID := range workflowSet {
		workflowIDs = append(workflowIDs, workflowID)
	}
	sort.Strings(workflowIDs)
	stateValues := make([]any, 0, len(workflowIDs))
	for _, workflowID := range workflowIDs {
		state, exists := store.State(workflowID)
		if !exists {
			b.Fatalf("workflow %s missing", workflowID)
		}
		stateValues = append(stateValues, state)
	}
	return parallelEncodingWorkload{categories: []parallelEncodingCategory{
		{name: "application_entries", values: entryValues},
		{name: "receipts", values: receiptValues},
		{name: "events", values: eventValues},
		{name: "task_results", values: taskResultValues},
		{name: "states", values: stateValues},
		{name: "applied_state", values: []any{store.Applied()}},
	}}
}

func (workload parallelEncodingWorkload) values() []any {
	var count int
	for _, category := range workload.categories {
		count += len(category.values)
	}
	values := make([]any, 0, count)
	for _, category := range workload.categories {
		values = append(values, category.values...)
	}
	return values
}

func parallelEncodingBatches(b *testing.B, commands []core.Command) []core.CommandBatch {
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

func parallelEncode(values []any, workers int) (int, error) {
	workers = min(workers, len(values))
	if workers == 0 {
		return 0, nil
	}
	results := make([]struct {
		data []byte
		err  error
	}, len(values))
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wait.Done()
			for index := worker; index < len(values); index += workers {
				results[index].data, results[index].err = json.Marshal(values[index])
			}
		}(worker)
	}
	wait.Wait()
	total := 0
	for _, result := range results {
		if result.err != nil {
			return 0, result.err
		}
		total += len(result.data)
	}
	return total, nil
}
