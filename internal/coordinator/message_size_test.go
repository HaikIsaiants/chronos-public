package coordinator

import (
	"encoding/json"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestMaxRaftMessageSizeFitsProposalReadyGroup(t *testing.T) {
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	entries := make([]*pb.Entry, 0, proposalReadyBatch)
	legacy := make([]*pb.Entry, 0, proposalReadyBatch)
	for sequence := range proposalReadyBatch {
		commands := bench.WorkloadBatch(uint64(sequence)*bench.WorkloadBatchSize, bench.WorkloadBatchSize)
		prepared, err := value.StageBatch(engine, commands)
		if err != nil {
			t.Fatal(err)
		}
		batch := LogBatch{Version: LogBatchVersion, Commands: prepared.Batches}
		data, err := encodeLogBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, &pb.Entry{Data: data})
		raw, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		legacy = append(legacy, &pb.Entry{Data: raw})
	}
	if size := proto.Size(&pb.Message{Entries: entries}); size > maxRaftMessageSize {
		t.Fatalf("compressed ready group message is %d bytes", size)
	}
	if size := proto.Size(&pb.Message{Entries: legacy[:5]}); size <= 1<<20 {
		t.Fatalf("five-entry message unexpectedly fits in one MiB: %d", size)
	}
}
