package coordinator

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/golang/snappy"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestLogBatchCodecGobSnappyRoundTrip(t *testing.T) {
	commands := make([]core.CommandBatch, 128)
	for index := range commands {
		commands[index] = logCodecCommand("request-00000000000000000000000000000000")
	}
	batch := LogBatch{Version: LogBatchVersion, Commands: commands}
	raw, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeLogBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(encoded, logBatchGobSnappyMagic) {
		t.Fatal("expected gob snappy codec")
	}
	if len(encoded) >= len(raw) {
		t.Fatalf("encoded batch is %d bytes, raw batch is %d", len(encoded), len(raw))
	}
	decoded, err := decodeLogBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatal("decoded batch differs")
	}
}

func TestLogBatchCodecExactRoundTrip(t *testing.T) {
	definition := core.WorkflowDefinition{
		Name: "workflow", Namespace: "namespace", StartAt: 11,
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "test", Dependencies: []string{"before"},
			Retry:         core.RetryPolicy{MaxAttempts: 3, InitialBackoffMillis: 5, BackoffMultiplier: 2, MaxBackoffMillis: 20},
			TimeoutMillis: 100, Compensation: &core.CompensationDefinition{TaskType: "undo"},
		}},
	}
	batch := LogBatch{Version: LogBatchVersion, Commands: []core.CommandBatch{{
		Version: core.CommandBatchVersion, RequestID: "request", RequestHash: "hash", WorkflowID: "workflow",
		Events: []core.Event{{
			ID: "event", Kind: core.EventTaskCompleted, Sequence: 7, RequestID: "request", RequestHash: "hash",
			WorkflowID: "workflow", TaskID: "task", AttemptID: "attempt", Attempt: 2, At: 19,
			Definition: &definition, Inputs: map[string]map[string]string{"input": {"key": "value"}},
			Output: map[string]string{"result": "ok"}, Error: "none", WorkerID: "worker", LeaseUntil: 25,
			Fence: 3, Idempotency: "idempotency", Timer: &core.TimerState{
				ID: "timer", TaskID: "task", Purpose: core.TimerTimeout, Deadline: 30, AttemptID: "attempt",
				Attempt: 2, Fence: 3, Status: core.TimerScheduled,
			},
			ExpandedTasks: []core.ExpandedTask{{Key: "child", Definition: definition.Tasks[0], Payload: map[string]string{"item": "one"}}},
			Reason:        "reason", Compensation: true,
		}},
	}}}
	encoded, err := encodeLogBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeLogBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatal("decoded batch differs")
	}
}

func TestLogBatchCodecLegacyRoundTrip(t *testing.T) {
	batch := logCodecBatch("request")
	data, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeLogBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatal("decoded batch differs")
	}
}

func TestLogBatchCodecLegacySnappyStreamRoundTrip(t *testing.T) {
	batch := logCodecBatch("request")
	raw, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	writer := snappy.NewBufferedWriter(&buffer)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeLogBatch(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatal("decoded batch differs")
	}
}

func TestCommittedDecodeUsesExactLocalProposal(t *testing.T) {
	batch := logCodecBatch("cached")
	data, err := encodeLogBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &Coordinator{proposed: []proposedBatch{{data: data, batch: batch}}}
	entry := &pb.Entry{Index: proto.Uint64(9), Term: proto.Uint64(3), Data: data}
	coordinator.cacheReadyEntries([]*pb.Entry{entry})
	decoded, err := coordinator.decodeCommitted([]*pb.Entry{entry})
	if err != nil || len(decoded) != 1 || !reflect.DeepEqual(decoded[0].Batches, batch.Commands) || coordinator.prepared != nil || coordinator.proposed != nil {
		t.Fatalf("cached decode differs: %+v %v", decoded, err)
	}
}

func TestLogBatchCodecCanonicalizesEmptySlices(t *testing.T) {
	batch := logCodecBatch("canonical")
	batch.Commands[0].Events[0].Definition = &core.WorkflowDefinition{
		Name: "workflow", Namespace: "namespace",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "test", Dependencies: []string{}}},
	}
	batch.Commands[0].Events[0].ExpandedTasks = []core.ExpandedTask{}
	if validateLogBatch(batch) == nil {
		t.Fatal("noncanonical batch accepted")
	}
	canonicalizeLogBatch(&batch)
	data, err := encodeLogBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeLogBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatal("canonical batch differs")
	}
}

func TestLogBatchCodecRejectsCommandLimit(t *testing.T) {
	commands := make([]core.CommandBatch, maxLogBatchCommands+1)
	for index := range commands {
		commands[index] = logCodecCommand("request")
	}
	if _, err := encodeLogBatch(LogBatch{Version: LogBatchVersion, Commands: commands}); err == nil {
		t.Fatal("oversized command count accepted")
	}
}

func TestCommittedDecodeRejectsMismatchedLocalProposal(t *testing.T) {
	batch := logCodecBatch("durable")
	data, err := encodeLogBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &Coordinator{prepared: map[uint64]proposedBatch{9: {data: []byte("other"), batch: LogBatch{Version: LogBatchVersion}}}}
	entry := &pb.Entry{Index: proto.Uint64(9), Term: proto.Uint64(3), Data: data}
	decoded, err := coordinator.decodeCommitted([]*pb.Entry{entry})
	if err != nil || len(decoded) != 1 || !reflect.DeepEqual(decoded[0].Batches, batch.Commands) {
		t.Fatalf("durable decode differs: %+v %v", decoded, err)
	}
}

func TestLogBatchCodecRejectsCorruption(t *testing.T) {
	commands := make([]core.CommandBatch, 128)
	for index := range commands {
		commands[index] = logCodecCommand("request")
	}
	data, err := encodeLogBatch(LogBatch{Version: LogBatchVersion, Commands: commands})
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("corrupt batch accepted")
	}
}

func TestLogBatchCodecRejectsVersion(t *testing.T) {
	data, err := json.Marshal(LogBatch{Version: LogBatchVersion + 1, Commands: []core.CommandBatch{{RequestID: "request"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("invalid version accepted")
	}
}

func TestLogBatchCodecRejectsCodecVersion(t *testing.T) {
	data := append([]byte(nil), logBatchGobSnappyMagic...)
	data[len(data)-1]++
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("invalid codec version accepted")
	}
}

func TestLogBatchCodecRejectsTruncation(t *testing.T) {
	data, err := encodeLogBatch(logCodecBatch("request"))
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{0, 1, len(logBatchCodecPrefix) - 1, len(logBatchGobSnappyMagic), len(data) / 2, len(data) - 1} {
		if _, err := decodeLogBatch(data[:length]); err == nil {
			t.Fatalf("truncated batch of length %d accepted", length)
		}
	}
}

func TestLogBatchCodecRejectsTrailingData(t *testing.T) {
	data, err := encodeLogBatch(logCodecBatch("request"))
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, 0)
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("trailing data accepted")
	}
}

func TestLogBatchCodecRejectsOversizedDecodedBatch(t *testing.T) {
	var header [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(header[:], uint64(maxLogBatchBytes+1))
	data := append(append([]byte(nil), logBatchGobSnappyMagic...), header[:length]...)
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("oversized decoded batch accepted")
	}
}

func TestLogBatchCodecRejectsGobTrailingValue(t *testing.T) {
	batch := logCodecBatch("request")
	var raw bytes.Buffer
	encoder := gob.NewEncoder(&raw)
	if err := encoder.Encode(batch); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(uint64(1)); err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), logBatchGobSnappyMagic...)
	data = append(data, snappy.Encode(nil, raw.Bytes())...)
	if _, err := decodeLogBatch(data); err == nil {
		t.Fatal("trailing gob value accepted")
	}
}

func TestLogBatchCodecRejectsOversizedSource(t *testing.T) {
	batch := logCodecBatch(strings.Repeat("x", maxLogBatchBytes))
	if _, err := encodeLogBatch(batch); err == nil {
		t.Fatal("oversized batch accepted")
	}
}

func FuzzLogBatchCodec(f *testing.F) {
	batch := logCodecBatch("request")
	legacy, err := json.Marshal(batch)
	if err != nil {
		f.Fatal(err)
	}
	encoded, err := encodeLogBatch(batch)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(legacy)
	f.Add(encoded)
	f.Add(logBatchCodecPrefix)
	f.Fuzz(func(t *testing.T, data []byte) {
		decodeLogBatch(data)
	})
}

func logCodecBatch(requestID string) LogBatch {
	return LogBatch{Version: LogBatchVersion, Commands: []core.CommandBatch{logCodecCommand(requestID)}}
}

func logCodecCommand(requestID string) core.CommandBatch {
	event := core.Event{
		ID: "event", Kind: core.EventTaskReady, Sequence: 1, RequestID: requestID,
		RequestHash: "hash", WorkflowID: "workflow", TaskID: "task", At: 1,
	}
	return core.CommandBatch{
		Version: core.CommandBatchVersion, RequestID: requestID, RequestHash: "hash",
		WorkflowID: "workflow", Events: []core.Event{event},
	}
}
