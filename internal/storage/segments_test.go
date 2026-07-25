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

func TestApplicationMaterializationUsesSegments(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "segments", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	persistApplicationEncodingRaft(t, store)
	if _, err := store.Apply(applicationEncodingFixture(t)); err != nil {
		t.Fatal(err)
	}
	eventRecords := 0
	if err := store.scan(8, func(key, data []byte) error {
		eventRecords++
		_, err := decodeEventSegment(key, data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	taskRecords := 0
	if err := store.scan(5, func(key, data []byte) error {
		taskRecords++
		_, err := decodeTaskResultSegment(key, data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if eventRecords != 3 || taskRecords != 1 {
		t.Fatalf("unexpected segment counts: %d %d", eventRecords, taskRecords)
	}
	key := materializedSegmentKey(8, "terminal", 2)
	data, err := store.get(key)
	if err != nil {
		t.Fatal(err)
	}
	segment, err := decodeEventSegment(key, data)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(map[string]struct{})
	for _, event := range segment.Events {
		requests[event.RequestID] = struct{}{}
	}
	if len(requests) != 2 {
		t.Fatalf("entry segment contains %d requests", len(requests))
	}
}

func TestEventSegmentsRejectKeyAndSequenceCorruption(t *testing.T) {
	t.Run("key", func(t *testing.T) {
		store, _, expected := receiptFixture(t)
		defer store.Close()
		data, err := store.get(segmentedKey(4, expected.RequestID))
		if err != nil {
			t.Fatal(err)
		}
		record, err := decodeReceiptRecord(data)
		if err != nil {
			t.Fatal(err)
		}
		key := materializedSegmentKey(8, record.WorkflowID, record.AppliedIndex)
		data, err = store.get(key)
		if err != nil {
			t.Fatal(err)
		}
		var segment eventSegment
		if err := decodeJSON(data, &segment); err != nil {
			t.Fatal(err)
		}
		segment.AppliedIndex++
		data, err = encodeJSON(segment)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.db.Set(key, data, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Receipt(expected.RequestID); !errors.Is(err, ErrRecordVersion) {
			t.Fatalf("mismatched segment returned %v", err)
		}
	})
	t.Run("gap", func(t *testing.T) {
		store, _, expected := receiptFixture(t)
		defer store.Close()
		key := materializedSegmentKey(8, expected.Result.WorkflowID, 3)
		data, err := store.get(key)
		if err != nil {
			t.Fatal(err)
		}
		var segment eventSegment
		if err := decodeJSON(data, &segment); err != nil {
			t.Fatal(err)
		}
		segment.FirstSequence++
		segment.LastSequence++
		for index := range segment.Events {
			segment.Events[index].Sequence++
			segment.Events[index].ID = core.EventID(segment.WorkflowID, segment.Events[index].Sequence, segment.Events[index].Kind)
		}
		data, err = encodeJSON(segment)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.db.Set(key, data, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Events(expected.Result.WorkflowID); !errors.Is(err, ErrRecordVersion) {
			t.Fatalf("sequence gap returned %v", err)
		}
	})
	t.Run("future", func(t *testing.T) {
		store, _, expected := receiptFixture(t)
		defer store.Close()
		history, err := store.Events(expected.Result.WorkflowID)
		if err != nil {
			t.Fatal(err)
		}
		event := history[len(history)-1]
		event.Sequence++
		event.ID = core.EventID(event.WorkflowID, event.Sequence, event.Kind)
		segment := eventSegment{
			Version: eventSegmentVersion, WorkflowID: event.WorkflowID, AppliedIndex: store.applied + 1,
			FirstSequence: event.Sequence, LastSequence: event.Sequence, Events: []core.Event{event},
		}
		data, err := encodeJSON(segment)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.db.Set(materializedSegmentKey(8, segment.WorkflowID, segment.AppliedIndex), data, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Events(expected.Result.WorkflowID); !errors.Is(err, ErrRecordVersion) {
			t.Fatalf("future segment returned %v", err)
		}
	})
}

func TestTaskResultSegmentsRejectDuplicateIdentity(t *testing.T) {
	store, _, _ := receiptFixture(t)
	defer store.Close()
	results, err := store.TaskResults()
	if err != nil || len(results) == 0 {
		t.Fatal(err)
	}
	duplicate := results[0]
	duplicate.AppliedIndex = 2
	segment := taskResultSegment{
		Version: taskResultSegmentVersion, WorkflowID: duplicate.WorkflowID,
		AppliedIndex: duplicate.AppliedIndex, Results: []TaskResult{duplicate},
	}
	data, err := encodeJSON(segment)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Set(materializedSegmentKey(5, segment.WorkflowID, segment.AppliedIndex), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TaskResults(); !errors.Is(err, ErrRecordVersion) {
		t.Fatalf("duplicate result returned %v", err)
	}
}

func TestInstallSnapshotRejectsDuplicateSuccessfulOrigin(t *testing.T) {
	source, err := Open(Config{Directory: t.TempDir(), ClusterID: "duplicate-origin", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	persistApplicationEncodingRaft(t, source)
	if _, err := source.Apply(applicationEncodingFixture(t)); err != nil {
		source.Close()
		t.Fatal(err)
	}
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := decodeSnapshotImage(snapshot.GetData())
	if err != nil {
		t.Fatal(err)
	}
	for _, outcome := range image.Outcomes {
		if outcome.Code == "" {
			image.Outcomes = append(image.Outcomes, outcome)
			break
		}
	}
	snapshot.Data, err = encodeSnapshotImage(image)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := Open(Config{Directory: t.TempDir(), ClusterID: "duplicate-origin", NodeID: 2, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	hard := &pb.HardState{Term: proto.Uint64(snapshot.GetMetadata().GetTerm()), Commit: proto.Uint64(snapshot.GetMetadata().GetIndex())}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("duplicate successful origin returned %v", err)
	}
}
