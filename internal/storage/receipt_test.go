package storage

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestReceiptRecordReconstructsExactPublicReceipt(t *testing.T) {
	store, config, expected := receiptFixture(t)
	data, err := store.get(segmentedKey(4, expected.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeReceiptRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	expectedRecord, err := compactReceipt(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, receiptMagic) || !reflect.DeepEqual(record, expectedRecord) {
		t.Fatalf("unexpected receipt record: %+v", record)
	}
	actual, exists, err := store.Receipt(expected.RequestID)
	if err != nil || !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("live receipt differs: %+v %t %v", actual, exists, err)
	}
	history, err := store.Events(expected.Result.WorkflowID)
	if err != nil || int(expected.Result.Events[len(expected.Result.Events)-1].Sequence) >= len(history) {
		t.Fatal(err)
	}
	next := history[expected.Result.Events[len(expected.Result.Events)-1].Sequence]
	if next.RequestID == expected.RequestID || len(actual.Result.Events) != len(expected.Result.Events) {
		t.Fatal("receipt included an adjacent request")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	actual, exists, err = store.Receipt(expected.RequestID)
	if err != nil || !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("reopened receipt differs: %+v %t %v", actual, exists, err)
	}
	storedOutcome, exists, err := store.Outcome(expected.RequestID, expected.RequestHash)
	if err != nil || !exists || !reflect.DeepEqual(storedOutcome, outcomeFromReceipt(expected)) {
		t.Fatalf("reopened outcome differs: %+v %t %v", storedOutcome, exists, err)
	}
	snapshot, err := store.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	destinationConfig := Config{Directory: t.TempDir(), ClusterID: config.ClusterID, NodeID: 2, Voters: config.Voters}
	destination, err := Open(destinationConfig)
	if err != nil {
		t.Fatal(err)
	}
	hard := &pb.HardState{Term: proto.Uint64(snapshot.GetMetadata().GetTerm()), Commit: proto.Uint64(snapshot.GetMetadata().GetIndex())}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		destination.Close()
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		destination.Close()
		t.Fatal(err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	destination, err = Open(destinationConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	actual, exists, err = destination.Receipt(expected.RequestID)
	if err != nil || !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("snapshot receipt differs: %+v %t %v", actual, exists, err)
	}
}

func TestReceiptRecordCodec(t *testing.T) {
	record := receiptRecord{
		Version: ReceiptVersion, RequestID: "request", RequestHash: "hash", AppliedIndex: 300,
		CommittedTerm: 4, WorkflowID: "workflow", FirstSequence: 7, LastSequence: 9,
	}
	first, err := encodeReceiptRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeReceiptRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := encodeJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || !bytes.HasPrefix(first, receiptMagic) || len(first) >= len(legacy) {
		t.Fatalf("unexpected receipt encodings: %d %d", len(first), len(legacy))
	}
	for name, data := range map[string][]byte{
		"binary": first,
		"legacy": legacy,
	} {
		t.Run(name, func(t *testing.T) {
			decoded, err := decodeReceiptRecord(data)
			if err != nil || !reflect.DeepEqual(decoded, record) {
				t.Fatalf("decoded %+v: %v", decoded, err)
			}
		})
	}
	invalidVersion := append([]byte(nil), first...)
	invalidVersion[len(receiptMagic)]++
	noncanonical := append(append([]byte(nil), first[:len(receiptMagic)]...), 0x82, 0)
	noncanonical = append(noncanonical, first[len(receiptMagic)+1:]...)
	for name, data := range map[string][]byte{
		"truncated":       first[:len(first)-1],
		"trailing":        append(append([]byte(nil), first...), 0),
		"invalid-version": invalidVersion,
		"invalid-varint":  append(append([]byte(nil), receiptMagic...), 0x80),
		"noncanonical":    noncanonical,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeReceiptRecord(data); !errors.Is(err, ErrRecordVersion) {
				t.Fatalf("invalid receipt returned %v", err)
			}
		})
	}
}

func TestReceiptRecordRejectsBrokenEventReferences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Store, receiptRecord) error
	}{
		{name: "missing-segment", mutate: func(store *Store, record receiptRecord) error {
			return store.db.Delete(materializedSegmentKey(8, record.WorkflowID, record.AppliedIndex), pebble.Sync)
		}},
		{name: "missing-first", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptSegment(store, record, func(segment *eventSegment) { segment.Events = segment.Events[1:] })
		}},
		{name: "missing-last", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptSegment(store, record, func(segment *eventSegment) { segment.Events = segment.Events[:len(segment.Events)-1] })
		}},
		{name: "missing-tail", mutate: func(store *Store, record receiptRecord) error {
			record.LastSequence++
			return writeReceiptRecord(store, record)
		}},
		{name: "invalid-range", mutate: func(store *Store, record receiptRecord) error {
			record.FirstSequence = record.LastSequence + 1
			return writeReceiptRecord(store, record)
		}},
		{name: "oversized-range", mutate: func(store *Store, record receiptRecord) error {
			record.LastSequence = record.FirstSequence + maxReceiptEvents
			return writeReceiptRecord(store, record)
		}},
		{name: "request", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.RequestID = "other" })
		}},
		{name: "hash", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.RequestHash = "other" })
		}},
		{name: "workflow", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.WorkflowID = "other" })
		}},
		{name: "sequence", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.Sequence++ })
		}},
		{name: "id", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.ID = "other" })
		}},
		{name: "time", mutate: func(store *Store, record receiptRecord) error {
			return mutateReceiptEvent(store, record, func(event *core.Event) { event.At = -1 })
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			if err := test.mutate(store, record); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Receipt(expected.RequestID); !errors.Is(err, ErrRecordVersion) {
				t.Fatalf("broken receipt returned %v", err)
			}
		})
	}
}

func receiptFixture(t *testing.T) (*Store, Config, Receipt) {
	t.Helper()
	config := Config{Directory: t.TempDir(), ClusterID: "receipt-record", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	persistApplicationEncodingRaft(t, store)
	outcomes, err := store.Apply(applicationEncodingFixture(t))
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	for _, outcome := range outcomes {
		if outcome.RequestID == "terminal-submit" && !outcome.Result.Duplicate {
			return store, config, Receipt{
				Version: ReceiptVersion, RequestID: outcome.RequestID, RequestHash: outcome.RequestHash,
				AppliedIndex: outcome.AppliedIndex, CommittedTerm: outcome.CommittedTerm, Result: outcome.Result,
			}
		}
	}
	store.Close()
	t.Fatal("receipt fixture is missing")
	return nil, Config{}, Receipt{}
}

func writeReceiptRecord(store *Store, record receiptRecord) error {
	data, err := encodeJSON(record)
	if err != nil {
		return err
	}
	return store.db.Set(segmentedKey(4, record.RequestID), data, pebble.Sync)
}

func mutateReceiptEvent(store *Store, record receiptRecord, mutate func(*core.Event)) error {
	return mutateReceiptSegment(store, record, func(segment *eventSegment) {
		index := record.FirstSequence - segment.FirstSequence
		mutate(&segment.Events[index])
	})
}

func mutateReceiptSegment(store *Store, record receiptRecord, mutate func(*eventSegment)) error {
	key := materializedSegmentKey(8, record.WorkflowID, record.AppliedIndex)
	data, err := store.get(key)
	if err != nil {
		return err
	}
	var segment eventSegment
	if err := decodeJSON(data, &segment); err != nil {
		return err
	}
	mutate(&segment)
	data, err = encodeJSON(segment)
	if err != nil {
		return err
	}
	return store.db.Set(key, data, pebble.Sync)
}
