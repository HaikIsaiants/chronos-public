package storage

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
)

var receiptMagic = []byte{0, 'C', 'H', 'R', 'R', 1}

type receiptRecord struct {
	Version       uint32 `json:"version"`
	RequestID     string `json:"request_id"`
	RequestHash   string `json:"request_hash"`
	AppliedIndex  uint64 `json:"applied_index"`
	CommittedTerm uint64 `json:"committed_term"`
	WorkflowID    string `json:"workflow_id"`
	FirstSequence uint64 `json:"first_sequence"`
	LastSequence  uint64 `json:"last_sequence"`
}

const maxReceiptEvents = 1 << 20

func compactReceipt(receipt Receipt) (receiptRecord, error) {
	if receipt.Version != ReceiptVersion || receipt.RequestID == "" || receipt.RequestHash == "" || receipt.AppliedIndex == 0 || receipt.CommittedTerm == 0 || receipt.Result.WorkflowID == "" || receipt.Result.Duplicate || len(receipt.Result.Events) == 0 {
		return receiptRecord{}, ErrRecordVersion
	}
	record := receiptRecord{
		Version: ReceiptVersion, RequestID: receipt.RequestID, RequestHash: receipt.RequestHash,
		AppliedIndex: receipt.AppliedIndex, CommittedTerm: receipt.CommittedTerm, WorkflowID: receipt.Result.WorkflowID,
		FirstSequence: receipt.Result.Events[0].Sequence, LastSequence: receipt.Result.Events[len(receipt.Result.Events)-1].Sequence,
	}
	if !validReceiptRecord(record) || record.LastSequence-record.FirstSequence != uint64(len(receipt.Result.Events)-1) {
		return receiptRecord{}, ErrRecordVersion
	}
	return record, nil
}

func decodeReceiptRecord(data []byte) (receiptRecord, error) {
	if bytes.HasPrefix(data, receiptMagic) {
		return decodeBinaryReceiptRecord(data[len(receiptMagic):])
	}
	var record receiptRecord
	if err := decodeJSON(data, &record); err != nil {
		return receiptRecord{}, err
	}
	if !validReceiptRecord(record) {
		return receiptRecord{}, ErrRecordVersion
	}
	return record, nil
}

func encodeReceiptRecord(record receiptRecord) ([]byte, error) {
	if !validReceiptRecord(record) {
		return nil, ErrRecordVersion
	}
	data := make([]byte, 0, len(receiptMagic)+len(record.RequestID)+len(record.RequestHash)+len(record.WorkflowID)+32)
	data = append(data, receiptMagic...)
	data = binary.AppendUvarint(data, uint64(record.Version))
	data = binary.AppendUvarint(data, record.AppliedIndex)
	data = binary.AppendUvarint(data, record.CommittedTerm)
	data = binary.AppendUvarint(data, record.FirstSequence)
	data = binary.AppendUvarint(data, record.LastSequence)
	data = appendReceiptString(data, record.RequestID)
	data = appendReceiptString(data, record.RequestHash)
	data = appendReceiptString(data, record.WorkflowID)
	return data, nil
}

func decodeBinaryReceiptRecord(data []byte) (receiptRecord, error) {
	values := [5]uint64{}
	for index := range values {
		var ok bool
		values[index], data, ok = readReceiptUint(data)
		if !ok {
			return receiptRecord{}, ErrRecordVersion
		}
	}
	requestID, data, ok := readReceiptString(data)
	if !ok {
		return receiptRecord{}, ErrRecordVersion
	}
	requestHash, data, ok := readReceiptString(data)
	if !ok {
		return receiptRecord{}, ErrRecordVersion
	}
	workflowID, data, ok := readReceiptString(data)
	if !ok || len(data) != 0 || values[0] > uint64(^uint32(0)) {
		return receiptRecord{}, ErrRecordVersion
	}
	record := receiptRecord{
		Version: uint32(values[0]), RequestID: requestID, RequestHash: requestHash,
		AppliedIndex: values[1], CommittedTerm: values[2], WorkflowID: workflowID,
		FirstSequence: values[3], LastSequence: values[4],
	}
	if !validReceiptRecord(record) {
		return receiptRecord{}, ErrRecordVersion
	}
	return record, nil
}

func appendReceiptString(data []byte, value string) []byte {
	data = binary.AppendUvarint(data, uint64(len(value)))
	return append(data, value...)
}

func readReceiptUint(data []byte) (uint64, []byte, bool) {
	value, size := binary.Uvarint(data)
	if size <= 0 {
		return 0, data, false
	}
	var encoded [binary.MaxVarintLen64]byte
	if binary.PutUvarint(encoded[:], value) != size {
		return 0, data, false
	}
	return value, data[size:], true
}

func readReceiptString(data []byte) (string, []byte, bool) {
	length, data, ok := readReceiptUint(data)
	if !ok || length > uint64(len(data)) {
		return "", data, false
	}
	return string(data[:int(length)]), data[int(length):], true
}

func validReceiptRecord(record receiptRecord) bool {
	return record.Version == ReceiptVersion && record.RequestID != "" && record.RequestHash != "" && record.AppliedIndex != 0 && record.CommittedTerm != 0 && record.WorkflowID != "" && record.FirstSequence != 0 && record.LastSequence >= record.FirstSequence && record.LastSequence-record.FirstSequence < maxReceiptEvents
}

func validReceiptEvent(record receiptRecord, event core.Event, sequence uint64) bool {
	return event.WorkflowID == record.WorkflowID && event.RequestID == record.RequestID && event.RequestHash == record.RequestHash && event.Sequence == sequence && event.ID == core.EventID(event.WorkflowID, event.Sequence, event.Kind) && event.At >= 0
}

func (s *Store) expandReceipt(record receiptRecord) (Receipt, error) {
	return s.expandReceiptCached(record, nil)
}

func (s *Store) expandReceiptCached(record receiptRecord, cache map[string]eventSegment) (Receipt, error) {
	if record.AppliedIndex > s.applied {
		return Receipt{}, ErrRecordVersion
	}
	key := materializedSegmentKey(8, record.WorkflowID, record.AppliedIndex)
	cacheKey := string(key)
	segment, exists := cache[cacheKey]
	if !exists {
		data, err := s.get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return Receipt{}, ErrRecordVersion
		}
		if err != nil {
			return Receipt{}, err
		}
		segment, err = decodeEventSegment(key, data)
		if err != nil {
			return Receipt{}, err
		}
		if cache != nil {
			cache[cacheKey] = segment
		}
	}
	if record.FirstSequence < segment.FirstSequence || record.LastSequence > segment.LastSequence {
		return Receipt{}, ErrRecordVersion
	}
	first := record.FirstSequence - segment.FirstSequence
	last := record.LastSequence - segment.FirstSequence
	if last >= uint64(len(segment.Events)) {
		return Receipt{}, ErrRecordVersion
	}
	events := append([]core.Event(nil), segment.Events[first:last+1]...)
	for index, event := range events {
		if !validReceiptEvent(record, event, record.FirstSequence+uint64(index)) {
			return Receipt{}, ErrRecordVersion
		}
	}
	return Receipt{
		Version: record.Version, RequestID: record.RequestID, RequestHash: record.RequestHash,
		AppliedIndex: record.AppliedIndex, CommittedTerm: record.CommittedTerm,
		Result: core.Result{WorkflowID: record.WorkflowID, Events: events},
	}, nil
}
