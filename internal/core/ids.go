package core

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
)

func WorkflowID(namespace, requestID string) string {
	return deterministicID("wf", namespace, requestID)
}

func AttemptID(workflowID, taskID string, attempt uint32) string {
	return deterministicID("attempt", workflowID, taskID, strconv.FormatUint(uint64(attempt), 10))
}

func IdempotencyKey(workflowID, taskID string) string {
	return deterministicID("idempotency", workflowID, taskID)
}

func LeaseRequestID(workflowID, taskID string, attempt uint32) string {
	return deterministicID("lease-request", workflowID, taskID, strconv.FormatUint(uint64(attempt), 10))
}

func ExpiryRequestID(attemptID string, fence uint64) string {
	return deterministicID("expiry-request", attemptID, strconv.FormatUint(fence, 10))
}

func CompletionRequestID(attemptID string, fence uint64) string {
	return deterministicID("completion-request", attemptID, strconv.FormatUint(fence, 10))
}

func FanoutTaskID(workflowID, parentTaskID, key string) string {
	return deterministicID("fanout-task", workflowID, parentTaskID, key)
}

func TimerID(workflowID, taskID, purpose string, ordinal uint32) string {
	return deterministicID("timer", workflowID, taskID, purpose, strconv.FormatUint(uint64(ordinal), 10))
}

func TimerRequestID(timerID string) string {
	return deterministicID("timer-request", timerID)
}

func CancelRequestID(workflowID string) string {
	return deterministicID("cancel-request", workflowID)
}

func CompensationIdempotencyKey(workflowID, taskID string) string {
	return deterministicID("compensation-idempotency", workflowID, taskID)
}

func EventID(workflowID string, sequence uint64, kind EventKind) string {
	return deterministicID("event", workflowID, strconv.FormatUint(sequence, 10), string(kind))
}

func deterministicID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(part)))
		hash.Write(length)
		hash.Write([]byte(part))
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil)[:12])
}
