package core

import "errors"

var (
	ErrInvalidDefinition = errors.New("invalid workflow definition")
	ErrInvalidCommand    = errors.New("invalid command")
	ErrInvalidTransition = errors.New("invalid transition")
	ErrRequestConflict   = errors.New("request id already used with different command")
	ErrWorkflowNotFound  = errors.New("workflow not found")
	ErrWorkflowExists    = errors.New("workflow already exists")
	ErrSequence          = errors.New("invalid event sequence")
	ErrTimeRegression    = errors.New("logical time cannot move backward")
	ErrTimeOverflow      = errors.New("logical time overflow")
	ErrBatchVersion      = errors.New("unsupported command batch version")
	ErrSnapshotVersion   = errors.New("unsupported engine snapshot version")
	ErrAttemptOverflow   = errors.New("task attempt overflow")
	ErrFencingOverflow   = errors.New("fencing token overflow")
	ErrLeaseExpired      = errors.New("execution lease expired")
	ErrFenced            = errors.New("execution attempt fenced")
)
