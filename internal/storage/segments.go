package storage

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

const eventSegmentVersion uint32 = 1
const taskResultSegmentVersion uint32 = 1
const maxSegmentItems = 1 << 20

type segmentID struct {
	workflowID   string
	appliedIndex uint64
}

type eventSegmentSource struct {
	workflowID   string
	appliedIndex uint64
	events       []core.Event
}

type eventSegment struct {
	Version       uint32       `json:"version"`
	WorkflowID    string       `json:"workflow_id"`
	AppliedIndex  uint64       `json:"applied_index"`
	FirstSequence uint64       `json:"first_sequence"`
	LastSequence  uint64       `json:"last_sequence"`
	Events        []core.Event `json:"events"`
}

type taskResultSegment struct {
	Version      uint32       `json:"version"`
	WorkflowID   string       `json:"workflow_id"`
	AppliedIndex uint64       `json:"applied_index"`
	Results      []TaskResult `json:"results"`
}

type taskResultIdentity struct {
	workflowID string
	taskID     string
	phase      string
	attemptID  string
}

func materializedSegmentKey(prefix byte, workflowID string, appliedIndex uint64) []byte {
	key := make([]byte, 13+len(workflowID))
	key[0] = prefix
	binary.BigEndian.PutUint32(key[1:5], uint32(len(workflowID)))
	copy(key[5:], workflowID)
	binary.BigEndian.PutUint64(key[5+len(workflowID):], appliedIndex)
	return key
}

func buildEventSegments(sources []eventSegmentSource, complete bool) ([]eventSegment, error) {
	groups := make(map[segmentID][]core.Event)
	for _, source := range sources {
		if source.workflowID == "" || source.appliedIndex == 0 || len(source.events) == 0 {
			return nil, ErrRecordVersion
		}
		id := segmentID{source.workflowID, source.appliedIndex}
		groups[id] = append(groups[id], source.events...)
	}
	segments := make([]eventSegment, 0, len(groups))
	for id, events := range groups {
		// sequence order keeps segment bytes stable
		sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
		segment := eventSegment{
			Version: eventSegmentVersion, WorkflowID: id.workflowID, AppliedIndex: id.appliedIndex,
			FirstSequence: events[0].Sequence, LastSequence: events[len(events)-1].Sequence, Events: events,
		}
		if !validEventSegment(segment) {
			return nil, ErrRecordVersion
		}
		segments = append(segments, segment)
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].WorkflowID != segments[j].WorkflowID {
			return segments[i].WorkflowID < segments[j].WorkflowID
		}
		return segments[i].AppliedIndex < segments[j].AppliedIndex
	})
	previousWorkflow := ""
	var previousLast uint64
	for _, segment := range segments {
		if segment.WorkflowID != previousWorkflow {
			if complete && segment.FirstSequence != 1 {
				return nil, ErrRecordVersion
			}
			previousWorkflow = segment.WorkflowID
		} else if previousLast == ^uint64(0) || segment.FirstSequence != previousLast+1 {
			return nil, ErrRecordVersion
		}
		previousLast = segment.LastSequence
	}
	return segments, nil
}

func validEventSegment(segment eventSegment) bool {
	if segment.Version != eventSegmentVersion || segment.WorkflowID == "" || segment.AppliedIndex == 0 || len(segment.Events) == 0 || len(segment.Events) > maxSegmentItems || segment.FirstSequence == 0 || segment.LastSequence < segment.FirstSequence || segment.LastSequence-segment.FirstSequence != uint64(len(segment.Events)-1) {
		return false
	}
	for index, event := range segment.Events {
		sequence := segment.FirstSequence + uint64(index)
		if event.WorkflowID != segment.WorkflowID || event.RequestID == "" || event.RequestHash == "" || event.Kind == "" || event.Sequence != sequence || event.ID != core.EventID(event.WorkflowID, event.Sequence, event.Kind) || event.At < 0 {
			return false
		}
	}
	return segment.Events[len(segment.Events)-1].Sequence == segment.LastSequence
}

func decodeEventSegment(key, data []byte) (eventSegment, error) {
	var segment eventSegment
	if err := decodeJSON(data, &segment); err != nil {
		return eventSegment{}, err
	}
	if !validEventSegment(segment) || !bytes.Equal(key, materializedSegmentKey(8, segment.WorkflowID, segment.AppliedIndex)) {
		return eventSegment{}, ErrRecordVersion
	}
	return segment, nil
}

func buildTaskResultSegments(results []TaskResult) ([]taskResultSegment, error) {
	groups := make(map[segmentID][]TaskResult)
	seen := make(map[taskResultIdentity]struct{}, len(results))
	for _, result := range results {
		if !validTaskResult(result) {
			return nil, ErrRecordVersion
		}
		identity := resultIdentity(result)
		if _, exists := seen[identity]; exists {
			return nil, ErrRecordVersion
		}
		seen[identity] = struct{}{}
		id := segmentID{result.WorkflowID, result.AppliedIndex}
		groups[id] = append(groups[id], result)
	}
	segments := make([]taskResultSegment, 0, len(groups))
	for id, values := range groups {
		sort.Slice(values, func(i, j int) bool { return values[i].Sequence < values[j].Sequence })
		segment := taskResultSegment{Version: taskResultSegmentVersion, WorkflowID: id.workflowID, AppliedIndex: id.appliedIndex, Results: values}
		if !validTaskResultSegment(segment) {
			return nil, ErrRecordVersion
		}
		segments = append(segments, segment)
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].WorkflowID != segments[j].WorkflowID {
			return segments[i].WorkflowID < segments[j].WorkflowID
		}
		return segments[i].AppliedIndex < segments[j].AppliedIndex
	})
	return segments, nil
}

func validTaskResultSegment(segment taskResultSegment) bool {
	if segment.Version != taskResultSegmentVersion || segment.WorkflowID == "" || segment.AppliedIndex == 0 || len(segment.Results) == 0 || len(segment.Results) > maxSegmentItems {
		return false
	}
	var previous uint64
	seen := make(map[taskResultIdentity]struct{}, len(segment.Results))
	for index, result := range segment.Results {
		if !validTaskResult(result) || result.WorkflowID != segment.WorkflowID || result.AppliedIndex != segment.AppliedIndex || index > 0 && result.Sequence <= previous {
			return false
		}
		identity := resultIdentity(result)
		if _, exists := seen[identity]; exists {
			return false
		}
		seen[identity] = struct{}{}
		previous = result.Sequence
	}
	return true
}

func validTaskResult(result TaskResult) bool {
	if result.Version != TaskResultVersion || result.WorkflowID == "" || result.TaskID == "" || result.TaskType == "" || result.AttemptID == "" || result.Attempt == 0 || result.Sequence == 0 || result.AppliedIndex == 0 || result.At < 0 || result.Phase != "normal" && result.Phase != "compensation" {
		return false
	}
	switch result.EventKind {
	case core.EventTaskCompleted:
		return result.Phase == "normal" && result.Status == core.TaskCompleted
	case core.EventTaskFailed, core.EventTaskTimedOut:
		return result.Status == core.TaskFailed
	case core.EventTaskCompensated:
		return result.Phase == "compensation" && result.Status == core.TaskCompensated
	default:
		return false
	}
}

func decodeTaskResultSegment(key, data []byte) (taskResultSegment, error) {
	var segment taskResultSegment
	if err := decodeJSON(data, &segment); err != nil {
		return taskResultSegment{}, err
	}
	if !validTaskResultSegment(segment) || !bytes.Equal(key, materializedSegmentKey(5, segment.WorkflowID, segment.AppliedIndex)) {
		return taskResultSegment{}, ErrRecordVersion
	}
	return segment, nil
}

func resultIdentity(result TaskResult) taskResultIdentity {
	return taskResultIdentity{result.WorkflowID, result.TaskID, result.Phase, result.AttemptID}
}

func sortTaskResults(results []TaskResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].WorkflowID != results[j].WorkflowID {
			return results[i].WorkflowID < results[j].WorkflowID
		}
		if results[i].TaskID != results[j].TaskID {
			return results[i].TaskID < results[j].TaskID
		}
		if results[i].Phase != results[j].Phase {
			return results[i].Phase < results[j].Phase
		}
		return results[i].AttemptID < results[j].AttemptID
	})
}
