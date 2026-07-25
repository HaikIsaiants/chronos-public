package storage

import (
	"encoding/json"
	"fmt"

	"github.com/HaikIsaiants/chronos/internal/core"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const recoveryReplayCommandsPerEntry = 256
const recoveryReplayEntriesPerWrite = 16
const recoveryReplayTerm uint64 = 2

type recoveryLogBatch struct {
	Version  uint32              `json:"version"`
	Commands []core.CommandBatch `json:"commands"`
}

func WriteRecoveryReplayFixture(config Config, snapshotIndex uint64, snapshotState core.EngineSnapshot, batches []core.CommandBatch) (uint64, error) {
	if snapshotIndex <= 1 || len(batches) == 0 {
		return 0, fmt.Errorf("invalid recovery replay fixture")
	}
	for _, batch := range batches {
		if batch.Version != core.CommandBatchVersion || batch.RequestID == "" || batch.RequestHash == "" || batch.WorkflowID == "" || len(batch.Events) == 0 {
			return 0, fmt.Errorf("invalid recovery replay command batch")
		}
	}
	store, err := Open(config)
	if err != nil {
		return 0, err
	}
	defer store.Close()
	store.mu.RLock()
	pristine := store.applied == 1 && store.snapshot.GetMetadata().GetIndex() == 1 && len(store.entryTerms) == 0 && len(store.engine.WorkflowIDs()) == 0
	store.mu.RUnlock()
	if !pristine {
		return 0, fmt.Errorf("recovery replay directory is not empty")
	}
	image, err := recoverySnapshotImage(config.ClusterID, snapshotIndex, recoveryReplayTerm, snapshotState)
	if err != nil {
		return 0, err
	}
	payload, err := encodeSnapshotImage(image)
	if err != nil {
		return 0, err
	}
	snapshot := &pb.Snapshot{
		Data: payload,
		Metadata: &pb.SnapshotMetadata{
			Index: proto.Uint64(snapshotIndex), Term: proto.Uint64(recoveryReplayTerm),
			ConfState: proto.Clone(store.conf).(*pb.ConfState),
		},
	}
	// Persist the snapshot before its replay tail
	hard := &pb.HardState{Term: proto.Uint64(recoveryReplayTerm), Commit: proto.Uint64(snapshotIndex)}
	if err := store.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		return 0, err
	}
	if err := store.InstallSnapshot(snapshot); err != nil {
		return 0, err
	}
	var entryCount uint64
	// Bound entries per write to mimic normal raft traffic
	for offset := 0; offset < len(batches); {
		entries := make([]*pb.Entry, 0, recoveryReplayEntriesPerWrite)
		for len(entries) < recoveryReplayEntriesPerWrite && offset < len(batches) {
			end := min(offset+recoveryReplayCommandsPerEntry, len(batches))
			data, err := json.Marshal(recoveryLogBatch{Version: 1, Commands: batches[offset:end]})
			if err != nil {
				return 0, err
			}
			entryCount++
			entries = append(entries, &pb.Entry{Index: proto.Uint64(snapshotIndex + entryCount), Term: proto.Uint64(recoveryReplayTerm), Data: data})
			offset = end
		}
		hard.Commit = proto.Uint64(snapshotIndex + entryCount)
		if err := store.SaveReady(raft.Ready{HardState: hard, Entries: entries}); err != nil {
			return 0, err
		}
	}
	if store.Applied().Index != snapshotIndex || hard.GetCommit() != snapshotIndex+entryCount {
		return 0, fmt.Errorf("recovery replay fixture state differs")
	}
	return entryCount, nil
}

func recoverySnapshotImage(clusterID string, index, term uint64, snapshot core.EngineSnapshot) (SnapshotImage, error) {
	image := SnapshotImage{
		Version: SnapshotVersion, ClusterID: clusterID, AppliedIndex: index,
		Term: term, Engine: snapshot,
	}
	states := make(map[string]*core.WorkflowState, len(snapshot.Workflows))
	for stateIndex := range snapshot.Workflows {
		state := &snapshot.Workflows[stateIndex]
		states[state.ID] = state
	}
	for _, request := range snapshot.Requests {
		result := request.Result
		result.Duplicate = false
		image.Outcomes = append(image.Outcomes, Outcome{
			Version: OutcomeVersion, RequestID: request.RequestID, RequestHash: request.RequestHash,
			AppliedIndex: index, CommittedTerm: term, Result: result,
		})
		state := states[result.WorkflowID]
		if state == nil {
			return SnapshotImage{}, fmt.Errorf("snapshot request %s has no workflow", request.RequestID)
		}
		for _, event := range result.Events {
			task, exists := state.Tasks[event.TaskID]
			metadata, ok := taskResult(event, index, task)
			if ok && !exists {
				return SnapshotImage{}, fmt.Errorf("snapshot event %s has no task", event.ID)
			}
			if ok {
				image.TaskResults = append(image.TaskResults, metadata)
			}
		}
	}
	return image, nil
}
