package storage_test

import (
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type workflowFixture struct {
	nextIndex       uint64
	scheduledID     string
	retryID         string
	fanoutID        string
	compensationID  string
	compensationRun core.Result
}

func TestWorkflowStatesPersistOnReopen(t *testing.T) {
	directory := t.TempDir()
	store := openStore(t, directory, 1, nil)
	fixture := buildWorkflowFixture(t, store)
	expected := store.Engine().Snapshot()
	expectedResults, err := store.TaskResults()
	if err != nil {
		t.Fatal(err)
	}
	assertWorkflowFixture(t, store.Engine(), fixture)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, directory, 1, nil)
	defer reopened.Close()
	if !reflect.DeepEqual(expected, reopened.Engine().Snapshot()) {
		t.Fatal("workflow state changed on reopen")
	}
	actualResults, err := reopened.TaskResults()
	if err != nil || !reflect.DeepEqual(expectedResults, actualResults) {
		t.Fatalf("workflow task results changed on reopen: %+v %v", actualResults, err)
	}
	assertWorkflowFixture(t, reopened.Engine(), fixture)
	applyCommand(t, reopened, fixture.nextIndex, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "compensation-complete", WorkflowID: fixture.compensationID,
		TaskID: "deploy", At: 6, Output: map[string]string{"rolled_back": "true"},
	}, fixture.compensationRun))
	results, err := reopened.TaskResults()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, result := range results {
		if result.WorkflowID == fixture.compensationID && result.TaskID == "deploy" && result.Phase == "compensation" {
			found = result.EventKind == core.EventTaskCompensated && result.TaskType == "rollback" && result.Status == core.TaskCompensated
		}
	}
	if !found {
		t.Fatalf("compensation result was not persisted: %+v", results)
	}
}

func TestWorkflowSnapshotInstallAndTail(t *testing.T) {
	source := openStore(t, t.TempDir(), 1, nil)
	defer source.Close()
	fixture := buildWorkflowFixture(t, source)
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	tailBatch, _, duplicate, err := source.Engine().Prepare(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "compensation-complete", WorkflowID: fixture.compensationID,
		TaskID: "deploy", At: 6, Output: map[string]string{"rolled_back": "true"},
	}, fixture.compensationRun))
	if err != nil || duplicate {
		t.Fatal(err)
	}
	persistRaft(t, source, fixture.nextIndex, 2)
	tail := storage.ApplicationEntry{
		Version: storage.ApplicationEntryVersion, Index: fixture.nextIndex, Term: 2, Batches: []core.CommandBatch{tailBatch},
	}
	if _, err := source.Apply([]storage.ApplicationEntry{tail}); err != nil {
		t.Fatal(err)
	}
	destination := openStore(t, t.TempDir(), 2, nil)
	defer destination.Close()
	hard := &pb.HardState{Term: proto.Uint64(2), Commit: proto.Uint64(snapshot.GetMetadata().GetIndex())}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	persistRaft(t, destination, fixture.nextIndex, 2)
	if _, err := destination.Apply([]storage.ApplicationEntry{tail}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source.Engine().Snapshot(), destination.Engine().Snapshot()) {
		t.Fatal("workflow snapshot plus tail differs from uninterrupted history")
	}
	sourceResults, _ := source.TaskResults()
	destinationResults, _ := destination.TaskResults()
	if !reflect.DeepEqual(sourceResults, destinationResults) {
		t.Fatal("workflow task results differ after snapshot installation")
	}
}

func buildWorkflowFixture(t *testing.T, store *storage.Store) workflowFixture {
	t.Helper()
	index := uint64(2)
	retry := core.RetryPolicy{MaxAttempts: 2, InitialBackoffMillis: 5, BackoffMultiplier: 2, MaxBackoffMillis: 20}
	scheduledDefinition := definition()
	scheduledDefinition.Name = "scheduled"
	scheduledDefinition.StartAt = 10
	scheduled, _ := applyCommand(t, store, index, core.Command{
		Kind: core.CommandSubmit, RequestID: "scheduled-submit", Definition: &scheduledDefinition,
	})
	index++
	retryDefinition := definition()
	retryDefinition.Name = "retry"
	retryDefinition.Tasks[0].Retry = retry
	retrySubmit, _ := applyCommand(t, store, index, core.Command{
		Kind: core.CommandSubmit, RequestID: "retry-submit", Definition: &retryDefinition,
	})
	index++
	retryStart, _ := applyCommand(t, store, index, lease(core.Command{
		Kind: core.CommandStart, RequestID: "retry-start", WorkflowID: retrySubmit.WorkflowID, TaskID: "task", At: 1,
	}))
	index++
	applyCommand(t, store, index, fenced(core.Command{
		Kind: core.CommandFail, RequestID: "retry-fail", WorkflowID: retrySubmit.WorkflowID,
		TaskID: "task", At: 2, Error: "retry",
	}, retryStart))
	index++
	base := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	fanoutDefinition := core.WorkflowDefinition{
		Name: "fanout", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "parent", Type: "discover", Retry: base, Fanout: &core.FanoutDefinition{
				TaskType: "child", AggregateTaskID: "aggregate", MaxItems: 2, Retry: base,
			}},
			{ID: "aggregate", Type: "aggregate", Dependencies: []string{"parent"}, Retry: base},
		},
	}
	fanoutSubmit, _ := applyCommand(t, store, index, core.Command{
		Kind: core.CommandSubmit, RequestID: "fanout-submit", Definition: &fanoutDefinition,
	})
	index++
	fanoutStart, _ := applyCommand(t, store, index, lease(core.Command{
		Kind: core.CommandStart, RequestID: "fanout-start", WorkflowID: fanoutSubmit.WorkflowID, TaskID: "parent", At: 1,
	}))
	index++
	applyCommand(t, store, index, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "fanout-complete", WorkflowID: fanoutSubmit.WorkflowID,
		TaskID: "parent", At: 2, Fanout: []core.FanoutItem{
			{Key: "two", Payload: map[string]string{"value": "2"}},
			{Key: "one", Payload: map[string]string{"value": "1"}},
		},
	}, fanoutStart))
	index++
	compensationDefinition := core.WorkflowDefinition{
		Name: "compensation", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "deploy", Type: "deploy", Retry: base, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
			{ID: "verify", Type: "verify", Dependencies: []string{"deploy"}, Retry: base},
		},
	}
	compensationSubmit, _ := applyCommand(t, store, index, core.Command{
		Kind: core.CommandSubmit, RequestID: "compensation-submit", Definition: &compensationDefinition,
	})
	index++
	deployStart, _ := applyCommand(t, store, index, lease(core.Command{
		Kind: core.CommandStart, RequestID: "deploy-start", WorkflowID: compensationSubmit.WorkflowID, TaskID: "deploy", At: 1,
	}))
	index++
	applyCommand(t, store, index, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "deploy-complete", WorkflowID: compensationSubmit.WorkflowID,
		TaskID: "deploy", At: 2, Output: map[string]string{"release": "v1"},
	}, deployStart))
	index++
	verifyStart, _ := applyCommand(t, store, index, lease(core.Command{
		Kind: core.CommandStart, RequestID: "verify-start", WorkflowID: compensationSubmit.WorkflowID, TaskID: "verify", At: 3,
	}))
	index++
	applyCommand(t, store, index, fenced(core.Command{
		Kind: core.CommandFail, RequestID: "verify-fail", WorkflowID: compensationSubmit.WorkflowID,
		TaskID: "verify", At: 4, Error: "verification failed",
	}, verifyStart))
	index++
	compensationRun, _ := applyCommand(t, store, index, lease(core.Command{
		Kind: core.CommandStart, RequestID: "compensation-start", WorkflowID: compensationSubmit.WorkflowID,
		TaskID: "deploy", At: 5,
	}))
	index++
	return workflowFixture{
		nextIndex: index, scheduledID: scheduled.WorkflowID, retryID: retrySubmit.WorkflowID,
		fanoutID: fanoutSubmit.WorkflowID, compensationID: compensationSubmit.WorkflowID,
		compensationRun: compensationRun,
	}
}

func assertWorkflowFixture(t *testing.T, engine *core.Engine, fixture workflowFixture) {
	t.Helper()
	scheduled, _ := engine.State(fixture.scheduledID)
	if scheduled.Status != core.WorkflowScheduled || !hasTimer(scheduled, core.TimerWorkflowStart) {
		t.Fatalf("scheduled workflow was not preserved: %+v", scheduled)
	}
	retry, _ := engine.State(fixture.retryID)
	if retry.Tasks["task"].Status != core.TaskPending || !hasTimer(retry, core.TimerRetry) {
		t.Fatalf("retry wait was not preserved: %+v", retry)
	}
	fanout, _ := engine.State(fixture.fanoutID)
	parent := fanout.Tasks["parent"]
	if !parent.FanoutExpanded || len(parent.Children) != 2 || len(fanout.Tasks["aggregate"].Definition.Dependencies) != 3 {
		t.Fatalf("expanded fanout was not preserved: %+v", fanout)
	}
	compensation, _ := engine.State(fixture.compensationID)
	deploy := compensation.Tasks["deploy"]
	if compensation.Status != core.WorkflowCompensating || deploy.Status != core.TaskCompensating || deploy.WorkerID == "" {
		t.Fatalf("mid-compensation state was not preserved: %+v", compensation)
	}
}

func hasTimer(state *core.WorkflowState, purpose core.TimerPurpose) bool {
	for _, timer := range state.Timers {
		if timer.Purpose == purpose && timer.Status == core.TimerScheduled {
			return true
		}
	}
	return false
}
