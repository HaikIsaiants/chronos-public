package coordinator

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/storage"
)

func TestThreeNodeQuorumRestartAndRetry(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition()
	submit := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	result, err := cluster.Submit(2, submit, 10000)
	if err != nil {
		t.Fatal(err)
	}
	workflowID := result.WorkflowID
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 3; id++ {
		node, _ := cluster.Node(id)
		state, exists := node.Store().Engine().State(workflowID)
		if !exists || state.Version != 2 {
			t.Fatalf("node %d did not apply submit", id)
		}
	}
	if err := cluster.Crash(1); err != nil {
		t.Fatal(err)
	}
	leader, err := cluster.WaitLeader(30)
	if err != nil {
		t.Fatal(err)
	}
	if leader == 1 {
		t.Fatal("crashed node remained leader")
	}
	start, err := cluster.Submit(3, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: workflowID, TaskID: "task", At: 1,
	}), 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(start.Events) != 1 || start.Events[0].Kind != core.EventTaskStarted {
		t.Fatalf("unexpected start result: %+v", start)
	}
	if err := cluster.Restart(1); err != nil {
		t.Fatal(err)
	}
	cluster.Heal()
	if err := cluster.Tick(12); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	duplicate, err := cluster.Submit(1, submit, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || !reflect.DeepEqual(duplicate.Events, result.Events) {
		t.Fatal("retry did not return the stored result")
	}
	converged, err := cluster.Converged()
	if err != nil || !converged {
		t.Fatalf("cluster did not converge: %v", err)
	}
}

func TestNoProgressWithoutQuorum(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Crash(2); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Crash(3); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	if _, err := cluster.Submit(1, command, 10000); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("minority submission returned %v", err)
	}
	node, _ := cluster.Node(1)
	if len(node.Store().Engine().Snapshot().Workflows) != 0 {
		t.Fatal("minority applied an uncommitted command")
	}
	if _, exists, err := node.Store().Receipt("submit"); err != nil || exists {
		t.Fatal("minority persisted a receipt")
	}
	if err := cluster.Restart(2); err != nil {
		t.Fatal(err)
	}
	cluster.Heal()
	if err := cluster.Tick(12); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Submit(2, command, 10000); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := node.Store().Receipt("submit"); err != nil || !exists {
		t.Fatal("restored quorum did not commit the command")
	}
}

func TestPartitionOverwritesMinoritySuffix(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition()
	submit, err := cluster.Submit(1, core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	cluster.Isolate(1)
	minority := lease(core.Command{
		Kind: core.CommandStart, RequestID: "minority", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1,
	})
	if err := cluster.Propose(1, []core.Command{minority}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Tick(12); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Campaign(2); err != nil {
		t.Fatal(err)
	}
	majority := lease(core.Command{
		Kind: core.CommandStart, RequestID: "majority", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2,
	})
	result, err := cluster.Submit(3, majority, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 || result.Events[0].RequestID != "majority" {
		t.Fatalf("unexpected majority result: %+v", result)
	}
	cluster.Heal()
	if err := cluster.Tick(20); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 3; id++ {
		node, _ := cluster.Node(id)
		if _, exists, err := node.Store().Receipt("minority"); err != nil || exists {
			t.Fatalf("node %d retained minority progress", id)
		}
		if _, exists, err := node.Store().Receipt("majority"); err != nil || !exists {
			t.Fatalf("node %d missed majority progress", id)
		}
	}
	converged, err := cluster.Converged()
	if err != nil || !converged {
		t.Fatalf("cluster did not converge: %v", err)
	}
}

func TestSnapshotCatchUpAndTailReplay(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	cluster.Isolate(3)
	definition := testDefinition()
	submit, err := cluster.Submit(1, core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}, 10000)
	if err != nil {
		t.Fatal(err)
	}
	start, err := cluster.Submit(2, lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1,
	}), 10000)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Snapshot(1); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Submit(1, fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "task",
		At: 2, Output: map[string]string{"value": "done"},
	}, start), 10000); err != nil {
		t.Fatal(err)
	}
	cluster.Heal()
	if err := cluster.Tick(30); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(20000); err != nil {
		t.Fatal(err)
	}
	node, _ := cluster.Node(3)
	state, exists := node.Store().Engine().State(submit.WorkflowID)
	if !exists || state.Status != core.WorkflowCompleted {
		t.Fatalf("snapshot follower did not catch up: %+v", state)
	}
	if retry, err := cluster.Submit(3, core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}, 10000); err != nil || !retry.Duplicate {
		t.Fatalf("snapshot lost dedupe receipt: %+v %v", retry, err)
	}
	converged, err := cluster.Converged()
	if err != nil || !converged {
		t.Fatalf("cluster did not converge: %v", err)
	}
}

func TestGroupCommitUsesOneRaftIndex(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition()
	workflowID := core.WorkflowID(definition.Namespace, "submit")
	commands := []core.Command{
		{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition},
		lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: workflowID, TaskID: "task", At: 1}),
	}
	results, err := cluster.SubmitBatch(2, commands, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || len(results[0].Events) == 0 || len(results[1].Events) == 0 {
		t.Fatalf("unexpected batch results: %+v", results)
	}
	if results[0].WorkflowID != workflowID || results[1].WorkflowID != workflowID || results[0].Events[0].Kind != core.EventWorkflowSubmitted || results[1].Events[0].Kind != core.EventTaskStarted {
		t.Fatalf("unexpected batch results: %+v", results)
	}
	node, _ := cluster.Node(1)
	firstReceipt, _, _ := node.Store().Receipt("submit")
	secondReceipt, _, _ := node.Store().Receipt("start")
	if firstReceipt.AppliedIndex != secondReceipt.AppliedIndex {
		t.Fatalf("batch used different raft indexes: %d %d", firstReceipt.AppliedIndex, secondReceipt.AppliedIndex)
	}
}

func TestFollowersDoNotRetainCommittedOutcomes(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition()
	command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	if err := cluster.Propose(1, []core.Command{command}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Drive(10000); err != nil {
		t.Fatal(err)
	}
	workflowID := core.WorkflowID(definition.Namespace, command.RequestID)
	for id := uint64(1); id <= 3; id++ {
		node, _ := cluster.Node(id)
		outcomes := node.TakeOutcomes()
		if id == 1 && (len(outcomes) != 1 || outcomes[0].RequestID != command.RequestID) {
			t.Fatalf("leader outcomes differ: %+v", outcomes)
		}
		if id != 1 && len(outcomes) != 0 {
			t.Fatalf("node %d retained follower outcomes", id)
		}
		if receipt, exists, err := node.Store().Receipt(command.RequestID); err != nil || !exists || receipt.Result.WorkflowID != workflowID {
			t.Fatalf("node %d receipt differs: %+v %v", id, receipt, err)
		}
		if state, exists := node.Store().State(workflowID); !exists || state.ID != workflowID {
			t.Fatalf("node %d state differs: %+v", id, state)
		}
	}
}

func TestProposalRequiresAppliedLogAndUniqueRequests(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	first := testDefinition()
	second := testDefinition()
	second.Name = "second"
	duplicate := []core.Command{
		{Kind: core.CommandSubmit, RequestID: "same", Definition: &first},
		{Kind: core.CommandSubmit, RequestID: "same", Definition: &second},
	}
	if err := cluster.Propose(1, duplicate); !errors.Is(err, core.ErrInvalidCommand) {
		t.Fatalf("duplicate request ids returned %v", err)
	}
	cluster.Isolate(1)
	if err := cluster.Propose(1, []core.Command{{Kind: core.CommandSubmit, RequestID: "first", Definition: &first}}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Propose(1, []core.Command{{Kind: core.CommandSubmit, RequestID: "second", Definition: &second}}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("proposal against unapplied log returned %v", err)
	}
}

func TestCrashBoundariesPreserveSafeRetry(t *testing.T) {
	for _, point := range []storage.Point{
		storage.RaftBeforeSync,
		storage.RaftAfterSync,
		storage.ApplyBeforeSync,
		storage.ApplyAfterSync,
	} {
		t.Run(string(point), func(t *testing.T) {
			armed := false
			fired := false
			cluster := newTestCluster(t, map[uint64]func(storage.Point) error{
				1: func(hit storage.Point) error {
					if armed && !fired && hit == point {
						fired = true
						return storage.ErrInjectedCrash
					}
					return nil
				},
			})
			if err := cluster.Elect(1); err != nil {
				t.Fatal(err)
			}
			definition := testDefinition()
			command := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
			armed = true
			if _, err := cluster.Submit(1, command, 10000); !errors.Is(err, storage.ErrInjectedCrash) {
				t.Fatalf("failpoint returned %v", err)
			}
			if err := cluster.Crash(1); err != nil {
				t.Fatal(err)
			}
			leader, err := cluster.WaitLeader(30)
			if err != nil {
				t.Fatal(err)
			}
			result, err := cluster.Submit(leader, command, 20000)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Events) != 2 {
				t.Fatalf("unexpected retry result: %+v", result)
			}
			if err := cluster.Restart(1); err != nil {
				t.Fatal(err)
			}
			cluster.Heal()
			if err := cluster.Tick(20); err != nil {
				t.Fatal(err)
			}
			for id := uint64(1); id <= 3; id++ {
				node, _ := cluster.Node(id)
				receipt, exists, err := node.Store().Receipt("submit")
				if err != nil || !exists || len(receipt.Result.Events) != 2 {
					t.Fatalf("node %d lost the command: %+v %v", id, receipt, err)
				}
				events, err := node.Store().Events(result.WorkflowID)
				if err != nil || len(events) != 2 {
					t.Fatalf("node %d duplicated history: %d %v", id, len(events), err)
				}
			}
			converged, err := cluster.Converged()
			if err != nil || !converged {
				t.Fatalf("cluster did not converge: %v", err)
			}
		})
	}
}

func TestRepeatedFunctionalFailoverMatchesReference(t *testing.T) {
	cluster := newTestCluster(t, nil)
	if err := cluster.Elect(1); err != nil {
		t.Fatal(err)
	}
	reference := core.NewEngine()
	commands := 0
	events := 0
	logicalTime := int64(0)
	for workflow := 0; workflow < 6; workflow++ {
		definition := benchmarkDefinition(workflow)
		submitCommand := core.Command{
			Kind: core.CommandSubmit, RequestID: fmt.Sprintf("w%d-submit", workflow), Definition: &definition,
		}
		leader, _ := cluster.Leader()
		submit, err := cluster.Submit(leader, submitCommand, 20000)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reference.Handle(submitCommand); err != nil {
			t.Fatal(err)
		}
		commands++
		events += len(submit.Events)
		for task := 0; task < 8; task++ {
			taskID := fmt.Sprintf("t%d", task)
			logicalTime++
			startCommand := lease(core.Command{
				Kind: core.CommandStart, RequestID: fmt.Sprintf("w%d-start-%d", workflow, task),
				WorkflowID: submit.WorkflowID, TaskID: taskID, At: logicalTime,
			})
			leader, _ = cluster.Leader()
			start, err := cluster.Submit(leader, startCommand, 20000)
			if err != nil {
				t.Fatal(err)
			}
			referenceStart, err := reference.Handle(startCommand)
			if err != nil {
				t.Fatal(err)
			}
			if start.Events[0].AttemptID != referenceStart.Events[0].AttemptID {
				t.Fatal("attempt ids diverged")
			}
			commands++
			events += len(start.Events)
			logicalTime++
			completeCommand := fenced(core.Command{
				Kind: core.CommandComplete, RequestID: fmt.Sprintf("w%d-complete-%d", workflow, task),
				WorkflowID: submit.WorkflowID, TaskID: taskID,
				At: logicalTime, Output: map[string]string{"value": taskID},
			}, start)
			leader, _ = cluster.Leader()
			complete, err := cluster.Submit(leader, completeCommand, 20000)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reference.Handle(completeCommand); err != nil {
				t.Fatal(err)
			}
			commands++
			events += len(complete.Events)
			if commands%6 == 0 {
				oldLeader, _ := cluster.Leader()
				if err := cluster.Crash(oldLeader); err != nil {
					t.Fatal(err)
				}
				if _, err := cluster.WaitLeader(30); err != nil {
					t.Fatal(err)
				}
				if err := cluster.Restart(oldLeader); err != nil {
					t.Fatal(err)
				}
				cluster.Heal()
				if err := cluster.Tick(12); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if commands != 102 || events != 156 {
		t.Fatalf("unexpected contract totals: %d commands %d events", commands, events)
	}
	if err := cluster.Drive(20000); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Tick(12); err != nil {
		t.Fatal(err)
	}
	expected := reference.Snapshot()
	if len(expected.Workflows) != 6 || len(expected.Requests) != 102 {
		t.Fatalf("unexpected reference coverage: %d workflows %d requests", len(expected.Workflows), len(expected.Requests))
	}
	expectedEvents := make(map[string][]core.Event, len(expected.Workflows))
	for _, event := range reference.Journal() {
		expectedEvents[event.WorkflowID] = append(expectedEvents[event.WorkflowID], event)
	}
	for id := uint64(1); id <= 3; id++ {
		node, _ := cluster.Node(id)
		applied := node.Store().Applied()
		for _, workflow := range expected.Workflows {
			actual, exists := node.Store().State(workflow.ID)
			if !exists || !reflect.DeepEqual(actual, &workflow) {
				t.Fatalf("node %d workflow %s differs from reference", id, workflow.ID)
			}
			history, err := node.Store().Events(workflow.ID)
			if err != nil || !reflect.DeepEqual(history, expectedEvents[workflow.ID]) {
				t.Fatalf("node %d workflow %s history differs from reference: %v", id, workflow.ID, err)
			}
		}
		for _, request := range expected.Requests {
			receipt, exists, err := node.Store().Receipt(request.RequestID)
			if err != nil || !exists || receipt.Version != storage.ReceiptVersion || receipt.RequestHash != request.RequestHash || !reflect.DeepEqual(receipt.Result, request.Result) || receipt.AppliedIndex == 0 || receipt.AppliedIndex > applied.Index || receipt.CommittedTerm == 0 {
				t.Fatalf("node %d request %s receipt differs from reference: %v", id, request.RequestID, err)
			}
			outcome, exists, err := node.Store().Outcome(request.RequestID, request.RequestHash)
			if err != nil || !exists || outcome.Version != storage.OutcomeVersion || outcome.Code != "" || outcome.AppliedIndex != receipt.AppliedIndex || outcome.CommittedTerm != receipt.CommittedTerm || !reflect.DeepEqual(outcome.Result, request.Result) {
				t.Fatalf("node %d request %s outcome differs from receipt: %v", id, request.RequestID, err)
			}
		}
	}
	converged, err := cluster.Converged()
	if err != nil || !converged {
		t.Fatalf("cluster did not converge: %v", err)
	}
}

func newTestCluster(t *testing.T, failpoints map[uint64]func(storage.Point) error) *Cluster {
	t.Helper()
	converted := make(map[uint64]storage.Failpoint, len(failpoints))
	for id, failpoint := range failpoints {
		converted[id] = failpoint
	}
	cluster, err := NewCluster(ClusterConfig{Directory: t.TempDir(), ClusterID: "test", Failpoints: converted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cluster.Close(); err != nil {
			t.Error(err)
		}
	})
	return cluster
}

func testDefinition() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name: "single", Namespace: "test",
		Tasks: []core.TaskDefinition{{
			ID: "task", Type: "test", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}},
	}
}

func benchmarkDefinition(index int) core.WorkflowDefinition {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	tasks := []core.TaskDefinition{
		{ID: "t0", Type: "test", Retry: retry},
		{ID: "t1", Type: "test", Dependencies: []string{"t0"}, Retry: retry},
		{ID: "t2", Type: "test", Dependencies: []string{"t0"}, Retry: retry},
		{ID: "t3", Type: "test", Dependencies: []string{"t0"}, Retry: retry},
		{ID: "t4", Type: "test", Dependencies: []string{"t1", "t2"}, Retry: retry},
		{ID: "t5", Type: "test", Dependencies: []string{"t3"}, Retry: retry},
		{ID: "t6", Type: "test", Dependencies: []string{"t4", "t5"}, Retry: retry},
		{ID: "t7", Type: "test", Dependencies: []string{"t6"}, Retry: retry},
	}
	return core.WorkflowDefinition{Name: fmt.Sprintf("workflow-%d", index), Namespace: "benchmark", Tasks: tasks}
}
