package core_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
)

func TestEngineDiamondWorkflow(t *testing.T) {
	engine := core.NewEngine()
	definition := diamondDefinition()
	commands := make([]core.Command, 0, 7)
	handle := func(command core.Command) core.Result {
		t.Helper()
		commands = append(commands, command)
		result, err := engine.Handle(command)
		if err != nil {
			t.Fatalf("handle %s: %v", command.Kind, err)
		}
		return result
	}
	submit := handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", At: 0, Definition: &definition})
	workflowID := submit.WorkflowID
	projection, _ := engine.Projection(workflowID)
	if !reflect.DeepEqual(projection.ReadyTasks, []string{"a", "b"}) {
		t.Fatalf("unexpected roots: %v", projection.ReadyTasks)
	}
	startB := handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-b", WorkflowID: workflowID, TaskID: "b", At: 1}))
	startA := handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-a", WorkflowID: workflowID, TaskID: "a", At: 2}))
	handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-b", WorkflowID: workflowID, TaskID: "b", At: 3, Output: map[string]string{"result": "B"}}, startB))
	projection, _ = engine.Projection(workflowID)
	if len(projection.ReadyTasks) != 0 || projection.RemainingDependencies["c"] != 1 {
		t.Fatalf("join became ready early: %+v", projection)
	}
	handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-a", WorkflowID: workflowID, TaskID: "a", At: 4, Output: map[string]string{"result": "A"}}, startA))
	projection, _ = engine.Projection(workflowID)
	if !reflect.DeepEqual(projection.ReadyTasks, []string{"c"}) || projection.RemainingDependencies["c"] != 0 {
		t.Fatalf("join did not become ready: %+v", projection)
	}
	startC := handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-c", WorkflowID: workflowID, TaskID: "c", At: 5}))
	state, _ := engine.State(workflowID)
	expectedInputs := map[string]map[string]string{"a": {"result": "A"}, "b": {"result": "B"}}
	if !reflect.DeepEqual(state.Tasks["c"].Inputs, expectedInputs) {
		t.Fatalf("unexpected inputs: %v", state.Tasks["c"].Inputs)
	}
	handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-c", WorkflowID: workflowID, TaskID: "c", At: 6, Output: map[string]string{"result": "C"}}, startC))
	state, _ = engine.State(workflowID)
	if state.Status != core.WorkflowCompleted {
		t.Fatalf("unexpected status: %s", state.Status)
	}
	projection, _ = engine.Projection(workflowID)
	if projection.Summary.Completed != 3 || projection.Summary.Total != 3 || len(projection.ReadyTasks) != 0 || len(projection.ActiveAttempts) != 0 {
		t.Fatalf("unexpected final projection: %+v", projection)
	}
	metrics := engine.Metrics()
	if metrics.CommandsAccepted != 7 || metrics.Transitions != 11 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	journalLength := len(engine.Journal())
	for _, command := range commands {
		result, err := engine.Handle(command)
		if err != nil || !result.Duplicate {
			t.Fatalf("exact retry failed: %v %+v", err, result)
		}
	}
	if len(engine.Journal()) != journalLength || engine.Metrics().Duplicates != uint64(len(commands)) {
		t.Fatal("duplicates changed history or counters")
	}
	conflict := commands[len(commands)-1]
	conflict.Output = map[string]string{"result": "different"}
	if !errors.Is(handleError(engine, conflict), core.ErrRequestConflict) || len(engine.Journal()) != journalLength {
		t.Fatal("conflicting request mutated history")
	}
	terminal := lease(core.Command{Kind: core.CommandStart, RequestID: "terminal", WorkflowID: workflowID, TaskID: "a", At: 7})
	if !errors.Is(handleError(engine, terminal), core.ErrInvalidTransition) || len(engine.Journal()) != journalLength {
		t.Fatal("terminal workflow accepted a new transition")
	}
	replayed, err := core.Replay(engine.Journal())
	if err != nil {
		t.Fatal(err)
	}
	replayedState, _ := replayed.State(workflowID)
	liveHash, _ := core.StateHash(state)
	replayHash, _ := core.StateHash(replayedState)
	if liveHash != replayHash {
		t.Fatalf("replay mismatch: %s %s", liveHash, replayHash)
	}
	replayedProjection, _ := replayed.Projection(workflowID)
	if !reflect.DeepEqual(projection, replayedProjection) {
		t.Fatal("replayed projection differs")
	}
	if replayed.Metrics().CommandsReceived != 0 || replayed.Metrics().CommandsAccepted != 0 || replayed.Metrics().Transitions != 0 {
		t.Fatalf("replay changed operational metrics: %+v", replayed.Metrics())
	}
	duplicate, err := replayed.Handle(commands[0])
	if err != nil || !duplicate.Duplicate || len(replayed.Journal()) != journalLength {
		t.Fatal("replayed deduplication state differs")
	}
	replayMetrics := replayed.Metrics()
	if replayMetrics.CommandsReceived != 1 || replayMetrics.CommandsAccepted != 0 || replayMetrics.Duplicates != 1 || replayMetrics.Transitions != 0 {
		t.Fatalf("replayed operational counters are wrong: %+v", replayMetrics)
	}
}

func TestDeterministicAttemptIDs(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	historyLength := len(engine.Journal())
	_, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "wrong", WorkflowID: submit.WorkflowID, TaskID: "task", AttemptID: "caller-selected", At: 1}))
	if !errors.Is(err, core.ErrInvalidCommand) || len(engine.Journal()) != historyLength {
		t.Fatalf("caller-selected attempt id accepted: %v", err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	expected := core.AttemptID(submit.WorkflowID, "task", 1)
	if start.Events[0].AttemptID != expected {
		t.Fatalf("unexpected attempt id: %s", start.Events[0].AttemptID)
	}
	retry, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 99}))
	if err != nil || !retry.Duplicate {
		t.Fatalf("semantic retry failed: %v %+v", err, retry)
	}
	_, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", AttemptID: expected, At: 99}))
	if !errors.Is(err, core.ErrRequestConflict) {
		t.Fatalf("changed retry accepted: %v", err)
	}
	partial, err := core.Replay(engine.Journal()[:2])
	if err != nil {
		t.Fatal(err)
	}
	state, _ := partial.State(submit.WorkflowID)
	invalid := start.Events[0]
	invalid.AttemptID = "invalid"
	if _, err := core.Apply(state, invalid); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("invalid replayed attempt accepted: %v", err)
	}
}

func TestUpstreamOutputsPreserveProvenance(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{
		Name: "inputs", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "a", Type: "source", Retry: retry},
			{ID: "a.b", Type: "source", Retry: retry},
			{ID: "join", Type: "join", Dependencies: []string{"a", "a.b"}, Retry: retry},
		},
	}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	for index, item := range []struct {
		task   string
		output map[string]string
	}{{"a", map[string]string{"b.c": "one"}}, {"a.b", map[string]string{"c": "two"}}} {
		at := int64(index*2 + 1)
		start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-" + item.task, WorkflowID: submit.WorkflowID, TaskID: item.task, At: at}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete-" + item.task, WorkflowID: submit.WorkflowID, TaskID: item.task, At: at + 1, Output: item.output}, start))
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-join", WorkflowID: submit.WorkflowID, TaskID: "join", At: 5}))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Tasks["join"].Inputs["a"]["b.c"] != "one" || state.Tasks["join"].Inputs["a.b"]["c"] != "two" {
		t.Fatalf("upstream outputs collided: %v", state.Tasks["join"].Inputs)
	}
	state.Tasks["join"].Inputs["a"]["b.c"] = "mutated"
	stored, _ := engine.State(submit.WorkflowID)
	if stored.Tasks["join"].Inputs["a"]["b.c"] != "one" {
		t.Fatal("caller mutated nested inputs")
	}
}

func TestFailureClosesActiveTasks(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{Name: "parallel", Namespace: "test", Tasks: []core.TaskDefinition{
		{ID: "a", Type: "task", Retry: retry},
		{ID: "b", Type: "task", Retry: retry},
	}}
	engine := core.NewEngine()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	startA, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-a", WorkflowID: submit.WorkflowID, TaskID: "a", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	startB, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start-b", WorkflowID: submit.WorkflowID, TaskID: "b", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail-a", WorkflowID: submit.WorkflowID, TaskID: "a", At: 2, Error: "failed"}, startA))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	projection, _ := engine.Projection(submit.WorkflowID)
	if state.Tasks["a"].Status != core.TaskFailed || state.Tasks["b"].Status != core.TaskCancelled || len(projection.ActiveAttempts) != 0 {
		t.Fatalf("failure left active work: %+v %+v", state, projection)
	}
	historyLength := len(engine.Journal())
	if !errors.Is(handleError(engine, fenced(core.Command{Kind: core.CommandComplete, RequestID: "late-b", WorkflowID: submit.WorkflowID, TaskID: "b", At: 3}, startB)), core.ErrFenced) || len(engine.Journal()) != historyLength {
		t.Fatal("cancelled attempt accepted completion")
	}
	replayed, err := core.Replay(engine.Journal())
	if err != nil {
		t.Fatal(err)
	}
	replayedState, _ := replayed.State(submit.WorkflowID)
	liveHash, _ := core.StateHash(state)
	replayedHash, _ := core.StateHash(replayedState)
	if liveHash != replayedHash {
		t.Fatalf("failed workflow replay mismatch: %s %s", liveHash, replayedHash)
	}
}

func TestEngineFailureIsTerminal(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Handle(fenced(core.Command{Kind: core.CommandFail, RequestID: "fail", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Error: "failed"}, start))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := engine.State(submit.WorkflowID)
	if state.Status != core.WorkflowFailed || state.Tasks["task"].Status != core.TaskFailed {
		t.Fatalf("unexpected failed state: %+v", state)
	}
	if !errors.Is(handleError(engine, fenced(core.Command{Kind: core.CommandComplete, RequestID: "late", WorkflowID: submit.WorkflowID, TaskID: "task", At: 3}, start)), core.ErrFenced) {
		t.Fatal("failed workflow accepted completion")
	}
}

func TestEngineOwnershipBoundaries(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submitCommand := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	submit, err := engine.Handle(submitCommand)
	if err != nil {
		t.Fatal(err)
	}
	definition.Tasks[0].ID = "mutated"
	submit.Events[0].Definition.Tasks[0].ID = "mutated"
	state, _ := engine.State(submit.WorkflowID)
	if _, exists := state.Tasks["task"]; !exists {
		t.Fatal("caller mutated stored definition")
	}
	start, err := engine.Handle(lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID, TaskID: "task", At: 1}))
	if err != nil {
		t.Fatal(err)
	}
	output := map[string]string{"value": "original"}
	_, err = engine.Handle(fenced(core.Command{Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID, TaskID: "task", At: 2, Output: output}, start))
	if err != nil {
		t.Fatal(err)
	}
	output["value"] = "mutated"
	state, _ = engine.State(submit.WorkflowID)
	state.Tasks["task"] = core.TaskState{}
	history := engine.Journal()
	history[len(history)-2].Output["value"] = "mutated"
	stored, _ := engine.State(submit.WorkflowID)
	if stored.Tasks["task"].Output["value"] != "original" || stored.Tasks["task"].Status != core.TaskCompleted {
		t.Fatal("caller mutated stored state or history")
	}
}

func TestApplyPurityAndSequenceValidation(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	result, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	history := engine.Journal()
	partial, err := core.Replay(history[:1])
	if err != nil {
		t.Fatal(err)
	}
	state, _ := partial.State(result.WorkflowID)
	before, _ := core.StateHash(state)
	next, err := core.Apply(state, history[1])
	if err != nil {
		t.Fatal(err)
	}
	afterOriginal, _ := core.StateHash(state)
	if before != afterOriginal || next.Tasks["task"].Status != core.TaskReady {
		t.Fatal("apply mutated its input or produced wrong state")
	}
	invalid := history[1]
	invalid.Sequence++
	if _, err := core.Apply(state, invalid); !errors.Is(err, core.ErrSequence) {
		t.Fatalf("sequence gap accepted: %v", err)
	}
	invalid = history[1]
	invalid.ID = "invalid"
	if _, err := core.Apply(state, invalid); !errors.Is(err, core.ErrSequence) {
		t.Fatalf("invalid event id accepted: %v", err)
	}
	if _, err := core.Replay(history[1:]); !errors.Is(err, core.ErrSequence) {
		t.Fatalf("history without submission accepted: %v", err)
	}
	invalidHistory := engine.Journal()
	invalidHistory[0].WorkflowID = ""
	invalidHistory[0].ID = core.EventID("", 1, core.EventWorkflowSubmitted)
	if _, err := core.Replay(invalidHistory); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("empty workflow id accepted: %v", err)
	}
	invalidHistory = engine.Journal()
	invalidHistory[0].At = -1
	if _, err := core.Replay(invalidHistory); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("negative event time accepted: %v", err)
	}
}

func handleError(engine *core.Engine, command core.Command) error {
	_, err := engine.Handle(command)
	return err
}

func singleDefinition() core.WorkflowDefinition {
	return core.WorkflowDefinition{
		Name: "single", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "test", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
}

func diamondDefinition() core.WorkflowDefinition {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	return core.WorkflowDefinition{
		Name: "diamond", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "c", Type: "join", Dependencies: []string{"b", "a"}, Retry: retry},
			{ID: "b", Type: "source", Retry: retry},
			{ID: "a", Type: "source", Retry: retry},
		},
	}
}
