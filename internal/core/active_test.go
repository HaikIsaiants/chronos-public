package core_test

import (
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestActiveWorkflowMembershipTracksTerminalTransition(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submitted, err := engine.Handle(core.Command{
		Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ids := engine.ActiveWorkflowIDs(); !reflect.DeepEqual(ids, []string{submitted.WorkflowID}) {
		t.Fatalf("unexpected active workflows after submission: %v", ids)
	}
	usage, exists, err := engine.WorkflowUsage(submitted.WorkflowID)
	if err != nil || !exists || usage != (core.WorkflowUsage{Namespace: "test", Workflows: 1, Tasks: 1, Ready: 1}) {
		t.Fatalf("unexpected submitted usage: %+v %t %v", usage, exists, err)
	}
	start, err := engine.Handle(lease(core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	usage, exists, err = engine.WorkflowUsage(submitted.WorkflowID)
	if err != nil || !exists || usage != (core.WorkflowUsage{Namespace: "test", Workflows: 1, Tasks: 1, Running: 1}) {
		t.Fatalf("unexpected running usage: %+v %t %v", usage, exists, err)
	}
	if _, err := engine.Handle(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submitted.WorkflowID, TaskID: "task", At: 2,
	}, start)); err != nil {
		t.Fatal(err)
	}
	if len(engine.ActiveWorkflowIDs()) != 0 || !reflect.DeepEqual(engine.WorkflowIDs(), []string{submitted.WorkflowID}) {
		t.Fatalf("terminal workflow membership is invalid: %v %v", engine.ActiveWorkflowIDs(), engine.WorkflowIDs())
	}
	usage, exists, err = engine.WorkflowUsage(submitted.WorkflowID)
	if err != nil || !exists || usage != (core.WorkflowUsage{Namespace: "test"}) {
		t.Fatalf("unexpected terminal usage: %+v %t %v", usage, exists, err)
	}
}

func TestActiveWorkflowIndexSurvivesReplayRestoreAndClone(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	for _, id := range []string{"terminal", "active"} {
		if _, err := engine.Handle(core.Command{
			Kind: core.CommandSubmit, RequestID: "submit-" + id, WorkflowID: id, Definition: &definition,
		}); err != nil {
			t.Fatal(err)
		}
	}
	start, err := engine.Handle(lease(core.Command{
		Kind: core.CommandStart, RequestID: "start-terminal", WorkflowID: "terminal", TaskID: "task", At: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(fenced(core.Command{
		Kind: core.CommandComplete, RequestID: "complete-terminal", WorkflowID: "terminal", TaskID: "task", At: 2,
	}, start)); err != nil {
		t.Fatal(err)
	}
	replayed, err := core.Replay(engine.Journal())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := core.Restore(engine.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string]*core.Engine{
		"live": engine, "replayed": replayed, "restored": restored, "cloned": engine.Clone(),
	} {
		if ids := candidate.ActiveWorkflowIDs(); !reflect.DeepEqual(ids, []string{"active"}) {
			t.Fatalf("%s active workflows differ: %v", name, ids)
		}
	}
}

func TestPrepareAndApplyMatchesCommittedValidation(t *testing.T) {
	fast := core.NewEngine()
	committed := core.NewEngine()
	definition := singleDefinition()
	commands := []core.Command{
		{Kind: core.CommandSubmit, RequestID: "submit", WorkflowID: "workflow", Definition: &definition},
		lease(core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: "workflow", TaskID: "task", At: 1}),
	}
	for _, command := range commands {
		fastBatch, fastResult, duplicate, err := fast.PrepareAndApply(command)
		if err != nil || duplicate {
			t.Fatalf("fast stage failed: %+v %+v %v %v", fastBatch, fastResult, duplicate, err)
		}
		batch, _, duplicate, err := committed.Prepare(command)
		if err != nil || duplicate {
			t.Fatalf("prepare failed: %+v %v %v", batch, duplicate, err)
		}
		result, err := committed.ApplyBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(fastBatch, batch) || !reflect.DeepEqual(fastResult, result) {
			t.Fatalf("fast stage differs: %+v %+v %+v %+v", fastBatch, batch, fastResult, result)
		}
	}
	if !reflect.DeepEqual(fast.Snapshot(), committed.Snapshot()) || !reflect.DeepEqual(fast.Journal(), committed.Journal()) || !reflect.DeepEqual(fast.Metrics(), committed.Metrics()) {
		t.Fatal("fast stage state differs from committed validation")
	}
	batch, result, duplicate, err := fast.PrepareAndApply(commands[0])
	if err != nil || !duplicate || batch.RequestHash == "" || !result.Duplicate {
		t.Fatalf("fast duplicate differs: %+v %+v %v %v", batch, result, duplicate, err)
	}
}

func TestResidentRequestHashDoesNotLoad(t *testing.T) {
	engine := core.NewEngine()
	loads := 0
	engine.SetLoaders(nil, func(string) (core.RequestReceipt, bool, error) {
		loads++
		return core.RequestReceipt{}, false, nil
	})
	if _, exists := engine.ResidentRequestHash("missing"); exists || loads != 0 {
		t.Fatalf("resident lookup loaded storage: exists=%t loads=%d", exists, loads)
	}
	definition := singleDefinition()
	if _, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "resident", WorkflowID: "workflow", Definition: &definition}); err != nil {
		t.Fatal(err)
	}
	before := loads
	hash, exists := engine.ResidentRequestHash("resident")
	if !exists || hash == "" || loads != before {
		t.Fatalf("resident lookup failed: hash=%q exists=%t loads=%d", hash, exists, loads)
	}
}
