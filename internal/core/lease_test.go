package core_test

import (
	"errors"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestLeaseRenewExpiryAndFencing(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	first, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "lease-1", WorkflowID: submit.WorkflowID,
		TaskID: "task", WorkerID: "worker-a", At: 10, LeaseUntil: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	granted := first.Events[0]
	if granted.Fence != 1 || granted.Idempotency != core.IdempotencyKey(submit.WorkflowID, "task") {
		t.Fatalf("invalid initial lease: %+v", granted)
	}
	renewed, err := engine.Handle(core.Command{
		Kind: core.CommandRenew, RequestID: "renew-1", WorkflowID: submit.WorkflowID,
		TaskID: "task", AttemptID: granted.AttemptID, WorkerID: granted.WorkerID,
		Fence: granted.Fence, At: 15, LeaseUntil: 30,
	})
	if err != nil || renewed.Events[0].Kind != core.EventLeaseRenewed {
		t.Fatalf("renewal failed: %+v %v", renewed, err)
	}
	expiredCompletion := core.Command{
		Kind: core.CommandComplete, RequestID: "at-boundary", WorkflowID: submit.WorkflowID,
		TaskID: "task", AttemptID: granted.AttemptID, WorkerID: granted.WorkerID,
		Fence: granted.Fence, At: 30,
	}
	if _, err := engine.Handle(expiredCompletion); !errors.Is(err, core.ErrLeaseExpired) {
		t.Fatalf("completion at expiry accepted: %v", err)
	}
	_, err = engine.Handle(core.Command{
		Kind: core.CommandExpire, RequestID: "expire-1", WorkflowID: submit.WorkflowID,
		TaskID: "task", AttemptID: granted.AttemptID, WorkerID: granted.WorkerID,
		Fence: granted.Fence, At: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "lease-2", WorkflowID: submit.WorkflowID,
		TaskID: "task", WorkerID: "worker-b", At: 31, LeaseUntil: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement := second.Events[0]
	if replacement.Attempt != 2 || replacement.Fence != 2 || replacement.AttemptID == granted.AttemptID ||
		replacement.Idempotency != granted.Idempotency {
		t.Fatalf("invalid replacement lease: %+v", replacement)
	}
	before, _ := engine.State(submit.WorkflowID)
	beforeHash, _ := core.StateHash(before)
	stale := core.Command{
		Kind: core.CommandComplete, RequestID: "stale", WorkflowID: submit.WorkflowID,
		TaskID: "task", AttemptID: granted.AttemptID, WorkerID: granted.WorkerID,
		Fence: granted.Fence, At: 32,
	}
	if _, err := engine.Handle(stale); !errors.Is(err, core.ErrFenced) {
		t.Fatalf("stale completion was not fenced: %v", err)
	}
	after, _ := engine.State(submit.WorkflowID)
	afterHash, _ := core.StateHash(after)
	if beforeHash != afterHash {
		t.Fatal("stale completion mutated state")
	}
	completion := core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID,
		TaskID: "task", AttemptID: replacement.AttemptID, WorkerID: replacement.WorkerID,
		Fence: replacement.Fence, At: 40, Output: map[string]string{"value": "done"},
	}
	result, err := engine.Handle(completion)
	if err != nil || result.Duplicate {
		t.Fatalf("current completion failed: %+v %v", result, err)
	}
	duplicate, err := engine.Handle(completion)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("lost-ack retry failed: %+v %v", duplicate, err)
	}
	history := len(engine.Journal())
	completion.RequestID = "second-completion"
	if _, err := engine.Handle(completion); !errors.Is(err, core.ErrFenced) || len(engine.Journal()) != history {
		t.Fatalf("second logical completion mutated state: %v", err)
	}
}

func TestActiveLeaseSurvivesSnapshot(t *testing.T) {
	engine := core.NewEngine()
	definition := singleDefinition()
	submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	start, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "lease", WorkflowID: submit.WorkflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := core.Restore(engine.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	state, _ := restored.State(submit.WorkflowID)
	event := start.Events[0]
	task := state.Tasks["task"]
	if task.AttemptID != event.AttemptID || task.WorkerID != event.WorkerID ||
		task.LeaseUntil != event.LeaseUntil || task.Fence != event.Fence ||
		task.Idempotency != event.Idempotency {
		t.Fatalf("active lease changed after restore: %+v", task)
	}
}

func FuzzLeaseCompletionFencing(f *testing.F) {
	f.Add(uint64(1), "worker")
	f.Add(uint64(0), "worker")
	f.Add(uint64(2), "stale")
	f.Fuzz(func(t *testing.T, fence uint64, worker string) {
		engine := core.NewEngine()
		definition := singleDefinition()
		submit, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition})
		if err != nil {
			t.Fatal(err)
		}
		start, err := engine.Handle(core.Command{
			Kind: core.CommandStart, RequestID: "start", WorkflowID: submit.WorkflowID,
			TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		event := start.Events[0]
		before, _ := engine.State(submit.WorkflowID)
		beforeHash, _ := core.StateHash(before)
		_, err = engine.Handle(core.Command{
			Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submit.WorkflowID,
			TaskID: "task", AttemptID: event.AttemptID, WorkerID: worker,
			Fence: fence, At: 2,
		})
		if fence == event.Fence && worker == event.WorkerID {
			if err != nil {
				t.Fatal(err)
			}
			return
		}
		if !errors.Is(err, core.ErrFenced) {
			t.Fatalf("invalid identity returned %v", err)
		}
		after, _ := engine.State(submit.WorkflowID)
		afterHash, _ := core.StateHash(after)
		if beforeHash != afterHash {
			t.Fatal("fenced completion mutated state")
		}
	})
}
