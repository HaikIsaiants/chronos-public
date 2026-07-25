package core_test

import (
	"errors"
	"math"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestVirtualClock(t *testing.T) {
	clock, err := core.NewVirtualClock(10)
	if err != nil {
		t.Fatal(err)
	}
	if clock.Now() != 10 {
		t.Fatalf("unexpected start: %d", clock.Now())
	}
	if err := clock.Advance(5); err != nil || clock.Now() != 15 {
		t.Fatalf("advance failed: %v %d", err, clock.Now())
	}
	if err := clock.Set(20); err != nil || clock.Now() != 20 {
		t.Fatalf("set failed: %v %d", err, clock.Now())
	}
	if !errors.Is(clock.Advance(-1), core.ErrTimeRegression) || clock.Now() != 20 {
		t.Fatal("negative advance changed time")
	}
	if !errors.Is(clock.Set(19), core.ErrTimeRegression) || clock.Now() != 20 {
		t.Fatal("backward set changed time")
	}
	if _, err := core.NewVirtualClock(-1); !errors.Is(err, core.ErrTimeRegression) {
		t.Fatalf("negative clock accepted: %v", err)
	}
	maxClock, err := core.NewVirtualClock(math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(maxClock.Advance(1), core.ErrTimeOverflow) || maxClock.Now() != math.MaxInt64 {
		t.Fatal("overflow changed time")
	}
}

func TestDeterministicIDs(t *testing.T) {
	workflow := core.WorkflowID("test", "request")
	if workflow != core.WorkflowID("test", "request") {
		t.Fatal("workflow id is not deterministic")
	}
	if workflow == core.WorkflowID("test", "other") {
		t.Fatal("workflow id collision")
	}
	attempt := core.AttemptID(workflow, "task", 1)
	if attempt != core.AttemptID(workflow, "task", 1) || attempt == core.AttemptID(workflow, "task", 2) {
		t.Fatal("attempt id is not deterministic and unique")
	}
	timer := core.TimerID(workflow, "task", "retry", 1)
	if timer != core.TimerID(workflow, "task", "retry", 1) || timer == core.TimerID(workflow, "task", "timeout", 1) {
		t.Fatal("timer id is not deterministic and unique")
	}
	event := core.EventID(workflow, 1, core.EventWorkflowSubmitted)
	if event != core.EventID(workflow, 1, core.EventWorkflowSubmitted) || event == core.EventID(workflow, 2, core.EventWorkflowSubmitted) {
		t.Fatal("event id is not deterministic and unique")
	}
}
