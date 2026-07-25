package bench_test

import (
	"testing"

	"github.com/HaikIsaiants/chronos/internal/bench"
	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestEngineBenchmarkCounters(t *testing.T) {
	if err := core.ValidateDefinition(pointer(bench.WorkloadDefinition())); err != nil {
		t.Fatal(err)
	}
	report, err := bench.Run(5)
	if err != nil {
		t.Fatal(err)
	}
	if report.Workflows != 5 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Commands != 5*bench.WorkloadCommandsPerWorkflow || report.Transitions != 5*bench.WorkloadTransitionsPerWorkflow {
		t.Fatalf("unexpected totals: %+v", report)
	}
	expected := map[core.EventKind]uint64{
		core.EventWorkflowSubmitted: 5,
		core.EventTaskReady:         40,
		core.EventTaskStarted:       40,
		core.EventTaskCompleted:     40,
		core.EventWorkflowCompleted: 5,
	}
	for kind, count := range expected {
		if report.TransitionsByKind[kind] != count {
			t.Fatalf("unexpected %s count: %d", kind, report.TransitionsByKind[kind])
		}
	}
	if report.DurationNanos <= 0 || report.TransitionsPerSecond <= 0 {
		t.Fatal("benchmark did not record timing")
	}
}

func TestWorkloadBatch(t *testing.T) {
	commands := bench.WorkloadBatch(4, 2)
	if uint64(len(commands)) != 2*bench.WorkloadCommandsPerWorkflow || commands[0].RequestID != "bench-4-submit" || commands[len(commands)-1].RequestID != "bench-5-complete-t7" {
		t.Fatalf("unexpected workload batch: %d %s %s", len(commands), commands[0].RequestID, commands[len(commands)-1].RequestID)
	}
	engine := core.NewEngine()
	for _, command := range commands {
		if _, err := engine.Handle(command); err != nil {
			t.Fatal(err)
		}
	}
	if engine.Metrics().Transitions != 2*bench.WorkloadTransitionsPerWorkflow {
		t.Fatalf("unexpected transition count: %d", engine.Metrics().Transitions)
	}
}

func TestEngineBenchmarkRejectsInvalidCount(t *testing.T) {
	if _, err := bench.Run(0); err == nil {
		t.Fatal("zero workflow benchmark accepted")
	}
}

func pointer(definition core.WorkflowDefinition) *core.WorkflowDefinition {
	return &definition
}
