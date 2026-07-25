package scheduler_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
)

func TestStageBatchMatchesSequentialWorkload(t *testing.T) {
	commands := workloadCommands()
	expectedEngine, expected := sequentialPreparation(t, commands)
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	actualEngine := core.NewEngine()
	actual, err := value.StageBatch(actualEngine, commands)
	if err != nil {
		t.Fatal(err)
	}
	expectedCommands := bench.WorkloadBatchSize * bench.WorkloadCommandsPerWorkflow
	expectedTransitions := bench.WorkloadBatchSize * bench.WorkloadTransitionsPerWorkflow
	if uint64(len(commands)) != expectedCommands || uint64(len(actual.Batches)) != expectedCommands || uint64(len(actualEngine.Journal())) != expectedTransitions {
		t.Fatalf("unexpected workload batch sizes: %d %d %d", len(commands), len(actual.Batches), len(actualEngine.Journal()))
	}
	if !reflect.DeepEqual(actual, expected) || !reflect.DeepEqual(actualEngine.Snapshot(), expectedEngine.Snapshot()) || !reflect.DeepEqual(actualEngine.Journal(), expectedEngine.Journal()) || !reflect.DeepEqual(actualEngine.Metrics(), expectedEngine.Metrics()) || !reflect.DeepEqual(actualEngine.ActiveWorkflowIDs(), expectedEngine.ActiveWorkflowIDs()) {
		t.Fatal("staged workload batch differs from isolated preparation")
	}
}

func TestStageBatchPreservesInterleavedWorkflows(t *testing.T) {
	grouped := workloadCommands()
	commands := make([]core.Command, 0, len(grouped))
	for commandIndex := range int(bench.WorkloadCommandsPerWorkflow) {
		for workflowIndex := range int(bench.WorkloadBatchSize) {
			commands = append(commands, grouped[workflowIndex*int(bench.WorkloadCommandsPerWorkflow)+commandIndex])
		}
	}
	expectedEngine, expected := sequentialPreparation(t, commands)
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	actualEngine := core.NewEngine()
	actual, err := value.StageBatch(actualEngine, commands)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) || !reflect.DeepEqual(actualEngine.Snapshot(), expectedEngine.Snapshot()) || !reflect.DeepEqual(actualEngine.Journal(), expectedEngine.Journal()) || !reflect.DeepEqual(actualEngine.Metrics(), expectedEngine.Metrics()) {
		t.Fatal("interleaved staging differs from isolated preparation")
	}
}

func TestStageBatchRollsBackAdmissionAndMidCommandFailures(t *testing.T) {
	config := testConfig()
	config.Global.Workflows = 1
	config.Default.Limits.Workflows = 1
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	first := submit("first", "test", "work", "task")
	second := submit("second", "test", "work", "task")
	before := engine.Snapshot()
	if _, err := value.StageBatch(engine, []core.Command{first, second}); !errors.Is(err, scheduler.ErrBackpressure) {
		t.Fatalf("unexpected admission failure: %v", err)
	}
	if !reflect.DeepEqual(engine.Snapshot(), before) || len(engine.Journal()) != 0 || engine.Metrics().CommandsAccepted != 0 {
		t.Fatal("admission failure retained staged state")
	}
	if _, exists := engine.ResidentRequestHash(first.RequestID); exists {
		t.Fatal("admission failure retained request index")
	}
	value, err = scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}

	definition := core.WorkflowDefinition{
		Name: "partial", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	submitted, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "partial", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	started, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start-partial", WorkflowID: submitted.WorkflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := started.Events[0]
	stateBefore := engine.Snapshot()
	journalBefore := engine.Journal()
	metricsBefore := engine.Metrics()
	temporary := submit("temporary", "other", "work", "task")
	invalidComplete := core.Command{
		Kind: core.CommandComplete, RequestID: "invalid-complete", WorkflowID: submitted.WorkflowID,
		TaskID: "task", AttemptID: lease.AttemptID, WorkerID: lease.WorkerID, Fence: lease.Fence, At: 2,
		Fanout: []core.FanoutItem{{Key: "unexpected"}},
	}
	if _, err := value.StageBatch(engine, []core.Command{temporary, invalidComplete}); !errors.Is(err, core.ErrInvalidCommand) {
		t.Fatalf("unexpected mid-command failure: %v", err)
	}
	if !reflect.DeepEqual(engine.Snapshot(), stateBefore) || !reflect.DeepEqual(engine.Journal(), journalBefore) || !reflect.DeepEqual(engine.Metrics(), metricsBefore) {
		t.Fatal("mid-command failure retained staged state")
	}
	if _, exists := engine.ResidentRequestHash(temporary.RequestID); exists {
		t.Fatal("mid-command failure retained earlier request")
	}
	state, exists := engine.State(submitted.WorkflowID)
	if !exists || state.Tasks["task"].Status != core.TaskRunning {
		t.Fatalf("mid-command failure mutated original task: %+v", state)
	}
}

func TestStageBatchPreservesIsolationDuplicatesAndConflicts(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	definition := core.WorkflowDefinition{
		Name: "workflow", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	submitCommand := core.Command{Kind: core.CommandSubmit, RequestID: "submit", Definition: &definition}
	workflowID := core.WorkflowID("test", "submit")
	start := core.Command{Kind: core.CommandStart, RequestID: "start", WorkflowID: workflowID, TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100}
	complete := core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: workflowID, TaskID: "task",
		AttemptID: core.AttemptID(workflowID, "task", 1), WorkerID: "worker", Fence: 1, At: 2, Output: map[string]string{"value": "done"},
	}
	engine := core.NewEngine()
	prepared, err := value.StageBatch(engine, []core.Command{submitCommand, start, complete})
	if err != nil {
		t.Fatal(err)
	}
	definition.Tasks[0].ID = "mutated"
	complete.Output["value"] = "input-mutated"
	prepared.Batches[0].Events[0].Definition.Tasks[0].ID = "batch-mutated"
	prepared.Batches[2].Events[0].Output["value"] = "batch-mutated"
	state, exists := engine.State(workflowID)
	if !exists || state.Tasks["task"].Output["value"] != "done" || state.Tasks["task"].Definition.ID != "task" {
		t.Fatalf("staging aliases caller data: %+v", state)
	}
	retry := core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: workflowID, TaskID: "task",
		AttemptID: core.AttemptID(workflowID, "task", 1), WorkerID: "worker", Fence: 1, At: 2, Output: map[string]string{"value": "done"},
	}
	_, result, duplicate, err := engine.PrepareAndApply(retry)
	if err != nil || !duplicate || result.Events[0].Output["value"] != "done" {
		t.Fatalf("stored result changed: %+v %t %v", result, duplicate, err)
	}
	result.Events[0].Output["value"] = "result-mutated"
	_, result, duplicate, err = engine.PrepareAndApply(retry)
	if err != nil || !duplicate || result.Events[0].Output["value"] != "done" {
		t.Fatalf("duplicate result aliases storage: %+v %t %v", result, duplicate, err)
	}

	exact := submit("same", "duplicate", "work", "task")
	duplicateEngine := core.NewEngine()
	duplicateBatch, err := value.StageBatch(duplicateEngine, []core.Command{exact, exact})
	if err != nil {
		t.Fatal(err)
	}
	immediate, exists := duplicateBatch.Immediate[exact.RequestID]
	if len(duplicateBatch.Batches) != 1 || len(duplicateBatch.Hashes) != 2 || duplicateBatch.Hashes[0] != duplicateBatch.Hashes[1] || !exists || !immediate.Duplicate || duplicateEngine.Metrics().CommandsAccepted != 1 {
		t.Fatalf("unexpected in-stage duplicate result: %+v", duplicateBatch)
	}

	conflictEngine := core.NewEngine()
	conflict := submit("same", "conflict", "work", "task")
	if _, err := value.StageBatch(conflictEngine, []core.Command{exact, conflict}); !errors.Is(err, core.ErrRequestConflict) {
		t.Fatalf("unexpected conflict result: %v", err)
	}
	if len(conflictEngine.WorkflowIDs()) != 0 || len(conflictEngine.Journal()) != 0 || conflictEngine.Metrics().CommandsAccepted != 0 {
		t.Fatal("request conflict retained transaction state")
	}
}

func BenchmarkWorkloadBatchPreparation(b *testing.B) {
	commands := workloadCommands()
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Run("isolated_per_command", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			engine := core.NewEngine()
			for _, command := range commands {
				if _, _, duplicate, err := engine.PrepareAndApply(command); err != nil || duplicate {
					b.Fatalf("prepare failed: %v %t", err, duplicate)
				}
			}
		}
	})
	b.Run("staging_transaction", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			engine := core.NewEngine()
			transaction := engine.BeginStaging()
			for _, command := range commands {
				if _, _, duplicate, err := transaction.PrepareAndApply(command); err != nil || duplicate {
					b.Fatalf("prepare failed: %v %t", err, duplicate)
				}
			}
			transaction.Commit()
		}
	})
	b.Run("scheduler_stage", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			engine := core.NewEngine()
			if _, err := value.StageBatch(engine, commands); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func workloadCommands() []core.Command {
	return bench.WorkloadBatch(0, bench.WorkloadBatchSize)
}

func sequentialPreparation(t *testing.T, commands []core.Command) (*core.Engine, scheduler.PreparedBatch) {
	t.Helper()
	engine := core.NewEngine()
	prepared := scheduler.PreparedBatch{Batches: make([]core.CommandBatch, 0, len(commands)), Immediate: make(map[string]core.Result), Hashes: make([]string, len(commands))}
	for index, command := range commands {
		batch, result, duplicate, err := engine.PrepareAndApply(command)
		if err != nil {
			t.Fatalf("command %d: %v", index, err)
		}
		prepared.Hashes[index] = batch.RequestHash
		if duplicate {
			prepared.Immediate[command.RequestID] = result
			continue
		}
		prepared.Batches = append(prepared.Batches, batch)
	}
	return engine, prepared
}
