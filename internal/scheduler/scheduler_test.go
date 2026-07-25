package scheduler_test

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/HaikIsaiants/chronos/internal/scheduler"
)

func TestAdmissionBoundsAndBatchAtomicity(t *testing.T) {
	config := testConfig()
	config.Global.Workflows = 2
	config.Global.Tasks = 3
	config.Global.Running = 3
	config.Default.Limits.Workflows = 1
	config.Default.Limits.Tasks = 2
	config.Default.Limits.Running = 2
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	first := submit("first", "a", "work", "one", "two")
	if err := value.CheckBatch(engine, []core.Command{first}); err != nil {
		t.Fatal(err)
	}
	if len(engine.ActiveWorkflowIDs()) != 0 {
		t.Fatal("admission check retained a speculative workflow")
	}
	if _, err := engine.Handle(first); err != nil {
		t.Fatal(err)
	}
	namespaceFull := submit("second", "a", "work", "three")
	if err := value.CheckBatch(engine, []core.Command{namespaceFull}); !errors.Is(err, scheduler.ErrBackpressure) {
		t.Fatalf("namespace limit was not enforced: %v", err)
	}
	batch := []core.Command{
		submit("second", "b", "work", "three"),
		submit("third", "c", "work", "four"),
	}
	if err := value.CheckBatch(engine, batch); !errors.Is(err, scheduler.ErrBackpressure) {
		t.Fatalf("combined batch limit was not enforced: %v", err)
	}
	if len(engine.WorkflowIDs()) != 1 || len(engine.ActiveWorkflowIDs()) != 1 {
		t.Fatal("admission check mutated engine")
	}
}

func TestPrepareBatchReturnsExactPlanAndRollsBack(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	submitCommand := submit("submit", "test", "work", "task")
	workflowID := core.WorkflowID("test", "submit")
	start := core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: workflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	}
	prepared, err := value.PrepareBatch(engine, []core.Command{submitCommand, start})
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Batches) != 2 || len(prepared.Immediate) != 0 || len(prepared.Hashes) != 2 {
		t.Fatalf("unexpected prepared batch: %+v", prepared)
	}
	for index, batch := range prepared.Batches {
		if prepared.Hashes[index] != batch.RequestHash {
			t.Fatalf("hash %d did not match prepared batch", index)
		}
	}
	if len(engine.WorkflowIDs()) != 0 || len(engine.ActiveWorkflowIDs()) != 0 {
		t.Fatal("prepared batch mutated engine")
	}
	replica := core.NewEngine()
	for _, batch := range prepared.Batches {
		if _, err := replica.ApplyBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	state, exists := replica.State(workflowID)
	if !exists || state.Tasks["task"].Status != core.TaskRunning {
		t.Fatalf("prepared batch did not reproduce state: %+v", state)
	}
	if _, err := engine.Handle(submitCommand); err != nil {
		t.Fatal(err)
	}
	next := submit("next", "test", "work", "task")
	duplicate, err := value.PrepareBatch(engine, []core.Command{submitCommand, next})
	if err != nil {
		t.Fatal(err)
	}
	result, exists := duplicate.Immediate[submitCommand.RequestID]
	firstHash, _ := core.HashCommand(submitCommand)
	secondHash, _ := core.HashCommand(next)
	if len(duplicate.Batches) != 1 || duplicate.Batches[0].RequestID != next.RequestID || len(duplicate.Hashes) != 2 || duplicate.Hashes[0] != firstHash || duplicate.Hashes[1] != secondHash || !exists || !result.Duplicate {
		t.Fatalf("duplicate preparation mismatch: %+v", duplicate)
	}
	if len(engine.WorkflowIDs()) != 1 {
		t.Fatal("mixed preparation mutated engine")
	}
}

func TestStageBatchRetainsSuccessAndRollsBackFailure(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	submitCommand := submit("submit", "test", "work", "task")
	workflowID := core.WorkflowID("test", "submit")
	start := core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: workflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	}
	prepared, err := value.StageBatch(engine, []core.Command{submitCommand, start})
	if err != nil || len(prepared.Batches) != 2 {
		t.Fatalf("stage failed: %+v %v", prepared, err)
	}
	state, exists := engine.State(workflowID)
	if !exists || state.Tasks["task"].Status != core.TaskRunning {
		t.Fatalf("stage was not retained: %+v", state)
	}
	before := engine.Snapshot()
	invalid := []core.Command{
		submit("temporary", "test", "work", "task"),
		{Kind: core.CommandStart, RequestID: "missing", WorkflowID: "missing", TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100},
	}
	if _, err := value.StageBatch(engine, invalid); !errors.Is(err, core.ErrWorkflowNotFound) {
		t.Fatalf("invalid stage returned %v", err)
	}
	if !reflect.DeepEqual(before, engine.Snapshot()) {
		t.Fatal("failed stage was retained")
	}
}

func TestRejectsOverflowingWeights(t *testing.T) {
	config := testConfig()
	config.Default.Weight = math.MaxInt
	if _, err := scheduler.New(config); err == nil {
		t.Fatal("overflowing weight was accepted")
	}
}

func TestWeightedFairnessUnderContinuousOverload(t *testing.T) {
	config := testConfig()
	config.Namespaces = map[string]scheduler.NamespaceConfig{}
	for namespace, weight := range map[string]int{"a": 1, "b": 2, "c": 4} {
		value := config.Default
		value.Weight = weight
		config.Namespaces[namespace] = value
	}
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	for _, namespace := range []string{"a", "b", "c"} {
		if _, err := engine.Handle(submit(namespace, namespace, "work", "task")); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[string]int{}
	for range 70000 {
		plan, exists := value.Plan(engine, []string{"work"})
		if !exists {
			t.Fatal("backlogged scheduler returned no work")
		}
		counts[plan.Task.Namespace]++
		if err := value.Commit(plan); err != nil {
			t.Fatal(err)
		}
	}
	if counts["a"] != 10000 || counts["b"] != 20000 || counts["c"] != 40000 {
		t.Fatalf("unexpected weighted shares: %v", counts)
	}
}

func TestCapabilityStealingAndRunningLimit(t *testing.T) {
	config := testConfig()
	config.Default.Limits.Running = 1
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	alpha := submit("alpha", "a", "alpha", "task")
	beta := submit("beta", "a", "beta", "task")
	alphaResult, err := engine.Handle(alpha)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(beta); err != nil {
		t.Fatal(err)
	}
	plan, exists := value.Plan(engine, []string{"beta"})
	if !exists || plan.Task.Capability != "beta" {
		t.Fatalf("compatible work was not selected: %+v", plan)
	}
	if _, exists := value.Plan(engine, []string{"missing"}); exists {
		t.Fatal("incompatible work was selected")
	}
	start := core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: alphaResult.WorkflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	}
	if err := value.CheckBatch(engine, []core.Command{start}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Handle(start); err != nil {
		t.Fatal(err)
	}
	if _, exists := value.Plan(engine, []string{"beta"}); exists {
		t.Fatal("namespace running limit was exceeded")
	}
	if err := value.CheckBatch(engine, []core.Command{
		{
			Kind: core.CommandStart, RequestID: "start-beta", WorkflowID: core.WorkflowID("a", "beta"),
			TaskID: "task", WorkerID: "worker", At: 2, LeaseUntil: 100,
		},
	}); !errors.Is(err, scheduler.ErrBackpressure) {
		t.Fatalf("manual start bypassed running limit: %v", err)
	}
}

func TestFanoutReservationAndExpansionAccounting(t *testing.T) {
	config := testConfig()
	config.Global.Tasks = 4
	config.Global.Running = 4
	config.Default.Limits.Tasks = 4
	config.Default.Limits.Running = 4
	value, err := scheduler.New(config)
	if err != nil {
		t.Fatal(err)
	}
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{
		Name: "fanout", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "plan", Type: "plan", Retry: retry, Fanout: &core.FanoutDefinition{
				TaskType: "work", AggregateTaskID: "join", MaxItems: 2, Retry: retry,
			}},
			{ID: "join", Type: "join", Dependencies: []string{"plan"}, Retry: retry},
		},
	}
	submit := core.Command{Kind: core.CommandSubmit, RequestID: "fanout", Definition: &definition}
	if err := value.CheckBatch(core.NewEngine(), []core.Command{submit}); err != nil {
		t.Fatal(err)
	}
	definition.Tasks[0].Fanout.MaxItems = 3
	if err := value.CheckBatch(core.NewEngine(), []core.Command{submit}); !errors.Is(err, scheduler.ErrBackpressure) {
		t.Fatalf("fanout reservation bypassed task limit: %v", err)
	}
	definition.Tasks[0].Fanout.MaxItems = 2
	engine := core.NewEngine()
	submitted, err := engine.Handle(submit)
	if err != nil {
		t.Fatal(err)
	}
	if stats := value.Stats(engine); stats.Tasks != 4 {
		t.Fatalf("reserved %d tasks", stats.Tasks)
	}
	started, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start-plan", WorkflowID: submitted.WorkflowID,
		TaskID: "plan", WorkerID: "worker", At: 1, LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := started.Events[0]
	_, err = engine.Handle(core.Command{
		Kind: core.CommandComplete, RequestID: "complete-plan", WorkflowID: submitted.WorkflowID,
		TaskID: "plan", AttemptID: lease.AttemptID, WorkerID: lease.WorkerID, Fence: lease.Fence, At: 2,
		Fanout: []core.FanoutItem{{Key: "one", Payload: map[string]string{"target": "one"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats := value.Stats(engine); stats.Tasks != 2 || stats.Ready != 1 {
		t.Fatalf("expanded usage mismatch: %+v", stats)
	}
}

func TestCompensationCapabilityAndRunningAccounting(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	definition := core.WorkflowDefinition{
		Name: "compensate", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "deploy", Type: "deploy", Retry: retry, Compensation: &core.CompensationDefinition{TaskType: "rollback"}},
			{ID: "verify", Type: "verify", Dependencies: []string{"deploy"}, Retry: retry},
		},
	}
	engine := core.NewEngine()
	submitted, err := engine.Handle(core.Command{Kind: core.CommandSubmit, RequestID: "compensate", Definition: &definition})
	if err != nil {
		t.Fatal(err)
	}
	completeTask(t, engine, submitted.WorkflowID, "deploy", "deploy")
	started, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start-verify", WorkflowID: submitted.WorkflowID,
		TaskID: "verify", WorkerID: "worker", At: 3, LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := started.Events[0]
	_, err = engine.Handle(core.Command{
		Kind: core.CommandFail, RequestID: "fail-verify", WorkflowID: submitted.WorkflowID,
		TaskID: "verify", AttemptID: lease.AttemptID, WorkerID: lease.WorkerID,
		Fence: lease.Fence, Error: "failed", At: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := value.Plan(engine, []string{"deploy"}); exists {
		t.Fatal("forward capability selected compensation")
	}
	plan, exists := value.Plan(engine, []string{"rollback"})
	if !exists || plan.Task.TaskID != "deploy" || plan.Task.Capability != "rollback" {
		t.Fatalf("compensation was not scheduled: %+v", plan)
	}
	if _, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start-rollback", WorkflowID: submitted.WorkflowID,
		TaskID: "deploy", WorkerID: "rollback-worker", At: 5, LeaseUntil: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if stats := value.Stats(engine); stats.Running != 1 {
		t.Fatalf("compensation running usage mismatch: %+v", stats)
	}
}

func TestCheckBatchRestoresActiveWorkflowIndex(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	submitted, err := engine.Handle(submit("workflow", "test", "work", "task"))
	if err != nil {
		t.Fatal(err)
	}
	start := core.Command{
		Kind: core.CommandStart, RequestID: "start", WorkflowID: submitted.WorkflowID,
		TaskID: "task", WorkerID: "worker", At: 1, LeaseUntil: 100,
	}
	complete := core.Command{
		Kind: core.CommandComplete, RequestID: "complete", WorkflowID: submitted.WorkflowID,
		TaskID: "task", AttemptID: core.AttemptID(submitted.WorkflowID, "task", 1), WorkerID: "worker", Fence: 1, At: 2,
	}
	if err := value.CheckBatch(engine, []core.Command{start, complete}); err != nil {
		t.Fatal(err)
	}
	ids := engine.ActiveWorkflowIDs()
	stats := value.Stats(engine)
	if len(ids) != 1 || ids[0] != submitted.WorkflowID || stats.Workflows != 1 || stats.Tasks != 1 || stats.Ready != 1 || stats.Running != 0 {
		t.Fatalf("speculative batch changed scheduler state: %v %+v", ids, stats)
	}
}

func TestStatsExcludeTerminalWorkflows(t *testing.T) {
	value, err := scheduler.New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	engine := core.NewEngine()
	if _, err := engine.Handle(submit("active", "active", "work", "task")); err != nil {
		t.Fatal(err)
	}
	terminal, err := engine.Handle(submit("terminal", "terminal", "work", "task"))
	if err != nil {
		t.Fatal(err)
	}
	completeTask(t, engine, terminal.WorkflowID, "task", "terminal")
	stats := value.Stats(engine)
	if stats.Workflows != 1 || stats.Tasks != 1 || stats.Ready != 1 || stats.Running != 0 || len(stats.Namespaces) != 1 {
		t.Fatalf("terminal workflow was included in scheduler stats: %+v", stats)
	}
	if stats.Namespaces[0].Namespace != "active" || stats.Namespaces[0].Workflows != 1 || stats.Namespaces[0].Tasks != 1 || stats.Namespaces[0].Ready != 1 {
		t.Fatalf("active namespace stats differ: %+v", stats.Namespaces[0])
	}
}

func completeTask(t *testing.T, engine *core.Engine, workflowID, taskID, request string) {
	t.Helper()
	started, err := engine.Handle(core.Command{
		Kind: core.CommandStart, RequestID: "start-" + request, WorkflowID: workflowID,
		TaskID: taskID, WorkerID: "worker", At: 1, LeaseUntil: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := started.Events[0]
	if _, err := engine.Handle(core.Command{
		Kind: core.CommandComplete, RequestID: "complete-" + request, WorkflowID: workflowID,
		TaskID: taskID, AttemptID: lease.AttemptID, WorkerID: lease.WorkerID,
		Fence: lease.Fence, At: 2, Output: map[string]string{"value": taskID},
	}); err != nil {
		t.Fatal(err)
	}
}

func testConfig() scheduler.Config {
	return scheduler.Config{
		Global: scheduler.Limits{Workflows: 100, Tasks: 100, Running: 100},
		Default: scheduler.NamespaceConfig{
			Weight: 1, Limits: scheduler.Limits{Workflows: 100, Tasks: 100, Running: 100},
		},
	}
}

func submit(requestID, namespace, taskType string, taskIDs ...string) core.Command {
	tasks := make([]core.TaskDefinition, len(taskIDs))
	for index, taskID := range taskIDs {
		tasks[index] = core.TaskDefinition{
			ID: taskID, Type: taskType,
			Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1},
		}
	}
	definition := core.WorkflowDefinition{Name: requestID, Namespace: namespace, Tasks: tasks}
	return core.Command{Kind: core.CommandSubmit, RequestID: requestID, Definition: &definition}
}

func FuzzWeightedSelection(f *testing.F) {
	f.Add(uint8(1), uint8(2), uint8(4))
	f.Add(uint8(255), uint8(0), uint8(17))
	f.Fuzz(func(t *testing.T, first, second, third uint8) {
		weights := []int{int(first%8) + 1, int(second%8) + 1, int(third%8) + 1}
		config := testConfig()
		config.Namespaces = map[string]scheduler.NamespaceConfig{}
		for index, namespace := range []string{"a", "b", "c"} {
			value := config.Default
			value.Weight = weights[index]
			config.Namespaces[namespace] = value
		}
		value, err := scheduler.New(config)
		if err != nil {
			t.Fatal(err)
		}
		engine := core.NewEngine()
		for _, namespace := range []string{"a", "b", "c"} {
			if _, err := engine.Handle(submit(namespace, namespace, "work", "task")); err != nil {
				t.Fatal(err)
			}
		}
		counts := map[string]int{}
		total := weights[0] + weights[1] + weights[2]
		for range total * 10 {
			plan, exists := value.Plan(engine, []string{"work"})
			if !exists {
				t.Fatal("scheduler returned no work")
			}
			counts[plan.Task.Namespace]++
			if err := value.Commit(plan); err != nil {
				t.Fatal(err)
			}
		}
		for index, namespace := range []string{"a", "b", "c"} {
			if counts[namespace] != weights[index]*10 {
				t.Fatalf("unexpected shares: %v", counts)
			}
		}
	})
}
