package bench

import (
	"fmt"
	"runtime"
	"time"

	"github.com/HaikIsaiants/chronos/internal/core"
)

const WorkloadCommandsPerWorkflow uint64 = 17
const WorkloadTransitionsPerWorkflow uint64 = 26
const WorkloadBatchSize uint64 = 15

type Report struct {
	GoVersion            string                    `json:"go_version"`
	OS                   string                    `json:"os"`
	Architecture         string                    `json:"architecture"`
	LogicalCPUs          int                       `json:"logical_cpus"`
	Workflows            int                       `json:"workflows"`
	Commands             uint64                    `json:"commands"`
	Transitions          uint64                    `json:"transitions"`
	TransitionsByKind    map[core.EventKind]uint64 `json:"transitions_by_kind"`
	DurationNanos        int64                     `json:"duration_nanos"`
	TransitionsPerSecond float64                   `json:"transitions_per_second"`
}

func Run(workflows int) (Report, error) {
	if workflows < 1 {
		return Report{}, fmt.Errorf("workflows must be positive")
	}
	engine := core.NewEngine()
	started := time.Now()
	for index := 0; index < workflows; index++ {
		_, commands := WorkloadCommands(uint64(index))
		for _, command := range commands {
			if _, err := engine.Handle(command); err != nil {
				return Report{}, err
			}
		}
	}
	duration := time.Since(started)
	metrics := engine.Metrics()
	return Report{
		GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH,
		LogicalCPUs: runtime.NumCPU(), Workflows: workflows,
		Commands: metrics.CommandsAccepted, Transitions: metrics.Transitions,
		TransitionsByKind: metrics.TransitionsByKind, DurationNanos: duration.Nanoseconds(),
		TransitionsPerSecond: float64(metrics.Transitions) / duration.Seconds(),
	}, nil
}

func WorkloadDefinition() core.WorkflowDefinition {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	return core.WorkflowDefinition{
		Name: "benchmark-diamond", Namespace: "benchmark",
		Tasks: []core.TaskDefinition{
			{ID: "t0", Type: "noop", Retry: retry},
			{ID: "t1", Type: "noop", Dependencies: []string{"t0"}, Retry: retry},
			{ID: "t2", Type: "noop", Dependencies: []string{"t0"}, Retry: retry},
			{ID: "t3", Type: "noop", Dependencies: []string{"t0"}, Retry: retry},
			{ID: "t4", Type: "noop", Dependencies: []string{"t1", "t2"}, Retry: retry},
			{ID: "t5", Type: "noop", Dependencies: []string{"t3"}, Retry: retry},
			{ID: "t6", Type: "noop", Dependencies: []string{"t4", "t5"}, Retry: retry},
			{ID: "t7", Type: "noop", Dependencies: []string{"t6"}, Retry: retry},
		},
	}
}

func WorkloadCommands(index uint64) (string, []core.Command) {
	definition := WorkloadDefinition()
	prefix := fmt.Sprintf("bench-%d", index)
	workflowID := core.WorkflowID(definition.Namespace, prefix+"-submit")
	// Use a separate logical time range per workflow
	at := func(ordinal uint64) int64 {
		return int64(index*WorkloadCommandsPerWorkflow + ordinal)
	}
	commands := make([]core.Command, 0, WorkloadCommandsPerWorkflow)
	commands = append(commands, core.Command{
		Kind: core.CommandSubmit, RequestID: prefix + "-submit", At: at(0), Definition: &definition,
	})
	ordinal := uint64(1)
	for _, task := range definition.Tasks {
		attemptID := core.AttemptID(workflowID, task.ID, 1)
		commands = append(commands, core.Command{
			Kind: core.CommandStart, RequestID: prefix + "-start-" + task.ID,
			WorkflowID: workflowID, TaskID: task.ID, AttemptID: attemptID,
			WorkerID: "benchmark", At: at(ordinal), LeaseUntil: at(ordinal) + 1_000_000_000_000,
		})
		ordinal++
		commands = append(commands, core.Command{
			Kind: core.CommandComplete, RequestID: prefix + "-complete-" + task.ID,
			WorkflowID: workflowID, TaskID: task.ID, AttemptID: attemptID,
			WorkerID: "benchmark", Fence: 1, At: at(ordinal), Output: map[string]string{"value": task.ID},
		})
		ordinal++
	}
	return workflowID, commands
}

func WorkloadBatch(start, count uint64) []core.Command {
	commands := make([]core.Command, 0, count*WorkloadCommandsPerWorkflow)
	for index := start; index < start+count; index++ {
		_, workflow := WorkloadCommands(index)
		commands = append(commands, workflow...)
	}
	return commands
}
