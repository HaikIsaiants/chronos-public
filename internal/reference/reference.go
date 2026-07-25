package reference

import (
	"fmt"
	"sort"

	"github.com/HaikIsaiants/chronos/internal/core"
)

type Result struct {
	Status  core.WorkflowStatus          `json:"status"`
	Order   []string                     `json:"order"`
	Outputs map[string]map[string]string `json:"outputs"`
}

func Run(definition core.WorkflowDefinition, outputs map[string]map[string]string) (Result, error) {
	tasks := make(map[string]core.TaskDefinition, len(definition.Tasks))
	for _, task := range definition.Tasks {
		if task.ID == "" {
			return Result{}, fmt.Errorf("task id is required")
		}
		if _, exists := tasks[task.ID]; exists {
			return Result{}, fmt.Errorf("duplicate task %s", task.ID)
		}
		tasks[task.ID] = task
	}
	for _, task := range definition.Tasks {
		for _, dependency := range task.Dependencies {
			if _, exists := tasks[dependency]; !exists {
				return Result{}, fmt.Errorf("task %s depends on missing task %s", task.ID, dependency)
			}
		}
	}
	completed := make(map[string]bool, len(tasks))
	result := Result{Status: core.WorkflowRunning, Order: []string{}, Outputs: make(map[string]map[string]string, len(tasks))}
	for len(completed) < len(tasks) {
		ready := make([]string, 0)
		for id, task := range tasks {
			if completed[id] {
				continue
			}
			eligible := true
			for _, dependency := range task.Dependencies {
				if !completed[dependency] {
					eligible = false
					break
				}
			}
			if eligible {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			return Result{}, fmt.Errorf("dependency graph cannot progress")
		}
		sort.Strings(ready)
		for _, id := range ready {
			completed[id] = true
			result.Order = append(result.Order, id)
			result.Outputs[id] = copyMap(outputs[id])
		}
	}
	result.Status = core.WorkflowCompleted
	return result, nil
}

func copyMap(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
