package core

import (
	"fmt"
	"sort"
	"strings"
)

func ValidateDefinition(definition *WorkflowDefinition) error {
	if definition == nil {
		return fmt.Errorf("%w: definition is required", ErrInvalidDefinition)
	}
	if strings.TrimSpace(definition.Name) == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidDefinition)
	}
	if strings.TrimSpace(definition.Namespace) == "" {
		return fmt.Errorf("%w: namespace is required", ErrInvalidDefinition)
	}
	if definition.StartAt < 0 {
		return fmt.Errorf("%w: start time cannot be negative", ErrInvalidDefinition)
	}
	if len(definition.Tasks) == 0 {
		return fmt.Errorf("%w: at least one task is required", ErrInvalidDefinition)
	}
	tasks := make(map[string]TaskDefinition, len(definition.Tasks))
	for _, task := range definition.Tasks {
		if strings.TrimSpace(task.ID) == "" {
			return fmt.Errorf("%w: task id is required", ErrInvalidDefinition)
		}
		if strings.TrimSpace(task.Type) == "" {
			return fmt.Errorf("%w: task %s type is required", ErrInvalidDefinition, task.ID)
		}
		if _, exists := tasks[task.ID]; exists {
			return fmt.Errorf("%w: duplicate task %s", ErrInvalidDefinition, task.ID)
		}
		if err := validateRetry(task.ID, task.Retry, task.TimeoutMillis); err != nil {
			return err
		}
		if task.Compensation != nil && strings.TrimSpace(task.Compensation.TaskType) == "" {
			return fmt.Errorf("%w: task %s compensation type is required", ErrInvalidDefinition, task.ID)
		}
		if task.Fanout != nil {
			fanout := task.Fanout
			if strings.TrimSpace(fanout.TaskType) == "" || strings.TrimSpace(fanout.AggregateTaskID) == "" {
				return fmt.Errorf("%w: task %s fanout is incomplete", ErrInvalidDefinition, task.ID)
			}
			if fanout.MaxItems == 0 || fanout.MaxItems > MaxFanoutItems {
				return fmt.Errorf("%w: task %s fanout maximum is invalid", ErrInvalidDefinition, task.ID)
			}
			if err := validateRetry(task.ID+" fanout", fanout.Retry, fanout.TimeoutMillis); err != nil {
				return err
			}
			if fanout.Compensation != nil && strings.TrimSpace(fanout.Compensation.TaskType) == "" {
				return fmt.Errorf("%w: task %s fanout compensation type is required", ErrInvalidDefinition, task.ID)
			}
		}
		tasks[task.ID] = task
	}
	for _, task := range definition.Tasks {
		seen := make(map[string]struct{}, len(task.Dependencies))
		for _, dependency := range task.Dependencies {
			if dependency == task.ID {
				return fmt.Errorf("%w: task %s depends on itself", ErrInvalidDefinition, task.ID)
			}
			if _, exists := tasks[dependency]; !exists {
				return fmt.Errorf("%w: task %s depends on missing task %s", ErrInvalidDefinition, task.ID, dependency)
			}
			if _, exists := seen[dependency]; exists {
				return fmt.Errorf("%w: task %s repeats dependency %s", ErrInvalidDefinition, task.ID, dependency)
			}
			seen[dependency] = struct{}{}
		}
		if task.Fanout != nil {
			aggregate, exists := tasks[task.Fanout.AggregateTaskID]
			if !exists || aggregate.ID == task.ID || !containsString(aggregate.Dependencies, task.ID) {
				return fmt.Errorf("%w: task %s fanout aggregate must depend on its parent", ErrInvalidDefinition, task.ID)
			}
		}
	}
	if hasCycle(tasks) {
		return fmt.Errorf("%w: dependency graph contains a cycle", ErrInvalidDefinition)
	}
	return nil
}

func validateRetry(taskID string, retry RetryPolicy, timeout int64) error {
	if retry.MaxAttempts < 1 {
		return fmt.Errorf("%w: task %s max attempts must be positive", ErrInvalidDefinition, taskID)
	}
	if retry.InitialBackoffMillis < 0 || retry.MaxBackoffMillis < 0 || timeout < 0 {
		return fmt.Errorf("%w: task %s durations cannot be negative", ErrInvalidDefinition, taskID)
	}
	if retry.BackoffMultiplier < 1 {
		return fmt.Errorf("%w: task %s backoff multiplier must be positive", ErrInvalidDefinition, taskID)
	}
	if retry.MaxBackoffMillis > 0 && retry.InitialBackoffMillis > retry.MaxBackoffMillis {
		return fmt.Errorf("%w: task %s initial backoff exceeds maximum", ErrInvalidDefinition, taskID)
	}
	return nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasCycle(tasks map[string]TaskDefinition) bool {
	indegree := make(map[string]int, len(tasks))
	dependents := make(map[string][]string, len(tasks))
	for id, task := range tasks {
		indegree[id] = len(task.Dependencies)
		for _, dependency := range task.Dependencies {
			dependents[dependency] = append(dependents[dependency], id)
		}
	}
	ready := make([]string, 0, len(tasks))
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	visited := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		visited++
		for _, dependent := range dependents[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	return visited != len(tasks)
}
