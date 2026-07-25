package core_test

import (
	"errors"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
)

func TestValidateDefinition(t *testing.T) {
	retry := core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}
	valid := core.WorkflowDefinition{
		Name: "valid", Namespace: "test",
		Tasks: []core.TaskDefinition{
			{ID: "a", Type: "test", Retry: retry},
			{ID: "b", Type: "test", Retry: retry},
			{ID: "c", Type: "test", Dependencies: []string{"a", "b"}, Retry: retry},
		},
	}
	if err := core.ValidateDefinition(&valid); err != nil {
		t.Fatalf("valid definition rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*core.WorkflowDefinition)
	}{
		{"empty name", func(def *core.WorkflowDefinition) { def.Name = "" }},
		{"empty namespace", func(def *core.WorkflowDefinition) { def.Namespace = "" }},
		{"no tasks", func(def *core.WorkflowDefinition) { def.Tasks = nil }},
		{"empty task id", func(def *core.WorkflowDefinition) { def.Tasks[0].ID = "" }},
		{"empty task type", func(def *core.WorkflowDefinition) { def.Tasks[0].Type = "" }},
		{"duplicate task", func(def *core.WorkflowDefinition) { def.Tasks[1].ID = "a" }},
		{"invalid attempts", func(def *core.WorkflowDefinition) { def.Tasks[0].Retry.MaxAttempts = 0 }},
		{"negative initial backoff", func(def *core.WorkflowDefinition) { def.Tasks[0].Retry.InitialBackoffMillis = -1 }},
		{"negative max backoff", func(def *core.WorkflowDefinition) { def.Tasks[0].Retry.MaxBackoffMillis = -1 }},
		{"invalid multiplier", func(def *core.WorkflowDefinition) { def.Tasks[0].Retry.BackoffMultiplier = 0 }},
		{"backoff range", func(def *core.WorkflowDefinition) {
			def.Tasks[0].Retry.InitialBackoffMillis = 2
			def.Tasks[0].Retry.MaxBackoffMillis = 1
		}},
		{"negative timeout", func(def *core.WorkflowDefinition) { def.Tasks[0].TimeoutMillis = -1 }},
		{"empty compensation", func(def *core.WorkflowDefinition) { def.Tasks[0].Compensation = &core.CompensationDefinition{} }},
		{"self dependency", func(def *core.WorkflowDefinition) { def.Tasks[0].Dependencies = []string{"a"} }},
		{"missing dependency", func(def *core.WorkflowDefinition) { def.Tasks[0].Dependencies = []string{"missing"} }},
		{"duplicate dependency", func(def *core.WorkflowDefinition) { def.Tasks[2].Dependencies = []string{"a", "a"} }},
		{"cycle", func(def *core.WorkflowDefinition) { def.Tasks[0].Dependencies = []string{"c"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := cloneDefinition(valid)
			test.mutate(&definition)
			err := core.ValidateDefinition(&definition)
			if !errors.Is(err, core.ErrInvalidDefinition) {
				t.Fatalf("expected invalid definition, got %v", err)
			}
		})
	}
	if !errors.Is(core.ValidateDefinition(nil), core.ErrInvalidDefinition) {
		t.Fatal("nil definition accepted")
	}
}

func cloneDefinition(definition core.WorkflowDefinition) core.WorkflowDefinition {
	copy := definition
	copy.Tasks = make([]core.TaskDefinition, len(definition.Tasks))
	for index, task := range definition.Tasks {
		copy.Tasks[index] = task
		copy.Tasks[index].Dependencies = append([]string(nil), task.Dependencies...)
		if task.Compensation != nil {
			compensation := *task.Compensation
			copy.Tasks[index].Compensation = &compensation
		}
	}
	return copy
}
