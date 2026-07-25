package scheduler

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/HaikIsaiants/chronos/internal/core"
)

var ErrBackpressure = errors.New("scheduler capacity exhausted")

type Limits struct {
	Workflows int
	Tasks     int
	Running   int
}

type NamespaceConfig struct {
	Weight int
	Limits Limits
}

type Config struct {
	Global     Limits
	Default    NamespaceConfig
	Namespaces map[string]NamespaceConfig
}

type AdmissionError struct {
	Namespace string
	Resource  string
	Used      int
	Requested int
	Limit     int
}

func (e AdmissionError) Error() string {
	scope := "global"
	if e.Namespace != "" {
		scope = "namespace " + e.Namespace
	}
	return fmt.Sprintf("%s %s capacity is %d, used %d, requested %d", scope, e.Resource, e.Limit, e.Used, e.Requested)
}

func (e AdmissionError) Unwrap() error {
	return ErrBackpressure
}

type Task struct {
	WorkflowID string
	TaskID     string
	Namespace  string
	Capability string
	UpdatedAt  int64
}

type Plan struct {
	Task    Task
	current map[string]int64
	epoch   uint64
}

type NamespaceStats struct {
	Namespace string
	Workflows int
	Tasks     int
	Ready     int
	Running   int
	Weight    int
}

type Stats struct {
	Workflows  int
	Tasks      int
	Ready      int
	Running    int
	Namespaces []NamespaceStats
}

type PreparedBatch struct {
	Batches   []core.CommandBatch
	Immediate map[string]core.Result
	Hashes    []string
}

type usage struct {
	workflows   int
	tasks       int
	ready       int
	running     int
	byNamespace map[string]*NamespaceStats
	candidates  map[string][]Task
}

type Scheduler struct {
	config  Config
	current map[string]int64
	epoch   uint64
}

func DefaultConfig() Config {
	return Config{
		Global:  Limits{Workflows: 100000, Tasks: 1000000, Running: 10000},
		Default: NamespaceConfig{Weight: 1, Limits: Limits{Workflows: 10000, Tasks: 100000, Running: 1000}},
	}
}

func New(config Config) (*Scheduler, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	copy := config
	copy.Namespaces = make(map[string]NamespaceConfig, len(config.Namespaces))
	for namespace, value := range config.Namespaces {
		if namespace == "" {
			return nil, fmt.Errorf("namespace is required")
		}
		copy.Namespaces[namespace] = value
	}
	return &Scheduler{config: copy, current: make(map[string]int64)}, nil
}

func validateConfig(config Config) error {
	if err := validateLimits(config.Global); err != nil {
		return fmt.Errorf("global limits: %w", err)
	}
	if config.Default.Weight < 1 || config.Default.Weight > math.MaxInt32 {
		return fmt.Errorf("default namespace weight must be positive")
	}
	if err := validateLimits(config.Default.Limits); err != nil {
		return fmt.Errorf("default namespace limits: %w", err)
	}
	for namespace, value := range config.Namespaces {
		if value.Weight < 1 || value.Weight > math.MaxInt32 {
			return fmt.Errorf("namespace %s weight must be positive", namespace)
		}
		if err := validateLimits(value.Limits); err != nil {
			return fmt.Errorf("namespace %s limits: %w", namespace, err)
		}
	}
	return nil
}

func validateLimits(limits Limits) error {
	if limits.Workflows < 1 || limits.Tasks < 1 || limits.Running < 1 {
		return fmt.Errorf("limits must be positive")
	}
	if limits.Running > limits.Tasks {
		return fmt.Errorf("running limit exceeds task limit")
	}
	return nil
}

func (s *Scheduler) CheckBatch(engine *core.Engine, commands []core.Command) error {
	_, err := s.PrepareBatch(engine, commands)
	return err
}

func (s *Scheduler) PrepareBatch(engine *core.Engine, commands []core.Command) (PreparedBatch, error) {
	return s.prepareBatch(engine, commands, false)
}

func (s *Scheduler) StageBatch(engine *core.Engine, commands []core.Command) (PreparedBatch, error) {
	return s.prepareBatch(engine, commands, true)
}

func (s *Scheduler) prepareBatch(engine *core.Engine, commands []core.Command, retain bool) (PreparedBatch, error) {
	// Each command sees the staged state left by the one before it
	transaction := engine.BeginStaging()
	prepare := transaction.PrepareAndApply
	if retain {
		prepare = transaction.Stage
	}
	state := inspect(engine, nil)
	restore := true
	defer func() {
		if restore {
			transaction.Rollback()
		}
	}()
	prepared := PreparedBatch{
		Batches:   make([]core.CommandBatch, 0, len(commands)),
		Immediate: make(map[string]core.Result), Hashes: make([]string, len(commands)),
	}
	for index, command := range commands {
		workflowID := command.WorkflowID
		if command.Kind == core.CommandSubmit && workflowID == "" && command.Definition != nil {
			workflowID = core.WorkflowID(command.Definition.Namespace, command.RequestID)
		}
		before, beforeExists, err := engine.WorkflowUsage(workflowID)
		if err != nil {
			return PreparedBatch{}, err
		}
		batch, result, duplicate, err := prepare(command)
		if err != nil {
			return PreparedBatch{}, err
		}
		prepared.Hashes[index] = batch.RequestHash
		if duplicate {
			prepared.Immediate[command.RequestID] = result
			continue
		}
		if command.Kind == core.CommandSubmit {
			if err := s.checkAdmission(state, command.Definition); err != nil {
				return PreparedBatch{}, err
			}
		}
		if command.Kind == core.CommandStart {
			if !beforeExists {
				return PreparedBatch{}, core.ErrWorkflowNotFound
			}
			if err := s.checkRunning(state, before.Namespace); err != nil {
				return PreparedBatch{}, err
			}
		}
		after, _, err := engine.WorkflowUsage(batch.WorkflowID)
		if err != nil {
			return PreparedBatch{}, err
		}
		accountWorkflow(&state, before, -1)
		accountWorkflow(&state, after, 1)
		prepared.Batches = append(prepared.Batches, batch)
	}
	restore = !retain
	if retain {
		transaction.Commit()
	}
	return prepared, nil
}

func (s *Scheduler) checkRunning(state usage, namespaceName string) error {
	if state.running+1 > s.config.Global.Running {
		return AdmissionError{Resource: "running", Used: state.running, Requested: 1, Limit: s.config.Global.Running}
	}
	namespace := state.namespace(namespaceName)
	limit := s.namespaceConfig(namespaceName).Limits.Running
	if namespace.Running+1 > limit {
		return AdmissionError{Namespace: namespaceName, Resource: "running", Used: namespace.Running, Requested: 1, Limit: limit}
	}
	return nil
}

func (s *Scheduler) checkAdmission(state usage, definition *core.WorkflowDefinition) error {
	if definition == nil {
		return core.ErrInvalidDefinition
	}
	requested := reservedTasks(definition)
	if state.workflows+1 > s.config.Global.Workflows {
		return AdmissionError{Resource: "workflow", Used: state.workflows, Requested: 1, Limit: s.config.Global.Workflows}
	}
	if requested > s.config.Global.Tasks-state.tasks {
		return AdmissionError{Resource: "task", Used: state.tasks, Requested: requested, Limit: s.config.Global.Tasks}
	}
	namespace := state.namespace(definition.Namespace)
	limits := s.namespaceConfig(definition.Namespace).Limits
	if namespace.Workflows+1 > limits.Workflows {
		return AdmissionError{Namespace: definition.Namespace, Resource: "workflow", Used: namespace.Workflows, Requested: 1, Limit: limits.Workflows}
	}
	if requested > limits.Tasks-namespace.Tasks {
		return AdmissionError{Namespace: definition.Namespace, Resource: "task", Used: namespace.Tasks, Requested: requested, Limit: limits.Tasks}
	}
	return nil
}

func accountWorkflow(state *usage, workflow core.WorkflowUsage, direction int) {
	if workflow.Workflows == 0 {
		return
	}
	state.workflows += workflow.Workflows * direction
	namespace := state.namespace(workflow.Namespace)
	namespace.Workflows += workflow.Workflows * direction
	state.tasks += workflow.Tasks * direction
	namespace.Tasks += workflow.Tasks * direction
	state.ready += workflow.Ready * direction
	namespace.Ready += workflow.Ready * direction
	state.running += workflow.Running * direction
	namespace.Running += workflow.Running * direction
}

func (s *Scheduler) Plan(engine *core.Engine, capabilities []string) (Plan, bool) {
	allowed := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		if capability != "" {
			allowed[capability] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return Plan{}, false
	}
	state := inspect(engine, allowed)
	if state.running >= s.config.Global.Running {
		return Plan{}, false
	}
	eligible := make([]string, 0, len(state.candidates))
	for namespace, tasks := range state.candidates {
		stats := state.namespace(namespace)
		if len(tasks) > 0 && stats.Running < s.namespaceConfig(namespace).Limits.Running {
			eligible = append(eligible, namespace)
		}
	}
	if len(eligible) == 0 {
		return Plan{}, false
	}
	sort.Strings(eligible)
	current := make(map[string]int64, len(eligible))
	var total int64
	for _, namespace := range eligible {
		weight := int64(s.namespaceConfig(namespace).Weight)
		if total > math.MaxInt64-weight || s.current[namespace] > math.MaxInt64-weight {
			return Plan{}, false
		}
		current[namespace] = s.current[namespace] + weight
		total += weight
	}
	selected := eligible[0]
	for _, namespace := range eligible[1:] {
		if current[namespace] > current[selected] {
			selected = namespace
		}
	}
	if current[selected] < math.MinInt64+total {
		return Plan{}, false
	}
	current[selected] -= total
	tasks := state.candidates[selected]
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].WorkflowID != tasks[j].WorkflowID {
			return tasks[i].WorkflowID < tasks[j].WorkflowID
		}
		if tasks[i].TaskID != tasks[j].TaskID {
			return tasks[i].TaskID < tasks[j].TaskID
		}
		return tasks[i].Capability < tasks[j].Capability
	})
	return Plan{Task: tasks[0], current: current, epoch: s.epoch}, true
}

func (s *Scheduler) Commit(plan Plan) error {
	if plan.epoch != s.epoch || plan.Task.WorkflowID == "" {
		return fmt.Errorf("stale scheduler plan")
	}
	// Advance fairness after the lease commits
	s.current = plan.current
	s.epoch++
	return nil
}

func (s *Scheduler) Stats(engine *core.Engine) Stats {
	state := inspect(engine, nil)
	result := Stats{
		Workflows: state.workflows, Tasks: state.tasks, Ready: state.ready, Running: state.running,
		Namespaces: make([]NamespaceStats, 0, len(state.byNamespace)),
	}
	for namespace, stats := range state.byNamespace {
		copy := *stats
		copy.Namespace = namespace
		copy.Weight = s.namespaceConfig(namespace).Weight
		result.Namespaces = append(result.Namespaces, copy)
	}
	sort.Slice(result.Namespaces, func(i, j int) bool {
		return result.Namespaces[i].Namespace < result.Namespaces[j].Namespace
	})
	return result
}

func inspect(engine *core.Engine, capabilities map[string]struct{}) usage {
	result := usage{byNamespace: make(map[string]*NamespaceStats), candidates: make(map[string][]Task)}
	if capabilities == nil {
		for _, workflowID := range engine.ActiveWorkflowIDs() {
			workflow, exists, err := engine.WorkflowUsage(workflowID)
			if err == nil && exists {
				accountWorkflow(&result, workflow, 1)
			}
		}
		return result
	}
	for _, workflowID := range engine.ActiveWorkflowIDs() {
		state, _ := engine.State(workflowID)
		result.workflows++
		namespace := result.namespace(state.Namespace)
		namespace.Workflows++
		for taskID, task := range state.Tasks {
			if terminalTask(task.Status) {
				continue
			}
			result.tasks++
			namespace.Tasks++
			if task.Definition.Fanout != nil && !task.FanoutExpanded {
				reserved := int(task.Definition.Fanout.MaxItems)
				result.tasks += reserved
				namespace.Tasks += reserved
			}
			switch task.Status {
			case core.TaskReady:
				result.ready++
				namespace.Ready++
				if capabilities == nil {
					continue
				}
				if _, allowed := capabilities[task.Definition.Type]; allowed {
					result.candidates[state.Namespace] = append(result.candidates[state.Namespace], Task{
						WorkflowID: workflowID, TaskID: taskID, Namespace: state.Namespace,
						Capability: task.Definition.Type, UpdatedAt: state.UpdatedAt,
					})
				}
			case core.TaskRunning:
				result.running++
				namespace.Running++
			case core.TaskCompensating:
				if !task.Compensation {
					continue
				}
				if task.WorkerID != "" {
					result.running++
					namespace.Running++
					continue
				}
				result.ready++
				namespace.Ready++
				if capabilities == nil || task.Definition.Compensation == nil {
					continue
				}
				capability := task.Definition.Compensation.TaskType
				if _, allowed := capabilities[capability]; allowed {
					result.candidates[state.Namespace] = append(result.candidates[state.Namespace], Task{
						WorkflowID: workflowID, TaskID: taskID, Namespace: state.Namespace,
						Capability: capability, UpdatedAt: state.UpdatedAt,
					})
				}
			}
		}
	}
	return result
}

func reservedTasks(definition *core.WorkflowDefinition) int {
	total := len(definition.Tasks)
	for _, task := range definition.Tasks {
		if task.Fanout != nil {
			total += int(task.Fanout.MaxItems)
		}
	}
	return total
}

func (u *usage) namespace(namespace string) *NamespaceStats {
	stats := u.byNamespace[namespace]
	if stats == nil {
		stats = &NamespaceStats{}
		u.byNamespace[namespace] = stats
	}
	return stats
}

func (s *Scheduler) namespaceConfig(namespace string) NamespaceConfig {
	if value, exists := s.config.Namespaces[namespace]; exists {
		return value
	}
	return s.config.Default
}

func terminalWorkflow(status core.WorkflowStatus) bool {
	switch status {
	case core.WorkflowCompleted, core.WorkflowFailed, core.WorkflowCancelled, core.WorkflowCompensated:
		return true
	default:
		return false
	}
}

func terminalTask(status core.TaskStatus) bool {
	switch status {
	case core.TaskCompleted, core.TaskFailed, core.TaskCancelled, core.TaskCompensated:
		return true
	default:
		return false
	}
}
