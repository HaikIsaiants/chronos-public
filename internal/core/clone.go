package core

import "sort"

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func cloneNestedMap(values map[string]map[string]string) map[string]map[string]string {
	if len(values) == 0 {
		return nil
	}
	copy := make(map[string]map[string]string, len(values))
	for key, value := range values {
		copy[key] = cloneMap(value)
	}
	return copy
}

func cloneTaskDefinition(task TaskDefinition) TaskDefinition {
	copy := task
	copy.Dependencies = cloneStrings(task.Dependencies)
	if task.Compensation != nil {
		compensation := *task.Compensation
		copy.Compensation = &compensation
	}
	if task.Fanout != nil {
		fanout := *task.Fanout
		if task.Fanout.Compensation != nil {
			compensation := *task.Fanout.Compensation
			fanout.Compensation = &compensation
		}
		copy.Fanout = &fanout
	}
	return copy
}

func cloneDefinition(definition *WorkflowDefinition) *WorkflowDefinition {
	if definition == nil {
		return nil
	}
	copy := &WorkflowDefinition{Name: definition.Name, Namespace: definition.Namespace, StartAt: definition.StartAt}
	copy.Tasks = make([]TaskDefinition, len(definition.Tasks))
	for index, task := range definition.Tasks {
		copy.Tasks[index] = cloneTaskDefinition(task)
	}
	return copy
}

func canonicalDefinition(definition *WorkflowDefinition) *WorkflowDefinition {
	copy := cloneDefinition(definition)
	if copy == nil {
		return nil
	}
	for index := range copy.Tasks {
		if copy.Tasks[index].Dependencies == nil {
			copy.Tasks[index].Dependencies = []string{}
		}
		sort.Strings(copy.Tasks[index].Dependencies)
	}
	sort.Slice(copy.Tasks, func(i, j int) bool {
		return copy.Tasks[i].ID < copy.Tasks[j].ID
	})
	return copy
}

func cloneEvent(event Event) Event {
	copy := event
	copy.Definition = cloneDefinition(event.Definition)
	copy.Inputs = cloneNestedMap(event.Inputs)
	copy.Output = cloneMap(event.Output)
	if event.Timer != nil {
		timer := *event.Timer
		copy.Timer = &timer
	}
	copy.ExpandedTasks = cloneExpandedTasks(event.ExpandedTasks)
	return copy
}

func canonicalFanoutItems(items []FanoutItem) []FanoutItem {
	copy := cloneFanoutItems(items)
	sort.Slice(copy, func(i, j int) bool {
		return copy[i].Key < copy[j].Key
	})
	return copy
}

func cloneEvents(events []Event) []Event {
	copy := make([]Event, len(events))
	for index, event := range events {
		copy[index] = cloneEvent(event)
	}
	return copy
}

func cloneState(state *WorkflowState) *WorkflowState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Tasks = make(map[string]TaskState, len(state.Tasks))
	for id, task := range state.Tasks {
		task.Definition = cloneTaskDefinition(task.Definition)
		task.Inputs = cloneNestedMap(task.Inputs)
		task.Output = cloneMap(task.Output)
		task.Children = cloneStrings(task.Children)
		task.Payload = cloneMap(task.Payload)
		copy.Tasks[id] = task
	}
	copy.Timers = make(map[string]TimerState, len(state.Timers))
	for id, timer := range state.Timers {
		copy.Timers[id] = timer
	}
	return &copy
}

func cloneStateForUpdate(state *WorkflowState) *WorkflowState {
	if state == nil {
		return nil
	}
	copy := *state
	copy.Tasks = make(map[string]TaskState, len(state.Tasks))
	for id, task := range state.Tasks {
		copy.Tasks[id] = task
	}
	copy.Timers = make(map[string]TimerState, len(state.Timers))
	for id, timer := range state.Timers {
		copy.Timers[id] = timer
	}
	return &copy
}

func cloneFanoutItems(items []FanoutItem) []FanoutItem {
	if items == nil {
		return nil
	}
	copy := make([]FanoutItem, len(items))
	for index, item := range items {
		copy[index] = FanoutItem{Key: item.Key, Payload: cloneMap(item.Payload)}
	}
	return copy
}

func cloneExpandedTasks(tasks []ExpandedTask) []ExpandedTask {
	if tasks == nil {
		return nil
	}
	copy := make([]ExpandedTask, len(tasks))
	for index, task := range tasks {
		copy[index] = ExpandedTask{Key: task.Key, Definition: cloneTaskDefinition(task.Definition), Payload: cloneMap(task.Payload)}
	}
	return copy
}
