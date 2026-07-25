package core

import "sort"

func Project(state *WorkflowState) Projection {
	if state == nil {
		return Projection{ReadyTasks: []string{}, RemainingDependencies: map[string]int{}, ActiveAttempts: []AttemptProjection{}, Timers: []TimerState{}}
	}
	projection := Projection{
		WorkflowID: state.ID, Status: state.Status, ReadyTasks: []string{},
		RemainingDependencies: make(map[string]int, len(state.Tasks)), ActiveAttempts: []AttemptProjection{}, Timers: []TimerState{},
	}
	for id, task := range state.Tasks {
		remaining := 0
		for _, dependency := range task.Definition.Dependencies {
			if state.Tasks[dependency].Status != TaskCompleted {
				remaining++
			}
		}
		projection.RemainingDependencies[id] = remaining
		if task.Status == TaskReady || task.Status == TaskCompensating && task.WorkerID == "" {
			projection.ReadyTasks = append(projection.ReadyTasks, id)
		}
		if task.Status == TaskRunning || task.Status == TaskCompensating && task.WorkerID != "" {
			projection.ActiveAttempts = append(projection.ActiveAttempts, AttemptProjection{
				TaskID: id, AttemptID: task.AttemptID, Attempt: task.Attempt, WorkerID: task.WorkerID,
				LeaseUntil: task.LeaseUntil, Fence: task.Fence, Idempotency: task.Idempotency,
			})
		}
		projection.Summary.Total++
		switch task.Status {
		case TaskPending:
			projection.Summary.Pending++
		case TaskReady:
			projection.Summary.Ready++
		case TaskRunning:
			projection.Summary.Running++
		case TaskCompleted:
			projection.Summary.Completed++
		case TaskFailed:
			projection.Summary.Failed++
		case TaskCancelled:
			projection.Summary.Cancelled++
		case TaskCompensating:
			projection.Summary.Compensating++
		case TaskCompensated:
			projection.Summary.Compensated++
		}
	}
	for _, timer := range state.Timers {
		projection.Timers = append(projection.Timers, timer)
	}
	sort.Strings(projection.ReadyTasks)
	sort.Slice(projection.ActiveAttempts, func(i, j int) bool {
		return projection.ActiveAttempts[i].TaskID < projection.ActiveAttempts[j].TaskID
	})
	sort.Slice(projection.Timers, func(i, j int) bool {
		if projection.Timers[i].Deadline == projection.Timers[j].Deadline {
			return projection.Timers[i].ID < projection.Timers[j].ID
		}
		return projection.Timers[i].Deadline < projection.Timers[j].Deadline
	})
	return projection
}
