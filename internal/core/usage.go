package core

type WorkflowUsage struct {
	Namespace string
	Workflows int
	Tasks     int
	Ready     int
	Running   int
}

func (e *Engine) WorkflowUsage(workflowID string) (WorkflowUsage, bool, error) {
	state, exists, err := e.resolveState(workflowID)
	if err != nil || !exists {
		return WorkflowUsage{}, exists, err
	}
	usage := WorkflowUsage{Namespace: state.Namespace}
	if terminalWorkflow(state.Status) {
		return usage, true, nil
	}
	usage.Workflows = 1
	for _, task := range state.Tasks {
		switch task.Status {
		case TaskCompleted, TaskFailed, TaskCancelled, TaskCompensated:
			continue
		}
		usage.Tasks++
		if task.Definition.Fanout != nil && !task.FanoutExpanded {
			usage.Tasks += int(task.Definition.Fanout.MaxItems)
		}
		switch task.Status {
		case TaskReady:
			usage.Ready++
		case TaskRunning:
			usage.Running++
		case TaskCompensating:
			if !task.Compensation {
				continue
			}
			if task.WorkerID == "" {
				usage.Ready++
			} else {
				usage.Running++
			}
		}
	}
	return usage, true, nil
}
