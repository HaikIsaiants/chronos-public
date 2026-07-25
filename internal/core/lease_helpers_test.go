package core_test

import "github.com/HaikIsaiants/chronos/internal/core"

func lease(command core.Command) core.Command {
	command.WorkerID = "test-worker"
	command.LeaseUntil = command.At + 1000
	return command
}

func fenced(command core.Command, start core.Result) core.Command {
	event := start.Events[0]
	command.AttemptID = event.AttemptID
	command.WorkerID = event.WorkerID
	command.Fence = event.Fence
	return command
}
