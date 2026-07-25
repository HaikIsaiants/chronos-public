package faultlab

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/HaikIsaiants/chronos/internal/coordinator"
	"github.com/HaikIsaiants/chronos/internal/core"
	"go.etcd.io/raft/v3"
)

type Fault string

const (
	FaultCrashRestart       Fault = "crash_restart"
	FaultPartitionHeal      Fault = "partition_heal"
	FaultDelayedHeartbeat   Fault = "delayed_heartbeat"
	FaultDuplicateRPC       Fault = "duplicate_rpc"
	FaultLostAcknowledgment Fault = "lost_acknowledgment"
	FaultWorkerLoss         Fault = "worker_loss"
)

type RunConfig struct {
	Directory        string
	Trace            Trace
	CutPoint         int
	Fault            Fault
	WorkerLossRecord *WorkerLossRecord
}

type WorkerLossRecord struct {
	Expired    core.Event
	Reassigned core.Event
}

func Run(config RunConfig) error {
	if config.Directory == "" || config.CutPoint < 0 || config.CutPoint >= len(config.Trace.Commands) || len(config.Trace.Commands) != len(config.Trace.Results) {
		return fmt.Errorf("invalid fault lab configuration")
	}
	cluster, err := coordinator.NewCluster(coordinator.ClusterConfig{Directory: config.Directory, ClusterID: "fault-lab"})
	if err != nil {
		return err
	}
	defer cluster.Close()
	if err := cluster.Elect(1); err != nil {
		return err
	}
	oracle := core.NewEngine()
	for index, original := range config.Trace.Commands {
		command := alignCommand(original, oracle)
		// feed the aligned command to the oracle first
		expected, err := oracle.Handle(command)
		if err != nil {
			return fmt.Errorf("oracle command %d: %w", index, err)
		}
		leader, err := highestTermLeader(cluster, 0)
		if err != nil {
			return err
		}
		var result core.Result
		if index == config.CutPoint && config.Fault == FaultLostAcknowledgment {
			if err := cluster.Propose(leader, []core.Command{command}); err != nil {
				return err
			}
			if err := cluster.Drive(100000); err != nil {
				return err
			}
			result, err = cluster.Submit(leader, command, 100000)
			if err != nil {
				return err
			}
			if !result.Duplicate {
				return fmt.Errorf("lost acknowledgement retry at %d was not duplicate", index)
			}
		} else {
			result, err = cluster.Submit(leader, command, 100000)
			if err != nil {
				return err
			}
		}
		if !reflect.DeepEqual(result.Events, expected.Events) {
			return fmt.Errorf("result at %d differs from uninterrupted execution", index)
		}
		if index != config.CutPoint {
			continue
		}
		switch config.Fault {
		case FaultCrashRestart:
			if err := crashRestart(cluster, leader); err != nil {
				return err
			}
		case FaultPartitionHeal:
			if err := partitionHeal(cluster, leader); err != nil {
				return err
			}
		case FaultDelayedHeartbeat:
			if err := delayedHeartbeat(cluster); err != nil {
				return err
			}
		case FaultDuplicateRPC:
			duplicate, err := cluster.Submit(leader, command, 100000)
			if err != nil {
				return err
			}
			if !duplicate.Duplicate || !reflect.DeepEqual(duplicate.Events, result.Events) {
				return fmt.Errorf("duplicate RPC at %d changed its result", index)
			}
			oracleDuplicate, err := oracle.Handle(command)
			if err != nil || !oracleDuplicate.Duplicate || !reflect.DeepEqual(duplicate.Events, oracleDuplicate.Events) {
				return fmt.Errorf("duplicate RPC oracle at %d changed its result", index)
			}
		case FaultLostAcknowledgment:
		case FaultWorkerLoss:
			record, err := workerLoss(cluster, oracle, leader, index)
			if err != nil {
				return err
			}
			if config.WorkerLossRecord != nil {
				*config.WorkerLossRecord = record
			}
		default:
			return fmt.Errorf("unknown fault %q", config.Fault)
		}
	}
	if err := cluster.Tick(20); err != nil {
		return err
	}
	if err := cluster.Drive(100000); err != nil {
		return err
	}
	converged, err := cluster.Converged()
	if err != nil {
		return err
	}
	if !converged {
		return fmt.Errorf("cluster did not converge")
	}
	expectedSnapshot := oracle.Snapshot()
	for id := uint64(1); id <= 3; id++ {
		node, exists := cluster.Node(id)
		if !exists {
			return fmt.Errorf("node %d is not running", id)
		}
		engine := node.Store().Engine()
		for index := range expectedSnapshot.Workflows {
			expected := &expectedSnapshot.Workflows[index]
			actual, exists := engine.State(expected.ID)
			if !exists || actual.Version != expected.Version {
				return fmt.Errorf("node %d workflow %s has the wrong sequence", id, expected.ID)
			}
			expectedHash, err := core.StateHash(expected)
			if err != nil {
				return err
			}
			actualHash, err := core.StateHash(actual)
			if err != nil {
				return err
			}
			if actualHash != expectedHash {
				return fmt.Errorf("node %d workflow %s has the wrong state hash", id, expected.ID)
			}
		}
		actual, err := node.Store().DurableSnapshot()
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actual, expectedSnapshot) {
			return fmt.Errorf("node %d differs from uninterrupted execution", id)
		}
	}
	return nil
}

func workerLoss(cluster *coordinator.Cluster, oracle *core.Engine, leader uint64, cutPoint int) (WorkerLossRecord, error) {
	workflowID, taskID, task, found := runningTask(oracle)
	if !found {
		var err error
		workflowID, taskID, task, err = auxiliaryTask(cluster, oracle, leader, cutPoint)
		if err != nil {
			return WorkerLossRecord{}, err
		}
	}
	state, _ := oracle.State(workflowID)
	at := max(state.UpdatedAt, task.LeaseUntil)
	expire := core.Command{
		Kind: core.CommandExpire, RequestID: fmt.Sprintf("fault-worker-loss-%d-expire", cutPoint),
		WorkflowID: workflowID, TaskID: taskID, AttemptID: task.AttemptID,
		WorkerID: task.WorkerID, Fence: task.Fence, At: at,
	}
	expired, err := submitOracle(cluster, oracle, leader, expire)
	if err != nil {
		return WorkerLossRecord{}, err
	}
	expiredEvent, found := eventOfKind(expired, core.EventLeaseExpired)
	if !found {
		return WorkerLossRecord{}, fmt.Errorf("worker loss at %d did not expire a lease", cutPoint)
	}
	state, _ = oracle.State(workflowID)
	task = state.Tasks[taskID]
	if task.Status != core.TaskReady && !(task.Status == core.TaskCompensating && task.WorkerID == "") {
		return WorkerLossRecord{}, fmt.Errorf("worker loss at %d did not release task %s", cutPoint, taskID)
	}
	start := core.Command{
		Kind: core.CommandStart, RequestID: fmt.Sprintf("fault-worker-loss-%d-start", cutPoint),
		WorkflowID: workflowID, TaskID: taskID, WorkerID: fmt.Sprintf("replacement-%d", cutPoint),
		At: at + 1, LeaseUntil: at + 2,
	}
	reassigned, err := submitOracle(cluster, oracle, leader, start)
	if err != nil {
		return WorkerLossRecord{}, err
	}
	reassignedEvent, found := eventOfKind(reassigned, core.EventTaskStarted)
	if !found || reassignedEvent.AttemptID == expiredEvent.AttemptID || reassignedEvent.Fence <= expiredEvent.Fence {
		return WorkerLossRecord{}, fmt.Errorf("worker loss at %d did not fence the expired attempt", cutPoint)
	}
	return WorkerLossRecord{Expired: expiredEvent, Reassigned: reassignedEvent}, nil
}

func auxiliaryTask(cluster *coordinator.Cluster, oracle *core.Engine, leader uint64, cutPoint int) (string, string, core.TaskState, error) {
	prefix := fmt.Sprintf("fault-worker-loss-%d-auxiliary", cutPoint)
	at := int64(1000 + cutPoint*10)
	definition := core.WorkflowDefinition{
		Name: prefix, Namespace: "fault-lab",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "fault", Retry: core.RetryPolicy{MaxAttempts: 2, BackoffMultiplier: 1}}},
	}
	submitted, err := submitOracle(cluster, oracle, leader, core.Command{
		Kind: core.CommandSubmit, RequestID: prefix + "-submit", At: at, Definition: &definition,
	})
	if err != nil {
		return "", "", core.TaskState{}, err
	}
	const taskID = "task"
	_, err = submitOracle(cluster, oracle, leader, core.Command{
		Kind: core.CommandStart, RequestID: prefix + "-start", WorkflowID: submitted.WorkflowID,
		TaskID: taskID, WorkerID: prefix + "-worker", At: at + 1, LeaseUntil: at + 2,
	})
	if err != nil {
		return "", "", core.TaskState{}, err
	}
	state, _ := oracle.State(submitted.WorkflowID)
	return submitted.WorkflowID, taskID, state.Tasks[taskID], nil
}

func submitOracle(cluster *coordinator.Cluster, oracle *core.Engine, leader uint64, command core.Command) (core.Result, error) {
	expected, err := oracle.Handle(command)
	if err != nil {
		return core.Result{}, err
	}
	actual, err := cluster.Submit(leader, command, 100000)
	if err != nil {
		return core.Result{}, err
	}
	if !reflect.DeepEqual(actual.Events, expected.Events) {
		return core.Result{}, fmt.Errorf("fault command %s differs from oracle", command.RequestID)
	}
	return actual, nil
}

func runningTask(engine *core.Engine) (string, string, core.TaskState, bool) {
	for _, workflowID := range engine.WorkflowIDs() {
		state, _ := engine.State(workflowID)
		ids := make([]string, 0, len(state.Tasks))
		for taskID := range state.Tasks {
			ids = append(ids, taskID)
		}
		sort.Strings(ids)
		for _, taskID := range ids {
			task := state.Tasks[taskID]
			if task.WorkerID != "" && (task.Status == core.TaskRunning || task.Status == core.TaskCompensating) && leaseCanExpire(state, taskID, task) {
				return workflowID, taskID, task, true
			}
		}
	}
	return "", "", core.TaskState{}, false
}

func leaseCanExpire(state *core.WorkflowState, taskID string, task core.TaskState) bool {
	for _, timer := range state.Timers {
		if timer.TaskID == taskID && timer.AttemptID == task.AttemptID && timer.Purpose == core.TimerTimeout && timer.Status == core.TimerScheduled && timer.Deadline <= task.LeaseUntil {
			return false
		}
	}
	return true
}

func alignCommand(command core.Command, engine *core.Engine) core.Command {
	state, exists := engine.State(command.WorkflowID)
	if !exists {
		return command
	}
	leaseDuration := command.LeaseUntil - command.At
	if command.At < state.UpdatedAt {
		command.At = state.UpdatedAt
	}
	if command.Kind == core.CommandFireTimer {
		if timer, exists := state.Timers[command.TimerID]; exists && timer.Status == core.TimerScheduled {
			command.At = max(command.At, timer.Deadline)
		} else {
			var selected core.TimerState
			for _, candidate := range state.Timers {
				if candidate.Status == core.TimerScheduled && (selected.ID == "" || candidate.Deadline < selected.Deadline || candidate.Deadline == selected.Deadline && candidate.ID < selected.ID) {
					selected = candidate
				}
			}
			if selected.ID != "" {
				command.TimerID = selected.ID
				command.At = max(command.At, selected.Deadline)
			}
		}
	}
	task, exists := state.Tasks[command.TaskID]
	if exists && command.Kind == core.CommandStart && leaseDuration > 0 {
		command.LeaseUntil = command.At + leaseDuration
	}
	if exists && command.Kind == core.CommandStart && task.Definition.TimeoutMillis > 0 {
		deadline := command.At + task.Definition.TimeoutMillis
		if command.LeaseUntil <= command.At || command.LeaseUntil > deadline {
			command.LeaseUntil = deadline
		}
	}
	if exists && task.WorkerID != "" {
		switch command.Kind {
		case core.CommandRenew:
			command.AttemptID = task.AttemptID
			command.WorkerID = task.WorkerID
			command.Fence = task.Fence
			if command.LeaseUntil <= task.LeaseUntil {
				command.LeaseUntil = task.LeaseUntil + 1000
			}
		case core.CommandComplete, core.CommandFail:
			command.AttemptID = task.AttemptID
			command.WorkerID = task.WorkerID
			command.Fence = task.Fence
		case core.CommandExpire:
			command.AttemptID = task.AttemptID
			command.WorkerID = task.WorkerID
			command.Fence = task.Fence
			command.At = max(command.At, task.LeaseUntil)
		}
	}
	if command.LeaseUntil > 0 && command.LeaseUntil <= command.At {
		command.LeaseUntil = command.At + 1000
	}
	return command
}

func crashRestart(cluster *coordinator.Cluster, leader uint64) error {
	if err := cluster.Crash(leader); err != nil {
		return err
	}
	if _, err := cluster.WaitLeader(30); err != nil {
		return err
	}
	if err := cluster.Restart(leader); err != nil {
		return err
	}
	cluster.Heal()
	return cluster.Tick(20)
}

func partitionHeal(cluster *coordinator.Cluster, leader uint64) error {
	cluster.Isolate(leader)
	if _, err := waitLeaderExcluding(cluster, leader, 30); err != nil {
		return err
	}
	cluster.Heal()
	return cluster.Tick(20)
}

func waitLeaderExcluding(cluster *coordinator.Cluster, excluded uint64, maxTicks int) (uint64, error) {
	for tick := 0; tick <= maxTicks; tick++ {
		if leader, err := highestTermLeader(cluster, excluded); err == nil {
			return leader, nil
		}
		if tick < maxTicks {
			if err := cluster.Tick(1); err != nil {
				return 0, err
			}
		}
	}
	return 0, coordinator.ErrNoLeader
}

func delayedHeartbeat(cluster *coordinator.Cluster) error {
	cluster.HoldHeartbeats()
	if err := cluster.Tick(5); err != nil {
		return err
	}
	return cluster.ReleaseHeartbeats()
}

func highestTermLeader(cluster *coordinator.Cluster, excluded uint64) (uint64, error) {
	selected := uint64(0)
	term := uint64(0)
	for id := uint64(1); id <= 3; id++ {
		if id == excluded {
			continue
		}
		node, exists := cluster.Node(id)
		if !exists {
			continue
		}
		status := node.Status()
		if status.Role == raft.StateLeader && status.LeaderID == id && (selected == 0 || status.Term > term) {
			selected = id
			term = status.Term
		}
	}
	if selected == 0 {
		return 0, coordinator.ErrNoLeader
	}
	return selected, nil
}
