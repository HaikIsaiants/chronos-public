package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/observability"
)

func TestProviderMetrics(t *testing.T) {
	provider, err := observability.New(context.Background(), observability.Config{InstanceID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { provider.Close(context.Background()) })
	provider.SetQueue(map[string]int{"team": 3}, 2)
	provider.SetWorker("worker", 4, 2)
	provider.ObserveDispatch(10 * time.Millisecond)
	provider.RecordElection()
	provider.SetRaft(true, 2, 4, 4)
	provider.RecordSnapshot("create", 1024, time.Millisecond, nil)
	events := []core.Event{
		{Kind: core.EventWorkflowSubmitted, WorkflowID: "workflow", At: 1},
		{Kind: core.EventTaskRetryScheduled, WorkflowID: "workflow"},
		{Kind: core.EventWorkflowCompleted, WorkflowID: "workflow", At: 1001},
	}
	provider.RecordCommit(1, events)
	metrics, err := provider.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool, len(metrics))
	for _, metric := range metrics {
		names[metric.GetName()] = true
	}
	for _, name := range []string{
		"chronos_queue_ready_tasks", "chronos_pending_proposals", "chronos_dispatch_commit_seconds",
		"chronos_task_retries_total", "chronos_worker_utilization_ratio", "chronos_raft_commits_total",
		"chronos_raft_elections_total", "chronos_snapshots_total", "chronos_workflow_duration_seconds",
	} {
		if !names[name] {
			t.Fatalf("metric %s is missing", name)
		}
	}
	provider.DeleteWorker("worker")
}

func TestProviderRegistriesAreIndependent(t *testing.T) {
	first, err := observability.New(context.Background(), observability.Config{InstanceID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := observability.New(context.Background(), observability.Config{InstanceID: "2"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		first.Close(context.Background())
		second.Close(context.Background())
	})
	first.SetQueue(map[string]int{"one": 1}, 0)
	second.SetQueue(map[string]int{"two": 2}, 0)
	if _, err := first.Registry.Gather(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Registry.Gather(); err != nil {
		t.Fatal(err)
	}
}

func TestRestoredWorkflowDuration(t *testing.T) {
	provider, err := observability.New(context.Background(), observability.Config{InstanceID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { provider.Close(context.Background()) })
	provider.SeedWorkflowStart("restored", 1)
	provider.RecordCommit(1, []core.Event{{
		Kind: core.EventWorkflowCompleted, WorkflowID: "restored", At: 1001,
	}})
	metrics, err := provider.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range metrics {
		if family.GetName() == "chronos_workflow_duration_seconds" && len(family.Metric) == 1 && family.Metric[0].GetHistogram().GetSampleCount() == 1 {
			return
		}
	}
	t.Fatal("restored workflow duration was not observed")
}
