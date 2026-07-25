package faultlab_test

import (
	"fmt"
	"testing"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/faultlab"
)

func TestReleaseTraceCoversEveryEventKind(t *testing.T) {
	trace, err := faultlab.ReleaseTrace()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range faultlab.RequiredEventKinds() {
		if trace.EventKinds[kind] == 0 {
			t.Fatalf("event kind %s is not covered", kind)
		}
	}
	if trace.ReleaseCommands < 1 || trace.ReleaseCommands >= len(trace.Commands) {
		t.Fatalf("invalid release command boundary: %d of %d", trace.ReleaseCommands, len(trace.Commands))
	}
}

func TestReleaseTraceModelsWorkerLossAndFencing(t *testing.T) {
	trace, err := faultlab.ReleaseTrace()
	if err != nil {
		t.Fatal(err)
	}
	var expired core.Event
	var reassigned core.Event
	for _, result := range trace.Results[:trace.ReleaseCommands] {
		for _, event := range result.Events {
			if event.TaskID != "build-web" {
				continue
			}
			switch event.Kind {
			case core.EventLeaseExpired:
				expired = event
			case core.EventTaskStarted:
				if expired.ID != "" {
					reassigned = event
				}
			}
		}
	}
	if expired.ID == "" || reassigned.ID == "" || reassigned.AttemptID == expired.AttemptID || reassigned.Fence <= expired.Fence {
		t.Fatalf("worker loss did not produce a fenced reassignment: %+v %+v", expired, reassigned)
	}
}

func TestReleaseTraceMatchesAcrossEveryFaultCutPoint(t *testing.T) {
	trace, err := faultlab.ReleaseTrace()
	if err != nil {
		t.Fatal(err)
	}
	faults := []faultlab.Fault{
		faultlab.FaultCrashRestart,
		faultlab.FaultPartitionHeal,
		faultlab.FaultDelayedHeartbeat,
		faultlab.FaultDuplicateRPC,
		faultlab.FaultLostAcknowledgment,
		// faultlab.FaultWorkerLoss,
	}
	for cutPoint := range trace.ReleaseCommands {
		for _, fault := range faults {
			name := fmt.Sprintf("%02d_%s_%s", cutPoint, trace.Commands[cutPoint].RequestID, fault)
			t.Run(name, func(t *testing.T) {
				err := faultlab.Run(faultlab.RunConfig{
					Directory: t.TempDir(), Trace: trace, CutPoint: cutPoint, Fault: fault,
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestWorkerLossInjectsFencedReassignmentAtEveryCutPoint(t *testing.T) {
	trace, err := faultlab.ReleaseTrace()
	if err != nil {
		t.Fatal(err)
	}
	for cutPoint := range trace.ReleaseCommands {
		t.Run(fmt.Sprintf("%02d_%s", cutPoint, trace.Commands[cutPoint].RequestID), func(t *testing.T) {
			var record faultlab.WorkerLossRecord
			err := faultlab.Run(faultlab.RunConfig{
				Directory: t.TempDir(), Trace: trace, CutPoint: cutPoint, Fault: faultlab.FaultWorkerLoss,
				WorkerLossRecord: &record,
			})
			if err != nil {
				t.Fatal(err)
			}
			expired := record.Expired
			reassigned := record.Reassigned
			if expired.Kind != core.EventLeaseExpired || reassigned.Kind != core.EventTaskStarted ||
				expired.WorkflowID == "" || expired.WorkflowID != reassigned.WorkflowID || expired.TaskID != reassigned.TaskID ||
				expired.AttemptID == "" || expired.AttemptID == reassigned.AttemptID || reassigned.Fence <= expired.Fence ||
				expired.WorkerID == "" || expired.WorkerID == reassigned.WorkerID {
				t.Fatalf("worker loss did not record a fenced reassignment: %+v %+v", expired, reassigned)
			}
			if cutPoint == 0 {
				expected := core.WorkflowID("fault-lab", "fault-worker-loss-0-auxiliary-submit")
				if expired.WorkflowID != expected {
					t.Fatalf("worker loss did not create the auxiliary workflow: %s", expired.WorkflowID)
				}
			}
		})
	}
}
