package coordinator

import (
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/bench"
	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
	pb "go.etcd.io/raft/v3/raftpb"
)

func BenchmarkProposalReadyGroup(b *testing.B) {
	// sizes := []int{8, 16}
	sizes := []int{1, 4, 8, proposalReadyBatch, 32}
	for _, size := range sizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			cluster, err := NewCluster(ClusterConfig{Directory: b.TempDir(), ClusterID: "ready-benchmark"})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := cluster.Close(); err != nil {
					b.Error(err)
				}
			})
			if err := cluster.Elect(1); err != nil {
				b.Fatal(err)
			}
			leader, _ := cluster.Node(1)
			value, err := scheduler.New(scheduler.DefaultConfig())
			if err != nil {
				b.Fatal(err)
			}
			planner := leader.Store().Engine()
			var workflow uint64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				groups := make([][]core.Command, size)
				for entry := range groups {
					groups[entry] = bench.WorkloadBatch(workflow, bench.WorkloadBatchSize)
					workflow += bench.WorkloadBatchSize
				}
				b.StartTimer()
				staged := make([]stagedBatch, 0, size)
				for _, commands := range groups {
					prepared, err := value.StageBatch(planner, commands)
					if err != nil {
						b.Fatal(err)
					}
					staged = append(staged, stagedBatch{commands: commands, batches: prepared.Batches})
				}
				if err := leader.stagePreparedGroup(staged); err != nil {
					b.Fatal(err)
				}
				if err := leader.ProcessReady(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := cluster.Drive(100000); err != nil {
					b.Fatal(err)
				}
				planner.RetainActive()
				if len(planner.WorkflowIDs()) != 0 {
					b.Fatal("planner retained terminal workflows")
				}
				if _, exists := planner.ResidentRequestHash(groups[0][0].RequestID); exists {
					b.Fatal("planner retained terminal requests")
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/entry")
		})
	}
}

func BenchmarkProposalReadyGroupBreakdown(b *testing.B) {
	cluster, err := NewCluster(ClusterConfig{Directory: b.TempDir(), ClusterID: "ready-breakdown"})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := cluster.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := cluster.Elect(1); err != nil {
		b.Fatal(err)
	}
	leader, _ := cluster.Node(1)
	value, err := scheduler.New(scheduler.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	planner := leader.Store().Engine()
	var workflow uint64
	var stageSamples []time.Duration
	var encodeSamples []time.Duration
	var stepSamples []time.Duration
	var readySamples []time.Duration
	var driveSamples []time.Duration
	var serialSamples []time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		groups := make([][]core.Command, proposalReadyBatch)
		for entry := range groups {
			groups[entry] = bench.WorkloadBatch(workflow, bench.WorkloadBatchSize)
			workflow += bench.WorkloadBatchSize
		}
		b.StartTimer()
		started := time.Now()
		staged := make([]stagedBatch, 0, proposalReadyBatch)
		for _, commands := range groups {
			prepared, err := value.StageBatch(planner, commands)
			if err != nil {
				b.Fatal(err)
			}
			staged = append(staged, stagedBatch{commands: commands, batches: prepared.Batches})
		}
		stageDuration := time.Since(started)
		started = time.Now()
		batches := make([]LogBatch, 0, proposalReadyBatch)
		for _, proposal := range staged {
			if err := leader.checkStagedProposal(proposal.commands); err != nil {
				b.Fatal(err)
			}
			batches = append(batches, LogBatch{Version: LogBatchVersion, Commands: proposal.batches})
		}
		encoded, err := encodeLogBatches(batches)
		if err != nil {
			b.Fatal(err)
		}
		entries := make([]*pb.Entry, len(encoded))
		for index, data := range encoded {
			entries[index] = &pb.Entry{Data: data}
		}
		encodeDuration := time.Since(started)
		started = time.Now()
		id := leader.id
		if err := leader.raw.Step(&pb.Message{Type: pb.MsgProp.Enum(), From: &id, Entries: entries}); err != nil {
			b.Fatal(err)
		}
		stepDuration := time.Since(started)
		started = time.Now()
		if err := leader.ProcessReady(); err != nil {
			b.Fatal(err)
		}
		readyDuration := time.Since(started)
		stageSamples = append(stageSamples, stageDuration)
		encodeSamples = append(encodeSamples, encodeDuration)
		stepSamples = append(stepSamples, stepDuration)
		readySamples = append(readySamples, readyDuration)
		serialSamples = append(serialSamples, stageDuration+encodeDuration+stepDuration+readyDuration)
		b.StopTimer()
		started = time.Now()
		if err := cluster.Drive(100000); err != nil {
			b.Fatal(err)
		}
		driveSamples = append(driveSamples, time.Since(started))
		planner.RetainActive()
		if len(planner.WorkflowIDs()) != 0 {
			b.Fatal("planner retained terminal workflows")
		}
		if _, exists := planner.ResidentRequestHash(groups[0][0].RequestID); exists {
			b.Fatal("planner retained terminal requests")
		}
		b.StartTimer()
	}
	b.StopTimer()
	entries := float64(proposalReadyBatch)
	b.ReportMetric(float64(medianDuration(stageSamples))/entries, "stage_med_ns/entry")
	b.ReportMetric(float64(medianDuration(encodeSamples))/entries, "codec_med_ns/entry")
	b.ReportMetric(float64(medianDuration(stepSamples))/entries, "step_med_ns/entry")
	b.ReportMetric(float64(medianDuration(readySamples))/entries, "ready_med_ns/entry")
	b.ReportMetric(float64(medianDuration(driveSamples))/entries, "drive_med_ns/entry")
	b.ReportMetric(float64(medianDuration(serialSamples))/entries, "serial_med_ns/entry")
}

func medianDuration(samples []time.Duration) time.Duration {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2]
}
