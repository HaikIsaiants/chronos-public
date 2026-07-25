package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/coordinator"
	"github.com/HaikIsaiants/chronos-public/internal/execution"
	"github.com/HaikIsaiants/chronos-public/internal/observability"
	"github.com/HaikIsaiants/chronos-public/internal/scheduler"
)

func main() {
	var id uint64
	var clusterID string
	var directory string
	var listen string
	var workerListen string
	var peerList string
	var weightList string
	var completionAckFaultFile string
	var lease time.Duration
	var maxPending int
	var otelEndpoint string
	var otelInsecure bool
	var serviceName string
	var snapshotEntries uint64
	var localExecutionAPI bool
	var gcPercent int
	flag.Uint64Var(&id, "id", 0, "coordinator id")
	flag.StringVar(&clusterID, "cluster", "chronos", "cluster id")
	flag.StringVar(&directory, "data", "", "data directory")
	flag.StringVar(&listen, "listen", "", "listen address")
	flag.StringVar(&workerListen, "worker-listen", "", "worker gRPC listen address")
	flag.StringVar(&peerList, "peers", "", "peer addresses")
	flag.StringVar(&weightList, "namespace-weights", "", "namespace weights")
	flag.StringVar(&completionAckFaultFile, "completion-ack-fault-file", "", "completion acknowledgement fault state file")
	flag.DurationVar(&lease, "lease", 30*time.Second, "worker lease duration")
	flag.IntVar(&maxPending, "max-pending", 1024, "maximum pending client proposals")
	flag.StringVar(&otelEndpoint, "otel-endpoint", "", "OpenTelemetry OTLP gRPC endpoint")
	flag.BoolVar(&otelInsecure, "otel-insecure", false, "use an insecure OpenTelemetry connection")
	flag.StringVar(&serviceName, "service-name", "chronos-coordinator", "OpenTelemetry service name")
	flag.Uint64Var(&snapshotEntries, "snapshot-entries", 100000, "applied entries between local snapshots")
	flag.BoolVar(&localExecutionAPI, "local-execution-api", false, "allow local execution command batches")
	flag.IntVar(&gcPercent, "gc-percent", 100, "garbage collection target percentage")
	flag.Parse()
	peers, err := parsePeers(peerList)
	weights, weightErr := parseWeights(weightList)
	if err != nil || weightErr != nil || id == 0 || directory == "" || listen == "" || lease <= 0 || maxPending < 1 || gcPercent < 1 {
		fmt.Fprintln(os.Stderr, "usage: chronos-coordinator -id <1|2|3> -data <path> -listen <host:port> -peers <id=url,id=url,id=url>")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		if weightErr != nil {
			fmt.Fprintln(os.Stderr, weightErr)
		}
		os.Exit(2)
	}
	debug.SetGCPercent(gcPercent)
	schedulerConfig := scheduler.DefaultConfig()
	schedulerConfig.Namespaces = make(map[string]scheduler.NamespaceConfig, len(weights))
	for namespace, weight := range weights {
		value := schedulerConfig.Default
		value.Weight = weight
		schedulerConfig.Namespaces[namespace] = value
	}
	provider, err := observability.New(context.Background(), observability.Config{
		ServiceName: serviceName, InstanceID: strconv.FormatUint(id, 10), Endpoint: otelEndpoint, Insecure: otelInsecure,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	runtime, err := coordinator.NewRuntime(coordinator.RuntimeConfig{
		ID: id, ClusterID: clusterID, Directory: directory, Listen: listen, WorkerListen: workerListen,
		Peers: peers, Scheduler: schedulerConfig, LeaseMillis: lease.Milliseconds(), MaxPending: maxPending,
		Observability: provider, SnapshotEntries: snapshotEntries,
		LocalExecutionAPI: localExecutionAPI,
		AckFailpoint:      completionAckFailpoint(completionAckFaultFile),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runtime.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func completionAckFailpoint(path string) execution.AckFailpoint {
	if path == "" {
		return nil
	}
	var mutex sync.Mutex
	return func(kind string) error {
		if kind != "completion" {
			return nil
		}
		mutex.Lock()
		defer mutex.Unlock()
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		switch strings.TrimSpace(string(data)) {
		case "armed":
			if err := os.WriteFile(path, []byte("lost"), 0o600); err != nil {
				return err
			}
			return fmt.Errorf("lost completion acknowledgement")
		case "lost":
			return os.WriteFile(path, []byte("retried"), 0o600)
		default:
			return nil
		}
	}
}

func parseWeights(value string) (map[string]int, error) {
	weights := make(map[string]int)
	if strings.TrimSpace(value) == "" {
		return weights, nil
	}
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid namespace weight %q", item)
		}
		weight, err := strconv.Atoi(parts[1])
		if err != nil || weight < 1 {
			return nil, fmt.Errorf("invalid namespace weight %q", item)
		}
		namespace := strings.TrimSpace(parts[0])
		if _, exists := weights[namespace]; exists {
			return nil, fmt.Errorf("duplicate namespace %s", namespace)
		}
		weights[namespace] = weight
	}
	return weights, nil
}

func parsePeers(value string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer %q", item)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer %q", item)
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate peer %d", id)
		}
		peers[id] = parts[1]
	}
	if len(peers) != 3 {
		return nil, fmt.Errorf("exactly three peers are required")
	}
	for id := uint64(1); id <= 3; id++ {
		if peers[id] == "" {
			return nil, fmt.Errorf("peer %d is required", id)
		}
	}
	return peers, nil
}
