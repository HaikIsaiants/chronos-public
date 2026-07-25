package observability

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/HaikIsaiants/chronos-public/internal/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/stats"
)

type Config struct {
	ServiceName string
	InstanceID  string
	Endpoint    string
	Insecure    bool
}

type Provider struct {
	Registry          *prometheus.Registry
	traces            *sdktrace.TracerProvider
	propagator        propagation.TextMapPropagator
	ready             *prometheus.GaugeVec
	pending           prometheus.Gauge
	dispatch          prometheus.Histogram
	retries           prometheus.Counter
	workerCredits     *prometheus.GaugeVec
	workerInflight    *prometheus.GaugeVec
	workerUtilization *prometheus.GaugeVec
	commits           prometheus.Counter
	committedEvents   prometheus.Counter
	elections         prometheus.Counter
	snapshots         *prometheus.CounterVec
	snapshotDuration  *prometheus.HistogramVec
	snapshotBytes     *prometheus.HistogramVec
	workflowDuration  *prometheus.HistogramVec
	raftLeader        prometheus.Gauge
	raftTerm          prometheus.Gauge
	raftCommitIndex   prometheus.Gauge
	raftAppliedIndex  prometheus.Gauge
	workflowMu        sync.Mutex
	workflowStarts    map[string]int64
}

func New(ctx context.Context, config Config) (*Provider, error) {
	if config.ServiceName == "" {
		config.ServiceName = "chronos-coordinator"
	}
	attributes := []attribute.KeyValue{attribute.String("service.name", config.ServiceName)}
	if config.InstanceID != "" {
		attributes = append(attributes, attribute.String("service.instance.id", config.InstanceID))
	}
	resources, err := resource.New(ctx, resource.WithAttributes(attributes...))
	if err != nil {
		return nil, err
	}
	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(resources)}
	if config.Endpoint != "" {
		exporterOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
		if config.Insecure {
			exporterOptions = append(exporterOptions, otlptracegrpc.WithInsecure())
		}
		exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	provider := &Provider{
		Registry: prometheus.NewRegistry(), traces: sdktrace.NewTracerProvider(options...),
		propagator:        propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
		workflowStarts:    make(map[string]int64),
		ready:             prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "chronos_queue_ready_tasks", Help: "Ready tasks waiting for dispatch."}, []string{"namespace"}),
		pending:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "chronos_pending_proposals", Help: "Client proposals waiting for durable application."}),
		dispatch:          prometheus.NewHistogram(prometheus.HistogramOpts{Name: "chronos_dispatch_commit_seconds", Help: "Time from scheduler selection to durable task start.", Buckets: prometheus.DefBuckets}),
		retries:           prometheus.NewCounter(prometheus.CounterOpts{Name: "chronos_task_retries_total", Help: "Durable semantic task retries scheduled."}),
		workerCredits:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "chronos_worker_credits", Help: "Dispatch credits advertised by a worker."}, []string{"worker"}),
		workerInflight:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "chronos_worker_inflight", Help: "Active attempts assigned to a worker."}, []string{"worker"}),
		workerUtilization: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "chronos_worker_utilization_ratio", Help: "Fraction of worker credits in use."}, []string{"worker"}),
		commits:           prometheus.NewCounter(prometheus.CounterOpts{Name: "chronos_raft_commits_total", Help: "Raft entries durably applied locally."}),
		committedEvents:   prometheus.NewCounter(prometheus.CounterOpts{Name: "chronos_raft_committed_events_total", Help: "Workflow events durably applied locally."}),
		elections:         prometheus.NewCounter(prometheus.CounterOpts{Name: "chronos_raft_elections_total", Help: "Terms in which this node became leader."}),
		snapshots:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "chronos_snapshots_total", Help: "Snapshot operations by kind and result."}, []string{"kind", "result"}),
		snapshotDuration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "chronos_snapshot_seconds", Help: "Snapshot operation duration.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 10)}, []string{"kind"}),
		snapshotBytes:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "chronos_snapshot_bytes", Help: "Snapshot payload size.", Buckets: prometheus.ExponentialBuckets(1024, 4, 12)}, []string{"kind"}),
		workflowDuration:  prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "chronos_workflow_duration_seconds", Help: "Logical workflow duration by terminal status.", Buckets: prometheus.ExponentialBuckets(0.001, 4, 12)}, []string{"status"}),
		raftLeader:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "chronos_raft_leader", Help: "One when this coordinator is the current leader."}),
		raftTerm:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "chronos_raft_term", Help: "Current Raft term."}),
		raftCommitIndex:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "chronos_raft_commit_index", Help: "Current Raft commit index."}),
		raftAppliedIndex:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "chronos_raft_applied_index", Help: "Current local applied index."}),
	}
	provider.Registry.MustRegister(
		provider.ready, provider.pending, provider.dispatch, provider.retries,
		provider.workerCredits, provider.workerInflight, provider.workerUtilization,
		provider.commits, provider.committedEvents, provider.elections, provider.snapshots,
		provider.snapshotDuration, provider.snapshotBytes, provider.workflowDuration,
		provider.raftLeader, provider.raftTerm, provider.raftCommitIndex, provider.raftAppliedIndex,
	)
	return provider, nil
}

func (p *Provider) Close(ctx context.Context) error {
	return p.traces.Shutdown(ctx)
}

func (p *Provider) Tracer() trace.Tracer {
	return p.traces.Tracer("github.com/HaikIsaiants/chronos-public")
}

func (p *Provider) HTTPHandler(name string, handler http.Handler) http.Handler {
	return otelhttp.NewHandler(handler, name, otelhttp.WithTracerProvider(p.traces), otelhttp.WithPropagators(p.propagator))
}

func (p *Provider) HTTPTransport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base, otelhttp.WithTracerProvider(p.traces), otelhttp.WithPropagators(p.propagator))
}

func (p *Provider) GRPCServerHandler() stats.Handler {
	return otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(p.traces), otelgrpc.WithPropagators(p.propagator))
}

func (p *Provider) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(p.Registry, promhttp.HandlerOpts{})
}

func (p *Provider) SetQueue(namespaces map[string]int, pending int) {
	p.ready.Reset()
	for namespace, count := range namespaces {
		p.ready.WithLabelValues(namespace).Set(float64(count))
	}
	p.pending.Set(float64(pending))
}

func (p *Provider) ObserveDispatch(duration time.Duration) {
	p.dispatch.Observe(duration.Seconds())
}

func (p *Provider) SetWorker(worker string, credits uint32, inflight int) {
	p.workerCredits.WithLabelValues(worker).Set(float64(credits))
	p.workerInflight.WithLabelValues(worker).Set(float64(inflight))
	utilization := 0.0
	if credits > 0 {
		utilization = float64(inflight) / float64(credits)
	}
	p.workerUtilization.WithLabelValues(worker).Set(utilization)
}

func (p *Provider) DeleteWorker(worker string) {
	p.workerCredits.DeleteLabelValues(worker)
	p.workerInflight.DeleteLabelValues(worker)
	p.workerUtilization.DeleteLabelValues(worker)
}

func (p *Provider) RecordCommit(entries int, events []core.Event) {
	p.commits.Add(float64(entries))
	p.committedEvents.Add(float64(len(events)))
	for _, event := range events {
		if event.Kind == core.EventWorkflowSubmitted {
			p.workflowMu.Lock()
			p.workflowStarts[event.WorkflowID] = event.At
			p.workflowMu.Unlock()
		}
		if event.Kind == core.EventTaskRetryScheduled {
			p.retries.Inc()
		}
		if !terminalEvent(event.Kind) {
			continue
		}
		p.workflowMu.Lock()
		started, exists := p.workflowStarts[event.WorkflowID]
		delete(p.workflowStarts, event.WorkflowID)
		p.workflowMu.Unlock()
		if !exists || event.At < started {
			continue
		}
		p.workflowDuration.WithLabelValues(terminalStatus(event.Kind)).Observe(float64(event.At-started) / 1000)
	}
}

func (p *Provider) SeedWorkflowStart(workflowID string, at int64) {
	p.workflowMu.Lock()
	defer p.workflowMu.Unlock()
	if _, exists := p.workflowStarts[workflowID]; !exists {
		p.workflowStarts[workflowID] = at
	}
}

func (p *Provider) RecordElection() {
	p.elections.Inc()
}

func (p *Provider) SetRaft(leader bool, term, commit, applied uint64) {
	if leader {
		p.raftLeader.Set(1)
	} else {
		p.raftLeader.Set(0)
	}
	p.raftTerm.Set(float64(term))
	p.raftCommitIndex.Set(float64(commit))
	p.raftAppliedIndex.Set(float64(applied))
}

func (p *Provider) RecordSnapshot(kind string, size int, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}
	p.snapshots.WithLabelValues(kind, result).Inc()
	p.snapshotDuration.WithLabelValues(kind).Observe(duration.Seconds())
	if size > 0 {
		p.snapshotBytes.WithLabelValues(kind).Observe(float64(size))
	}
}

func terminalEvent(kind core.EventKind) bool {
	switch kind {
	case core.EventWorkflowCompleted, core.EventWorkflowFailed, core.EventWorkflowCancelled, core.EventWorkflowCompensated:
		return true
	default:
		return false
	}
}

func terminalStatus(kind core.EventKind) string {
	switch kind {
	case core.EventWorkflowCompleted:
		return string(core.WorkflowCompleted)
	case core.EventWorkflowFailed:
		return string(core.WorkflowFailed)
	case core.EventWorkflowCancelled:
		return string(core.WorkflowCancelled)
	default:
		return string(core.WorkflowCompensated)
	}
}
