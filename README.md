# Chronos

Chronos is a distributed workflow engine built around deterministic event-sourced state, Raft consensus, and Pebble storage.

## Features

- Three-node replicated coordinator cluster using `etcd/raft`
- Durable workflow state, event history, snapshots, and request deduplication
- Scheduled starts, retries, timeouts, cancellation, fan-out, aggregation, and compensation
- Worker leases with capability matching, credits, idempotency keys, and fencing tokens
- Linearizable reads, Prometheus metrics, and OpenTelemetry tracing

## Run a workflow

```text
go run ./cmd/chronos run ./examples/release.json
```

## Run a coordinator cluster

Start one coordinator in each of three terminals:

```text
go run ./cmd/chronos-coordinator -id 1 -cluster local -data .data/node-1 -listen 127.0.0.1:7101 -worker-listen 127.0.0.1:7201 -peers 1=http://127.0.0.1:7101,2=http://127.0.0.1:7102,3=http://127.0.0.1:7103
go run ./cmd/chronos-coordinator -id 2 -cluster local -data .data/node-2 -listen 127.0.0.1:7102 -worker-listen 127.0.0.1:7202 -peers 1=http://127.0.0.1:7101,2=http://127.0.0.1:7102,3=http://127.0.0.1:7103
go run ./cmd/chronos-coordinator -id 3 -cluster local -data .data/node-3 -listen 127.0.0.1:7103 -worker-listen 127.0.0.1:7203 -peers 1=http://127.0.0.1:7101,2=http://127.0.0.1:7102,3=http://127.0.0.1:7103
```

`GET /v1/status` returns the current leader. Followers redirect writes and linearizable workflow reads to it. `POST /v1/commands` submits one workflow, `POST /v1/batches` submits a batch, and `POST /v1/snapshot` creates a snapshot and compacts the Raft log.

Start a worker after the cluster is available:

```text
go run ./cmd/chronos-worker -coordinators 127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203 -id worker-1 -capabilities work -credits 4
```

The worker reconnects after leader changes. Coordinator flags control namespace weights, lease duration, and the pending-request limit.

## Test

```text
go test ./...
go test -race ./...
go vet ./...
powershell -File scripts/process-e2e.ps1
```

## Load and benchmark

An in-process engine benchmark:

```text
go run ./cmd/chronos-bench -workflows 1000
```

To send workflows through a running cluster:

```text
go run ./cmd/chronos-load -coordinator http://127.0.0.1:7101 -namespaces 3 -workflows 1000 -concurrency 16 -task-type work -wait
```

Recovery, replicated throughput, and failover runs are in [Benchmarks](benchmarks/README.md).

## More information

- [Architecture](docs/architecture.md)
- [Safety models](formal/README.md)
- [Observability](observability/README.md)
