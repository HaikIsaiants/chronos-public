# Benchmarks

## Engine benchmark

```text
go run ./cmd/chronos-bench -workflows 1000
```

This runs deterministic workflows and prints duration, command count, and transition count. Increase `-workflows` for a longer run.

## Recovery benchmark

```text
go run ./cmd/chronos-bench -workflows 1000 -tail-commands 1200 -recovery-directory .data/recovery-benchmark
```

This restores a snapshot, replays a committed log tail, and compares the recovered workflows with the originals. `-tail-commands` accepts up to twice the workflow count and defaults to one command per workflow. Use a new empty directory for each run.

## Cluster load

Start a coordinator cluster and worker, then run:

```text
go run ./cmd/chronos-load -coordinator http://127.0.0.1:7101 -namespaces 3 -workflows 1000 -concurrency 16 -task-type work -wait
```

The load generator distributes workflows across namespaces. Add `-wait` to include completion time.

## Cluster transition throughput

Start each coordinator with `-local-execution-api`, use a fresh data directory, and run:

```text
go run ./cmd/chronos-load -coordinator http://127.0.0.1:7101 -local-execution -workflows 10000 -concurrency 128 -batch-size 15 -timeout 5m
```

Local execution sends the eight-task workload through Raft and Pebble without involving worker dispatch. It prints workflow count, transition count, runtime, and average rate. Use `-start-index` to avoid reusing workflow identifiers when running against an existing cluster.

## Process test

```text
powershell -File scripts/process-e2e.ps1
```

The process test kills a leader and a worker, retries a dropped acknowledgement, and checks fencing and snapshot recovery. It also prints the time until the new leader is ready.

## Results

The recorded run from July 21, 2026 used Windows, 32 logical CPUs, and `GOMAXPROCS=4`.

| Measurement | Result | Context |
| --- | ---: | --- |
| Coordinator transitions | 50,271 per second | Lowest 60-second window in a 64-second measurement |
| Recovery | 15.191 seconds | 100,000 workflows, 120,000 tail commands, and no state or version mismatches |
| Leader failover | 1.912 seconds | One trial with no conflicting sequences or lost acknowledgements |

The same numbers are in [results.json](results.json).
