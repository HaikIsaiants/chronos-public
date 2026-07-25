# Architecture

## Workflow state

The core is an event-sourced state machine. A command is checked against the current workflow and produces an ordered batch of events. The engine applies that batch to a copy first, so a bad transition cannot leave half of a command behind.

Time and identifiers come from commands and event envelopes. The reducer has no clock, storage access, or random source, which keeps replay deterministic. Request IDs are global and are paired with a hash of the command. Retrying the same request returns its stored result. Reusing an ID for different work is rejected.

Tasks become ready after their dependencies finish. When a task starts, it receives its direct dependency outputs. Scheduled starts, retries, and timeouts all use durable timer records rebuilt from workflow state after a restart.

Fan-out is recorded as one expansion event. Child IDs come from the workflow, parent task, and item key, and the aggregate task gains those children as dependencies in the same event.

## Replication and storage

Chronos runs three coordinators, each with an `etcd/raft` state machine and a Pebble database. Only the leader builds command batches. Followers redirect clients and apply committed entries in the same order as the leader.

Raft state is synced before messages that depend on it are sent. After an entry commits, its workflow changes, events, request receipts, task results, and applied index are written in one synced Pebble batch. The client is answered after that write finishes.

New Raft entries use Snappy-compressed Gob blocks. The decoder still accepts the earlier JSON form. Application records are compressed and grouped into workflow and applied-index segments instead of storing one Pebble record per transition.

Command responses include the applied Raft index and term. Those values are kept with the request receipt, so a retry after restart or leader failover returns the original proof and result.

Snapshots contain workflow state, request outcomes, and task results. Installing one replaces the application keyspaces and then applies later committed entries. Workflow events remain in Pebble when the Raft log is compacted. On an ordinary restart, Chronos loads materialized state and checks that the remaining application log agrees with Raft.

Linearizable reads use Raft `ReadIndex` and wait for the local applied index to reach the returned barrier.

## Workers and scheduling

Workers connect to the leader over a bidirectional gRPC stream. The first message gives the worker ID, supported task types, and available credits. Reconnecting with the same ID replaces the old session and redelivers any active assignments found in committed workflow state.

A disconnect does not revoke work immediately. Its leases stay active until their deadlines, which prevents the scheduler from handing the same current attempt to another worker during a short reconnect.

The scheduler matches ready tasks to capabilities and credits, then chooses namespaces by configured weight. Its fairness cursor advances only after the selected start command commits. An assignment is sent after the lease is durable.

Cluster and namespace limits bound admitted and running work. Batch admission is atomic. A separate pending-request limit returns `429 backpressure` when the HTTP proposal queue is full.

## Leases and failures

Starting a task creates a lease with an attempt ID, stable idempotency key, deadline, worker ID, and fencing token. Renewals extend the deadline without changing the attempt or token. A result is accepted only when it matches the active lease. Expired work can be assigned again with a higher token.

Failures and timeouts use the configured retry budget. Lease expiry does not. Durable timers handle backoff and ensure that completion, failure, and timeout races settle on one result for an attempt.

Cancellation invalidates running work and pending timers. If completed tasks have compensation handlers, they run one at a time in reverse completion order using the same lease and retry machinery.

Chronos delivers assignments and results at least once. Task handlers use the supplied idempotency key and fencing token to protect external side effects. The bundled worker is a simulator.

## Observability

Coordinators expose Prometheus metrics and carry OpenTelemetry context across HTTP, Raft, and worker traffic. The Grafana dashboard uses one replicated counter value rather than summing the same committed event across all three nodes.
