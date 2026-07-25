# Chronos Observability

Each coordinator exposes local Prometheus metrics at `/metrics`. `prometheus.yml` scrapes the three default local coordinator addresses. `grafana/chronos-overview.json` can be imported into Grafana with a Prometheus data source selected during import.

The dashboard uses `max` for replicated counters so each event is counted once. Election counters are summed across the cluster.

Set `-otel-endpoint` to export traces over OTLP gRPC. For a local collector without TLS, also set `-otel-insecure`.
