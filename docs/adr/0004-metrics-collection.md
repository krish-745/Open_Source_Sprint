# 4. Metrics collection via periodic snapshots

Status: Accepted

## Context
Operators need health and performance visibility (queue depth, worker status, DLQ size) without instrumenting every code path or running an external metrics stack.

## Decision
`MetricsCollector` captures a full system snapshot on an interval and stores it in Redis:

- `startMetricsCollection(intervalMs)` schedules `captureSnapshot` repeatedly.
- Each snapshot aggregates queue stats, per-worker metrics, task totals, and process stats, and is written to `snapshot:<epochMillis>` with a TTL (7 days).
- Read APIs (`getLatestSnapshot`, `getHealthStatus`, queue/worker views) serve the most recent snapshot.

## Consequences
- Pros: cheap to read (one key), self-expiring via TTL, no external dependency.
- Cons: metrics are as fresh as the interval (default 60s), not real-time. `getLatestSnapshot` scans snapshot keys, so very short intervals + long retention increase key counts and read cost; keys must be compared by numeric timestamp.
- Future option: push snapshots to a time-series store or expose a Prometheus endpoint if finer granularity is required.
