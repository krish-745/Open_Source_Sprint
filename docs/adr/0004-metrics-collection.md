# ADR 0004: Metrics Collection

## Status
Proposed

## Context
Operating a distributed task scheduler requires visibility into queue health, throughput, worker utilization, and error rates. We need to collect and query metrics without introducing write-heavy bottlenecks.

## Decision
We implement a **Periodic Snapshotting and TTL-based Retention** model for metrics:
1.  **Task Execution Metrics**: Worker execution success, duration, and metadata are saved immediately upon task completion to `metrics:worker:<workerId>:<timestamp>`.
2.  **System Snapshots**: A dedicated routine polls queues, workers, and system resources periodically (e.g. every 60 seconds) to capture aggregate state, saving to `snapshot:<timestamp>`.
3.  **Retention Policy**: All metrics keys and snapshots are written with a Redis Time-To-Live (TTL) of 7 days (`EX: 7 * 24 * 60 * 60`).
4.  **Querying**: The dashboard and health endpoints read the latest snapshot to calculate health indexes.

## Consequences
*   **Pros**:
    *   Saves memory by automatically purging metrics data after 7 days.
    *   Separates real-time task writes from intensive queue aggregation queries.
    *   Allows building historically trended charts over a 7-day window.
*   **Cons**:
    *   No long-term metrics storage; historical metrics are lost after 7 days unless exported externally.
