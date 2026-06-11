# 2. Redis data structures

Status: Accepted

## Context
Tasks, queues, workers, and metrics each have different access patterns: point lookups, priority ordering, time-range cleanup, counters, and FIFO inspection. We need to map these onto Redis types deliberately.

## Decision
Use a type per access pattern:

| Data | Key | Redis type | Why |
|------|-----|-----------|-----|
| Task record | `task:<id>` | String (JSON) | O(1) point lookup by id |
| Queue (priority) | `queue:<name>` | Sorted set | Score = priority, `zRange REV` for top-N dispatch |
| Global task index | `tasks:index` | Sorted set | Score = creation time, enables time-range cleanup |
| Queue stats | `queue:<name>:stats` | Hash | Per-status counters via `hIncrBy` |
| Dead letter queue | `dlq:tasks` | List | Append-and-inspect of failed tasks |
| Handler → workers | `worker:handlers:map:<handler>` | Set | Membership of workers that can run a handler |
| Workers index | `workers:index` | Sorted set | Enumerate workers, score = registration time |
| Metrics snapshot | `snapshot:<epochMillis>` | String + TTL | Time-series snapshots, expire after retention |

## Consequences
- Pros: each operation uses the cheapest matching Redis command; counters and ordering are server-side.
- Cons: data is spread across several keys, so multi-key updates (status change + stats + queue) are not atomic by default and need care to stay consistent (see queue-stats and race-condition issues).
- Snapshot keys embed a numeric timestamp; comparisons must be numeric, not lexicographic.
