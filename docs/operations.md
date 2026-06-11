# Operational Runbook

Operating TaskFlow in production: deploy, scale, monitor, and recover.

## Overview

TaskFlow is a Node.js/TypeScript service backed by Redis. A single process exposes the HTTP API, runs the polling scheduler, and collects metrics. Redis is the only external dependency.

| Component | Where |
|-----------|-------|
| HTTP API + scheduler + metrics | `node dist/index.js` (`npm start`) |
| State / coordination | Redis |
| Config | environment variables (see below) |

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `PORT` | `3000` | HTTP listen port |
| `LOG_LEVEL` | `info` | Log verbosity |

## Deploy

1. Provision Redis 6+ (managed service or container). For local/dev:
   ```bash
   docker-compose up -d   # starts redis:7-alpine with a healthcheck
   ```
2. Build and start the app:
   ```bash
   npm ci
   npm run build
   REDIS_URL=redis://<host>:6379 PORT=3000 npm start
   ```
3. Verify:
   ```bash
   curl http://localhost:3000/healthz        # {"status":"ok"} — process is up
   curl http://localhost:3000/api/health     # aggregated system health
   ```

Graceful shutdown is handled on `SIGTERM`/`SIGINT`: the scheduler and metrics loop stop and Redis is closed before exit.

## Scale workers

Workers are registered via the API and tracked in Redis.

- Register a worker:
  ```bash
  curl -X POST http://localhost:3000/api/workers \
    -H 'Content-Type: application/json' \
    -d '{"name":"worker-1","handlers":["reportGenerator"],"maxConcurrent":5}'
  ```
- Increase throughput by registering more workers and/or raising `maxConcurrent`.
- Keep workers alive with periodic heartbeats:
  ```bash
  curl -X POST http://localhost:3000/api/workers/<workerId>/heartbeat
  ```
- Dispatch favours the least-busy worker with free capacity for the requested handler.

## Monitor health

| Check | Endpoint |
|-------|----------|
| Liveness | `GET /healthz` |
| System health (workers online, DLQ size) | `GET /api/health` |
| Latest snapshot | `GET /api/metrics` |
| Queue metrics | `GET /api/metrics/queues` |
| Worker performance | `GET /api/metrics/workers` |

Health degrades to `degraded`/`critical` when no/low workers are online or the dead letter queue grows large (>100).

## Handle failures

- **Redis unreachable**: the client retries with backoff (up to 10 attempts). If `getRedisClient` throws "not initialized", the process started before Redis was ready — restart after Redis is healthy.
- **Tasks stuck pending**: confirm at least one worker is registered for the task's `handler` and has free capacity (`GET /api/metrics/workers`).
- **Failed tasks**: tasks that exhaust `maxRetries` move to the dead letter queue (`dlq:tasks`). A growing DLQ indicates a systematic handler or downstream failure.
- **Stale workers**: workers that stop heartbeating are marked offline by the stale-worker check; re-register or restart them.

## Debugging

- Increase log detail with `LOG_LEVEL=debug`.
- Inspect Redis directly:
  ```bash
  redis-cli KEYS 'task:*'              # task records
  redis-cli ZCARD queue:default        # queue depth
  redis-cli HGETALL queue:default:stats # per-status counters
  redis-cli LLEN dlq:tasks             # dead letter queue size
  ```
- Fetch a single task: `GET /api/tasks/<taskId>`.

## Maintenance

- Clean up old completed/failed tasks:
  ```bash
  curl -X POST http://localhost:3000/api/cleanup -H 'Content-Type: application/json' -d '{"hoursAgo":24}'
  ```

## Troubleshooting quick reference

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| `/healthz` fails | process down | check logs, restart |
| `/api/health` = critical | no workers online | register/restart workers |
| Tasks never run | no worker for handler / no capacity | add workers, raise `maxConcurrent` |
| DLQ growing | handler or downstream failing | inspect `dlq:tasks`, fix handler |
| "Redis client not initialized" | started before Redis ready | ensure Redis up, restart app |
