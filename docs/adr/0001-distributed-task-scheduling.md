# 1. Distributed task scheduling approach

Status: Accepted

## Context
TaskFlow must schedule and execute tasks across multiple worker processes, support delayed and recurring tasks, and run as more than one instance without double-processing work. There is no dedicated message broker in the stack; Redis is already used for storage.

## Decision
Use Redis as the single coordination backend rather than introducing a separate broker (e.g. RabbitMQ/Kafka).

- Tasks are persisted in Redis and enqueued into per-queue sorted sets keyed by priority (`TaskQueue`).
- A polling scheduler (`TaskScheduler`) periodically promotes due/delayed tasks and dispatches work.
- Multiple instances coordinate through a Redis-based distributed lock so only one scheduler acts at a time.
- Workers pull tasks and report status/heartbeats back through Redis (`WorkerPool`).

## Consequences
- Pros: one infrastructure dependency, simple deployment via `docker-compose`, atomic operations available through Redis commands.
- Cons: polling adds latency versus a push-based broker; throughput is bounded by Redis. Correct locking is essential to avoid duplicate processing (see issues on concurrent schedulers).
- Future option: migrate to Redis Streams or a dedicated broker if throughput requirements outgrow polling.
