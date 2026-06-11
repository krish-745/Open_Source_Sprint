# TaskFlow - Operational Runbook

This runbook provides guidelines for deploying, scaling, monitoring, and troubleshooting TaskFlow in production environments.

---

## 1. Deployment Guide

### Prerequisites
*   Node.js 18+ and npm 9+
*   Redis 6+ (highly available Cluster or Sentinel setup recommended for production)

### Environment Configuration
Configure environment variables using a `.env` file or direct container injection:
```env
REDIS_URL=redis://localhost:6379
PORT=3000
LOG_LEVEL=info
```

### Manual Deployment
1.  Clone the repository and install production dependencies:
    ```bash
    npm ci --only=production
    ```
2.  Build the TypeScript codebase:
    ```bash
    npm run build
    ```
3.  Start the application using a process manager like PM2:
    ```bash
    pm2 start dist/index.js --name "taskflow-api"
    ```

### Docker Deployment
Run using Docker Compose:
```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
  taskflow:
    build: .
    ports:
      - "3000:3000"
    environment:
      - REDIS_URL=redis://redis:6379
      - PORT=3000
      - LOG_LEVEL=info
    depends_on:
      - redis
volumes:
  redis_data:
```

---

## 2. Scaling Workers

TaskFlow scales horizontally by running multiple worker processes subscribing to the same queues.

### Horizontal Scaling
To spawn additional workers, run more worker instances pointing to the same Redis URL:
```bash
# Registering a new worker process (e.g. Worker-2)
curl -X POST http://localhost:3000/api/workers \
  -H "Content-Type: application/json" \
  -d '{"name": "Worker-2", "handlers": ["dataProcessor"], "maxConcurrent": 10}'
```

### Concurrency Tuning
*   **CPU-bound tasks**: Set `maxConcurrent` to match the number of physical cores available on the worker's machine.
*   **I/O-bound tasks**: Concurrency can be set higher (e.g. 10–20 concurrent tasks) to maximize throughput.

---

## 3. Health Monitoring

Monitor the overall system health and database activity to ensure reliable task dispatch.

### Health Check Endpoint
Query `/api/health` regularly (e.g., using Prometheus or Uptime Kuma):
```json
{
  "status": "healthy",
  "workers": {
    "online": 2,
    "total": 2
  },
  "queues": 1,
  "dlqSize": 0,
  "issues": [],
  "timestamp": "2026-06-11T12:00:00.000Z"
}
```
*   **Degraded status**: Trigger alerts if status returns `degraded` (e.g., some workers offline, or DLQ size > 100).
*   **Critical status**: Trigger paging alerts if status returns `critical` (e.g., 0 online workers).

### Key Performance Indicators (KPIs)
*   **Queue Latency**: Measure how long tasks remain in the `pending` state before moving to `processing`.
*   **Task Success Rate**: Keep `successRate` (retrieved via `/api/workers/:id/metrics`) above 98%.
*   **DLQ Size**: Must remain near 0. A growing Dead Letter Queue indicates systemic code failures or service outages.

---

## 4. Failure Handling

### Stale Worker Detection
If a worker process crashes or loses network connectivity:
*   The worker stops sending heartbeats.
*   The scheduler runs `WorkerPool.checkStaleWorkers()` every minute.
*   Any worker without a heartbeat for 60 seconds is marked `offline`.

### Dead Letter Queue (DLQ) Recovery
Tasks that exceed `maxRetries` are sent to `dlq:tasks`. To recover:
1.  Inspect the failure reason by retrieving tasks from the DLQ list in Redis.
2.  Fix the underlying worker logic, database issue, or external dependency.
3.  Re-queue the task by creating a new task containing the same payload, or use the DLQ retry endpoints.

---

## 5. Debugging & Troubleshooting

### Adjusting Log Levels
In the `.env` file, set `LOG_LEVEL` to `debug` for detailed tracing:
```env
LOG_LEVEL=debug
```

### Redis Inspection
Use `redis-cli` to inspect the internal queue state:
```bash
# List all active queues
keys queue:*

# Get the size of the default queue
zcard queue:default

# Inspect details of a specific task
get task:<taskId>
```

### Common Troubleshooting Scenarios

#### Scenario A: Tasks remain stuck in "pending" or "queued"
*   **Cause**: No online workers support the requested task `handler`, or all online workers are at maximum capacity.
*   **Resolution**: Check `/api/health` and verify that workers are online and their `capacity` is not at 100%. Register a new worker for the task handler if necessary.

#### Scenario B: Redis Connection Timeout Errors
*   **Cause**: Redis server is overloaded, down, or network issues are preventing connectivity.
*   **Resolution**: Check Redis resource consumption (CPU, memory). Verify connectivity with `redis-cli ping`. Implement connection pooling or replication if throughput is extremely high.
