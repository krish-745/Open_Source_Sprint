# TaskFlow - Distributed Task Scheduler

A production-grade distributed task scheduling and execution framework with real-time monitoring, worker management, and advanced features like task dependencies, recurring tasks, and metrics collection.

## Features

- **Task Queue Management**: Create, track, and execute tasks with priority-based scheduling
- **Worker Pool**: Register and manage multiple workers with capacity tracking
- **Task Dependencies**: Support for complex workflows with task dependencies
- **Delayed & Recurring Tasks**: Schedule tasks to run at specific times or on recurring schedules
- **Retry Mechanism**: Automatic retry with configurable max attempts
- **Dead Letter Queue**: Failed tasks moved to DLQ for inspection
- **Real-time Monitoring**: Comprehensive metrics and health checks
- **Distributed Scheduling**: Redis-based locking for multi-instance deployments

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   HTTP API Server                           │
├─────────────────────────────────────────────────────────────┤
│  Task Management │ Worker Management │ Metrics │ Admin      │
├─────────────────────────────────────────────────────────────┤
│                   Service Layer                             │
├─────────────────────────────────────────────────────────────┤
│ TaskQueue │ WorkerPool │ TaskExecutor │ TaskScheduler │ Metrics
├─────────────────────────────────────────────────────────────┤
│                    Redis Backend                            │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Node.js 18+
- Redis 6+ (or Docker)
- npm 9+

## Installation

```bash
npm install
```

## Configuration

Copy the example environment file and adjust as needed:

```bash
cp .env.example .env
```

Available variables:

```env
REDIS_URL=redis://localhost:6379
PORT=3000
LOG_LEVEL=info
```

## Running

### 1. Start Redis

Using Docker (recommended):

```bash
docker-compose up -d
```

Or install Redis locally and start it.

### 2. Start the Application

```bash
# Development
npm run dev

# Production
npm run build
npm start
```

### 3. Run Tests

```bash
npm test

# Watch mode
npm run test:watch
```

## Sample Task Handlers

The application comes with three built-in demo handlers:

| Handler | Description | Simulated Duration |
|---------|-------------|-------------------|
| `dataProcessor` | Processes data records | ~1s |
| `emailSender` | Sends email messages | ~500ms |
| `reportGenerator` | Generates reports | ~2s |

## API Examples

### Create a Task

```bash
curl -X POST http://localhost:3000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Process Data",
    "handler": "dataProcessor",
    "payload": { "dataset": "large_file.csv" },
    "priority": "high",
    "maxRetries": 3,
    "timeout": 60000
  }'
```

### Register a Worker

```bash
curl -X POST http://localhost:3000/api/workers \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Worker-1",
    "handlers": ["dataProcessor", "emailSender"],
    "maxConcurrent": 5,
    "tags": ["cpu-intensive"]
  }'
```

### Get Task

```bash
curl http://localhost:3000/api/tasks/{taskId}
```

### Get Queue Tasks

```bash
curl http://localhost:3000/api/queues/default/tasks?limit=50&offset=0
```

### Health Check

```bash
curl http://localhost:3000/api/health
```

### Worker Heartbeat

```bash
curl -X POST http://localhost:3000/api/workers/{workerId}/heartbeat
```

## API Documentation

A full OpenAPI 3.0 specification is available in [`openapi.yml`](./openapi.yml), covering every endpoint with request/response schemas and examples.

To explore it with Swagger UI:

- Paste the file contents into the [Swagger Editor](https://editor.swagger.io/), or
- Serve it locally:
  ```bash
  npx @redocly/cli preview-docs openapi.yml
  ```

## Project Structure

```
src/
├── index.ts                 # Application entry point & sample handlers
├── types/
│   └── index.ts            # Type definitions
├── services/
│   ├── redis.ts            # Redis client management
│   ├── task-queue.ts       # Task queue operations
│   ├── worker-pool.ts      # Worker management
│   ├── task-executor.ts    # Task execution logic
│   ├── task-scheduler.ts   # Scheduling and delays
│   └── metrics-collector.ts # Metrics and monitoring
├── routes/
│   └── api.ts              # API endpoints
└── utils/
    └── logger.ts           # Logging utility
```

## Task Lifecycle

```
pending → queued → processing → completed
                 ↓
              (error)
                 ↓
               retry → processing → completed
                 ↓
            (max retries)
                 ↓
              failed → dead_letter_queue
```

## Key Concepts

### Tasks
- Atomic units of work with retries and timeouts
- Support for dependencies and scheduling
- Automatic failure handling with exponential backoff

### Workers
- Process tasks from queues
- Report capacity and health metrics
- Support multiple handler types

### Queues
- Priority-based task ordering
- Dedicated queues for different task types
- Statistics and monitoring

### Scheduling
- Delayed task execution
- Recurring tasks with cron expressions
- Distributed lock-based coordination

## Performance Considerations

- Use task dependencies sparingly to avoid deadlocks
- Set appropriate timeouts for tasks
- Monitor worker capacity and scale accordingly
- Regular cleanup of completed tasks
- Use appropriate queue priorities

## Security

- Validate all task payloads
- Implement authentication for worker registration
- Encrypt sensitive data in payloads
- Monitor task execution for anomalies

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, workflow guidelines, and how to claim issues.

## License

MIT — see [LICENSE](LICENSE) for details.
