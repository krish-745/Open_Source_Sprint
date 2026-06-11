# Contributing to TaskFlow

Thank you for contributing to TaskFlow! This guide will help you get started.

## Prerequisites

- Node.js 18+
- Redis 6+ (or Docker)
- npm 9+

## Getting Started

1. **Fork and clone** the repository
2. **Install dependencies**
   ```bash
   npm install
   ```
3. **Start Redis** (using Docker):
   ```bash
   docker-compose up -d
   ```
4. **Create environment file**
   ```bash
   cp .env.example .env
   ```
5. **Run in development mode**
   ```bash
   npm run dev
   ```
6. **Run tests**
   ```bash
   npm test
   ```

## Claiming Issues

1. Check the [Issues](../../issues) page for open issues
2. Comment on the issue you want to work on
3. Wait for a maintainer to assign it to you
4. Only work on assigned issues to avoid duplicate effort

## Issue Difficulty Levels

| Label | Level | Description |
|-------|-------|-------------|
| **LOW** | Beginner | Good first issues, simple bug fixes |
| **MEDIUM** | Intermediate | Feature additions, moderate complexity |
| **HIGH** | Advanced | Complex bugs, architecture changes |
| **VERY HIGH** | Expert | Research-level, deep system knowledge |

## Development Workflow

1. Create a new branch from `main`:
   ```bash
   git checkout -b fix/issue-number-short-description
   ```
2. Make your changes following the code style
3. Write or update tests
4. Ensure all tests pass:
   ```bash
   npm test
   ```
5. Ensure TypeScript compiles without errors:
   ```bash
   npm run type-check
   ```
6. Commit with a clear message:
   ```bash
   git commit -m "fix(task-queue): add cycle detection for dependencies (#2)"
   ```
7. Push and open a Pull Request

## Code Style

- Follow TypeScript strict mode
- Use descriptive variable names
- Add JSDoc comments for public methods
- Keep functions focused and small
- Handle errors explicitly

## Pull Request Guidelines

- Reference the issue number in your PR title (e.g., `Fix #1: Race condition in task status update`)
- Include a description of what changed and why
- Add tests for new features or bug fixes
- Update documentation if needed
- Ensure CI checks pass
- Request review from a maintainer

## Project Structure

```
src/
├── index.ts                 # Application entry point
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

## Need Help?

- Check existing issues and PRs for context
- Ask questions on the issue thread
- Reach out to maintainers

Happy contributing! 🚀
