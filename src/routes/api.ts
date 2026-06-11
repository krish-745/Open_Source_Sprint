import express, { Request, Response, NextFunction } from 'express';
import { TaskQueue, QueueFullError } from '../services/task-queue';
import { WorkerPool } from '../services/worker-pool';
import { TaskExecutor } from '../services/task-executor';
import { TaskScheduler } from '../services/task-scheduler';
import { MetricsCollector } from '../services/metrics-collector';
import { getRedisStatus } from '../services/redis';
import logger from '../utils/logger';

const router = express.Router();

// Middleware for logging
router.use((req: Request, res: Response, next: NextFunction) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    logger.info({ method: req.method, path: req.path, status: res.statusCode, duration });
  });
  next();
});

// Task endpoints

router.post('/tasks', async (req: Request, res: Response) => {
  try {
    const { name, handler, payload, queueName, priority, maxRetries, timeout, tags } = req.body;

    if (!name || !handler) {
      return res.status(400).json({ error: 'Missing required fields: name, handler' });
    }

    if (!TaskExecutor.hasHandler(handler)) {
      return res.status(400).json({ error: `Unknown handler: ${handler}` });
    }

    const task = await TaskQueue.createTask(name, handler, payload || {}, {
      queueName,
      priority,
      maxRetries,
      timeout,
      tags,
    });

    res.status(201).json(task);
  } catch (error: any) {
    logger.error({ error }, 'Create task error');
    
    if (error instanceof QueueFullError) {
      return res.status(429).json({ error: error.message });
    }
    
    res.status(500).json({ error: error.message });
  }
});

router.post('/tasks/batch', async (req: Request, res: Response) => {
  try {
    const { tasks } = req.body;

    if (!Array.isArray(tasks) || tasks.length === 0) {
      return res.status(400).json({ error: 'tasks must be a non-empty array' });
    }

    // Validate everything before creating anything.
    for (const [index, t] of tasks.entries()) {
      if (!t.name || !t.handler) {
        return res.status(400).json({ error: `Invalid task at index ${index}: name and handler are required` });
      }
      if (!TaskExecutor.hasHandler(t.handler)) {
        return res.status(400).json({ error: `Unknown handler at index ${index}: ${t.handler}` });
      }
    }

    const created = await TaskQueue.createTasksBatch(
      tasks.map((t: any) => ({
        name: t.name,
        handler: t.handler,
        payload: t.payload,
        options: {
          queueName: t.queueName,
          priority: t.priority,
          maxRetries: t.maxRetries,
          timeout: t.timeout,
          tags: t.tags,
        },
      }))
    );

    res.status(201).json({ count: created.length, taskIds: created.map((t) => t.id) });
  } catch (error: any) {
    logger.error({ error }, 'Batch create error');
    res.status(400).json({ error: error.message });
  }
});

router.get('/tasks/:taskId', async (req: Request, res: Response) => {
  try {
    const { taskId } = req.params;
    const task = await TaskQueue.getTask(taskId);

    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    res.json(task);
  } catch (error: any) {
    logger.error({ error }, 'Get task error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/queues/:queueName/tasks', async (req: Request, res: Response) => {
  try {
    const { queueName } = req.params;
    const { limit = '100', offset = '0' } = req.query;

    const tasks = await TaskQueue.getQueueTasks(queueName, parseInt(limit as string), parseInt(offset as string));
    const stats = await TaskQueue.getQueueStats(queueName);

    res.json({ tasks, stats });
  } catch (error: any) {
    logger.error({ error }, 'Get queue tasks error');
    res.status(500).json({ error: error.message });
  }
});

// Worker endpoints

router.post('/workers', async (req: Request, res: Response) => {
  try {
    const { name, handlers, maxConcurrent, version, tags } = req.body;

    if (!name || !handlers || !Array.isArray(handlers)) {
      return res.status(400).json({ error: 'Missing required fields: name, handlers (array)' });
    }

    const worker = await WorkerPool.registerWorker(name, handlers, {
      maxConcurrent,
      version,
      tags,
    });

    res.status(201).json(worker);
  } catch (error: any) {
    logger.error({ error }, 'Register worker error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/workers/:workerId', async (req: Request, res: Response) => {
  try {
    const { workerId } = req.params;
    const worker = await WorkerPool.getWorker(workerId);

    if (!worker) {
      return res.status(404).json({ error: 'Worker not found' });
    }

    res.json(worker);
  } catch (error: any) {
    logger.error({ error }, 'Get worker error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/workers/:workerId/metrics', async (req: Request, res: Response) => {
  try {
    const { workerId } = req.params;
    const metrics = await WorkerPool.getWorkerMetrics(workerId);
    res.json(metrics);
  } catch (error: any) {
    logger.error({ error }, 'Get worker metrics error');
    res.status(500).json({ error: error.message });
  }
});

router.post('/workers/:workerId/heartbeat', async (req: Request, res: Response) => {
  try {
    const { workerId } = req.params;
    const exists = await WorkerPool.heartbeat(workerId);

    if (!exists) {
      return res.status(404).json({ error: 'Worker not found' });
    }

    res.json({ success: true });
  } catch (error: any) {
    logger.error({ error }, 'Heartbeat error');
    res.status(500).json({ error: error.message });
  }
});

// Metrics endpoints

router.get('/health', async (req: Request, res: Response) => {
  try {
    const health = await MetricsCollector.getHealthStatus();
    res.json(health);
  } catch (error: any) {
    logger.error({ error }, 'Health check error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/health/redis', (req: Request, res: Response) => {
  const status = getRedisStatus();
  res.status(status.connected ? 200 : 503).json(status);
});

router.get('/metrics', async (req: Request, res: Response) => {
  try {
    const snapshot = await MetricsCollector.getLatestSnapshot();
    res.json(snapshot);
  } catch (error: any) {
    logger.error({ error }, 'Get metrics error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/metrics/queues', async (req: Request, res: Response) => {
  try {
    const metrics = await MetricsCollector.getDetailedQueueMetrics();
    res.json(metrics);
  } catch (error: any) {
    logger.error({ error }, 'Get queue metrics error');
    res.status(500).json({ error: error.message });
  }
});

router.get('/metrics/workers', async (req: Request, res: Response) => {
  try {
    const metrics = await MetricsCollector.getWorkerPerformance();
    res.json(metrics);
  } catch (error: any) {
    logger.error({ error }, 'Get worker metrics error');
    res.status(500).json({ error: error.message });
  }
});

// Admin endpoints

router.post('/cleanup', async (req: Request, res: Response) => {
  try {
    const { hoursAgo = 24 } = req.body;
    const deleted = await TaskQueue.cleanupOldTasks(hoursAgo);
    res.json({ deleted, hoursAgo });
  } catch (error: any) {
    logger.error({ error }, 'Cleanup error');
    res.status(500).json({ error: error.message });
  }
});

router.post('/workers/:workerId/unregister', async (req: Request, res: Response) => {
  try {
    const { workerId } = req.params;
    await WorkerPool.unregisterWorker(workerId);
    res.json({ success: true });
  } catch (error: any) {
    logger.error({ error }, 'Unregister worker error');
    res.status(500).json({ error: error.message });
  }
});

export default router;
