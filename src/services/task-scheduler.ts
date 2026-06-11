import cron from 'node-cron';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { TaskQueue } from './task-queue';
import { WorkerPool } from './worker-pool';

const SCHEDULED_TASKS_KEY = 'scheduled:tasks';
const SCHEDULER_LOCK = 'scheduler:lock';

export class TaskScheduler {
  private static cronJobs: Map<string, cron.ScheduledTask> = new Map();
  private static schedulerRunning = false;

  /**
   * Schedule a one-time delayed task
   */
  static async scheduleDelayed(
    taskId: string,
    delayMs: number,
    callback: () => Promise<void>
  ): Promise<void> {
    const client = getRedisClient();
    const scheduleTime = Date.now() + delayMs;

    await client.zAdd(SCHEDULED_TASKS_KEY, {
      score: scheduleTime,
      value: JSON.stringify({ taskId, callback: callback.toString() }),
    });

    logger.info({ taskId, delayMs }, 'Task scheduled with delay');
  }

  /**
   * Schedule a recurring task with cron expression
   */
  static scheduleRecurring(
    taskName: string,
    cronExpression: string,
    handler: () => Promise<void>
  ): void {
    try {
      if (!cron.validate(cronExpression)) {
        throw new Error(`Invalid cron expression: ${cronExpression}`);
      }

      const job = cron.schedule(cronExpression, async () => {
        try {
          await handler();
        } catch (error) {
          logger.error({ taskName, error }, 'Recurring task failed');
        }
      });

      this.cronJobs.set(taskName, job);
      logger.info({ taskName, cronExpression }, 'Recurring task scheduled');
    } catch (error) {
      logger.error({ taskName, error }, 'Failed to schedule recurring task');
      throw error;
    }
  }

  /**
   * Stop a scheduled recurring task
   */
  static stopRecurring(taskName: string): void {
    const job = this.cronJobs.get(taskName);
    if (job) {
      job.stop();
      this.cronJobs.delete(taskName);
      logger.info({ taskName }, 'Recurring task stopped');
    }
  }

  /**
   * Start the scheduler daemon
   */
  static async startScheduler(pollIntervalMs: number = 5000): Promise<void> {
    if (this.schedulerRunning) {
      logger.warn('Scheduler already running');
      return;
    }

    this.schedulerRunning = true;
    logger.info({ pollIntervalMs }, 'Task scheduler started');

    const processScheduledTasks = async () => {
      if (!this.schedulerRunning) return;

      try {
        const client = getRedisClient();
        const now = Date.now();

        // Acquire lock for distributed scheduling
        const lockKey = `${SCHEDULER_LOCK}`;
        const lockId = `scheduler-${Date.now()}`;

        const acquired = await client.set(lockKey, lockId, {
          NX: true,
          EX: 10,
        });

        if (!acquired) {
          // Another instance is processing
          return;
        }

        // Get all tasks due to run
        const dueTasks = await client.zRange(SCHEDULED_TASKS_KEY, 0, now, { BY: 'SCORE' });

        // Process scheduled tasks
        for (const taskData of dueTasks) {
          try {
            const { taskId } = JSON.parse(taskData);
            const task = await TaskQueue.getTask(taskId);

            if (task) {
              // Move to queue for processing
              await TaskQueue.updateTaskStatus(taskId, 'queued');
              logger.info({ taskId }, 'Scheduled task moved to queue');
            }

            await client.zRem(SCHEDULED_TASKS_KEY, taskData);
          } catch (error) {
            logger.error({ error, taskData }, 'Failed to process scheduled task');
          }
        }

        // Recover tasks orphaned by crashed workers while holding the lock.
        await TaskQueue.recoverStaleTasks();

        // Enforce SLA across all tasks
        await TaskScheduler.enforceSLA();

        // Release lock
        const currentLock = await client.get(lockKey);
        if (currentLock === lockId) {
          await client.del(lockKey);
        }
      } catch (error) {
        logger.error({ error }, 'Scheduler error');
      }

      // Schedule next run
      setTimeout(processScheduledTasks, pollIntervalMs);
    };

    // Start the polling loop
    processScheduledTasks();
  }

  /**
   * Enforce SLA across all tasks
   * 1. Finds pending/queued tasks that violate their SLA
   * 2. Preempts processing low-priority tasks if high/critical SLA is violated
   */
  static async enforceSLA(): Promise<void> {
    const client = getRedisClient();
    const queues = await client.keys('queue:*');
    
    const totalViolations = {
      critical: 0,
      high: 0,
      medium: 0,
      low: 0
    };

    const PRIORITY_SLA = {
      critical: 5000,
      high: 15000,
      medium: 60000,
      low: 300000,
    };

    const now = Date.now();

    for (const queueKey of queues) {
      if (queueKey.endsWith(':stats')) continue;
      const queueName = queueKey.replace('queue:', '');

      // Get queued tasks
      const taskIds = await client.zRange(queueKey, 0, -1);
      
      let hasHighPriorityAtRisk = false;

      for (const taskId of taskIds) {
        const task = await TaskQueue.getTask(taskId);
        if (!task || (task.status !== 'queued' && task.status !== 'pending' && task.status !== 'retry')) continue;

        // Ensure task is actually runnable (not blocked by dependencies or scheduled for the future)
        if (task.dependencies && task.dependencies.length > 0) {
          const depsResolved = await (TaskQueue as any)._checkDependencies(task.dependencies);
          if (!depsResolved) continue;
        }
        if (task.scheduledFor && new Date(task.scheduledFor).getTime() > now) {
          continue;
        }

        const sla = PRIORITY_SLA[task.priority as keyof typeof PRIORITY_SLA] || 60000;
        const taskAge = now - new Date(task.createdAt).getTime();

        if (taskAge > sla) {
          // Check if we already recorded this violation
          if (!task.metadata?.slaViolated) {
            totalViolations[task.priority as keyof typeof PRIORITY_SLA]++;
            logger.warn({ taskId, priority: task.priority, age: taskAge, sla }, 'SLA Violated');
            
            task.metadata = task.metadata || {};
            task.metadata.slaViolated = true;
            await client.set(`task:${task.id}`, JSON.stringify(task));
          }
          
          if (task.priority === 'critical' || task.priority === 'high') {
            hasHighPriorityAtRisk = true;
          }
        }
      }

      // Preempt low priority tasks if high/critical is at risk
      if (hasHighPriorityAtRisk) {
        // Query active workers instead of all historical tasks (fixes massive N+1 performance bug)
        const allWorkers = await client.zRange('workers:index', 0, -1);
        for (const workerId of allWorkers) {
          const activeTasks = await client.lRange(`worker:${workerId}:tasks`, 0, -1);
          for (const tid of activeTasks) {
            const t = await TaskQueue.getTask(tid);
            if (t && t.status === 'processing' && (t.priority === 'low' || t.priority === 'medium') && t.queue === queueName) {
              logger.warn({ taskId: t.id }, 'Preempting task to free resources for high-priority SLA risk');
              const previousWorkerId = t.workerId;
              await TaskQueue.updateTaskStatus(t.id, 'pending', { workerId: undefined, startedAt: undefined });
              
              if (previousWorkerId) {
                await WorkerPool.preemptTask(previousWorkerId, t.id);
              }

              const score = t.priority === 'low' ? 1 : 10;
              await client.zAdd(queueKey, { score: score + Math.random(), value: t.id });
            }
          }
        }
      }
    }

    // Update Redis metric
    for (const [priority, count] of Object.entries(totalViolations)) {
      if (count > 0) {
        await client.hIncrBy('metrics:sla_violations', priority, count);
      }
    }
  }

  /**
   * Stop the scheduler
   */
  static async stopScheduler(): Promise<void> {
    this.schedulerRunning = false;

    // Stop all cron jobs
    for (const job of this.cronJobs.values()) {
      job.stop();
    }
    this.cronJobs.clear();

    logger.info('Task scheduler stopped');
  }

  /**
   * Get all pending scheduled tasks
   */
  static async getPendingScheduledTasks(): Promise<any[]> {
    const client = getRedisClient();
    const tasks = await client.zRangeWithScores(SCHEDULED_TASKS_KEY, 0, -1);

    return tasks.map((item) => ({
      ...JSON.parse(item.value),
      scheduledAt: item.score,
    }));
  }
}
