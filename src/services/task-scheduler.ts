import cron from 'node-cron';
import { v4 as uuidv4 } from 'uuid';
import { getRedisClient } from './redis';
import logger from '../utils/logger';
import { TaskQueue } from './task-queue';

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
  static async startScheduler(pollIntervalMs: number = 5000, lockTtlSeconds: number = 10): Promise<void> {
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
        const lockId = `scheduler-${uuidv4()}`;

        const acquired = await client.set(lockKey, lockId, {
          NX: true,
          EX: lockTtlSeconds,
        });

        if (acquired) {
          // Start heartbeat to renew lock before it expires
          const renewScript = `
            if redis.call("get", KEYS[1]) == ARGV[1] then
              return redis.call("expire", KEYS[1], ARGV[2])
            else
              return 0
            end
          `;
          
          const heartbeatInterval = setInterval(async () => {
            try {
              await client.eval(renewScript, {
                keys: [lockKey],
                arguments: [lockId, lockTtlSeconds.toString()]
              });
            } catch (err) {
              logger.error({ err }, 'Failed to renew scheduler lock');
            }
          }, (lockTtlSeconds * 1000) / 2);

          try {
            // Get all tasks due to run
            const dueTasks = await client.zRange(SCHEDULED_TASKS_KEY, 0, now, { BY: 'SCORE' });

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
          } finally {
            clearInterval(heartbeatInterval);

            // Release lock atomically
            const releaseScript = `
              if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
              else
                return 0
              end
            `;
            await client.eval(releaseScript, {
              keys: [lockKey],
              arguments: [lockId]
            });
          }
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
