import re

with open('src/services/task-executor.ts', 'r', encoding='utf-8') as f:
    content = f.read()

# Block 1
block1_search = """<<<<<<< HEAD
import { getRedisClient } from './redis';
=======
import { TaskHooks } from './task-hooks';

export interface TaskExecutionContext {
  /** Handlers can poll this to cooperatively stop work when cancelled. */
  isCancelled: () => boolean;
}
>>>>>>> main"""
block1_replace = """import { getRedisClient } from './redis';
import { TaskHooks } from './task-hooks';

export interface TaskExecutionContext {
  /** Handlers can poll this to cooperatively stop work when cancelled. */
  isCancelled: () => boolean;
}"""
content = content.replace(block1_search, block1_replace)

# Block 2
block2_search = """<<<<<<< HEAD
      // If task is already completed/failed, skip processing to avoid straggler workers overriding state.
      const currentTask = await TaskQueue.getTask(task.id);
      if (currentTask && (currentTask.status === 'completed' || currentTask.status === 'failed')) {
        logger.warn({ taskId: task.id, workerId }, 'Task already finished, skipping execution');
        // Still need to decrement the worker's currentTasks counter.
        await WorkerPool.completeTask(workerId, task.id, {
          duration: Date.now() - startTime,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
=======
      // Skip tasks cancelled before they start.
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled before execution');
>>>>>>> main
        return;
      }"""

block2_replace = """      // Skip tasks cancelled before they start.
      if (this.isCancelled(task.id)) {
        await TaskQueue.updateTaskStatus(task.id, 'cancelled');
        logger.info({ taskId: task.id }, 'Task cancelled before execution');
        return;
      }

      // If task is already completed/failed, skip processing to avoid straggler workers overriding state.
      const currentTask = await TaskQueue.getTask(task.id);
      if (currentTask && (currentTask.status === 'completed' || currentTask.status === 'failed')) {
        logger.warn({ taskId: task.id, workerId }, 'Task already finished, skipping execution');
        // Still need to decrement the worker's currentTasks counter.
        await WorkerPool.completeTask(workerId, task.id, {
          duration: Date.now() - startTime,
          success: false,
          retriesUsed: task.retries,
          memory: 0,
          cpu: 0,
        });
        return;
      }"""
content = content.replace(block2_search, block2_replace)

with open('src/services/task-executor.ts', 'w', encoding='utf-8') as f:
    f.write(content)
