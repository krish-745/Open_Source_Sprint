import logger from '../utils/logger';
import { Task } from '../types';

export interface CallbackOptions {
  maxRetries?: number;
  timeoutMs?: number;
}

/**
 * Deliver a task's result to its `callbackUrl` via HTTP POST.
 *
 * Retries on network/non-2xx failures with exponential backoff, applies a
 * per-attempt timeout, and includes task metadata in the payload. Returns true
 * if delivery succeeded, false otherwise. Never throws — callback delivery is a
 * best-effort side effect that must not break task completion.
 */
export async function deliverCallback(
  task: Task,
  result: any,
  options: CallbackOptions = {}
): Promise<boolean> {
  if (!task.callbackUrl) {
    return false;
  }

  const maxRetries = options.maxRetries ?? 2;
  const timeoutMs = options.timeoutMs ?? 5000;

  const payload = {
    taskId: task.id,
    name: task.name,
    status: task.status,
    result,
    completedAt: task.completedAt,
    metadata: task.metadata,
  };

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(task.callbackUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      clearTimeout(timer);
      if (response.ok) {
        logger.info({ taskId: task.id, callbackUrl: task.callbackUrl }, 'Callback delivered');
        return true;
      }
      logger.warn({ taskId: task.id, status: response.status }, 'Callback returned non-2xx');
    } catch (error) {
      clearTimeout(timer);
      logger.warn({ taskId: task.id, attempt: attempt + 1, error: String(error) }, 'Callback delivery failed');
    }

    if (attempt < maxRetries) {
      await new Promise((resolve) => setTimeout(resolve, 100 * 2 ** attempt));
    }
  }

  logger.error({ taskId: task.id, callbackUrl: task.callbackUrl }, 'Callback delivery exhausted retries');
  return false;
}
