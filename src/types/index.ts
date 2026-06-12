/**
 * Core type definitions for the task scheduler system
 */

export interface Task {
  id: string;
  name: string;
  description: string;
  priority: 'low' | 'medium' | 'high' | 'critical';
  status: TaskStatus;
  handler: string; // Function name to execute
  payload: Record<string, any>;
  retries: number;
  maxRetries: number;
  timeout: number; // in milliseconds
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  error?: string;
  result?: any;
  workerId?: string;
  queue: string;
  dependencies: string[]; // Task IDs this task depends on
  scheduledFor?: Date; // For delayed tasks
  branches?: TaskBranch[]; // Conditional next-steps evaluated against the result
  recurrence?: RecurrenceRule;
  tags: string[];
  metadata: Record<string, any>;
}

/**
 * A conditional branch: if the task result matches `condition`, the referenced
 * next task/template should run. `condition` is matched against the stringified
 * result; prefix it with `regex:` to match with a regular expression.
 */
export interface TaskBranch {
  condition: string;
  nextTaskId?: string;
  nextTemplate?: string;
}

export type TaskStatus =
  | 'pending'
  | 'queued'
  | 'processing'
  | 'completed'
  | 'failed'
  | 'retry'
  | 'cancelled'
  | 'blocked'; // Task waiting for dependencies

export interface RecurrenceRule {
  frequency: 'once' | 'daily' | 'weekly' | 'monthly' | 'custom';
  interval: number;
  cronExpression?: string;
  maxOccurrences?: number;
}

export interface Worker {
  id: string;
  name: string;
  status: WorkerStatus;
  handlers: string[];
  maxConcurrent: number;
  currentTasks: number;
  totalProcessed: number;
  totalFailed: number;
  lastHeartbeat: Date;
  registeredAt: Date;
  version: string;
  capacity: number; // 0-100 percentage
  tags: string[];
}

export type WorkerStatus = 'online' | 'offline' | 'busy' | 'idle' | 'maintenance';

export interface Queue {
  name: string;
  priority: number;
  maxSize: number;
  currentSize: number;
  processors: number;
  deadLetterThreshold: number;
  retentionPolicy: RetentionPolicy;
}

export interface RetentionPolicy {
  successfulTasks: number; // hours
  failedTasks: number; // hours
  cancelledTasks: number; // hours
}

export interface SchedulerConfig {
  redisUrl: string;
  port: number;
  workers: number;
  maxTaskAge: number; // hours
  pollInterval: number; // milliseconds
  healthCheckInterval: number; // milliseconds
  maxQueueSize: number;
}

export interface TaskExecutionMetrics {
  taskId: string;
  duration: number;
  memory: number;
  cpu: number;
  success: boolean;
  retriesUsed: number;
}

export interface WorkerMetrics {
  workerId: string;
  tasksProcessed: number;
  successRate: number;
  avgDuration: number;
  lastUpdated: Date;
}

export interface QueueMetrics {
  queueName: string;
  pendingTasks: number;
  processingTasks: number;
  completedTasks: number;
  failedTasks: number;
  avgWaitTime: number;
  avgProcessTime: number;
}
