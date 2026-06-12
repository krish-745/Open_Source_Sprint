import { TaskQueue } from './task-queue';
import { Task } from '../types';

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    name: 'demo',
    description: 'Task: demo',
    priority: 'medium',
    status: 'completed',
    handler: 'noop',
    payload: {},
    retries: 0,
    maxRetries: 3,
    timeout: 30000,
    createdAt: new Date(),
    queue: 'default',
    dependencies: [],
    tags: [],
    metadata: {},
    ...overrides,
  };
}

describe('TaskQueue.evaluateBranches', () => {
  it('returns an empty array when no branches are defined', () => {
    expect(TaskQueue.evaluateBranches(buildTask(), { status: 'ok' })).toEqual([]);
  });

  it('matches a branch by substring of the stringified result', () => {
    const task = buildTask({
      branches: [
        { condition: 'approved', nextTaskId: 'ship' },
        { condition: 'rejected', nextTaskId: 'refund' },
      ],
    });
    const matched = TaskQueue.evaluateBranches(task, { decision: 'approved' });
    expect(matched.map((b) => b.nextTaskId)).toEqual(['ship']);
  });

  it('matches a branch by regex', () => {
    const task = buildTask({
      branches: [{ condition: 'regex:score":\\s*9[0-9]', nextTemplate: 'highScore' }],
    });
    const matched = TaskQueue.evaluateBranches(task, { score: 95 });
    expect(matched.map((b) => b.nextTemplate)).toEqual(['highScore']);
  });

  it('can match multiple branches', () => {
    const task = buildTask({
      branches: [
        { condition: 'urgent', nextTaskId: 'a' },
        { condition: 'vip', nextTaskId: 'b' },
      ],
    });
    const matched = TaskQueue.evaluateBranches(task, { tags: 'urgent vip' });
    expect(matched.map((b) => b.nextTaskId).sort()).toEqual(['a', 'b']);
  });

  it('returns no matches when nothing matches', () => {
    const task = buildTask({ branches: [{ condition: 'approved', nextTaskId: 'ship' }] });
    expect(TaskQueue.evaluateBranches(task, { decision: 'pending' })).toEqual([]);
  });
});
