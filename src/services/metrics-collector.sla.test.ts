import { MetricsCollector } from './metrics-collector';
import { Task } from '../types';

function buildTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task',
    name: 'demo',
    description: 'Task: demo',
    priority: 'high',
    status: 'pending',
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

describe('MetricsCollector SLA', () => {
  it('marks a task compliant when it starts within its priority target', () => {
    const created = new Date(1_000_000);
    const started = new Date(1_000_000 + 10_000); // 10s wait, high target is 30s
    const res = MetricsCollector.evaluateSla(buildTask({ priority: 'high', createdAt: created, startedAt: started }));
    expect(res.compliant).toBe(true);
    expect(res.waitMs).toBe(10_000);
  });

  it('marks a task violated when it waits beyond its target', () => {
    const created = new Date(1_000_000);
    const now = 1_000_000 + 60_000; // 60s waiting, high target 30s, not started
    const res = MetricsCollector.evaluateSla(buildTask({ priority: 'high', createdAt: created }), now);
    expect(res.compliant).toBe(false);
  });

  it('aggregates compliance overall and per priority and lists violations', () => {
    const t0 = 1_000_000;
    const now = t0 + 60_000;
    const tasks = [
      buildTask({ id: 'a', priority: 'high', createdAt: new Date(t0), startedAt: new Date(t0 + 5_000) }), // compliant
      buildTask({ id: 'b', priority: 'high', createdAt: new Date(t0) }), // 60s wait > 30s -> violation
      buildTask({ id: 'c', priority: 'low', createdAt: new Date(t0) }), // 60s < 600s -> compliant
    ];

    const report = MetricsCollector.checkSlaCompliance(tasks, now);
    expect(report.total).toBe(3);
    expect(report.compliant).toBe(2);
    expect(report.violations.map((v) => v.taskId)).toEqual(['b']);
    expect(report.byPriority.high.complianceRate).toBeCloseTo(0.5);
    expect(report.byPriority.low.complianceRate).toBe(1);
  });
});
