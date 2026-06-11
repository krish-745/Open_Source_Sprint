import { WorkerPool } from './worker-pool';

describe('WorkerPool.evaluateAutoScaling', () => {
  const baseNow = 1_000_000_000;

  beforeEach(() => {
    WorkerPool.autoScaleConfig = {
      scaleUpQueueDepth: 50,
      saturatedCapacityPct: 80,
      cooldownMs: 30_000,
    };
  });

  it('scales up when the queue is deep and workers are saturated', () => {
    const decision = WorkerPool.evaluateAutoScaling(100, 90, baseNow);
    expect(decision.action).toBe('scale_up');
  });

  it('scales down when the queue is empty', () => {
    // Use a time well past any prior scale event to clear cooldown.
    const decision = WorkerPool.evaluateAutoScaling(0, 0, baseNow + 10_000_000);
    expect(decision.action).toBe('scale_down');
  });

  it('does nothing when within thresholds', () => {
    const decision = WorkerPool.evaluateAutoScaling(10, 40, baseNow + 20_000_000);
    expect(decision.action).toBe('none');
  });

  it('respects the cooldown to prevent thrashing', () => {
    const t = baseNow + 30_000_000;
    const first = WorkerPool.evaluateAutoScaling(100, 90, t); // triggers scale_up, sets cooldown
    expect(first.action).toBe('scale_up');

    const second = WorkerPool.evaluateAutoScaling(100, 90, t + 1_000); // within cooldown
    expect(second.action).toBe('none');
    expect(second.reason).toMatch(/cooldown/);
  });
});
