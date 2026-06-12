import { deliverCallback } from './task-callbacks';
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

afterEach(() => {
  jest.restoreAllMocks();
});

describe('deliverCallback', () => {
  it('returns false when no callbackUrl is set', async () => {
    const fetchSpy = jest.fn();
    (global as any).fetch = fetchSpy;
    expect(await deliverCallback(buildTask(), { ok: true })).toBe(false);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('POSTs the result and returns true on a 2xx response', async () => {
    const fetchSpy = jest.fn().mockResolvedValue({ ok: true, status: 200 });
    (global as any).fetch = fetchSpy;

    const ok = await deliverCallback(buildTask({ callbackUrl: 'https://hook.test/cb' }), { value: 42 });

    expect(ok).toBe(true);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toBe('https://hook.test/cb');
    expect(init.method).toBe('POST');
    expect(JSON.parse(init.body).result).toEqual({ value: 42 });
  });

  it('retries on failure and eventually returns false', async () => {
    const fetchSpy = jest.fn().mockRejectedValue(new Error('network'));
    (global as any).fetch = fetchSpy;

    const ok = await deliverCallback(
      buildTask({ callbackUrl: 'https://hook.test/cb' }),
      {},
      { maxRetries: 2, timeoutMs: 50 }
    );

    expect(ok).toBe(false);
    expect(fetchSpy).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it('retries then succeeds', async () => {
    const fetchSpy = jest
      .fn()
      .mockRejectedValueOnce(new Error('network'))
      .mockResolvedValue({ ok: true, status: 200 });
    (global as any).fetch = fetchSpy;

    const ok = await deliverCallback(
      buildTask({ callbackUrl: 'https://hook.test/cb' }),
      {},
      { maxRetries: 2, timeoutMs: 50 }
    );

    expect(ok).toBe(true);
    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });
});
