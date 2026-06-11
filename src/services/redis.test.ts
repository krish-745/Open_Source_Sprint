import { withRedisRetry, getRedisStatus, isRedisConnected } from './redis';

describe('withRedisRetry', () => {
  it('returns the result when the operation succeeds first try', async () => {
    const op = jest.fn().mockResolvedValue('ok');
    await expect(withRedisRetry(op, 3, 1)).resolves.toBe('ok');
    expect(op).toHaveBeenCalledTimes(1);
  });

  it('retries transient failures and eventually succeeds', async () => {
    const op = jest
      .fn()
      .mockRejectedValueOnce(new Error('blip'))
      .mockRejectedValueOnce(new Error('blip'))
      .mockResolvedValue('ok');
    await expect(withRedisRetry(op, 3, 1)).resolves.toBe('ok');
    expect(op).toHaveBeenCalledTimes(3);
  });

  it('rethrows the last error after exhausting the retry budget', async () => {
    const op = jest.fn().mockRejectedValue(new Error('boom'));
    await expect(withRedisRetry(op, 2, 1)).rejects.toThrow('boom');
    expect(op).toHaveBeenCalledTimes(3); // initial attempt + 2 retries
  });
});

describe('getRedisStatus', () => {
  it('reports disconnected when the client is not initialized', () => {
    expect(isRedisConnected()).toBe(false);
    expect(getRedisStatus()).toEqual({ connected: false, url: null });
  });
});
