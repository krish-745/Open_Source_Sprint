import { MetricsCollector } from './metrics-collector';
import * as redis from './redis';

jest.mock('./redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

describe('MetricsCollector.getLatestSnapshot', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  it('returns the snapshot with the largest numeric timestamp even when keys differ in length', async () => {
    const older = { timestamp: 'older' };
    const newer = { timestamp: 'newer' };
    // Numerically, 10000000000000 > 9999999999999, but lexicographically
    // 'snapshot:10000000000000' sorts before 'snapshot:9999999999999'.
    const store: Record<string, string> = {
      'snapshot:9999999999999': JSON.stringify(older),
      'snapshot:10000000000000': JSON.stringify(newer),
    };
    const mockClient = {
      keys: jest.fn().mockResolvedValue(Object.keys(store)),
      get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    const result = await MetricsCollector.getLatestSnapshot();

    expect(result).toEqual(newer);
    expect(mockClient.get).toHaveBeenCalledWith('snapshot:10000000000000');
  });

  it('returns the latest snapshot for timestamps that are close together', async () => {
    const store: Record<string, string> = {
      'snapshot:1700000000001': JSON.stringify({ timestamp: 'a' }),
      'snapshot:1700000000003': JSON.stringify({ timestamp: 'c' }),
      'snapshot:1700000000002': JSON.stringify({ timestamp: 'b' }),
    };
    const mockClient = {
      keys: jest.fn().mockResolvedValue(Object.keys(store)),
      get: jest.fn((key: string) => Promise.resolve(store[key] ?? null)),
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    const result = await MetricsCollector.getLatestSnapshot();

    expect(result).toEqual({ timestamp: 'c' });
  });

  it('returns null when no snapshots exist', async () => {
    const mockClient = {
      keys: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
    };
    mockedGetRedisClient.mockReturnValue(mockClient as any);

    expect(await MetricsCollector.getLatestSnapshot()).toBeNull();
  });
});
