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

describe('MetricsCollector Retention and Memory Warnings', () => {
  let mockClient: any;

  beforeEach(() => {
    mockClient = {
      keys: jest.fn().mockResolvedValue([]),
      get: jest.fn(),
      set: jest.fn().mockResolvedValue('OK'),
      del: jest.fn().mockResolvedValue(1),
      zRange: jest.fn().mockResolvedValue([]),
      zCard: jest.fn().mockResolvedValue(0),
      lLen: jest.fn().mockResolvedValue(0),
    };
    mockedGetRedisClient.mockReturnValue(mockClient);
    MetricsCollector.maxSnapshots = 1000;
    MetricsCollector.maxHeapMemoryBytes = 524288000;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should clean up the oldest snapshots when maxSnapshots is exceeded', async () => {
    MetricsCollector.maxSnapshots = 2;
    const existingSnapshots = [
      'snapshot:100',
      'snapshot:200',
      'snapshot:300',
      'snapshot:400',
    ];
    mockClient.keys.mockImplementation(async (pattern: string) => {
      if (pattern.startsWith('snapshot:')) {
        return existingSnapshots;
      }
      return [];
    });

    await MetricsCollector.captureSnapshot();

    expect(mockClient.del).toHaveBeenCalledWith('snapshot:100');
    expect(mockClient.del).toHaveBeenCalledWith('snapshot:200');
    expect(mockClient.del).not.toHaveBeenCalledWith('snapshot:300');
    expect(mockClient.del).not.toHaveBeenCalledWith('snapshot:400');
  });

  it('should degrade system health when memory usage is too high', async () => {
    MetricsCollector.maxHeapMemoryBytes = 100 * 1024 * 1024; // 100 MB
    const snapshotWithHighMemory = {
      timestamp: new Date(),
      queues: {},
      workers: {
        'worker-1': { status: 'online' }
      },
      tasks: { deadLetterQueueSize: 0 },
      system: {
        memoryUsage: { heapUsed: 150 * 1024 * 1024 } // 150 MB
      }
    };

    mockClient.keys.mockResolvedValue(['snapshot:1700000000000']);
    mockClient.get.mockResolvedValue(JSON.stringify(snapshotWithHighMemory));

    const health = await MetricsCollector.getHealthStatus();

    expect(health.status).toBe('degraded');
    expect(health.issues).toContain('High heap memory usage: 150 MB');
  });
});
