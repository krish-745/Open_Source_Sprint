import { MetricsCollector } from '../metrics-collector';
import { getRedisClient } from '../redis';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
const mockRedisClient = {
  keys: jest.fn().mockImplementation(async (pattern: string) => {
    // Return all keys that match snapshot:*
    return Object.keys(store).filter(key => key.startsWith('snapshot:'));
  }),
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('MetricsCollector - getLatestSnapshot', () => {
  beforeEach(() => {
    // Clear the store before each test
    for (const key in store) {
      delete store[key];
    }
    jest.clearAllMocks();
  });

  it('should sort snapshot keys numerically, not lexicographically', async () => {
    // If sorted lexicographically: snapshot:1000, snapshot:1001, snapshot:999
    // => snapshot:999 would be considered the latest (incorrect).
    // If sorted numerically: snapshot:999, snapshot:1000, snapshot:1001
    // => snapshot:1001 would be considered the latest (correct).

    const snapshot999 = { timestamp: new Date(999), queues: {}, workers: {}, tasks: {}, system: {} };
    const snapshot1000 = { timestamp: new Date(1000), queues: {}, workers: {}, tasks: {}, system: {} };
    const snapshot1001 = { timestamp: new Date(1001), queues: {}, workers: {}, tasks: {}, system: {} };

    store['snapshot:999'] = JSON.stringify(snapshot999);
    store['snapshot:1000'] = JSON.stringify(snapshot1000);
    store['snapshot:1001'] = JSON.stringify(snapshot1001);

    const latest = await MetricsCollector.getLatestSnapshot();
    
    expect(latest).toBeDefined();
    // The latest snapshot should be 1001, not 999
    expect(latest?.timestamp).toBe(snapshot1001.timestamp.toISOString());
  });

  it('should handle closely spaced timestamps correctly', async () => {
    // Timestamps separated by only 1 millisecond
    const snapshotBase = { timestamp: new Date(1718105741000), queues: {}, workers: {}, tasks: {}, system: {} };
    const snapshotPlus1 = { timestamp: new Date(1718105741001), queues: {}, workers: {}, tasks: {}, system: {} };

    store['snapshot:1718105741000'] = JSON.stringify(snapshotBase);
    store['snapshot:1718105741001'] = JSON.stringify(snapshotPlus1);

    const latest = await MetricsCollector.getLatestSnapshot();
    
    expect(latest).toBeDefined();
    expect(latest?.timestamp).toBe(snapshotPlus1.timestamp.toISOString());
  });

  it('should return null if no snapshots exist', async () => {
    const latest = await MetricsCollector.getLatestSnapshot();
    expect(latest).toBeNull();
  });
});
