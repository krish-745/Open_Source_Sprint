import { MetricsCollector } from './metrics-collector';
import * as redis from './redis';

jest.mock('./redis');

const mockedGetRedisClient = redis.getRedisClient as jest.Mock;

function makeClient(snapshotKeys: string[]) {
  return {
    // queue:*:stats lookups return nothing; snapshot:* returns our keys.
    keys: jest.fn((pattern: string) =>
      Promise.resolve(pattern.includes('snapshot') ? snapshotKeys : [])
    ),
    zRange: jest.fn().mockResolvedValue([]),
    zCard: jest.fn().mockResolvedValue(0),
    lLen: jest.fn().mockResolvedValue(0),
    set: jest.fn().mockResolvedValue('OK'),
    del: jest.fn().mockResolvedValue(1),
  } as any;
}

afterEach(() => jest.clearAllMocks());

describe('MetricsCollector snapshot retention', () => {
  it('prunes oldest snapshots beyond the configured limit', async () => {
    MetricsCollector.setMaxSnapshots(2);
    const client = makeClient([
      'snapshot:1000',
      'snapshot:3000',
      'snapshot:2000',
      'snapshot:4000',
    ]);
    mockedGetRedisClient.mockReturnValue(client);

    await MetricsCollector.captureSnapshot();

    // Keep the two newest (4000, 3000); delete the two oldest.
    expect(client.del).toHaveBeenCalledWith('snapshot:1000');
    expect(client.del).toHaveBeenCalledWith('snapshot:2000');
    expect(client.del).not.toHaveBeenCalledWith('snapshot:3000');
    expect(client.del).not.toHaveBeenCalledWith('snapshot:4000');
  });

  it('does not prune when under the limit', async () => {
    MetricsCollector.setMaxSnapshots(10);
    const client = makeClient(['snapshot:1000', 'snapshot:2000']);
    mockedGetRedisClient.mockReturnValue(client);

    await MetricsCollector.captureSnapshot();

    expect(client.del).not.toHaveBeenCalled();
  });
});
