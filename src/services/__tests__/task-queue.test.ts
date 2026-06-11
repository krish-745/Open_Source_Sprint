import { TaskQueue } from '../task-queue';
import { getRedisClient } from '../redis';
import { Task } from '../../types';

// Mock the Redis client using an in-memory store
const store: Record<string, string> = {};
let taskIdsIndex: string[] = [];

const mockRedisClient = {
  get: jest.fn().mockImplementation(async (key: string) => store[key] || null),
  zRange: jest.fn().mockImplementation(async (key: string, start: number, stop: number) => {
    return taskIdsIndex;
  }),
};

jest.mock('../redis', () => ({
  getRedisClient: () => mockRedisClient,
}));

describe('TaskQueue - searchTasks', () => {
  beforeEach(() => {
    for (const key in store) delete store[key];
    taskIdsIndex = [];
    jest.clearAllMocks();
  });

  const createTaskMock = (id: string, name: string, description: string): Task => {
    return {
      id,
      name,
      description,
      priority: 'medium',
      status: 'pending',
      handler: 'testHandler',
      payload: {},
      retries: 0,
      maxRetries: 3,
      timeout: 30000,
      createdAt: new Date(),
      queue: 'default',
      dependencies: [],
      tags: [],
      metadata: {},
    };
  };

  it('should return empty array when query is empty', async () => {
    const results = await TaskQueue.searchTasks('');
    expect(results).toEqual([]);
  });

  it('should find tasks by case-insensitive name and description and score them', async () => {
    const task1 = createTaskMock('1', 'Report Generator', 'Generates weekly sales reports');
    const task2 = createTaskMock('2', 'Email Dispatcher', 'Sends reports via email');
    const task3 = createTaskMock('3', 'Database Backup', 'Backs up sales database');

    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    store['task:3'] = JSON.stringify(task3);
    taskIdsIndex = ['1', '2', '3'];

    // Search for "report"
    // task1 matches name exactly (part of name): score 5 + description contains "reports" score 2 = 7
    // task2 matches description contains "reports": score 2
    // task3 doesn't match
    const results = await TaskQueue.searchTasks('report');
    
    expect(results.length).toBe(2);
    expect(results[0].taskId).toBe('1');
    expect(results[0].score).toBe(7);
    expect(results[1].taskId).toBe('2');
    expect(results[1].score).toBe(2);
  });

  it('should rank exact name match highest', async () => {
    const task1 = createTaskMock('1', 'Backup', 'Backup description');
    const task2 = createTaskMock('2', 'Database Backup Service', 'Database backup description');

    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    taskIdsIndex = ['1', '2'];

    const results = await TaskQueue.searchTasks('Backup');
    expect(results.length).toBe(2);
    expect(results[0].taskId).toBe('1'); // exact name match => score 10 + description match 2 = 12
    expect(results[1].taskId).toBe('2'); // partial name match => score 5 + description match 2 = 7
  });

  it('should handle special characters safely without crashing', async () => {
    const task = createTaskMock('1', 'Special $&* Task', 'Contains special chars');
    store['task:1'] = JSON.stringify(task);
    taskIdsIndex = ['1'];

    const results = await TaskQueue.searchTasks('$&*');
    expect(results.length).toBe(1);
    expect(results[0].taskId).toBe('1');
  });

  it('should respect the limit parameter', async () => {
    const task1 = createTaskMock('1', 'Report Generator 1', 'Desc');
    const task2 = createTaskMock('2', 'Report Generator 2', 'Desc');

    store['task:1'] = JSON.stringify(task1);
    store['task:2'] = JSON.stringify(task2);
    taskIdsIndex = ['1', '2'];

    const results = await TaskQueue.searchTasks('Report', 1);
    expect(results.length).toBe(1);
  });
});
