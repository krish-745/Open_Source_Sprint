import express from 'express';
import dotenv from 'dotenv';
import { initializeRedis, closeRedis } from './services/redis';
import { TaskScheduler } from './services/task-scheduler';
import { MetricsCollector } from './services/metrics-collector';
import { TaskExecutor } from './services/task-executor';
import apiRoutes from './routes/api';
import logger from './utils/logger';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// Middleware
app.use(express.json());

// Health check endpoint
app.get('/healthz', (req, res) => {
  res.json({ status: 'ok' });
});

// API routes
app.use('/api', apiRoutes);

// Error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error({ error: err }, 'Unhandled error');
  res.status(500).json({ error: 'Internal server error' });
});

// Graceful shutdown
async function shutdown() {
  logger.info('Shutting down gracefully...');

  await TaskScheduler.stopScheduler();
  MetricsCollector.stopMetricsCollection();
  await closeRedis();

  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Register sample task handlers
function registerSampleHandlers() {
  TaskExecutor.registerHandler('dataProcessor', async (payload) => {
    logger.info({ payload }, 'Processing data...');
    // Simulate processing work
    await new Promise((resolve) => setTimeout(resolve, 1000));
    return { processed: true, records: payload.records || 0 };
  });

  TaskExecutor.registerHandler('emailSender', async (payload) => {
    logger.info({ payload }, 'Sending email...');
    await new Promise((resolve) => setTimeout(resolve, 500));
    return { sent: true, to: payload.to || 'unknown' };
  });

  TaskExecutor.registerHandler('reportGenerator', async (payload) => {
    logger.info({ payload }, 'Generating report...');
    await new Promise((resolve) => setTimeout(resolve, 2000));
    return { report: 'generated', format: payload.format || 'pdf' };
  });

  logger.info('Sample task handlers registered');
}

// Start server
async function start() {
  try {
    // Initialize Redis
    await initializeRedis(REDIS_URL);
    logger.info('Redis initialized');

    // Register handlers
    registerSampleHandlers();

    // Start scheduler
    await TaskScheduler.startScheduler();

    // Start metrics collection
    await MetricsCollector.startMetricsCollection();

    // Start HTTP server
    app.listen(PORT, () => {
      logger.info({ port: PORT }, 'Server started');
    });
  } catch (error) {
    logger.error({ error }, 'Failed to start server');
    process.exit(1);
  }
}

start();
