import pino from 'pino';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'SYS:standard',
      ignore: 'pid,hostname',
    },
  },
});

/**
 * Create a child logger bound to a trace id, so all log lines emitted for a
 * task chain carry the same correlation id for distributed tracing.
 */
export function traceLogger(traceId: string) {
  return logger.child({ traceId });
}

export default logger;
