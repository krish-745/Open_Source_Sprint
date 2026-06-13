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
 * OpenTelemetry-compatible trace context fields.
 * These names match the OTel log data model so the log record can be
 * exported to any OTel-compatible backend without further transformation.
 */
export interface TraceContext {
  /** 128-bit trace identifier shared by all spans in the same trace chain. */
  trace_id: string;
  /** 64-bit span identifier for the current task (its task ID). */
  span_id: string;
  /** span_id of the immediate parent span, if any. */
  parent_span_id?: string;
}

/**
 * Create a child logger that has the given trace context fields pre-bound.
 * Every message emitted from the returned logger will automatically include
 * `trace_id`, `span_id`, and (when present) `parent_span_id`, making the
 * logs searchable by correlation ID and directly exportable via OTel collectors.
 *
 * @example
 * const tlog = withTrace({ trace_id: task.traceId!, span_id: task.id, parent_span_id: task.parentSpanId });
 * tlog.info('task started');  // → { trace_id: '…', span_id: '…', msg: 'task started' }
 */
export function withTrace(ctx: TraceContext): pino.Logger {
  return logger.child(ctx);
}

export default logger;
