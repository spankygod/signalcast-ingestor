export interface IngestorSettings {
  eventsPollIntervalMs: number;
  marketsPollIntervalMs: number;
  outcomesPollIntervalMs: number;
  heartbeatIntervalMs: number;
  queueDrainIntervalMs: number;
  eventBatchSize: number;
  tickRetentionMinutes: number;
  maxConcurrentJobs: number;
  redisEnabled: boolean;
  eventQueueBacklogThreshold: number;
}

export const settings: IngestorSettings = {
  eventsPollIntervalMs: Number(process.env.EVENTS_POLL_INTERVAL_MS || 60_000),
  marketsPollIntervalMs: Number(process.env.MARKETS_POLL_INTERVAL_MS || 90_000),
  outcomesPollIntervalMs: Number(process.env.OUTCOMES_POLL_INTERVAL_MS || 90_000),
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS || 30_000),
  queueDrainIntervalMs: Number(process.env.QUEUE_DRAIN_INTERVAL_MS || 5_000),
  eventBatchSize: Number(process.env.EVENT_BATCH_SIZE || 25),
  tickRetentionMinutes: Number(process.env.TICK_RETENTION_MINUTES || 60),
  maxConcurrentJobs: Number(process.env.MAX_CONCURRENT_JOBS || 4),
  redisEnabled: process.env.REDIS_ENABLED !== 'false',
  eventQueueBacklogThreshold: Number(process.env.EVENT_QUEUE_BACKLOG_THRESHOLD || 500)
};
