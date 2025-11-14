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
  dbWriterBatchSize: {
    events: number;
    markets: number;
    outcomes: number;
    ticks: number;
  };
  bootstrapEventsPageSize: number;
  steadyEventsPageSize: number;
  bootstrapMarketsPageSize: number;
  steadyMarketsPageSize: number;
  marketQueueBacklogThreshold: number;
  tickQueueBacklogThreshold: number;
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
  eventQueueBacklogThreshold: Number(process.env.EVENT_QUEUE_BACKLOG_THRESHOLD || 500),
  dbWriterBatchSize: {
    events: Number(process.env.DB_WRITER_BATCH_SIZE_EVENTS || 100),
    markets: Number(process.env.DB_WRITER_BATCH_SIZE_MARKETS || 50),
    outcomes: Number(process.env.DB_WRITER_BATCH_SIZE_OUTCOMES || 100),
    ticks: Number(process.env.DB_WRITER_BATCH_SIZE_TICKS || 100),
  },
  bootstrapEventsPageSize: Number(process.env.BOOTSTRAP_EVENTS_PAGE_SIZE || 100),
  steadyEventsPageSize: Number(process.env.STEADY_EVENTS_PAGE_SIZE || 25),
  bootstrapMarketsPageSize: Number(process.env.BOOTSTRAP_MARKETS_PAGE_SIZE || 100),
  steadyMarketsPageSize: Number(process.env.STEADY_MARKETS_PAGE_SIZE || 25),
  marketQueueBacklogThreshold: Number(process.env.MARKET_QUEUE_BACKLOG_THRESHOLD || 200),
  tickQueueBacklogThreshold: Number(process.env.TICK_QUEUE_BACKLOG_THRESHOLD || 1000)
};
