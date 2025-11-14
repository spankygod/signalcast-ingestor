import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { config, features } from './settings';

/**
 * Redis stream configuration for SignalCast architecture
 */
export const REDIS_STREAMS = {
  EVENTS: 'sc:events',
  MARKETS: 'sc:markets',
  OUTCOMES: 'sc:outcomes',
  TICKS: 'sc:ticks',
  DEADLETTER: 'sc:deadletter',
} as const;

/**
 * Redis consumer group configuration
 */
export const REDIS_CONSUMER_GROUPS = {
  PRIMARY: 'signalcast',
  DEADLETTER: 'signalcast-deadletter',
} as const;

/**
 * Redis key prefixes for different data types
 */
export const REDIS_KEYS = {
  WORKER_STATUS: 'sc:worker:status:',
  WORKER_HEARTBEAT: 'sc:worker:heartbeat:',
  AUTOSCALER_STATE: 'sc:autoscaler:state',
  LOCK: 'sc:lock:',
  COUNTER: 'sc:counter:',
  METRICS: 'sc:metrics:',
  SESSION: 'sc:session:',
  CACHE: 'sc:cache:',
} as const;

/**
 * Redis connection configuration
 */
export const redisConfig = {
  socket: {
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    connectTimeout: config.REDIS_CONNECT_TIMEOUT_MS,
    lazyConnect: config.REDIS_LAZY_CONNECT,
    // Reconnect strategy
    reconnectStrategy: (retries: number) => {
      const delay = Math.min(retries * 1000, 30000); // Max 30 seconds
      console.log(`Redis reconnection attempt ${retries + 1}, delaying ${delay}ms`);
      return delay;
    },
  },
  ...(config.REDIS_PASSWORD && { password: config.REDIS_PASSWORD }),
  database: config.REDIS_DB,
  // Connection naming for monitoring
  name: 'signalcast-ingestor',
  // Performance optimizations
  readonly: false,
  // Disable offline queue if not needed
  offlineQueue: true,
  // Enable command buffering
  enableOfflineQueue: true,
  // Maximum retry attempts for commands
  maxRetriesPerRequest: 3,
  // Command timeout
  commandTimeout: 5000,
};

/**
 * Create Redis client instance
 */
export const redisClient: RedisClientType = createClient(redisConfig);

/**
 * Redis stream consumer configuration
 */
export const STREAM_CONSUMER_CONFIG = {
  // Consumer group settings
  GROUP_NAME: REDIS_CONSUMER_GROUPS.PRIMARY,
  CONSUMER_NAME: () => `worker-${process.pid}-${Date.now()}`,

  // Reading configuration
  BLOCK_TIMEOUT_MS: 2000, // How long to block waiting for messages
  COUNT_PER_READ: 100, // Max messages to read per XREAD call

  // Stream configuration
  STREAMS: Object.values(REDIS_STREAMS),
  DEADLETTER_STREAM: REDIS_STREAMS.DEADLETTER,

  // Acknowledgment timeout
  PENDING_TIMEOUT_MS: 60000, // 1 minute to process a message

  // Maximum number of times to attempt processing before deadletter
  MAX_DELIVERY_COUNT: 3,
} as const;

/**
 * Redis stream writer configuration
 */
export const STREAM_WRITER_CONFIG = {
  // Maximum stream length (trimming)
  MAX_STREAM_LENGTH: 1000000, // 1M entries per stream

  // Batch writing configuration
  BATCH_SIZE: config.TICK_BATCH_SIZE,
  BATCH_TIMEOUT_MS: 1000, // Max time to wait before flushing batch

  // Field mappings for different stream types
  FIELDS: {
    EVENTS: ['polymarket_id', 'slug', 'title', 'category', 'liquidity', 'active', 'closed', 'timestamp'],
    MARKETS: ['polymarket_id', 'event_id', 'question', 'liquidity', 'current_price', 'active', 'closed', 'timestamp'],
    OUTCOMES: ['polymarket_id', 'market_id', 'title', 'price', 'probability', 'status', 'timestamp'],
    TICKS: ['market_id', 'price', 'best_bid', 'best_ask', 'liquidity', 'volume_24h', 'timestamp'],
  },
} as const;

/**
 * Redis health check configuration
 */
export const REDIS_HEALTH_CHECK = {
  QUERY: 'PING',
  TIMEOUT_MS: 5000,
  INTERVAL_MS: 30000, // Check every 30 seconds
  EXPECTED_RESPONSE: 'PONG',
} as const;

/**
 * Redis autoscaling state management
 */
export const AUTOSCALING_STATE = {
  KEY: REDIS_KEYS.AUTOSCALER_STATE,
  TTL_SECONDS: 300, // 5 minutes TTL

  // State fields
  FIELDS: {
    WORKER_COUNT: 'worker_count',
    LAST_SCALE_UP: 'last_scale_up',
    LAST_SCALE_DOWN: 'last_scale_down',
    QUEUE_DEPTH: 'queue_depth',
    PROCESSING_RATE: 'processing_rate',
    AVERAGE_LATENCY: 'average_latency',
    LAST_UPDATE: 'last_update',
  },

  // Thresholds (stored in Redis for dynamic updates)
  THRESHOLDS: {
    SCALE_UP: 'scale_up_threshold',
    SCALE_DOWN: 'scale_down_threshold',
    MIN_WORKERS: 'min_workers',
    MAX_WORKERS: 'max_workers',
  },
} as const;

/**
 * Worker heartbeat configuration
 */
export const WORKER_HEARTBEAT = {
  TTL_SECONDS: 60, // 1 minute heartbeat timeout
  INTERVAL_MS: config.HEARTBEAT_INTERVAL_MS,

  // Heartbeat data fields
  FIELDS: {
    PID: 'pid',
    WORKER_TYPE: 'worker_type',
    START_TIME: 'start_time',
    LAST_HEARTBEAT: 'last_heartbeat',
    PROCESSED_COUNT: 'processed_count',
    ERROR_COUNT: 'error_count',
    STATUS: 'status', // active, idle, error, shutting_down
    MEMORY_USAGE: 'memory_usage',
    CPU_USAGE: 'cpu_usage',
  },
} as const;

/**
 * Lock configuration for coordinating workers
 */
export const LOCK_CONFIG = {
  DEFAULT_TTL_SECONDS: 30,
  EXTENSION_INTERVAL_MS: 15000, // Extend lock every 15 seconds
  MAX_EXTENSION_ATTEMPTS: 3,

  // Lock names
  LOCKS: {
    AUTOSCALER: `${REDIS_KEYS.LOCK}autoscaler`,
    HEALTH_CHECK: `${REDIS_KEYS.LOCK}health_check`,
    CLEANUP: `${REDIS_KEYS.LOCK}cleanup`,
    MAINTENANCE: `${REDIS_KEYS.LOCK}maintenance`,
  },
} as const;

/**
 * Metrics configuration for performance monitoring
 */
export const METRICS_CONFIG = {
  RETENTION_SECONDS: 86400, // 24 hours

  // Metric names
  NAMES: {
    MESSAGES_PROCESSED: `${REDIS_KEYS.METRICS}messages_processed`,
    MESSAGES_FAILED: `${REDIS_KEYS.METRICS}messages_failed`,
    PROCESSING_TIME: `${REDIS_KEYS.METRICS}processing_time`,
    QUEUE_DEPTH: `${REDIS_KEYS.METRICS}queue_depth`,
    WORKER_COUNT: `${REDIS_KEYS.METRICS}worker_count`,
    ERROR_RATE: `${REDIS_KEYS.METRICS}error_rate`,
    THROUGHPUT: `${REDIS_KEYS.METRICS}throughput`,
  },

  // Aggregation intervals
  INTERVALS: {
    MINUTE: 60,
    FIVE_MINUTES: 300,
    HOUR: 3600,
    DAY: 86400,
  },
} as const;

/**
 * Cache configuration for API responses and computed data
 */
export const CACHE_CONFIG = {
  DEFAULT_TTL_SECONDS: 300, // 5 minutes
  MAX_TTL_SECONDS: 3600, // 1 hour

  // Cache keys
  KEYS: {
    ACTIVE_EVENTS: `${REDIS_KEYS.CACHE}active_events`,
    EVENT_DETAILS: `${REDIS_KEYS.CACHE}event_details:`,
    MARKET_DATA: `${REDIS_KEYS.CACHE}market_data:`,
    OUTCOME_DATA: `${REDIS_KEYS.CACHE}outcome_data:`,
    API_RESPONSE: `${REDIS_KEYS.CACHE}api_response:`,
  },
} as const;

/**
 * Event handlers for Redis client
 */
redisClient.on('connect', () => {
  console.log('Redis client connected successfully');
});

redisClient.on('ready', () => {
  console.log('Redis client ready for commands');
  if (features.debug) {
    console.log('Redis connection details:', {
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      db: config.REDIS_DB,
    });
  }
});

redisClient.on('error', (err) => {
  console.error('Redis client error:', err);
});

redisClient.on('reconnecting', () => {
  console.log('Redis client reconnecting...');
});

redisClient.on('end', () => {
  console.log('Redis client connection ended');
});

/**
 * Initialize Redis client and setup
 */
export async function initializeRedis(): Promise<void> {
  try {
    if (!config.REDIS_ENABLED) {
      console.log('Redis is disabled, skipping initialization');
      return;
    }

    await redisClient.connect();

    // Create consumer groups if they don't exist
    await setupConsumerGroups();

    console.log('Redis initialized successfully');
  } catch (error) {
    console.error('Failed to initialize Redis:', error);
    throw error;
  }
}

/**
 * Setup consumer groups for streams
 */
export async function setupConsumerGroups(): Promise<void> {
  try {
    const streams = Object.values(REDIS_STREAMS);

    for (const stream of streams) {
      try {
        // Try to create consumer group (will fail if already exists)
        await redisClient.xGroupCreate(
          stream,
          REDIS_CONSUMER_GROUPS.PRIMARY,
          '0',
          {
            MKSTREAM: true, // Create stream if it doesn't exist
          }
        );
        console.log(`Created consumer group ${REDIS_CONSUMER_GROUPS.PRIMARY} for stream ${stream}`);
      } catch (error: any) {
        if (error.code === 'BUSYGROUP') {
          // Consumer group already exists, which is fine
          if (features.debug) {
            console.log(`Consumer group ${REDIS_CONSUMER_GROUPS.PRIMARY} already exists for stream ${stream}`);
          }
        } else {
          console.warn(`Error creating consumer group for stream ${stream}:`, error);
        }
      }
    }
  } catch (error) {
    console.error('Error setting up consumer groups:', error);
    throw error;
  }
}

/**
 * Graceful shutdown for Redis
 */
export async function closeRedisConnection(): Promise<void> {
  try {
    if (redisClient.isOpen) {
      await redisClient.quit();
      console.log('Redis connection closed successfully');
    }
  } catch (error) {
    console.error('Error closing Redis connection:', error);
    throw error;
  }
}

/**
 * Utility functions for common Redis operations
 */
export const redisUtils = {
  /**
   * Add message to stream
   */
  async addToStream(streamName: string, data: Record<string, string>, maxLength?: number): Promise<string> {
    const serialized = Object.entries(data).reduce<Record<string, string>>((acc, [key, value]) => {
      acc[key] = value;
      return acc;
    }, {});

    const options = maxLength
      ? {
        TRIM: {
          strategy: 'MAXLEN' as const,
          strategyModifier: '~' as const,
          threshold: maxLength,
        },
      }
      : undefined;

    return redisClient.xAdd(streamName, '*', serialized, options);
  },

  /**
   * Read from consumer group
   */
  async readFromGroup(
    consumerGroup: string,
    consumerName: string,
    count: number = 100,
    blockMs: number = 2000
  ): Promise<Array<[string, any[]]>> {
    const streams = Object.values(REDIS_STREAMS).map(stream => ({
      key: stream,
      id: '>' as const,
    }));

    const result = await redisClient.xReadGroup(
      consumerGroup,
      consumerName,
      streams,
      {
        COUNT: count,
        BLOCK: blockMs,
      }
    );

    return (result as unknown as Array<[string, any[]]>) || [];
  },

  /**
   * Acknowledge processed messages
   */
  async acknowledgeMessages(streamName: string, consumerGroup: string, messageIds: string[]): Promise<number> {
    if (messageIds.length === 0) {
      return 0;
    }
    const args: Array<string> = ['XACK', streamName, consumerGroup, ...messageIds];
    const acknowledged = await redisClient.sendCommand<number>(args);
    return acknowledged ?? 0;
  },

  /**
   * Move failed message to dead letter queue
   */
  async moveToDeadletter(streamName: string, messageId: string, error: string): Promise<void> {
    await redisClient.xAdd(REDIS_STREAMS.DEADLETTER, '*', {
      original_stream: streamName,
      original_id: messageId,
      error,
      timestamp: new Date().toISOString(),
      worker_id: process.pid.toString(),
    });
  },
};

/**
 * Export Redis configuration
 */
export default {
  client: redisClient,
  config: redisConfig,
  streams: REDIS_STREAMS,
  consumerGroups: REDIS_CONSUMER_GROUPS,
  keys: REDIS_KEYS,
  consumerConfig: STREAM_CONSUMER_CONFIG,
  writerConfig: STREAM_WRITER_CONFIG,
  healthCheck: REDIS_HEALTH_CHECK,
  autoscaling: AUTOSCALING_STATE,
  heartbeat: WORKER_HEARTBEAT,
  locks: LOCK_CONFIG,
  metrics: METRICS_CONFIG,
  cache: CACHE_CONFIG,
  initialize: initializeRedis,
  setupConsumerGroups,
  close: closeRedisConnection,
  utils: redisUtils,
};
