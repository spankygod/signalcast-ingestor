/**
 * Application constants and polling intervals
 * Based on the confirmed SignalCast architecture requirements
 */

/**
 * Polling intervals for different data sources
 * All values are in milliseconds unless specified otherwise
 */
export const POLLING_INTERVALS = {
  // Polling frequencies (confirmed architecture)
  EVENTS: 5000,      // 5 seconds - Events poller
  MARKETS: 2000,     // 2 seconds - Markets poller
  OUTCOMES: 2000,    // 2 seconds - Outcomes poller
  TICKS: 1000,       // 1 second - Real-time price ticks

  // System intervals
  HEARTBEAT: 30000,      // 30 seconds - Worker heartbeat
  AUTOSCALING: 10000,    // 10 seconds - Autoscaling checks
  HEALTH_CHECK: 60000,   // 1 minute - Health checks
  CLEANUP: 300000,       // 5 minutes - Cleanup tasks
  METRICS: 60000,        // 1 minute - Metrics collection

  // Queue management
  QUEUE_DRAIN: 5000,     // 5 seconds - Queue processing
  RETRY_DELAY: 5000,     // 5 seconds - Retry delay for failed jobs
  BACKOFF_INITIAL: 1000, // 1 second - Initial backoff

  // Data retention
  TICK_RETENTION: 60 * 60 * 1000, // 1 hour - Tick data retention
  LOG_RETENTION: 7 * 24 * 60 * 60 * 1000, // 7 days - Log retention
} as const;

/**
 * Worker configuration constants
 */
export const WORKER_CONFIG = {
  // Worker types
  TYPES: {
    EVENTS_POLLER: 'events-poller',
    MARKETS_POLLER: 'markets-poller',
    OUTCOMES_POLLER: 'outcomes-poller',
    DB_WRITER: 'db-writer',
    AUTOSCALER: 'autoscaler',
    HEARTBEAT: 'heartbeat',
  },

  // Worker states
  STATES: {
    STARTING: 'starting',
    RUNNING: 'running',
    IDLE: 'idle',
    ERROR: 'error',
    STOPPING: 'stopping',
    STOPPED: 'stopped',
  },

  // Default settings
  DEFAULT_TIMEOUT: 30000, // 30 seconds
  MAX_MEMORY_MB: 512,     // 512MB memory limit
  MAX_CPU_PERCENT: 80,    // 80% CPU usage limit
} as const;

/**
 * Autoscaling configuration
 * Based on the confirmed architecture requirements
 */
export const AUTOSCALING_CONFIG = {
  // Worker count limits
  MIN_WORKERS: 1,
  MAX_WORKERS: 3,

  // Scaling thresholds (queue depth)
  SCALE_UP_THRESHOLD: 5000,   // Scale up when queue > 5K items
  SCALE_DOWN_THRESHOLD: 500,  // Scale down when queue < 500 items

  // Timing
  CHECK_INTERVAL: POLLING_INTERVALS.AUTOSCALING, // 10 seconds
  COOLDOWN_PERIOD: 60000,     // 1 minute cooldown between scaling actions
  SCALE_UP_DELAY: 5000,       // 5 seconds delay before scaling up
  SCALE_DOWN_DELAY: 15000,    // 15 seconds delay before scaling down

  // Performance thresholds
  MAX_LATENCY_MS: 5000,       // 5 seconds max processing latency
  MIN_THROUGHPUT_PER_MINUTE: 100, // Minimum items per minute per worker
} as const;

/**
 * Queue configuration
 * Based on the confirmed Redis stream architecture
 */
export const QUEUE_CONFIG = {
  // Queue names (matching Redis streams)
  QUEUES: {
    EVENTS: 'sc:events',
    MARKETS: 'sc:markets',
    OUTCOMES: 'sc:outcomes',
    TICKS: 'sc:ticks',
    DEADLETTER: 'sc:deadletter',
  },

  // Consumer group
  CONSUMER_GROUP: 'signalcast',

  // Batch processing
  BATCH_SIZE: 100,            // Max items per batch
  BATCH_TIMEOUT: 2000,        // 2 seconds max wait for batch

  // Concurrency
  CONCURRENT_CONSUMERS: 2,    // Concurrent consumers per queue
  MAX_PARALLEL_PROCESSES: 10, // Max parallel processing jobs

  // Acknowledgment and timeouts
  ACK_TIMEOUT: 60000,         // 1 minute to acknowledge message
  VISIBILITY_TIMEOUT: 30000,  // 30 seconds visibility timeout
  MAX_RECEIVE_COUNT: 3,       // Max attempts before deadletter

  // Queue depth limits
  MAX_QUEUE_DEPTH: 100000,    // 100K items max queue depth
  HIGH_WATERMARK: 75000,      // 75K items high watermark
  LOW_WATERMARK: 25000,       // 25K items low watermark
} as const;

/**
 * Data processing limits
 */
export const DATA_LIMITS = {
  // API request limits
  MAX_EVENTS_PER_REQUEST: 100,
  MAX_MARKETS_PER_REQUEST: 100,
  MAX_OUTCOMES_PER_REQUEST: 200,
  MAX_TICKS_PER_BATCH: 1000,

  // Database batch limits
  DB_BATCH_SIZE_EVENTS: 50,
  DB_BATCH_SIZE_MARKETS: 100,
  DB_BATCH_SIZE_OUTCOMES: 200,
  DB_BATCH_SIZE_TICKS: 500,

  // Memory limits for data structures
  MAX_CONCURRENT_EVENTS: 1000,
  MAX_CONCURRENT_MARKETS: 5000,
  MAX_CONCURRENT_OUTCOMES: 10000,
  MAX_TICK_BUFFER_SIZE: 5000,

  // Processing timeouts
  PROCESSING_TIMEOUT_EVENT: 30000,  // 30 seconds
  PROCESSING_TIMEOUT_MARKET: 10000, // 10 seconds
  PROCESSING_TIMEOUT_OUTCOME: 10000, // 10 seconds
  PROCESSING_TIMEOUT_TICK: 1000,    // 1 second
} as const;

/**
 * Retry configuration
 */
export const RETRY_CONFIG = {
  // Max attempts by operation type
  MAX_ATTEMPTS: {
    API_REQUEST: 3,
    DATABASE_WRITE: 5,
    REDIS_OPERATION: 3,
    WEBOCKET_CONNECTION: 10,
    WORKER_STARTUP: 3,
  },

  // Backoff strategies
  BACKOFF: {
    INITIAL_DELAY: 1000,     // 1 second
    MAX_DELAY: 60000,        // 1 minute
    MULTIPLIER: 2,           // Exponential backoff
    JITTER: true,            // Add random jitter
  },

  // Retry conditions
  RETRYABLE_ERRORS: [
    'ECONNRESET',
    'ETIMEDOUT',
    'ENOTFOUND',
    'ECONNREFUSED',
    'timeout',
    'rate_limit_exceeded',
    'temporary_failure',
  ],
} as const;

/**
 * Data validation constants
 */
export const VALIDATION_CONSTANTS = {
  // String length limits
  MAX_TITLE_LENGTH: 500,
  MAX_DESCRIPTION_LENGTH: 2000,
  MAX_SLUG_LENGTH: 200,
  MAX_CATEGORY_LENGTH: 100,
  MAX_ID_LENGTH: 100,

  // Numeric value ranges
  PRICE_MIN: 0,
  PRICE_MAX: 1,
  PROBABILITY_MIN: 0,
  PROBABILITY_MAX: 1,
  LIQUIDITY_MIN: 0,
  VOLUME_MIN: 0,
  RELEVANCE_SCORE_MIN: 0,
  RELEVANCE_SCORE_MAX: 100,

  // Status values
  EVENT_STATUSES: ['active', 'inactive', 'closed', 'archived'],
  MARKET_STATUSES: ['active', 'inactive', 'resolved', 'cancelled'],
  OUTCOME_STATUSES: ['active', 'inactive', 'won', 'lost'],

  // Categories (common Polymarket categories)
  CATEGORIES: [
    'politics',
    'sports',
    'entertainment',
    'technology',
    'business',
    'science',
    'health',
    'world-events',
    'cryptocurrency',
    'other',
  ],
} as const;

/**
 * Performance monitoring constants
 */
export const PERFORMANCE_CONSTANTS = {
  // Metrics collection intervals
  METRICS_INTERVAL: POLLING_INTERVALS.METRICS, // 1 minute

  // Performance thresholds (milliseconds)
  RESPONSE_TIME_THRESHOLDS: {
    FAST: 100,      // < 100ms is fast
    NORMAL: 500,    // < 500ms is normal
    SLOW: 2000,     // < 2s is acceptable
    CRITICAL: 5000, // > 5s is critical
  },

  // Throughput metrics
  THROUGHPUT_THRESHOLDS: {
    MIN_EVENTS_PER_MINUTE: 12,    // 1 event per 5 seconds
    MIN_MARKETS_PER_MINUTE: 30,   // 1 market per 2 seconds
    MIN_OUTCOMES_PER_MINUTE: 30,  // 1 outcome per 2 seconds
    MIN_TICKS_PER_MINUTE: 60,     // 1 tick per second
  },

  // Memory usage thresholds (percentage)
  MEMORY_THRESHOLDS: {
    WARNING: 70,    // 70% memory usage warning
    CRITICAL: 90,   // 90% memory usage critical
    MAX: 95,        // 95% memory usage max
  },

  // CPU usage thresholds (percentage)
  CPU_THRESHOLDS: {
    WARNING: 70,    // 70% CPU usage warning
    CRITICAL: 90,   // 90% CPU usage critical
    MAX: 95,        // 95% CPU usage max
  },
} as const;

/**
 * Logging constants
 */
export const LOGGING_CONSTANTS = {
  // Log levels
  LEVELS: {
    ERROR: 0,
    WARN: 1,
    INFO: 2,
    DEBUG: 3,
  },

  // Log formats
  FORMATS: {
    JSON: 'json',
    PRETTY: 'pretty',
    STRUCTURED: 'structured',
  },

  // Log categories
  CATEGORIES: {
    SYSTEM: 'system',
    API: 'api',
    DATABASE: 'database',
    REDIS: 'redis',
    WEBSOCKET: 'websocket',
    WORKER: 'worker',
    AUTOSCALER: 'autoscaler',
    PERFORMANCE: 'performance',
    ERROR: 'error',
  },

  // Sensitive data to redact
  SENSITIVE_FIELDS: [
    'password',
    'secret',
    'token',
    'key',
    'authorization',
    'cookie',
    'session',
    'credential',
  ],
} as const;

/**
 * Error codes and messages
 */
export const ERROR_CODES = {
  // System errors (1000-1999)
  SYSTEM_STARTUP_FAILED: 1000,
  SYSTEM_SHUTDOWN_FAILED: 1001,
  WORKER_STARTUP_FAILED: 1002,
  WORKER_CRASHED: 1003,

  // Configuration errors (2000-2999)
  INVALID_CONFIG: 2000,
  MISSING_ENV_VAR: 2001,
  INVALID_POLLING_INTERVAL: 2002,

  // Database errors (3000-3999)
  DB_CONNECTION_FAILED: 3000,
  DB_QUERY_FAILED: 3001,
  DB_TIMEOUT: 3002,
  DB_CONSTRAINT_VIOLATION: 3003,

  // Redis errors (4000-4999)
  REDIS_CONNECTION_FAILED: 4000,
  REDIS_TIMEOUT: 4001,
  REDIS_STREAM_ERROR: 4002,

  // API errors (5000-5999)
  API_REQUEST_FAILED: 5000,
  API_RATE_LIMITED: 5001,
  API_AUTH_FAILED: 5002,
  API_INVALID_RESPONSE: 5003,

  // WebSocket errors (6000-6999)
  WS_CONNECTION_FAILED: 6000,
  WS_MESSAGE_PARSE_FAILED: 6001,
  WS_SUBSCRIPTION_FAILED: 6002,

  // Data processing errors (7000-7999)
  DATA_VALIDATION_FAILED: 7000,
  DATA_TRANSFORMATION_FAILED: 7001,
  BATCH_PROCESSING_FAILED: 7002,

  // Queue errors (8000-8999)
  QUEUE_FULL: 8000,
  MESSAGE_PROCESSING_FAILED: 8001,
  DEADLETTER_OVERFLOW: 8002,
} as const;

/**
 * Application metadata
 */
export const APP_METADATA = {
  NAME: 'signalcast-ingestor',
  VERSION: '1.0.0',
  DESCRIPTION: 'SignalCast Polymarket Data Ingestion System',
  AUTHOR: 'SignalCast Team',
  HOMEPAGE: 'https://github.com/signalcast/ingestor',

  // Build and deployment info
  BUILD_NUMBER: process.env['BUILD_NUMBER'] || 'unknown',
  COMMIT_SHA: process.env['COMMIT_SHA'] || 'unknown',
  DEPLOYMENT_ENV: process.env['NODE_ENV'] || 'development',
  STARTUP_TIME: new Date().toISOString(),

  // Node.js info
  NODE_VERSION: process.version,
  PLATFORM: process.platform,
  ARCH: process.arch,
} as const;

/**
 * Export all constants as named export for index.ts compatibility
 */
export const constants = {
  polling: POLLING_INTERVALS,
  workers: WORKER_CONFIG,
  autoscaling: AUTOSCALING_CONFIG,
  queues: QUEUE_CONFIG,
  data: DATA_LIMITS,
  retry: RETRY_CONFIG,
  validation: VALIDATION_CONSTANTS,
  performance: PERFORMANCE_CONSTANTS,
  logging: LOGGING_CONSTANTS,
  errors: ERROR_CODES,
  metadata: APP_METADATA,
};

/**
 * Export all constants as default export
 */
export default constants;