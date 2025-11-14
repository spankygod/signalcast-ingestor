/**
 * Main configuration aggregation for SignalCast ingestion system
 * Exports all configuration modules in a centralized location
 */

// Core configuration modules
import { config, features, isDevelopment, isProduction, isTest } from './settings';
import {
  pool,
  DATABASE_TABLES,
  COLUMNS,
  INDEXES,
  BATCH_CONFIG,
  QUERIES,
  QUERY_TIMEOUTS,
  DB_HEALTH_CHECK,
  closeDatabasePool,
} from './database';
import {
  redisClient,
  REDIS_STREAMS,
  REDIS_CONSUMER_GROUPS,
  REDIS_KEYS,
  STREAM_CONSUMER_CONFIG,
  STREAM_WRITER_CONFIG,
  REDIS_HEALTH_CHECK,
  AUTOSCALING_STATE,
  WORKER_HEARTBEAT,
  LOCK_CONFIG,
  METRICS_CONFIG,
  CACHE_CONFIG,
  initializeRedis,
  closeRedisConnection,
} from './redis';
import {
  POLYMARKET_CONFIG,
  API_ENDPOINTS,
  WS_CONFIG,
  DATA_MAPPING,
  VALIDATION,
  REQUEST_CONFIG,
  PAGINATION,
  FILTERING,
  ERROR_HANDLING,
  AUTH_CONFIG,
  CACHING,
} from './polymarket';
import {
  constants,
  POLLING_INTERVALS,
  WORKER_CONFIG,
  AUTOSCALING_CONFIG,
  QUEUE_CONFIG,
  DATA_LIMITS,
  RETRY_CONFIG,
  VALIDATION_CONSTANTS,
  PERFORMANCE_CONSTANTS,
  LOGGING_CONSTANTS,
  ERROR_CODES,
  APP_METADATA,
} from './constants';

// Re-export environment configuration
export { config, features, isDevelopment, isProduction, isTest };
export type { EnvConfig } from './settings';

// Re-export database configuration
export {
  pool,
  DATABASE_TABLES,
  COLUMNS,
  INDEXES,
  BATCH_CONFIG,
  QUERIES,
  QUERY_TIMEOUTS,
  DB_HEALTH_CHECK,
  closeDatabasePool,
} from './database';

// Re-export Redis configuration
export {
  redisClient,
  REDIS_STREAMS,
  REDIS_CONSUMER_GROUPS,
  REDIS_KEYS,
  STREAM_CONSUMER_CONFIG,
  STREAM_WRITER_CONFIG,
  REDIS_HEALTH_CHECK,
  AUTOSCALING_STATE,
  WORKER_HEARTBEAT,
  LOCK_CONFIG,
  METRICS_CONFIG,
  CACHE_CONFIG,
  initializeRedis,
  closeRedisConnection,
} from './redis';

// Re-export Polymarket configuration
export {
  POLYMARKET_CONFIG,
  API_ENDPOINTS,
  WS_CONFIG,
  DATA_MAPPING,
  VALIDATION,
  REQUEST_CONFIG,
  PAGINATION,
  FILTERING,
  ERROR_HANDLING,
  AUTH_CONFIG,
  CACHING,
} from './polymarket';

// Re-export constants
export {
  constants,
  POLLING_INTERVALS,
  WORKER_CONFIG,
  AUTOSCALING_CONFIG,
  QUEUE_CONFIG,
  DATA_LIMITS,
  RETRY_CONFIG,
  VALIDATION_CONSTANTS,
  PERFORMANCE_CONSTANTS,
  LOGGING_CONSTANTS,
  ERROR_CODES,
  APP_METADATA,
} from './constants';

/**
 * Unified configuration interface
 * Provides a single point of access to all configuration values
 */
export interface AppConfig {
  // Environment
  env: {
    nodeEnv: string;
    isDevelopment: boolean;
    isProduction: boolean;
    isTest: boolean;
    features: {
      debug: boolean;
      metrics: boolean;
      deadletter: boolean;
      autoscaling: boolean;
      heartbeat: boolean;
      redis: boolean;
      logging: boolean;
    };
  };

  // Database configuration
  database: {
    pool: any; // PostgreSQL Pool instance
    tables: typeof DATABASE_TABLES;
    columns: typeof COLUMNS;
    indexes: typeof INDEXES;
    batchConfig: typeof BATCH_CONFIG;
    queries: typeof QUERIES;
    timeouts: typeof QUERY_TIMEOUTS;
    healthCheck: typeof DB_HEALTH_CHECK;
  };

  // Redis configuration
  redis: {
    client: any; // Redis client instance
    streams: typeof REDIS_STREAMS;
    consumerGroups: typeof REDIS_CONSUMER_GROUPS;
    keys: typeof REDIS_KEYS;
    consumerConfig: typeof STREAM_CONSUMER_CONFIG;
    writerConfig: typeof STREAM_WRITER_CONFIG;
    healthCheck: typeof REDIS_HEALTH_CHECK;
    autoscaling: typeof AUTOSCALING_STATE;
    heartbeat: typeof WORKER_HEARTBEAT;
    locks: typeof LOCK_CONFIG;
    metrics: typeof METRICS_CONFIG;
    cache: typeof CACHE_CONFIG;
  };

  // Polymarket configuration
  polymarket: {
    config: typeof POLYMARKET_CONFIG;
    endpoints: typeof API_ENDPOINTS;
    websocket: typeof WS_CONFIG;
    dataMapping: typeof DATA_MAPPING;
    validation: typeof VALIDATION;
    request: typeof REQUEST_CONFIG;
    pagination: typeof PAGINATION;
    filtering: typeof FILTERING;
    errorHandling: typeof ERROR_HANDLING;
    auth: typeof AUTH_CONFIG;
    caching: typeof CACHING;
  };

  // Application constants
  constants: {
    polling: typeof POLLING_INTERVALS;
    workers: typeof WORKER_CONFIG;
    autoscaling: typeof AUTOSCALING_CONFIG;
    queues: typeof QUEUE_CONFIG;
    data: typeof DATA_LIMITS;
    retry: typeof RETRY_CONFIG;
    validation: typeof VALIDATION_CONSTANTS;
    performance: typeof PERFORMANCE_CONSTANTS;
    logging: typeof LOGGING_CONSTANTS;
    errors: typeof ERROR_CODES;
    metadata: typeof APP_METADATA;
  };
}

/**
 * Complete application configuration object
 * This provides a structured way to access all configuration values
 */
export const appConfig: AppConfig = {
  env: {
    nodeEnv: config.NODE_ENV,
    isDevelopment,
    isProduction,
    isTest,
    features,
  },

  database: {
    pool,
    tables: DATABASE_TABLES,
    columns: COLUMNS,
    indexes: INDEXES,
    batchConfig: BATCH_CONFIG,
    queries: QUERIES,
    timeouts: QUERY_TIMEOUTS,
    healthCheck: DB_HEALTH_CHECK,
  },

  redis: {
    client: redisClient,
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
  },

  polymarket: {
    config: POLYMARKET_CONFIG,
    endpoints: API_ENDPOINTS,
    websocket: WS_CONFIG,
    dataMapping: DATA_MAPPING,
    validation: VALIDATION,
    request: REQUEST_CONFIG,
    pagination: PAGINATION,
    filtering: FILTERING,
    errorHandling: ERROR_HANDLING,
    auth: AUTH_CONFIG,
    caching: CACHING,
  },

  constants: {
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
  },
};

/**
 * Configuration initialization and validation
 * This function should be called during application startup
 */
export async function initializeConfiguration(): Promise<void> {
  try {
    console.log('Initializing SignalCast configuration...');

    // Log environment info
    console.log('Environment:', {
      nodeEnv: config.NODE_ENV,
      development: isDevelopment,
      production: isProduction,
      debug: features.debug,
    });

    // Initialize Redis if enabled
    if (config.REDIS_ENABLED) {
      await initializeRedis();
      console.log('Redis configuration initialized');
    } else {
      console.log('Redis is disabled, skipping initialization');
    }

    // Test database connection (optional in test environment)
    if (!isTest && config.DATABASE_URL) {
      try {
        await pool.query('SELECT 1 as health_check');
        console.log('Database connection verified');
      } catch (error) {
        console.warn('Database connection test failed:', error);
        // Don't throw error - let application handle database issues gracefully
      }
    }

    // Log key configuration values (without secrets)
    console.log('Configuration summary:', {
      polling: {
        events: `${POLLING_INTERVALS.EVENTS}ms`,
        markets: `${POLLING_INTERVALS.MARKETS}ms`,
        outcomes: `${POLLING_INTERVALS.OUTCOMES}ms`,
        ticks: `${POLLING_INTERVALS.TICKS}ms`,
      },
      autoscaling: {
        minWorkers: AUTOSCALING_CONFIG.MIN_WORKERS,
        maxWorkers: AUTOSCALING_CONFIG.MAX_WORKERS,
        scaleUpThreshold: AUTOSCALING_CONFIG.SCALE_UP_THRESHOLD,
        scaleDownThreshold: AUTOSCALING_CONFIG.SCALE_DOWN_THRESHOLD,
      },
      queue: {
        batchSize: QUEUE_CONFIG.BATCH_SIZE,
        consumerGroup: QUEUE_CONFIG.CONSUMER_GROUP,
        streams: Object.keys(REDIS_STREAMS),
      },
      database: {
        poolSize: config.DB_POOL_BUDGET,
        tables: Object.keys(DATABASE_TABLES),
      },
    });

    console.log('SignalCast configuration initialized successfully');
  } catch (error) {
    console.error('Failed to initialize configuration:', error);
    throw error;
  }
}

/**
 * Graceful shutdown of all configuration resources
 * This function should be called during application shutdown
 */
export async function shutdownConfiguration(): Promise<void> {
  try {
    console.log('Shutting down SignalCast configuration...');

    // Close Redis connection
    if (redisClient.isOpen) {
      await closeRedisConnection();
      console.log('Redis connection closed');
    }

    // Close database pool
    await closeDatabasePool();
    console.log('Database connection pool closed');

    console.log('SignalCast configuration shutdown complete');
  } catch (error) {
    console.error('Error during configuration shutdown:', error);
    throw error;
  }
}

/**
 * Configuration validation utilities
 */
export const configValidators = {
  /**
   * Validate that all required environment variables are present
   */
  validateEnvironment(): boolean {
    const requiredEnvVars = [
      'DATABASE_URL',
      'REDIS_HOST',
      'POLYMARKET_API_KEY',
      'POLYMARKET_SECRET',
      'POLYMARKET_PASSPHRASE',
    ];

    const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);

    if (missingVars.length > 0) {
      console.error('Missing required environment variables:', missingVars);
      return false;
    }

    return true;
  },

  /**
   * Validate polling intervals are reasonable
   */
  validatePollingIntervals(): boolean {
    const { EVENTS, MARKETS, OUTCOMES, TICKS } = POLLING_INTERVALS;

    // Ticks should be most frequent
    if (TICKS > OUTCOMES || TICKS > MARKETS || TICKS > EVENTS) {
      console.error('Tick polling interval should be shortest');
      return false;
    }

    // Check minimum intervals (not too fast)
    const MIN_INTERVAL = 500; // 500ms minimum
    if (TICKS < MIN_INTERVAL) {
      console.error('Tick polling interval too fast (min 500ms)');
      return false;
    }

    return true;
  },

  /**
   * Validate autoscaling configuration
   */
  validateAutoscaling(): boolean {
    const { MIN_WORKERS, MAX_WORKERS, SCALE_UP_THRESHOLD, SCALE_DOWN_THRESHOLD } = AUTOSCALING_CONFIG;

    if (MIN_WORKERS >= MAX_WORKERS) {
      console.error('MIN_WORKERS must be less than MAX_WORKERS');
      return false;
    }

    if (SCALE_UP_THRESHOLD <= SCALE_DOWN_THRESHOLD) {
      console.error('SCALE_UP_THRESHOLD must be greater than SCALE_DOWN_THRESHOLD');
      return false;
    }

    return true;
  },

  /**
   * Run all validation checks
   */
  async runAllValidations(): Promise<boolean> {
    const results = await Promise.all([
      this.validateEnvironment(),
      this.validatePollingIntervals(),
      this.validateAutoscaling(),
    ]);

    return results.every(result => result === true);
  },
};

/**
 * Configuration utilities for common operations
 */
export const configUtils = {
  /**
   * Get configuration for a specific worker type
   */
  getWorkerConfig(workerType: string) {
    const { polling, workers } = constants;

    switch (workerType) {
      case workers.TYPES.EVENTS_POLLER:
        return {
          pollingInterval: polling.EVENTS,
          queue: REDIS_STREAMS.EVENTS,
          batchSize: BATCH_CONFIG.EVENTS.BATCH_SIZE,
        };

      case workers.TYPES.MARKETS_POLLER:
        return {
          pollingInterval: polling.MARKETS,
          queue: REDIS_STREAMS.MARKETS,
          batchSize: BATCH_CONFIG.MARKETS.BATCH_SIZE,
        };

      case workers.TYPES.OUTCOMES_POLLER:
        return {
          pollingInterval: polling.OUTCOMES,
          queue: REDIS_STREAMS.OUTCOMES,
          batchSize: BATCH_CONFIG.OUTCOMES.BATCH_SIZE,
        };

      case workers.TYPES.DB_WRITER:
        return {
          pollingInterval: polling.QUEUE_DRAIN,
          queues: Object.values(REDIS_STREAMS),
          batchSizes: {
            events: BATCH_CONFIG.EVENTS.BATCH_SIZE,
            markets: BATCH_CONFIG.MARKETS.BATCH_SIZE,
            outcomes: BATCH_CONFIG.OUTCOMES.BATCH_SIZE,
            ticks: BATCH_CONFIG.TICKS.BATCH_SIZE,
          },
        };

      case workers.TYPES.AUTOSCALER:
        return {
          pollingInterval: polling.AUTOSCALING,
          config: AUTOSCALING_CONFIG,
        };

      case workers.TYPES.HEARTBEAT:
        return {
          pollingInterval: polling.HEARTBEAT,
          config: WORKER_HEARTBEAT,
        };

      default:
        throw new Error(`Unknown worker type: ${workerType}`);
    }
  },

  /**
   * Check if feature is enabled
   */
  isFeatureEnabled(feature: keyof typeof features): boolean {
    return features[feature];
  },

  /**
   * Get safe configuration for logging (without secrets)
   */
  getSafeConfig() {
    return {
      env: {
        nodeEnv: config.NODE_ENV,
        isDevelopment,
        isProduction,
        features,
      },
      polling: POLLING_INTERVALS,
      autoscaling: AUTOSCALING_CONFIG,
      queue: {
        batchSize: QUEUE_CONFIG.BATCH_SIZE,
        consumerGroup: QUEUE_CONFIG.CONSUMER_GROUP,
      },
      database: {
        poolSize: config.DB_POOL_BUDGET,
      },
      metadata: APP_METADATA,
    };
  },
};

/**
 * Default export - complete configuration object
 */
export default appConfig;