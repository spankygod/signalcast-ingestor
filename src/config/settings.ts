import { z } from 'zod';

/**
 * Environment variable schema with validation
 * All required environment variables must be present or startup will fail
 */
const envSchema = z.object({
  // Node Environment
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),

  // Database Configuration (Supabase/PostgreSQL)
  DATABASE_URL: z.string().url('DATABASE_URL must be a valid PostgreSQL connection string'),

  // Individual database components (fallback if DATABASE_URL not provided)
  DB_HOST: z.string().optional(),
  DB_PORT: z.coerce.number().int().min(1).max(65535).optional(),
  DB_NAME: z.string().optional(),
  DB_USER: z.string().optional(),
  DB_PASSWORD: z.string().optional(),

  // Database Connection Pool Settings
  DB_POOL_BUDGET: z.coerce.number().int().min(1).max(50).default(10),
  DB_CONNECTION_TIMEOUT_MS: z.coerce.number().int().min(1000).default(30000),
  DB_IDLE_TIMEOUT_MS: z.coerce.number().int().min(1000).default(30000),

  // Redis Configuration
  REDIS_HOST: z.string().min(1, 'REDIS_HOST is required'),
  REDIS_PORT: z.coerce.number().int().min(1).max(65535).default(6379),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_DB: z.coerce.number().int().min(0).max(15).default(0),
  REDIS_ENABLED: z.coerce.boolean().default(true),
  REDIS_CONNECT_TIMEOUT_MS: z.coerce.number().int().min(1000).default(10000),
  REDIS_LAZY_CONNECT: z.coerce.boolean().default(true),

  // Polymarket API Configuration
  POLYMARKET_API_KEY: z.string().min(1, 'POLYMARKET_API_KEY is required'),
  POLYMARKET_SECRET: z.string().min(1, 'POLYMARKET_SECRET is required'),
  POLYMARKET_PASSPHRASE: z.string().min(1, 'POLYMARKET_PASSPHRASE is required'),
  POLYMARKET_WS_URL: z.string().url('POLYMARKET_WS_URL must be a valid WebSocket URL'),
  POLYMARKET_WS_PATH: z.string().default('/ws/market'),
  POLYMARKET_API_BASE_URL: z.string().url().default('https://clob.polymarket.com'),

  // Rate Limiting
  POLYMARKET_RATE_LIMIT_MS: z.coerce.number().int().min(100).default(200),
  POLYMARKET_BATCH_SIZE: z.coerce.number().int().min(1).max(100).default(10),

  // Worker Configuration
  MAX_CONCURRENT_JOBS: z.coerce.number().int().min(1).max(20).default(4),
  JOB_MAX_ATTEMPTS: z.coerce.number().int().min(1).max(10).default(5),
  JOB_RETRY_DELAY_MS: z.coerce.number().int().min(1000).default(5000),
  JOB_BACKOFF_MULTIPLIER: z.coerce.number().min(1).max(5).default(2),

  // Batch Processing
  EVENT_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(5),
  MARKET_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(10),
  OUTCOME_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(20),
  TICK_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(100),

  // Autoscaling Configuration
  AUTOSCALER_ENABLED: z.coerce.boolean().default(true),
  AUTOSCALER_MIN_WORKERS: z.coerce.number().int().min(1).max(10).default(1),
  AUTOSCALER_MAX_WORKERS: z.coerce.number().int().min(1).max(50).default(3),
  AUTOSCALER_CHECK_INTERVAL_MS: z.coerce.number().int().min(5000).default(10000),
  AUTOSCALER_SCALE_UP_THRESHOLD: z.coerce.number().int().min(100).default(5000),
  AUTOSCALER_SCALE_DOWN_THRESHOLD: z.coerce.number().int().min(50).default(500),
  AUTOSCALER_COOLDOWN_MS: z.coerce.number().int().min(30000).default(60000),

  // Queue Configuration
  QUEUE_DRAIN_INTERVAL_MS: z.coerce.number().int().min(1000).default(5000),
  QUEUE_CONCURRENCY: z.coerce.number().int().min(1).max(20).default(5),
  QUEUE_TIMEOUT_MS: z.coerce.number().int().min(5000).default(30000),

  // Dead Letter Queue
  DEADLETTER_ENABLED: z.coerce.boolean().default(true),
  DEADLETTER_MAX_RETRIES: z.coerce.number().int().min(1).max(10).default(3),
  DEADLETTER_RETENTION_HOURS: z.coerce.number().int().min(1).max(168).default(24),

  // Heartbeat Configuration
  HEARTBEAT_ENABLED: z.coerce.boolean().default(true),
  HEARTBEAT_INTERVAL_MS: z.coerce.number().int().min(5000).default(30000),
  HEARTBEAT_TIMEOUT_MS: z.coerce.number().int().min(10000).default(60000),

  // Logging Configuration
  LOG_LEVEL: z.enum(['error', 'warn', 'info', 'debug']).default('info'),
  LOG_FORMAT: z.enum(['json', 'pretty']).default('json'),
  LOG_FILE_ENABLED: z.coerce.boolean().default(true),
  LOG_FILE_PATH: z.string().default('./logs/app.log'),
  LOG_MAX_FILE_SIZE: z.string().default('10MB'),
  LOG_MAX_FILES: z.coerce.number().int().min(1).max(100).default(5),

  // Debug Configuration
  DEBUG_SQL_QUERIES: z.coerce.boolean().default(false),
  DEBUG_QUERY_TIMEOUT: z.coerce.number().int().min(5000).default(30000),
  DEBUG_DETAILED_TIMING: z.coerce.boolean().default(false),
  DEBUG_MEMORY_USAGE: z.coerce.boolean().default(false),

  // Data Retention
  TICK_RETENTION_MINUTES: z.coerce.number().int().min(1).default(60),
  EVENT_HISTORY_RETENTION_DAYS: z.coerce.number().int().min(1).default(90),

  // Performance Monitoring
  METRICS_ENABLED: z.coerce.boolean().default(false),
  METRICS_PORT: z.coerce.number().int().min(1024).max(65535).default(9090),
  METRICS_PATH: z.string().default('/metrics'),

  // Security
  CORS_ORIGIN: z.string().default('*'),
  RATE_LIMIT_WINDOW_MS: z.coerce.number().int().min(60000).default(900000), // 15 minutes
  RATE_LIMIT_MAX_REQUESTS: z.coerce.number().int().min(1).default(100),
});

/**
 * Type-safe environment configuration
 */
export type EnvConfig = z.infer<typeof envSchema>;

/**
 * Validate and parse environment variables
 * Throws detailed error if required variables are missing or invalid
 */
function validateEnv(): EnvConfig {
  try {
    return envSchema.parse(process.env);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const missingVars = error.issues
        .filter((err) => err.code === 'invalid_type' && (err as any).received === 'undefined')
        .map(err => `${err.path.join('.')} (${err.message})`);

      const invalidVars = error.issues
        .filter((err) => err.code !== 'invalid_type' || (err as any).received !== 'undefined')
        .map(err => `${err.path.join('.')} (${err.message})`);

      let errorMessage = 'Environment validation failed:\n';

      if (missingVars.length > 0) {
        errorMessage += '\nMissing required environment variables:\n';
        missingVars.forEach(varError => {
          errorMessage += `  - ${varError}\n`;
        });
      }

      if (invalidVars.length > 0) {
        errorMessage += '\nInvalid environment variables:\n';
        invalidVars.forEach(varError => {
          errorMessage += `  - ${varError}\n`;
        });
      }

      throw new Error(errorMessage);
    }
    throw error;
  }
}

/**
 * Export validated configuration
 * This will throw on startup if environment is invalid
 */
export const config = validateEnv();

/**
 * Helper function to build database URL from components if DATABASE_URL not provided
 */
export function buildDatabaseUrl(): string {
  if (config.DATABASE_URL) {
    return config.DATABASE_URL;
  }

  if (!config.DB_HOST || !config.DB_NAME || !config.DB_USER || !config.DB_PASSWORD) {
    throw new Error('Either DATABASE_URL or all DB components (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD) must be provided');
  }

  const port = config.DB_PORT || 5432;
  return `postgresql://${config.DB_USER}:${config.DB_PASSWORD}@${config.DB_HOST}:${port}/${config.DB_NAME}`;
}

/**
 * Development/Production helpers
 */
export const isDevelopment = config.NODE_ENV === 'development';
export const isProduction = config.NODE_ENV === 'production';
export const isTest = config.NODE_ENV === 'test';

/**
 * Feature flags based on environment
 */
export const features = {
  debug: isDevelopment || config.DEBUG_SQL_QUERIES || config.DEBUG_DETAILED_TIMING,
  metrics: config.METRICS_ENABLED,
  deadletter: config.DEADLETTER_ENABLED,
  autoscaling: config.AUTOSCALER_ENABLED,
  heartbeat: config.HEARTBEAT_ENABLED,
  redis: config.REDIS_ENABLED,
  logging: config.LOG_FILE_ENABLED,
};

/**
 * Export configuration for external modules
 */
export default config;