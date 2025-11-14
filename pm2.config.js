/**
 * Simple PM2 Configuration for Deployment
 * Starts the main SignalCast orchestrator which manages all workers
 */

require('dotenv').config();

module.exports = {
  apps: [{
    name: 'signalcast-ingestor',
    script: 'src/index.ts',
    interpreter: 'node',
    interpreter_args: '--require ts-node/register --require dotenv/config',
    instances: 1,
    exec_mode: 'fork',
    autorestart: true,
    watch: false,
    max_memory_restart: '512M',
    env_file: '.env',
    env: {
      NODE_ENV: 'production',
      PROCESS_TYPE: 'orchestrator',
      TS_NODE_PROJECT: './tsconfig.json',
      // Database Configuration
      DATABASE_URL: process.env.DATABASE_URL,

      // Redis Configuration
      REDIS_HOST: process.env.REDIS_HOST,
      REDIS_PORT: process.env.REDIS_PORT,
      REDIS_ENABLED: process.env.REDIS_ENABLED,

      // Polymarket API Configuration
      POLYMARKET_API_KEY: process.env.POLYMARKET_API_KEY,
      POLYMARKET_SECRET: process.env.POLYMARKET_SECRET,
      POLYMARKET_PASSPHRASE: process.env.POLYMARKET_PASSPHRASE,
      POLYMARKET_WS_URL: process.env.POLYMARKET_WS_URL,
      POLYMARKET_WS_PATH: process.env.POLYMARKET_WS_PATH,
      POLYMARKET_API_BASE_URL: process.env.POLYMARKET_API_BASE_URL,
      POLYMARKET_RATE_LIMIT_MS: process.env.POLYMARKET_RATE_LIMIT_MS,

      // Application Configuration
      LOG_LEVEL: process.env.LOG_LEVEL || 'info',
      JOB_MAX_ATTEMPTS: process.env.JOB_MAX_ATTEMPTS,

      // Polling Intervals
      EVENTS_POLL_INTERVAL_MS: process.env.EVENTS_POLL_INTERVAL_MS,
      MARKETS_POLL_INTERVAL_MS: process.env.MARKETS_POLL_INTERVAL_MS,
      OUTCOMES_POLL_INTERVAL_MS: process.env.OUTCOMES_POLL_INTERVAL_MS,
      HEARTBEAT_INTERVAL_MS: process.env.HEARTBEAT_INTERVAL_MS,
      QUEUE_DRAIN_INTERVAL_MS: process.env.QUEUE_DRAIN_INTERVAL_MS,

      // Worker Configuration
      EVENT_BATCH_SIZE: process.env.EVENT_BATCH_SIZE,
      TICK_RETENTION_MINUTES: process.env.TICK_RETENTION_MINUTES,
      MAX_CONCURRENT_JOBS: process.env.MAX_CONCURRENT_JOBS,
      DB_POOL_BUDGET: process.env.DB_POOL_BUDGET
    },
    error_file: './logs/signalcast-error.log',
    out_file: './logs/signalcast-out.log',
    log_file: './logs/signalcast-combined.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: false,
    time: true,
    kill_timeout: 15000,
    wait_ready: true,
    listen_timeout: 10000
  }]
};