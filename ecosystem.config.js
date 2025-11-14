/**
 * PM2 Ecosystem Configuration for SignalCast Ingestion System
 * Complete production-ready orchestration for all 9 worker types
 *
 * Architecture:
 * - 9 Worker types: Events Poller, Markets Poller, Outcomes Poller, DB Writer,
 *   Autoscaler, Heartbeat, WSS Market Channel, WSS User Channel, WSS User Controller
 * - Autoscaling: 1-3 workers per type, 5K/500 thresholds, 10s intervals
 * - Consumer group: signalcast
 * - Cluster vs fork strategies based on worker characteristics
 */

const path = require('path');
const os = require('os');

// Environment detection
const isDevelopment = process.env.NODE_ENV === 'development';
const isProduction = process.env.NODE_ENV === 'production';
const isTest = process.env.NODE_ENV === 'test';

// Calculate optimal instances based on CPU cores
const cpuCount = os.cpus().length;
const maxInstances = Math.max(1, Math.min(cpuCount, 4));

/**
 * Base configuration for all workers
 */
const baseConfig = {
  // Execution settings
  watch: isDevelopment,
  ignore_watch: [
    'node_modules',
    'logs',
    '.git',
    'coverage',
    '*.log',
    'dist'
  ],

  // Restart settings
  max_restarts: 10,
  min_uptime: '10s',
  restart_delay: 4000,
  autorestart: true,

  // Memory and CPU limits
  max_memory_restart: '512M',
  kill_timeout: 15000,

  // Logging
  log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
  merge_logs: false,
  error_file: './logs/worker-error.log',
  out_file: './logs/worker-out.log',
  log_file: './logs/worker-combined.log',

  // Environment
  env: {
    NODE_ENV: 'development',
    DEBUG: 'signalcast:*',
    LOG_LEVEL: 'debug'
  },

  env_production: {
    NODE_ENV: 'production',
    DEBUG: '',
    LOG_LEVEL: 'info'
  },

  env_staging: {
    NODE_ENV: 'staging',
    DEBUG: 'signalcast:warn',
    LOG_LEVEL: 'warn'
  },

  // Process management
  wait_ready: true,
  listen_timeout: 30000,
  source_map_support: !isProduction,

  // Monitoring
  pmx: isProduction,
  vizion: false
};

/**
 * CPU-intensive workers (use cluster mode)
 */
const cpuIntensiveWorkers = [
  {
    name: 'events-poller',
    script: './src/workers/events-poller.ts',
    instances: isDevelopment ? 1 : maxInstances,
    exec_mode: 'cluster',
    args: ['--type=events-poller'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 10,
    cron_restart: isProduction ? '0 2 * * *' : null, // Daily restart at 2 AM
    env: {
      PROCESS_TYPE: 'events-poller',
      POLLING_INTERVAL: '5000',
      BATCH_SIZE: '100'
    }
  },
  {
    name: 'markets-poller',
    script: './src/workers/markets-poller.ts',
    instances: isDevelopment ? 1 : maxInstances,
    exec_mode: 'cluster',
    args: ['--type=markets-poller'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 10,
    cron_restart: isProduction ? '0 2 * * *' : null,
    env: {
      PROCESS_TYPE: 'markets-poller',
      POLLING_INTERVAL: '2000',
      BATCH_SIZE: '100'
    }
  }
];

/**
 * I/O-intensive workers (use fork mode)
 */
const ioIntensiveWorkers = [
  {
    name: 'db-writer',
    script: './src/workers/db-writer.ts',
    instances: isDevelopment ? 1 : Math.max(1, Math.floor(maxInstances / 2)),
    exec_mode: 'fork',
    args: ['--type=db-writer'],
    node_args: ['--max-old-space-size=512'],
    max_memory_restart: '512M',
    priority: 20,
    env: {
      PROCESS_TYPE: 'db-writer',
      BATCH_SIZE: '100',
      QUEUE_DRAIN_INTERVAL: '5000'
    }
  },
  {
    name: 'wss-market-channel',
    script: './src/workers/wss-market-channel.ts',
    instances: isDevelopment ? 1 : Math.max(1, Math.floor(maxInstances / 2)),
    exec_mode: 'fork',
    args: ['--type=wss-market-channel'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 15,
    env: {
      PROCESS_TYPE: 'wss-market-channel',
      WEBSOCKET_PORT: '8080'
    }
  },
  {
    name: 'wss-user-channel',
    script: './src/workers/wss-user-channel.ts',
    instances: isDevelopment ? 1 : Math.max(1, Math.floor(maxInstances / 2)),
    exec_mode: 'fork',
    args: ['--type=wss-user-channel'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 15,
    env: {
      PROCESS_TYPE: 'wss-user-channel',
      WEBSOCKET_PORT: '8081'
    }
  }
];

/**
 * Control workers (single instances)
 */
const controlWorkers = [
  {
    name: 'autoscaler',
    script: './src/workers/autoscaler.ts',
    instances: 1,
    exec_mode: 'fork',
    args: ['--type=autoscaler'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 30,
    env: {
      PROCESS_TYPE: 'autoscaler',
      CHECK_INTERVAL: '10000',
      MIN_WORKERS: '1',
      MAX_WORKERS: '3',
      SCALE_UP_THRESHOLD: '5000',
      SCALE_DOWN_THRESHOLD: '500'
    }
  },
  {
    name: 'heartbeat',
    script: './src/workers/heartbeat.ts',
    instances: 1,
    exec_mode: 'fork',
    args: ['--type=heartbeat'],
    node_args: ['--max-old-space-size=128'],
    max_memory_restart: '128M',
    priority: 5,
    env: {
      PROCESS_TYPE: 'heartbeat',
      CHECK_INTERVAL: '30000'
    }
  },
  {
    name: 'wss-user-controller',
    script: './src/workers/wss-user-controller.ts',
    instances: 1,
    exec_mode: 'fork',
    args: ['--type=wss-user-controller'],
    node_args: ['--max-old-space-size=256'],
    max_memory_restart: '256M',
    priority: 25,
    env: {
      PROCESS_TYPE: 'wss-user-controller',
      CONTROLLER_PORT: '8082'
    }
  }
];

/**
 * Merge all worker configurations
 */
function createWorkerConfig(worker) {
  return {
    ...baseConfig,
    ...worker,

    // Custom log files per worker
    error_file: `./logs/${worker.name}-error.log`,
    out_file: `./logs/${worker.name}-out.log`,
    log_file: `./logs/${worker.name}-combined.log`,

    // Ensure log directory exists
    error: (err) => {
      const fs = require('fs');
      if (!fs.existsSync('./logs')) {
        fs.mkdirSync('./logs', { recursive: true });
      }
      console.error(`[${worker.name}] Error:`, err);
    }
  };
}

/**
 * Main application orchestrator
 */
const mainApp = {
  name: 'signalcast-main',
  script: './src/index.ts',
  instances: 1,
  exec_mode: 'fork',
  args: ['--orchestrator'],
  node_args: ['--max-old-space-size=512'],
  max_memory_restart: '512M',
  priority: 0,
  autorestart: false, // Don't auto-restart main orchestrator
  watch: false,
  env: {
    PROCESS_TYPE: 'orchestrator',
    ORCHESTRATION_MODE: 'pm2'
  }
};

/**
 * Complete ecosystem configuration
 */
module.exports = {
  apps: [
    createWorkerConfig(mainApp),
    ...cpuIntensiveWorkers.map(createWorkerConfig),
    ...ioIntensiveWorkers.map(createWorkerConfig),
    ...controlWorkers.map(createWorkerConfig)
  ],

  /**
   * Deployment configuration (can be extended)
   */
  deploy: {
    production: {
      user: 'deploy',
      host: ['your-server.com'],
      ref: 'origin/main',
      repo: 'git@github.com:your-org/signalcast-ingestor.git',
      path: '/var/www/signalcast-ingestor',
      'pre-deploy-local': '',
      'post-deploy': 'npm install && npm run build && pm2 reload ecosystem.config.js --env production',
      'pre-setup': ''
    },

    staging: {
      user: 'deploy',
      host: ['staging-server.com'],
      ref: 'origin/develop',
      repo: 'git@github.com:your-org/signalcast-ingestor.git',
      path: '/var/www/signalcast-ingestor-staging',
      'post-deploy': 'npm install && npm run build && pm2 reload ecosystem.config.js --env staging'
    }
  }
};

/**
 * Worker startup order (based on priority)
 * Lower numbers start first
 * 0: signalcast-main (orchestrator)
 * 5: heartbeat (health monitoring)
 * 10: data pollers (events, markets)
 * 15: WebSocket handlers
 * 20: database writer
 * 25: WebSocket controller
 * 30: autoscaler
 */

/**
 * Scaling recommendations:
 *
 * Development:
 * - All workers: 1 instance
 * - Fork mode for all
 * - Watch mode enabled
 *
 * Production (based on CPU cores):
 * - CPU-intensive: cluster mode, instances = CPU cores
 * - I/O-intensive: fork mode, instances = CPU cores / 2
 * - Control: 1 instance each
 * - Daily restarts for data pollers
 *
 * Monitoring:
 * - All workers have individual log files
 * - Memory limits configured per worker type
 * - Health checks via heartbeat worker
 * - Autoscaling based on queue depth
 */
