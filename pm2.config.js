/**
 * Simple PM2 Configuration for Deployment
 * Starts the main SignalCast orchestrator which manages all workers
 */

module.exports = {
  apps: [{
    name: 'signalcast-ingestor',
    script: 'src/index.ts',
    interpreter: 'ts-node',
    instances: 1,
    exec_mode: 'fork',
    autorestart: true,
    watch: false,
    max_memory_restart: '512M',
    env: {
      NODE_ENV: 'production',
      PROCESS_TYPE: 'orchestrator',
      TS_NODE_PROJECT: './tsconfig.json'
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