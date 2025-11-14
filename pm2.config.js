module.exports = {
  apps: [
    {
      name: "ingestor-main",
      script: "dist/index.js",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "600M",
      restart_delay: 4000,
      max_restarts: 10,
      min_uptime: "10s",
      env: {
        NODE_ENV: "production"
      }
    },
    {
      name: "db-writer-v2",
      script: "dist/workers/db-writer-standalone-v2.js",
      instances: 1,
      autorestart: true,
      max_restarts: 5,
      restart_delay: 2000,
      min_uptime: "10s",
      watch: false,
      max_memory_restart: "300M",
      env: {
        WORKER_ID: "1",
        NODE_ENV: "production"
      }
    },
    {
      name: "autoscaler-v2",
      script: "dist/workers/autoscaler-standalone-v2.js",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "200M",
      restart_delay: 2000,
      max_restarts: 10,
      min_uptime: "10s",
      env: {
        NODE_ENV: "production"
      }
    }
  ],
};
