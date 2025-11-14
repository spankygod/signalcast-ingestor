module.exports = {
  apps: [
    {
      name: "ingestor-main",
      script: "dist/index.js",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "600M",
    },
    {
      name: "db-writer-1",
      script: "dist/workers/db-writer-standalone.js",
      instances: 1,
      autorestart: false,
      max_restarts: 0,
      watch: false,
      max_memory_restart: "300M",
    },
    {
      name: "autoscaler",
      script: "dist/workers/autoscaler-standalone.js",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "200M",
    }
  ],
};
