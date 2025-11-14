import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { dbWriterWorkerV2 } from "./db-writer-v2";
import { heartbeatMonitor } from "./heartbeat";

const workerId = process.env.WORKER_ID || 'unknown';
const isScalable = process.env.IS_SCALABLE === 'true';

logger.info("db-writer-v2 standalone starting", {
  workerId,
  isScalable,
  nodeId: process.env.NODE_ENV || 'development'
});

// Start heartbeat monitor
logger.info("db-writer-v2 starting heartbeat monitor");
heartbeatMonitor.start();

// Start db-writer worker
logger.info("db-writer-v2 starting worker process", {
  workerId,
  targetQueue: "signalcast:queue:events, markets, outcomes, ticks"
});
console.log("[DB-WRITER-DEBUG] About to start dbWriterWorkerV2...");
try {
  dbWriterWorkerV2.start();
  console.log("[DB-WRITER-DEBUG] dbWriterWorkerV2 started successfully");
} catch (error) {
  console.log("[DB-WRITER-DEBUG] Failed to start dbWriterWorkerV2:", error);
}

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('db-writer-v2 standalone received shutdown signal', { signal });
  dbWriterWorkerV2.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);