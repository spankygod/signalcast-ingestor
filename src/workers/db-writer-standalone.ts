import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { dbWriterWorker } from "./db-writer";
import { heartbeatMonitor } from "./heartbeat";

logger.info("db-writer standalone starting");

// Start heartbeat monitor
heartbeatMonitor.start();

// Start db-writer worker
dbWriterWorker.start();

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('db-writer standalone received shutdown signal', { signal });
  dbWriterWorker.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);