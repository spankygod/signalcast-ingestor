import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { dbWriterWorkerV2 } from "./db-writer-v2";
import { heartbeatMonitor } from "./heartbeat";

logger.info("db-writer-v2 standalone starting");

// Start heartbeat monitor
heartbeatMonitor.start();

// Start db-writer worker
dbWriterWorkerV2.start();

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('db-writer-v2 standalone received shutdown signal', { signal });
  dbWriterWorkerV2.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);