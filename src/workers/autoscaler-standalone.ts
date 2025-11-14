import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { autoscaler } from "./autoscaler";
import { heartbeatMonitor } from "./heartbeat";

logger.info("autoscaler standalone starting");

// Start heartbeat monitor
heartbeatMonitor.start();

// Start the autoscaler
autoscaler.start();

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('autoscaler standalone received shutdown signal', { signal });
  autoscaler.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);