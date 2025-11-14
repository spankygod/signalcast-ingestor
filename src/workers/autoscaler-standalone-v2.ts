import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { autoscalerV2 } from "./autoscaler-v2";
import { heartbeatMonitor } from "./heartbeat";

logger.info("autoscaler-v2 standalone starting");

// Start heartbeat monitor
heartbeatMonitor.start();

// Start the autoscaler
autoscalerV2.start();

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('autoscaler-v2 standalone received shutdown signal', { signal });
  autoscalerV2.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);