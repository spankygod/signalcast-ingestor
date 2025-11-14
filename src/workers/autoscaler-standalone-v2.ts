import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "../lib/logger";
import { autoscalerV2 } from "./autoscaler-v2";
import { heartbeatMonitor } from "./heartbeat";

logger.info("autoscaler-v2 standalone starting", {
  nodeEnv: process.env.NODE_ENV,
  workerId: process.env.WORKER_ID || 'autoscaler'
});

try {
  // Start heartbeat monitor
  logger.info("autoscaler-v2 starting heartbeat monitor");
  heartbeatMonitor.start();

  // Start the autoscaler
  logger.info("autoscaler-v2 starting autoscaler logic");
  autoscalerV2.start();

  logger.info("autoscaler-v2 successfully started");
} catch (error) {
  logger.error("autoscaler-v2 failed to start", {
    error: formatError(error),
    stack: (error as any)?.stack
  });
  process.exit(1);
}

const shutdown = async (signal: NodeJS.Signals) => {
  logger.info('autoscaler-v2 standalone received shutdown signal', { signal });
  autoscalerV2.stop();
  heartbeatMonitor.stop();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);