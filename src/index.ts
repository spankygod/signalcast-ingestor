import dotenv from "dotenv";
dotenv.config();

import logger, { formatError } from "./lib/logger";
import { eventsPollerV2 } from "./workers/events-poller-v2";
import { marketsPoller } from "./workers/markets-poller";
import { outcomesPoller } from "./workers/outcomes-poller";
import { marketChannelWorker } from "./workers/wss-market-channel";
import { heartbeatMonitor } from "./workers/heartbeat";

const workers = [
  { name: 'events-poller-v2', start: () => eventsPollerV2.start(), stop: () => eventsPollerV2.stop() },
  { name: 'markets-poller', start: () => marketsPoller.start(), stop: () => marketsPoller.stop() },
  { name: 'outcomes-poller', start: () => outcomesPoller.start(), stop: () => outcomesPoller.stop() },
  { name: 'wss-market-channel', start: () => marketChannelWorker.start(), stop: () => marketChannelWorker.stop() },
  { name: 'heartbeat', start: () => heartbeatMonitor.start(), stop: () => heartbeatMonitor.stop() }
];

export function startIngestor(): void {
  logger.info('ingestor booting workers');
  workers.forEach(worker => {
    try {
      worker.start();
    } catch (error) {
      logger.error(`ingestor failed to start ${worker.name}`, { error: formatError(error) });
    }
  });
}

export async function stopIngestor(): Promise<void> {
  logger.info('ingestor stopping workers');
  for (const worker of workers) {
    try {
      worker.stop();
    } catch (error) {
      logger.error(`ingestor failed to stop ${worker.name}`, { error: formatError(error) });
    }
  }
}

if (require.main === module) {
  startIngestor();

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info('ingestor received shutdown signal', { signal });
    await stopIngestor();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
