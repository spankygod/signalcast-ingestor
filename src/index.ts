import dotenv from "dotenv";
dotenv.config();

import logger from "./lib/logger";
import { eventsPoller } from "./workers/events-poller";
import { marketsPoller } from "./workers/markets-poller";
import { outcomesPoller } from "./workers/outcomes-poller";
import { marketChannelWorker } from "./workers/wss-market-channel";
import { dbWriterWorker } from "./workers/db-writer";
import { heartbeatMonitor } from "./workers/heartbeat";

const workers = [
  { name: 'events-poller', start: () => eventsPoller.start(), stop: () => eventsPoller.stop() },
  { name: 'markets-poller', start: () => marketsPoller.start(), stop: () => marketsPoller.stop() },
  { name: 'outcomes-poller', start: () => outcomesPoller.start(), stop: () => outcomesPoller.stop() },
  { name: 'wss-market-channel', start: () => marketChannelWorker.start(), stop: () => marketChannelWorker.stop() },
  { name: 'db-writer', start: () => dbWriterWorker.start(), stop: () => dbWriterWorker.stop() },
  { name: 'heartbeat', start: () => heartbeatMonitor.start(), stop: () => heartbeatMonitor.stop() }
];

export function startIngestor(): void {
  logger.info('ingestor', 'booting workers');
  workers.forEach(worker => {
    try {
      worker.start();
    } catch (error) {
      logger.error('ingestor', `failed to start ${worker.name}`, error);
    }
  });
}

export async function stopIngestor(): Promise<void> {
  logger.info('ingestor', 'stopping workers');
  for (const worker of workers) {
    try {
      worker.stop();
    } catch (error) {
      logger.error('ingestor', `failed to stop ${worker.name}`, error);
    }
  }
}

if (require.main === module) {
  startIngestor();

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info('ingestor', 'received shutdown signal', { signal });
    await stopIngestor();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
