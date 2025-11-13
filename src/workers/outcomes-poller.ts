import logger, { formatError } from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeOutcome } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';

export class OutcomesPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  start(): void {
    if (this.timer) return;

    logger.info('[outcomes-poller] starting');
    void this.poll();
    this.timer = setInterval(
      () => void this.poll(),
      settings.outcomesPollIntervalMs
    );
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async poll(): Promise<void> {
    if (this.isRunning) {
      logger.debug('[outcomes-poller] skipping run, still in-flight');
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.outcomesPoller);
    let offset = 0;
    const limit = 100;
    let fetched = 0;

    try {
      while (true) {
        const markets = await polymarketClient.listMarkets({
          limit,
          offset,
          closed: false,
          order: 'createdAt',
          ascending: false
        });

        if (markets.length === 0) break;

        const filtered = politicsFilter.filterMarkets(markets);
        for (const market of filtered) {
          for (const outcome of market.outcomes || []) {
            await pushUpdate('outcome', normalizeOutcome(outcome, market));
          }
        }

        fetched += markets.length;
        offset += limit;
        heartbeatMonitor.beat(WORKERS.outcomesPoller, { fetched, offset });

        if (markets.length < limit || fetched > 1000) {
          break;
        }
      }

      logger.info(`[outcomes-poller] run finished fetched=${fetched} offset=${offset}`);
    } catch (error) {
      logger.error('[outcomes-poller] failed to poll outcomes', { error: formatError(error) });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.outcomesPoller);
    }
  }
}

export const outcomesPoller = new OutcomesPoller();
