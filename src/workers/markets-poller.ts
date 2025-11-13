import logger, { formatError } from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient, PolymarketMarket } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeMarket } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';

export class MarketsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  start(): void {
    if (this.timer) return;

    logger.info('[markets-poller] starting');
    void this.poll();
    this.timer = setInterval(
      () => void this.poll(),
      settings.marketsPollIntervalMs
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
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.marketsPoller);
    let offset = 0;
    const limit = 100;
    let fetched = 0;
    let politicsFiltered = 0;
    let queuedUpdates = 0;

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
        politicsFiltered += filtered.length;

        for (const market of filtered) {
          const eventMeta = market.event ?? { id: market.id };
          await pushUpdate('market', normalizeMarket(market, eventMeta));
          queuedUpdates++;
        }

        fetched += markets.length;
        offset += limit;

        logger.info(`[markets-poller] batch complete | fetched=${fetched} | offset=${offset} | politics=${politicsFiltered} | queued=${queuedUpdates}`);

        heartbeatMonitor.beat(WORKERS.marketsPoller, { fetched, offset, politicsFiltered, queuedUpdates });

        if (markets.length < limit) break;
      }

      const politicsPercent = fetched > 0 ? ((politicsFiltered / fetched) * 100).toFixed(1) : '0.0';
      logger.info(`[markets-poller] ✓ Completed | ${fetched} fetched | ${politicsFiltered} politics (${politicsPercent}%) | ${queuedUpdates} queued`);
    } catch (error) {
      logger.error('[markets-poller] poll failed', {
        error: formatError(error),
        fetched,
        politicsFiltered,
        queuedUpdates
      });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.marketsPoller);
    }
  }
}

export const marketsPoller = new MarketsPoller();
