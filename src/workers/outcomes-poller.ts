import logger, { formatError } from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeOutcome } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';
import { bootstrap } from '../lib/bootstrap';

export class OutcomesPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private async slowBootstrapSleep() {
    return new Promise(res => setTimeout(res, 3_000)); // 3s - reduce API pressure
  }

  private async pageThrottle() {
    const delayMs = Math.round(1000 / 7);
    return new Promise(res => setTimeout(res, delayMs));
  }

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

    // Check bootstrap flags BEFORE making any API calls
    const eventsDone = await bootstrap.isDone("events_done");
    if (!eventsDone) {
      logger.info("[bootstrap] outcomes-poller waiting for events_done flag");
      await this.slowBootstrapSleep();
      return;
    }

    const marketsDone = await bootstrap.isDone("markets_done");
    if (!marketsDone) {
      logger.info("[bootstrap] outcomes-poller waiting for markets_done flag");
      await this.slowBootstrapSleep();
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.outcomesPoller);
    let offset = 0;
    let fetched = 0;

    try {
      const limit = 100;

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

        await this.pageThrottle();
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
