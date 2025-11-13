import logger, { formatError } from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient, PolymarketMarket, PolymarketEvent } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeEvent, normalizeMarket } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { QUEUES, WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';
import { redis } from '../lib/redis';

export class MarketsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private knownEvents = new Set<string>();

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

    if (await this.shouldPauseForEventBacklog()) {
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
          const eventId = this.getEventId(market);
          if (!eventId) {
            logger.warn('[markets-poller] skipping market with missing event id', { polymarketId: market.id });
            continue;
          }

          await this.ensureEventQueued(eventId, market.event);
          await pushUpdate('market', normalizeMarket(market, { id: eventId }));
          queuedUpdates++;
        }

        fetched += markets.length;
        offset += limit;

        logger.info(`[markets-poller] | ${fetched}/${offset} | politics=${politicsFiltered} | queued=${queuedUpdates}`);

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

  private async shouldPauseForEventBacklog(): Promise<boolean> {
    try {
      const backlog = await redis.llen(QUEUES.events);
      if (backlog >= settings.eventQueueBacklogThreshold) {
        logger.warn('[markets-poller] pausing run due to event queue backlog', {
          backlog,
          threshold: settings.eventQueueBacklogThreshold
        });
        heartbeatMonitor.markIdle(WORKERS.marketsPoller, { backlog, reason: 'event-backlog' });
        return true;
      }
    } catch (error) {
      logger.warn('[markets-poller] failed to read event queue backlog', { error: formatError(error) });
    }
    return false;
  }

  private getEventId(market: PolymarketMarket): string | null {
    return market.event?.id ?? market.eventId ?? market.id ?? null;
  }

  private async ensureEventQueued(eventId: string, event?: PolymarketEvent): Promise<void> {
    if (this.knownEvents.has(eventId)) {
      return;
    }

    let eventData: PolymarketEvent | null | undefined = event;
    if (!eventData) {
      try {
        eventData = await polymarketClient.getEvent(eventId);
      } catch (lookupError) {
        logger.warn('[markets-poller] failed to fetch event for market', {
          eventId,
          error: formatError(lookupError)
        });
        return;
      }
    }

    if (!eventData) {
      logger.warn('[markets-poller] event lookup returned empty result', { eventId });
      return;
    }

    await pushUpdate('event', normalizeEvent(eventData));
    this.knownEvents.add(eventId);
  }
}

export const marketsPoller = new MarketsPoller();
