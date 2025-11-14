import logger, { formatError } from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient, PolymarketMarket, PolymarketEvent } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeEvent, normalizeMarket } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { QUEUES, WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';
import { redis } from '../lib/redis';
import { bootstrap } from '../lib/bootstrap';

// LRU Cache implementation to prevent memory leaks
class LRUCache<K, V> {
  private cache = new Map<K, { value: V; timestamp: number }>();
  private maxSize: number;
  private ttlMs: number;

  constructor(maxSize: number, ttlMs: number) {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;
  }

  get(key: K): V | undefined {
    const item = this.cache.get(key);
    if (!item) return undefined;

    if (Date.now() - item.timestamp > this.ttlMs) {
      this.cache.delete(key);
      return undefined;
    }

    this.cache.delete(key);
    this.cache.set(key, item);
    return item.value;
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) {
        this.cache.delete(firstKey);
      }
    }

    this.cache.set(key, { value, timestamp: Date.now() });
  }

  has(key: K): boolean {
    return this.get(key) !== undefined;
  }

  clear(): void {
    this.cache.clear();
  }

  size(): number {
    const now = Date.now();
    const keysToDelete: K[] = [];
    this.cache.forEach((item, key) => {
      if (now - item.timestamp > this.ttlMs) {
        keysToDelete.push(key);
      }
    });
    keysToDelete.forEach(key => this.cache.delete(key));
    return this.cache.size;
  }
}

export class MarketsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  // 10k events, 1h TTL – good even for Pro tier
  private knownEvents = new LRUCache<string, boolean>(10000, 60 * 60 * 1000);

  private async slowBootstrapSleep() {
    return new Promise(res => setTimeout(res, 1000)); // 1s
  }

  private async pageThrottle(marketsDone: boolean) {
    const targetPerSecond = marketsDone ? 7 : 5;
    const delayMs = Math.round(1000 / targetPerSecond);
    return new Promise(res => setTimeout(res, delayMs));
  }

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
    this.cleanup();
  }

  private cleanup(): void {
    this.knownEvents.clear();
    logger.info('[markets-poller] cleanup completed, cache cleared');
  }

  private async poll(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    const cacheSize = this.knownEvents.size();
    if (cacheSize > 0 && cacheSize % 1000 === 0) {
      logger.info('[markets-poller] memory monitor', {
        knownEventsCount: cacheSize,
        cacheMaxSize: 10000
      });
    }

    if (await this.shouldPauseForEventBacklog()) {
      heartbeatMonitor.markIdle(WORKERS.marketsPoller, { reason: 'event-backlog' });
      return;
    }

    // Wait for events to be loaded before proceeding
    const eventsDone = await bootstrap.isDone("events_done");
    if (!eventsDone) {
      logger.info("[bootstrap] waiting for events to finish before markets");
      await this.slowBootstrapSleep();
      return;
    }
    const marketsDone = await bootstrap.isDone("markets_done");

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.marketsPoller, { state: 'running' });

    let offset = 0;
    const limit = marketsDone ? 100 : 25;
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

          // Only queue markets if events are done
          const eventsDoneAgain = await bootstrap.isDone("events_done");
          if (!eventsDoneAgain) {
            return; // Do not queue markets before events exist
          }

          await pushUpdate('market', normalizeMarket(market, { id: eventId }));
          queuedUpdates++;
        }

        fetched += markets.length;
        offset += limit;

        logger.info(`[markets-poller] | ${fetched}/${offset} | politics=${politicsFiltered} | queued=${queuedUpdates}`);
        heartbeatMonitor.beat(WORKERS.marketsPoller, {
          fetched,
          offset,
          politicsFiltered,
          queuedUpdates
        });

        if (markets.length < limit) break;

        await this.pageThrottle(marketsDone);
      }

      // Set bootstrap flag when complete
      if (!marketsDone) {
        await bootstrap.setDone("markets_done");
        logger.info("[bootstrap] markets initial load complete");
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
      heartbeatMonitor.markIdle(WORKERS.marketsPoller, { state: 'idle' });
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
    this.knownEvents.set(eventId, true);
  }
}

export const marketsPoller = new MarketsPoller();
