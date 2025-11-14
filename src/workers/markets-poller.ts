// src/workers/markets-poller.ts
import logger, { formatError } from "../lib/logger";
import { settings } from "../config/settings";
import {
  polymarketClient,
  PolymarketMarket,
  PolymarketEvent,
} from "../config/polymarket";
import { pushUpdate } from "../queues/updates.queue";
import { normalizeEvent, normalizeMarket } from "../utils/normalizeMarket";
import { heartbeatMonitor } from "./heartbeat";
import { QUEUES, WORKERS } from "../utils/constants";
import { politicsFilter } from "../utils/politicsFilter";
import { redis } from "../lib/redis";
import { bootstrap } from "../lib/bootstrap";

// simple LRU cache so we don’t requeue same event 10k times
class LRUCache<K, V> {
  private cache = new Map<K, { value: V; at: number }>();
  constructor(
    private maxSize: number,
    private ttlMs: number,
  ) {}

  get(key: K): V | undefined {
    const entry = this.cache.get(key);
    if (!entry) return;
    if (Date.now() - entry.at > this.ttlMs) {
      this.cache.delete(key);
      return;
    }
    // touch
    this.cache.delete(key);
    this.cache.set(key, { value: entry.value, at: Date.now() });
    return entry.value;
  }

  has(key: K): boolean {
    return this.get(key) !== undefined;
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) this.cache.delete(key);
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      if (firstKey !== undefined) this.cache.delete(firstKey);
    }
    this.cache.set(key, { value, at: Date.now() });
  }

  clear(): void {
    this.cache.clear();
  }
}

export class MarketsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private knownEvents = new LRUCache<string, boolean>(
    10_000,
    60 * 60 * 1000,
  ); // 10k, 1h ttl

  start(): void {
    if (this.timer) return;

    logger.info("[markets-poller] starting");
    void this.poll();
    this.timer = setInterval(
      () => void this.poll(),
      settings.marketsPollIntervalMs,
    );
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.knownEvents.clear();
  }

  private async slowBootstrapSleep() {
    return new Promise((res) => setTimeout(res, 1_000)); // 1s
  }

  private async shouldPauseForBacklog(): Promise<boolean> {
    try {
      const backlog = await redis.llen(QUEUES.markets);
      if (backlog >= settings.marketQueueBacklogThreshold) {
        logger.warn("[markets-poller] pausing run due to market queue backlog", {
          backlog,
          threshold: settings.marketQueueBacklogThreshold,
        });
        return true;
      }
    } catch (error) {
      logger.warn("[markets-poller] failed to read market queue backlog", {
        error: formatError(error),
      });
    }
    return false;
  }

  private getEventId(market: PolymarketMarket): string | null {
    return market.event?.id ?? market.eventId ?? market.id ?? null;
  }

  private async ensureEventQueued(
    eventId: string,
    event?: PolymarketEvent,
  ): Promise<void> {
    if (this.knownEvents.has(eventId)) return;

    let eventData: PolymarketEvent | null | undefined = event;
    if (!eventData) {
      try {
        eventData = await polymarketClient.getEvent(eventId);
      } catch (e) {
        logger.warn("[markets-poller] failed to fetch event", {
          eventId,
          error: formatError(e),
        });
        return;
      }
    }

    if (!eventData) {
      logger.warn("[markets-poller] event lookup returned empty", { eventId });
      return;
    }

    await pushUpdate("event", normalizeEvent(eventData));
    this.knownEvents.set(eventId, true);
  }

  private async poll(): Promise<void> {
    if (this.isRunning) {
      logger.debug("[markets-poller] skipping run, still in-flight");
      return;
    }

    // ❗ hard gate – don’t spam markets until events bootstrap is done
    const eventsDone = await bootstrap.isDone("events_done");
    if (!eventsDone) {
      logger.info(
        "[markets-poller] waiting for events bootstrap to complete before polling markets",
      );
      await this.slowBootstrapSleep();
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.marketsPoller, { state: "running" });

    let offset = 0;
    const isBootstrap = !(await bootstrap.isDone("markets_done"));
    const limit = isBootstrap
      ? settings.bootstrapMarketsPageSize
      : settings.steadyMarketsPageSize;

    let fetched = 0;
    let politicsFiltered = 0;
    let queuedUpdates = 0;

    try {
      while (true) {
        if (await this.shouldPauseForBacklog()) {
          await this.slowBootstrapSleep();
          continue;
        }

        const markets = await polymarketClient.listMarkets({
          limit,
          offset,
          closed: false,
          order: "createdAt",
          ascending: false,
        });

        if (!markets.length) break;

        const filtered = politicsFilter.filterMarkets(markets);
        politicsFiltered += filtered.length;

        for (const market of filtered) {
          const eventId = this.getEventId(market);
          if (!eventId) {
            logger.warn(
              "[markets-poller] skipping market with missing event id",
              { marketId: market.id },
            );
            continue;
          }

          // Ensure parent in queue/db
          await this.ensureEventQueued(eventId, market.event);

          await pushUpdate("market", normalizeMarket(market, { id: eventId }));
          queuedUpdates++;
        }

        fetched += markets.length;
        offset += limit;

        heartbeatMonitor.beat(WORKERS.marketsPoller, {
          fetched,
          offset,
          politicsFiltered,
          queuedUpdates,
        });

        if (markets.length < limit) break;

        if (isBootstrap) {
          await this.slowBootstrapSleep();
        }
      }

      if (isBootstrap) {
        await bootstrap.setDone("markets_done");
        logger.info("[bootstrap] markets initial load complete");
      }

      const politicsPercent =
        fetched > 0
          ? ((politicsFiltered / fetched) * 100).toFixed(1)
          : "0.0";
      logger.info(
        `[markets-poller] ✓ Completed | ${fetched} fetched | ${politicsFiltered} politics (${politicsPercent}%) | ${queuedUpdates} queued | bootstrap=${isBootstrap}`,
      );
    } catch (error) {
      logger.error("[markets-poller] poll failed", {
        error: formatError(error),
        fetched,
        politicsFiltered,
        queuedUpdates,
      });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.marketsPoller, { state: "idle" });
    }
  }
}

export const marketsPoller = new MarketsPoller();
