// src/workers/events-poller.ts
import logger, { formatError } from "../lib/logger";
import { settings } from "../config/settings";
import { polymarketClient } from "../config/polymarket";
import { pushUpdate } from "../queues/updates.queue";
import { normalizeEvent } from "../utils/normalizeMarket";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";
import { politicsFilter } from "../utils/politicsFilter";
import { bootstrap } from "../lib/bootstrap";
import { redis } from "../lib/redis";
import { QUEUES } from "../utils/constants";

export class EventsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  private async pageThrottle(isBootstrap: boolean) {
    // During bootstrap: ~1 page / second
    // After bootstrap: ~2 pages / second
    const targetPerSecond = isBootstrap ? 1 : 2;
    const delayMs = Math.round(1000 / targetPerSecond);
    return new Promise((res) => setTimeout(res, delayMs));
  }

  start(): void {
    if (this.timer) return;

    logger.info("[events-poller] starting");
    void this.poll();
    this.timer = setInterval(
      () => void this.poll(),
      settings.eventsPollIntervalMs,
    );
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async shouldPauseForBacklog(): Promise<boolean> {
    try {
      const backlog = await redis.llen(QUEUES.events);
      if (backlog >= settings.eventQueueBacklogThreshold) {
        logger.warn("[events-poller] pausing run due to event queue backlog", {
          backlog,
          threshold: settings.eventQueueBacklogThreshold,
        });
        return true;
      }
    } catch (error) {
      logger.warn("[events-poller] failed to read event queue backlog", {
        error: formatError(error),
      });
    }
    return false;
  }

  private async poll(): Promise<void> {
    if (this.isRunning) {
      logger.debug("[events-poller] skipping run, still in-flight");
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.eventsPoller);
    let offset = 0;
    let fetched = 0;

    const isBootstrap = !(await bootstrap.isDone("events_done"));
    const limit = isBootstrap
      ? settings.bootstrapEventsPageSize
      : settings.steadyEventsPageSize;

    try {
      while (true) {
        if (await this.shouldPauseForBacklog()) {
          await this.pageThrottle(isBootstrap);
          continue;
        }

        const events = await polymarketClient.listEvents({
          limit,
          offset,
          closed: false,
          order: "createdAt",
          ascending: false,
        });

        if (events.length === 0) break;

        const filtered = politicsFilter.filterEvents(events);

        for (const event of filtered) {
          await pushUpdate("event", normalizeEvent(event));
        }

        fetched += events.length;
        offset += limit;

        heartbeatMonitor.beat(WORKERS.eventsPoller, { fetched, offset });

        if (events.length < limit) {
          // last page
          break;
        }

        await this.pageThrottle(isBootstrap);
      }

      if (isBootstrap) {
        await bootstrap.setDone("events_done");
        logger.info("[bootstrap] events initial load complete");
      }

      logger.info(
        `[events-poller] run finished fetched=${fetched} offset=${offset} bootstrap=${isBootstrap}`,
      );
    } catch (error) {
      logger.error("[events-poller] failed to poll events", {
        error: formatError(error),
      });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.eventsPoller);
    }
  }
}

export const eventsPoller = new EventsPoller();
