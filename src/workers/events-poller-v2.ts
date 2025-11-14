import logger, { formatError } from "../lib/logger";
import { settings } from "../config/settings";
import { polymarketClient } from "../config/polymarket";
import { pushUpdate } from "../queues/updates.queue";
import { normalizeEvent } from "../utils/normalizeMarket";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";
import { politicsFilter } from "../utils/politicsFilter";
import { BootstrapCoordinator } from "../lib/bootstrap-coordinator";
import { redis } from "../lib/redis";
import { QUEUES } from "../utils/constants";

export class EventsPollerV2 {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private isBootstrap = true;

  async start(): Promise<void> {
    if (this.timer) return;

    logger.info("[events-poller-v2] starting");

    // Determine if we're in bootstrap mode
    this.isBootstrap = !(await BootstrapCoordinator.isStageComplete("events"));

    logger.info("[events-poller-v2] mode determined", {
      isBootstrap: this.isBootstrap,
      pollInterval: settings.eventsPollIntervalMs
    });

    // Start initial poll immediately
    await this.poll();

    // Set up recurring polling
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
    logger.info("[events-poller-v2] stopped");
  }

  private async shouldPauseForBacklog(): Promise<boolean> {
    try {
      const backlog = await redis.llen(QUEUES.events);
      const shouldPause = backlog >= settings.eventQueueBacklogThreshold;

      if (shouldPause || backlog > 0) {
        logger.info("[events-poller-v2] queue status", {
          backlog,
          threshold: settings.eventQueueBacklogThreshold,
          willPause: shouldPause,
          mode: this.isBootstrap ? "bootstrap" : "steady"
        });
      }

      return shouldPause;
    } catch (error) {
      logger.error("[events-poller-v2] failed to check queue backlog", {
        error: formatError(error)
      });
      return false;
    }
  }

  private async poll(): Promise<void> {
    if (this.isRunning) {
      logger.debug("[events-poller-v2] poll already running, skipping");
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.eventsPoller);

    const startTime = Date.now();
    let totalFetched = 0;
    let totalQueued = 0;
    let pageCount = 0;

    try {
      logger.info("[events-poller-v2] starting poll cycle", {
        isBootstrap: this.isBootstrap,
        pageSize: this.isBootstrap ? settings.bootstrapEventsPageSize : settings.steadyEventsPageSize
      });

      // In bootstrap mode, we need to ensure we get ALL events
      // In steady mode, we just get recent events
      const limit = this.isBootstrap ? settings.bootstrapEventsPageSize : settings.steadyEventsPageSize;
      let offset = 0;
      let hasMore = true;

      while (hasMore) {
        // Check for backlog
        if (await this.shouldPauseForBacklog()) {
          logger.info("[events-poller-v2] pausing due to backlog");
          break;
        }

        // Check if we should stop (in case bootstrap completed during our run)
        if (this.isBootstrap && await BootstrapCoordinator.isStageComplete("events")) {
          logger.info("[events-poller-v2] bootstrap completed during poll cycle, stopping");
          break;
        }

        pageCount++;
        logger.info("[events-poller-v2] fetching page", {
          pageCount,
          offset,
          limit,
          totalFetchedSoFar: totalFetched
        });

        const events = await polymarketClient.listEvents({
          limit,
          offset,
          closed: false,
          order: "createdAt",
          ascending: false,
        });

        if (!events || events.length === 0) {
          logger.info("[events-poller-v2] no more events available");
          hasMore = false;
          break;
        }

        const filtered = politicsFilter.filterEvents(events);

        logger.info("[events-poller-v2] page processed", {
          eventsReceived: events.length,
          afterFilter: filtered.length,
          filteredOut: events.length - filtered.length
        });

        // Queue filtered events
        for (const event of filtered) {
          await pushUpdate("event", normalizeEvent(event));
        }

        totalFetched += events.length;
        totalQueued += filtered.length;
        offset += limit;

        heartbeatMonitor.beat(WORKERS.eventsPoller, {
          fetched: totalFetched,
          queued: totalQueued,
          pages: pageCount
        });

        // If we got fewer events than the limit, we've reached the end
        if (events.length < limit) {
          logger.info("[events-poller-v2] reached end of event list");
          hasMore = false;
          break;
        }

        // Throttle between pages to respect API limits
        const throttleDelay = this.isBootstrap ? 1000 : 500;
        await new Promise(resolve => setTimeout(resolve, throttleDelay));
      }

      // If this was bootstrap mode and we think we're done, verify and set barrier
      if (this.isBootstrap && pageCount > 0) {
        logger.info("[events-poller-v2] bootstrap fetch complete", {
          totalFetched,
          totalQueued,
          pageCount,
          duration: Date.now() - startTime
        });

        // Set the bootstrap barrier
        await BootstrapCoordinator.setStageBarrier("events", totalQueued);

        // Transition out of bootstrap mode
        this.isBootstrap = false;
        logger.info("[events-poller-v2] transitioned to steady state mode");
      }

      logger.info("[events-poller-v2] poll cycle completed", {
        isBootstrap: this.isBootstrap,
        totalFetched,
        totalQueued,
        pageCount,
        duration: Date.now() - startTime
      });

    } catch (error) {
      logger.error("[events-poller-v2] poll cycle failed", {
        error: formatError(error),
        totalFetched,
        totalQueued,
        pageCount,
        isBootstrap: this.isBootstrap
      });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.eventsPoller);
    }
  }

  async forceBootstrapMode(): Promise<void> {
    logger.info("[events-poller-v2] forcing bootstrap mode");
    this.isBootstrap = true;
  }

  getMode(): "bootstrap" | "steady" {
    return this.isBootstrap ? "bootstrap" : "steady";
  }
}

export const eventsPollerV2 = new EventsPollerV2();