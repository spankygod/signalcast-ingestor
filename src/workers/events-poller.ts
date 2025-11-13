import logger from '../lib/logger';
import { settings } from '../config/settings';
import { polymarketClient } from '../config/polymarket';
import { pushUpdate } from '../queues/updates.queue';
import { normalizeEvent } from '../utils/normalizeMarket';
import { heartbeatMonitor } from './heartbeat';
import { WORKERS } from '../utils/constants';
import { politicsFilter } from '../utils/politicsFilter';

export class EventsPoller {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  start(): void {
    if (this.timer) return;

    logger.info('[events-poller] starting');
    void this.poll();
    this.timer = setInterval(
      () => void this.poll(),
      settings.eventsPollIntervalMs
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
      logger.debug('[events-poller] skipping run, still in-flight');
      return;
    }

    this.isRunning = true;
    heartbeatMonitor.beat(WORKERS.eventsPoller);
    let offset = 0;
    const limit = 100;
    let fetched = 0;

    try {
      while (true) {
        const events = await polymarketClient.listEvents({
          limit,
          offset,
          closed: false,
          order: 'createdAt',
          ascending: false
        });

        if (events.length === 0) break;

        const filtered = politicsFilter.filterEvents(events);
        for (const event of filtered) {
          await pushUpdate('event', normalizeEvent(event));
        }

        fetched += events.length;
        offset += limit;
        heartbeatMonitor.beat(WORKERS.eventsPoller, { fetched, offset });

        if (events.length < limit || fetched > 1000) {
          break;
        }
      }

      logger.info(`[events-poller] run finished fetched=${fetched} offset=${offset}`);
    } catch (error) {
      logger.error('[events-poller] failed to poll events', { error });
    } finally {
      this.isRunning = false;
      heartbeatMonitor.markIdle(WORKERS.eventsPoller);
    }
  }
}

export const eventsPoller = new EventsPoller();
