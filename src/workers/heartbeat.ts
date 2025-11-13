import logger, { formatError } from "../lib/logger";
import { settings } from "../config/settings";
import { redis } from "../lib/redis";
import { QUEUES } from "../utils/constants";

type WorkerState = {
  status: 'idle' | 'running';
  lastBeat: number;
  meta?: Record<string, unknown>;
};

export class HeartbeatMonitor {
  private timer: NodeJS.Timeout | null = null;
  private workers = new Map<string, WorkerState>();

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, settings.heartbeatIntervalMs);
    void this.tick();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  beat(workerId: string, meta?: Record<string, unknown>): void {
    this.workers.set(workerId, {
      status: 'running',
      lastBeat: Date.now(),
      meta: meta || {}
    });
  }

  markIdle(workerId: string, meta?: Record<string, unknown>): void {
    this.workers.set(workerId, {
      status: 'idle',
      lastBeat: Date.now(),
      meta: meta || {}
    });
  }

  getWorkers(): Record<string, WorkerState> {
    return Object.fromEntries(this.workers.entries());
  }

  private async tick(): Promise<void> {
    await this.logQueueDepth();
    this.inspectWorkers();
  }

  private async logQueueDepth(): Promise<void> {
    try {
      const queueStats: Record<string, number> = {};
      for (const [name, queue] of Object.entries(QUEUES)) {
        queueStats[name] = await redis.llen(queue);
      }
      logger.info('heartbeat queue stats', {
        queues: queueStats,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      logger.error('heartbeat failed to read queue depth', { error: formatError(error) });
    }
  }

  private inspectWorkers(): void {
    const now = Date.now();
    const threshold = settings.heartbeatIntervalMs * 2;

    for (const [workerId, state] of this.workers.entries()) {
      const lagMs = now - state.lastBeat;
      if (lagMs > threshold) {
        logger.warn('heartbeat worker missing recent report', { workerId, lagMs, state });
      }
    }
  }
}

export const heartbeatMonitor = new HeartbeatMonitor();
