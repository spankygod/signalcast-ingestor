import pm2 from "pm2";
import logger, { formatError } from "../lib/logger";
import { redis } from "../lib/redis";
import { bootstrap } from "../lib/bootstrap";
import { QUEUES } from "../utils/constants";
import { heartbeatMonitor } from "./heartbeat";

const MAX_WORKERS = 5;
const MIN_WORKERS = 1;
const POLL_INTERVAL_MS = 5000;
const EVENTS_QUEUE_KEY = QUEUES.events;

let autoscalerIdle = false;

class Autoscaler {
  private timer: NodeJS.Timeout | null = null;
  private scalingInProgress = false;

  start(): void {
    if (this.timer) return;
    logger.info("[autoscale] starting bootstrap autoscaler");
    this.timer = setInterval(() => void this.checkAndScale(), POLL_INTERVAL_MS);
    void this.checkAndScale();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    logger.info("[autoscale] stopped");
  }

  private async checkAndScale(): Promise<void> {
    if (this.scalingInProgress) return;

    // Check if events bootstrap is done
    const eventsDone = await bootstrap.isDone("events_done");
    if (eventsDone) {
      if (!autoscalerIdle) {
        logger.info("[autoscale] events bootstrap complete, scaling down to 1 worker");
        await this.scaleToWorkers(1);
        autoscalerIdle = true;
        heartbeatMonitor.markIdle("autoscaler");
      }
      return;
    }

    // Only scale during bootstrap
    await this.scaleBasedOnEventsQueue();
  }

  private async scaleBasedOnEventsQueue(): Promise<void> {
    try {
      const eventsQueueLength = await redis.llen(EVENTS_QUEUE_KEY);

      let desiredWorkers = MIN_WORKERS;

      if (eventsQueueLength >= 7000) {
        desiredWorkers = 5;
      } else if (eventsQueueLength >= 3000) {
        desiredWorkers = 4;
      } else if (eventsQueueLength >= 1000) {
        desiredWorkers = 3;
      } else if (eventsQueueLength >= 200) {
        desiredWorkers = 2;
      } else {
        desiredWorkers = 1;
      }

      desiredWorkers = Math.min(desiredWorkers, MAX_WORKERS);

      logger.info("[autoscale] queue check", {
        queueLength: eventsQueueLength,
        desiredWorkers,
        thresholds: {
          "5workers": 7000,
          "4workers": 3000,
          "3workers": 1000,
          "2workers": 200,
          "1worker": 0
        }
      });

      await this.scaleToWorkers(desiredWorkers);

    } catch (error) {
      logger.error("[autoscale] failed to check queue length", {
        error: formatError(error)
      });
    }
  }

  private async scaleToWorkers(desiredCount: number): Promise<void> {
    if (this.scalingInProgress) return;

    this.scalingInProgress = true;

    try {
      const currentWorkers = await this.getCurrentDbWriterWorkers();

      logger.info("[autoscale] scaling evaluation", {
        currentWorkers,
        desiredCount,
        action: currentWorkers === desiredCount ? "none" :
                currentWorkers < desiredCount ? "scale_up" : "scale_down"
      });

      if (currentWorkers === desiredCount) {
        return; // No scaling needed
      }

      if (currentWorkers < desiredCount) {
        await this.scaleUp(currentWorkers, desiredCount);
      } else {
        await this.scaleDown(currentWorkers, desiredCount);
      }

      heartbeatMonitor.beat("autoscaler", {
        workers: desiredCount,
        action: currentWorkers < desiredCount ? "scaled_up" : "scaled_down"
      });

    } catch (error) {
      logger.error("[autoscale] scaling operation failed", {
        error: formatError(error)
      });
    } finally {
      this.scalingInProgress = false;
    }
  }

  private async getCurrentDbWriterWorkers(): Promise<number> {
    return new Promise((resolve, reject) => {
      pm2.connect((err) => {
        if (err) {
          reject(err);
          return;
        }

        pm2.list((listErr, list) => {
          pm2.disconnect();

          if (listErr) {
            reject(listErr);
            return;
          }

          const dbWriterCount = list.filter(p =>
            p.name && p.name.startsWith("db-writer-")
          ).length;

          resolve(dbWriterCount);
        });
      });
    });
  }

  private async scaleUp(current: number, target: number): Promise<void> {
    logger.info(`[autoscale] scaling up from ${current} to ${target} workers`);

    const promises: Promise<void>[] = [];
    for (let i = current + 1; i <= target; i++) {
      const promise = new Promise<void>((resolve, reject) => {
        pm2.start({
          script: "dist/workers/db-writer.js",
          name: `db-writer-${i}`,
          instances: 1,
          autorestart: false,
          max_restarts: 0,
        }, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
      promises.push(promise);
    }

    await Promise.all(promises);
    logger.info(`[autoscale] scaled workers: ${target}`);
  }

  private async scaleDown(current: number, target: number): Promise<void> {
    logger.info(`[autoscale] scaling down from ${current} to ${target} workers`);

    // Never scale down below 1 worker
    const actualTarget = Math.max(target, 1);

    // Get current db-writer processes and remove the highest numbered ones
    const currentProcesses = await this.getCurrentDbWriterProcesses();
    const toRemove = currentProcesses
      .filter(p => p.name && p.name.startsWith("db-writer-"))
      .sort((a, b) => {
        const aNum = parseInt(a.name?.split("-")[2] || "0");
        const bNum = parseInt(b.name?.split("-")[2] || "0");
        return bNum - aNum; // Sort descending (highest numbers first)
      })
      .slice(0, current - actualTarget);

    const promises: Promise<void>[] = [];
    for (const proc of toRemove) {
      // Never delete db-writer-1
      if (proc.name === "db-writer-1") continue;

      const promise = new Promise<void>((resolve, reject) => {
        pm2.delete(proc.name, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
      promises.push(promise);
    }

    await Promise.all(promises);
    logger.info(`[autoscale] scaled workers: ${actualTarget}`);
  }

  private async getCurrentDbWriterProcesses(): Promise<any[]> {
    return new Promise((resolve, reject) => {
      pm2.connect((err) => {
        if (err) {
          reject(err);
          return;
        }

        pm2.list((listErr, list) => {
          pm2.disconnect();

          if (listErr) {
            reject(listErr);
            return;
          }

          resolve(list);
        });
      });
    });
  }
}

export const autoscaler = new Autoscaler();
