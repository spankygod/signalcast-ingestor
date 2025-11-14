import pm2 from "pm2";
import logger, { formatError } from "../lib/logger";
import { redis } from "../lib/redis";
import { BootstrapCoordinator } from "../lib/bootstrap-coordinator";
import { QUEUES } from "../utils/constants";
import { heartbeatMonitor } from "./heartbeat";
import { settings } from "../config/settings";

const MAX_WORKERS = 5;
const MIN_WORKERS = 1;
const POLL_INTERVAL_MS = 5000;
const EVENTS_QUEUE_KEY = QUEUES.events;
const COORDINATION_LOCK_TTL = 30; // 30 seconds

interface ScalingDecision {
  currentWorkers: number;
  desiredWorkers: number;
  queueLength: number;
  reason: string;
  bootstrapStatus: any;
}

interface WorkerInfo {
  name: string;
  pid: number;
  pm_id: number;
  status: string;
  cpu: number;
  memory: number;
}

export class AutoscalerV2 {
  private timer: NodeJS.Timeout | null = null;
  private scalingInProgress = false;
  private isPrimary = false;
  private lastScalingDecision: ScalingDecision | null = null;
  private coordinationKey = "autoscaler:coordination_lock";

  async start(): Promise<void> {
    if (this.timer) return;

    // Try to become primary autoscaler
    this.isPrimary = await this.acquireCoordinationLock();

    if (!this.isPrimary) {
      logger.info("[autoscaler-v2] starting as standby autoscaler");
      // Standby mode: just monitor and become primary if current primary fails
      this.timer = setInterval(() => void this.monitorPrimary(), POLL_INTERVAL_MS);
      return;
    }

    logger.info("[autoscaler-v2] starting as primary autoscaler");
    this.timer = setInterval(() => void this.checkAndScale(), POLL_INTERVAL_MS);
    void this.checkAndScale();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;

    if (this.isPrimary) {
      this.releaseCoordinationLock();
    }

    logger.info("[autoscaler-v2] stopped");
  }

  private async acquireCoordinationLock(): Promise<boolean> {
    try {
      const result = await redis.setWithEX(
        this.coordinationKey,
        JSON.stringify({
          hostname: process.env.HOSTNAME || 'unknown',
          pid: process.pid,
          startTime: Date.now()
        }),
        COORDINATION_LOCK_TTL
      );

      return result === "OK";
    } catch (error) {
      logger.error("[autoscaler-v2] failed to acquire coordination lock", {
        error: formatError(error)
      });
      return false;
    }
  }

  private async releaseCoordinationLock(): Promise<void> {
    try {
      await redis.del(this.coordinationKey);
    } catch (error) {
      logger.error("[autoscaler-v2] failed to release coordination lock", {
        error: formatError(error)
      });
    }
  }

  private async monitorPrimary(): Promise<void> {
    try {
      const lockInfo = await redis.get(this.coordinationKey);

      if (!lockInfo) {
        logger.info("[autoscaler-v2] primary lost, attempting to become primary");
        this.isPrimary = await this.acquireCoordinationLock();

        if (this.isPrimary) {
          logger.info("[autoscaler-v2] promoted to primary");
          // Restart as primary
          if (this.timer) clearInterval(this.timer);
          this.timer = setInterval(() => void this.checkAndScale(), POLL_INTERVAL_MS);
          void this.checkAndScale();
        }
      }
    } catch (error) {
      logger.error("[autoscaler-v2] failed to monitor primary", {
        error: formatError(error)
      });
    }
  }

  private async checkAndScale(): Promise<void> {
    if (this.scalingInProgress) return;

    try {
      // Refresh coordination lock
      await this.refreshCoordinationLock();

      // Check if bootstrap is complete
      const bootstrapStatus = await BootstrapCoordinator.getBootstrapStatus();

      if (bootstrapStatus.complete) {
        await this.handlePostBootstrapScaling();
        return;
      }

      // Bootstrap mode: focus on events queue drainage
      await this.scaleBasedOnEventsQueue(bootstrapStatus);

    } catch (error) {
      logger.error("[autoscaler-v2] scaling check failed", {
        error: formatError(error)
      });
    }
  }

  private async refreshCoordinationLock(): Promise<void> {
    try {
      const current = await redis.get(this.coordinationKey);
      if (current) {
        await redis.expire(this.coordinationKey, COORDINATION_LOCK_TTL);
      }
    } catch (error) {
      logger.error("[autoscaler-v2] failed to refresh coordination lock", {
        error: formatError(error)
      });
    }
  }

  private async handlePostBootstrapScaling(): Promise<void> {
    const currentWorkers = await this.getCurrentDbWriterWorkers();

    // In post-bootstrap mode, maintain 1-2 workers based on load
    const queueLengths = await this.getAllQueueLengths();
    const totalBacklog = Object.values(queueLengths).reduce((sum, len) => sum + len, 0);

    let desiredWorkers = 1;
    if (totalBacklog > 1000) {
      desiredWorkers = 2;
    }

    if (currentWorkers !== desiredWorkers) {
      logger.info("[autoscaler-v2] post-bootstrap scaling", {
        currentWorkers,
        desiredWorkers,
        queueLengths,
        totalBacklog
      });

      await this.scaleToWorkers(desiredWorkers, "post-bootstrap optimization");
    }
  }

  private async scaleBasedOnEventsQueue(bootstrapStatus: any): Promise<void> {
    try {
      const eventsQueueLength = await redis.llen(EVENTS_QUEUE_KEY);
      const currentWorkers = await this.getCurrentDbWriterWorkers();

      let desiredWorkers = MIN_WORKERS;
      let reason = "";

      if (eventsQueueLength >= 7000) {
        desiredWorkers = 5;
        reason = "critical backlog (>=7000)";
      } else if (eventsQueueLength >= 3000) {
        desiredWorkers = 4;
        reason = "high backlog (>=3000)";
      } else if (eventsQueueLength >= 1000) {
        desiredWorkers = 3;
        reason = "medium backlog (>=1000)";
      } else if (eventsQueueLength >= 200) {
        desiredWorkers = 2;
        reason = "low backlog (>=200)";
      } else {
        desiredWorkers = 1;
        reason = "minimal backlog (<200)";
      }

      // Consider bootstrap stage
      if (!bootstrapStatus.events) {
        // During events bootstrap, be more aggressive
        desiredWorkers = Math.max(desiredWorkers, 2);
        reason += " + events bootstrap";
      }

      const decision: ScalingDecision = {
        currentWorkers,
        desiredWorkers,
        queueLength: eventsQueueLength,
        reason,
        bootstrapStatus
      };

      this.lastScalingDecision = decision;

      logger.info("[autoscaler-v2] scaling decision", {
        queueLength: eventsQueueLength,
        currentWorkers,
        desiredWorkers,
        reason,
        bootstrapStage: this.getCurrentBootstrapStage(bootstrapStatus)
      });

      if (currentWorkers !== desiredWorkers) {
        await this.scaleToWorkers(desiredWorkers, reason);
      }

    } catch (error) {
      logger.error("[autoscaler-v2] failed to scale based on events queue", {
        error: formatError(error)
      });
    }
  }

  private getCurrentBootstrapStage(bootstrapStatus: any): string {
    if (!bootstrapStatus.events) return "events";
    if (!bootstrapStatus.markets) return "markets";
    if (!bootstrapStatus.outcomes) return "outcomes";
    return "complete";
  }

  private async getAllQueueLengths(): Promise<Record<string, number>> {
    const lengths: Record<string, number> = {};

    for (const [name, queueKey] of Object.entries(QUEUES)) {
      try {
        lengths[name] = await redis.llen(queueKey);
      } catch (error) {
        lengths[name] = 0;
      }
    }

    return lengths;
  }

  private async scaleToWorkers(desiredCount: number, reason: string): Promise<void> {
    if (this.scalingInProgress) return;

    this.scalingInProgress = true;

    try {
      const currentWorkers = await this.getCurrentDbWriterWorkers();

      if (currentWorkers === desiredCount) {
        return;
      }

      if (currentWorkers < desiredCount) {
        await this.scaleUp(currentWorkers, desiredCount, reason);
      } else {
        await this.scaleDown(currentWorkers, desiredCount, reason);
      }

      heartbeatMonitor.beat("autoscaler-v2", {
        workers: desiredCount,
        action: currentWorkers < desiredCount ? "scaled_up" : "scaled_down",
        reason
      });

    } catch (error) {
      logger.error("[autoscaler-v2] scaling operation failed", {
        desiredCount,
        reason,
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

  private async scaleUp(current: number, target: number, reason: string): Promise<void> {
    logger.info(`[autoscaler-v2] scaling up from ${current} to ${target} workers`, {
      reason
    });

    const promises: Promise<void>[] = [];

    for (let i = current + 1; i <= target; i++) {
      const promise = new Promise<void>((resolve, reject) => {
        const workerName = `db-writer-${i}`;

        pm2.start({
          script: "dist/workers/db-writer-standalone.js",
          name: workerName,
          instances: 1,
          autorestart: false,
          max_restarts: 0,
          env: {
            WORKER_ID: i.toString(),
            IS_SCALABLE: "true"
          }
        }, (err, proc) => {
          if (err) {
            logger.error(`[autoscaler-v2] failed to start ${workerName}`, {
              error: formatError(err)
            });
            reject(err);
          } else {
            logger.info(`[autoscaler-v2] started ${workerName}`);
            resolve();
          }
        });
      });

      promises.push(promise);
    }

    try {
      await Promise.all(promises);
      logger.info(`[autoscaler-v2] successfully scaled to ${target} workers`, {
        reason
      });
    } catch (error) {
      logger.error(`[autoscaler-v2] partial scale-up failure`, {
        target,
        current,
        error: formatError(error)
      });
    }
  }

  private async scaleDown(current: number, target: number, reason: string): Promise<void> {
    logger.info(`[autoscaler-v2] scaling down from ${current} to ${target} workers`, {
      reason
    });

    // Never scale down below 1 worker
    const actualTarget = Math.max(target, 1);

    if (actualTarget >= current) {
      return;
    }

    try {
      const currentProcesses = await this.getCurrentDbWriterProcesses();

      // Get workers to remove (highest numbered first, except db-writer-1)
      const toRemove = currentProcesses
        .filter(p => p.name && p.name.startsWith("db-writer-"))
        .sort((a, b) => {
          const aNum = parseInt(a.name?.split("-")[2] || "0");
          const bNum = parseInt(b.name?.split("-")[2] || "0");
          return bNum - aNum; // Sort descending (highest numbers first)
        })
        .slice(0, current - actualTarget)
        .filter(p => p.name !== "db-writer-1"); // Never remove db-writer-1

      const promises: Promise<void>[] = [];

      for (const proc of toRemove) {
        const promise = new Promise<void>((resolve, reject) => {
          pm2.delete(proc.name, (err) => {
            if (err) {
              logger.error(`[autoscaler-v2] failed to stop ${proc.name}`, {
                error: formatError(err)
              });
              reject(err);
            } else {
              logger.info(`[autoscaler-v2] stopped ${proc.name}`);
              resolve();
            }
          });
        });

        promises.push(promise);
      }

      await Promise.all(promises);

      logger.info(`[autoscaler-v2] successfully scaled down to ${actualTarget} workers`, {
        reason,
        removed: toRemove.length
      });

    } catch (error) {
      logger.error(`[autoscaler-v2] scale-down failure`, {
        target: actualTarget,
        current,
        error: formatError(error)
      });
    }
  }

  private async getCurrentDbWriterProcesses(): Promise<WorkerInfo[]> {
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

          const workers = list
            .filter(p => p.name && p.name.startsWith("db-writer-"))
            .map(p => ({
              name: p.name!,
              pid: p.pid || 0,
              pm_id: p.pm_id || 0,
              status: p.pm2_env?.status || 'unknown',
              cpu: p.monit?.cpu || 0,
              memory: p.monit?.memory || 0
            }));

          resolve(workers);
        });
      });
    });
  }

  // Health check and monitoring methods
  getLastScalingDecision(): ScalingDecision | null {
    return this.lastScalingDecision;
  }

  isPrimaryInstance(): boolean {
    return this.isPrimary;
  }

  async getSystemStatus(): Promise<{
    isPrimary: boolean;
    currentWorkers: number;
    lastDecision: ScalingDecision | null;
    queueLengths: Record<string, number>;
    bootstrapStatus: any;
  }> {
    const currentWorkers = await this.getCurrentDbWriterWorkers();
    const queueLengths = await this.getAllQueueLengths();
    const bootstrapStatus = await BootstrapCoordinator.getBootstrapStatus();

    return {
      isPrimary: this.isPrimary,
      currentWorkers,
      lastDecision: this.lastScalingDecision,
      queueLengths,
      bootstrapStatus
    };
  }
}

export const autoscalerV2 = new AutoscalerV2();