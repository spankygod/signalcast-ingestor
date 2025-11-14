import { redis } from "./redis";
import { QUEUES } from "../utils/constants";
import logger, { formatError } from "./logger";

interface QueueBarrier {
  stage: "events" | "markets" | "outcomes";
  timestamp: number;
  expectedCount?: number;
}

export class BootstrapCoordinator {
  private static readonly BARRIER_TTL = 300; // 5 minutes
  private static readonly LOCK_TTL = 60; // 1 minute

  static async waitForStage(
    stage: "events" | "markets" | "outcomes",
    maxWaitMs = 300_000
  ): Promise<boolean> {
    const barrierKey = `bootstrap:barrier:${stage}`;
    const checkKey = `bootstrap:check:${stage}`;

    // Set our checkpoint
    await redis.setex(checkKey, this.BARRIER_TTL, Date.now().toString());

    logger.info(`[bootstrap-coordinator] waiting for ${stage} stage barrier`);

    const startTime = Date.now();
    while (Date.now() - startTime < maxWaitMs) {
      // Check if barrier exists (stage complete)
      const barrier = await redis.get(barrierKey);
      if (barrier) {
        logger.info(`[bootstrap-coordinator] ${stage} stage barrier reached`, {
          barrierTime: barrier,
          waitTime: Date.now() - startTime
        });
        return true;
      }

      // Check if previous stages are complete
      if (stage === "markets" && !(await this.isStageComplete("events"))) {
        await this.sleep(2000);
        continue;
      }
      if (stage === "outcomes" && !(await this.isStageComplete("markets"))) {
        await this.sleep(2000);
        continue;
      }

      await this.sleep(1000);
    }

    logger.warn(`[bootstrap-coordinator] timeout waiting for ${stage} barrier`);
    return false;
  }

  static async setStageBarrier(
    stage: "events" | "markets" | "outcomes",
    processedCount: number
  ): Promise<void> {
    const barrierKey = `bootstrap:barrier:${stage}`;
    const barrierData: QueueBarrier = {
      stage,
      timestamp: Date.now(),
      expectedCount: processedCount
    };

    await redis.setex(barrierKey, this.BARRIER_TTL, JSON.stringify(barrierData));

    logger.info(`[bootstrap-coordinator] set ${stage} barrier`, {
      processedCount,
      timestamp: barrierData.timestamp
    });
  }

  static async isStageComplete(stage: "events" | "markets" | "outcomes"): Promise<boolean> {
    const barrierKey = `bootstrap:barrier:${stage}`;
    const barrier = await redis.get(barrierKey);
    return barrier !== null;
  }

  static async acquireLock(stage: string, ttl = this.LOCK_TTL): Promise<boolean> {
    const lockKey = `bootstrap:lock:${stage}`;
    const result = await redis.setWithEX(lockKey, "1", ttl);
    return result === "OK";
  }

  static async releaseLock(stage: string): Promise<void> {
    const lockKey = `bootstrap:lock:${stage}`;
    await redis.del(lockKey);
  }

  static async getQueueDepth(queueName: string): Promise<number> {
    try {
      return await redis.llen(queueName);
    } catch (error) {
      logger.error(`[bootstrap-coordinator] failed to get queue depth`, {
        queue: queueName,
        error: formatError(error)
      });
      return 0;
    }
  }

  static async areQueuesEmptyForStage(
    stage: "events" | "markets" | "outcomes"
  ): Promise<boolean> {
    const stageQueues = {
      events: [QUEUES.events],
      markets: [QUEUES.markets],
      outcomes: [QUEUES.outcomes]
    };

    const queues = stageQueues[stage];
    for (const queue of queues) {
      const depth = await this.getQueueDepth(queue);
      if (depth > 0) {
        logger.info(`[bootstrap-coordinator] ${stage} queue not empty`, {
          queue,
          depth
        });
        return false;
      }
    }

    return true;
  }

  private static sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  static async isBootstrapComplete(): Promise<boolean> {
    return (
      await this.isStageComplete("events") &&
      await this.isStageComplete("markets") &&
      await this.isStageComplete("outcomes")
    );
  }

  static async getBootstrapStatus(): Promise<{
    events: boolean;
    markets: boolean;
    outcomes: boolean;
    complete: boolean;
  }> {
    const events = await this.isStageComplete("events");
    const markets = await this.isStageComplete("markets");
    const outcomes = await this.isStageComplete("outcomes");

    return {
      events,
      markets,
      outcomes,
      complete: events && markets && outcomes
    };
  }
}