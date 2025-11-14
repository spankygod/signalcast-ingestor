import { sql, inArray, eq, and, desc } from "drizzle-orm";
import logger, { formatError } from "../lib/logger";
import {
  dbInstance,
  events,
  markets,
  outcomes,
  marketPricesRealtime,
} from "../lib/db";
import {
  NormalizedEvent,
  NormalizedMarket,
  NormalizedOutcome,
  normalizeMarket,
  normalizeEvent,
} from "../utils/normalizeMarket";
import { NormalizedTick } from "../utils/normalizeTick";
import {
  pullNextUpdate,
  pushUpdate,
  UpdateJob,
  UpdateKind,
} from "../queues/updates.queue";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";
import { settings } from "../config/settings";
import { BootstrapCoordinator } from "../lib/bootstrap-coordinator";
import { redis } from "../lib/redis";
import { QUEUES } from "../utils/constants";

interface RetryableJob extends UpdateJob<any> {
  attempts: number;
  lastAttempt: number;
  nextRetryAt: number;
  reason?: string;
}

export class DbWriterWorkerV2 {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;
  private retryQueue = new Map<string, RetryableJob>();
  private starting = false;
  private lastDrainTime = 0;
  private lastSuccessfulOperation = Date.now();
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private deadlockDetectionTimer: NodeJS.Timeout | null = null;

  // Configuration
  private readonly DRAIN_INTERVAL = 1000; // Increased from 200ms
  private readonly MAX_RETRY_ATTEMPTS = 5;
  private readonly RETRY_BACKOFF_MS = [5000, 15000, 45000, 120000, 300000];
  private readonly RETRY_STORAGE_PREFIX = "signalcast:retry:db-writer";
  private readonly RETRY_STORAGE_TTL_SECONDS = 60 * 60 * 24; // 24h
  private readonly HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
  private readonly DEADLOCK_TIMEOUT = 60000; // 60 seconds
  private readonly OPERATION_TIMEOUT = 30000; // 30 seconds per operation

  private getJobIdentifier(job: UpdateJob<any>): string {
    const payload = job.payload as Record<string, unknown>;
    return (
      (payload?.polymarket_id as string | undefined) ||
      (payload?.market_polymarket_id as string | undefined) ||
      (payload?.event_polymarket_id as string | undefined) ||
      (payload?.id as string | undefined) ||
      job.queuedAt ||
      Math.random().toString(36).slice(2)
    );
  }

  private getRetryJobKey(job: UpdateJob<any>): string {
    return `${job.kind}:${this.getJobIdentifier(job)}`;
  }

  private getPersistedRetryKey(jobKey: string): string {
    return `${this.RETRY_STORAGE_PREFIX}:${jobKey}`;
  }

  private async restorePersistedRetries(): Promise<void> {
    try {
      const pattern = `${this.RETRY_STORAGE_PREFIX}:*`;
      const keys = await redis.keys(pattern);
      if (!keys.length) return;

      let restored = 0;
      for (const key of keys) {
        const payload = await redis.get(key);
        if (!payload) continue;
        try {
          const job = JSON.parse(payload) as RetryableJob;
          if (!job || typeof job.nextRetryAt !== "number") continue;
          const jobKey = key.slice(this.RETRY_STORAGE_PREFIX.length + 1);
          this.retryQueue.set(jobKey, job);
          restored++;
        } catch (error) {
          logger.warn("[db-writer-v2] failed to parse persisted retry job", {
            key,
            error: formatError(error)
          });
        }
      }

      if (restored > 0) {
        logger.info("[db-writer-v2] restored retry queue from redis", {
          restored
        });
      }
    } catch (error) {
      logger.error("[db-writer-v2] failed to restore retry queue", {
        error: formatError(error)
      });
    }
  }

  private async persistRetryJob(jobKey: string, job: RetryableJob): Promise<void> {
    try {
      await redis.setWithEX(
        this.getPersistedRetryKey(jobKey),
        JSON.stringify(job),
        this.RETRY_STORAGE_TTL_SECONDS
      );
    } catch (error) {
      logger.warn("[db-writer-v2] failed to persist retry job", {
        jobKey,
        error: formatError(error)
      });
    }
  }

  private async removePersistedRetry(jobKey: string): Promise<void> {
    try {
      await redis.del(this.getPersistedRetryKey(jobKey));
    } catch (error) {
      logger.warn("[db-writer-v2] failed to delete persisted retry job", {
        jobKey,
        error: formatError(error)
      });
    }
  }

  private async scheduleRetryJob(
    job: UpdateJob<any>,
    reason: string,
    attempts = job.attempts ?? 0
  ): Promise<void> {
    const nextAttempt = attempts + 1;
    if (nextAttempt > this.MAX_RETRY_ATTEMPTS) {
      logger.error("[db-writer-v2] retry limit hit", {
        kind: job.kind,
        reason,
        attempts: nextAttempt
      });
      return;
    }

    const backoffMs =
      this.RETRY_BACKOFF_MS[
        Math.min(nextAttempt - 1, this.RETRY_BACKOFF_MS.length - 1)
      ] ?? this.RETRY_BACKOFF_MS[this.RETRY_BACKOFF_MS.length - 1] ?? 300000;

    const retryJob: RetryableJob = {
      ...job,
      attempts: nextAttempt,
      lastAttempt: Date.now(),
      nextRetryAt: Date.now() + backoffMs,
      reason
    };

    const jobKey = this.getRetryJobKey(job);
    this.retryQueue.set(jobKey, retryJob);
    await this.persistRetryJob(jobKey, retryJob);

    logger.warn("[db-writer-v2] job scheduled for retry", {
      kind: job.kind,
      reason,
      attempt: nextAttempt,
      nextRetryIn: backoffMs
    });
  }

  start(): void {
    if (this.timer || this.starting) return;
    this.starting = true;
    logger.info("[db-writer-v2] starting with health monitoring");
    void this.boot();
  }

  private async boot(): Promise<void> {
    try {
      await this.restorePersistedRetries();
      this.timer = setInterval(() => void this.drain(), this.DRAIN_INTERVAL);
      this.startHealthMonitoring();
      this.startDeadlockDetection();
      await this.drain();
    } catch (error) {
      logger.error("[db-writer-v2] failed to initialize", {
        error: formatError(error)
      });
      if (this.timer) {
        clearInterval(this.timer);
        this.timer = null;
      }
      this.stopHealthMonitoring();
      this.stopDeadlockDetection();
    } finally {
      this.starting = false;
    }
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    this.stopHealthMonitoring();
    this.stopDeadlockDetection();
    logger.info("[db-writer-v2] stopped with health monitoring");
  }

  private startHealthMonitoring(): void {
    this.healthCheckTimer = setInterval(() => {
      this.performHealthCheck();
    }, this.HEALTH_CHECK_INTERVAL);
  }

  private stopHealthMonitoring(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }
  }

  private startDeadlockDetection(): void {
    this.deadlockDetectionTimer = setInterval(() => {
      this.checkForDeadlocks();
    }, this.DEADLOCK_TIMEOUT);
  }

  private stopDeadlockDetection(): void {
    if (this.deadlockDetectionTimer) {
      clearInterval(this.deadlockDetectionTimer);
      this.deadlockDetectionTimer = null;
    }
  }

  private performHealthCheck(): void {
    const now = Date.now();
    const timeSinceLastDrain = now - this.lastDrainTime;
    const timeSinceLastSuccess = now - this.lastSuccessfulOperation;

    logger.info("[db-writer-v2] health check", {
      workerId: process.env.WORKER_ID || 'unknown',
      timeSinceLastDrain,
      timeSinceLastSuccess,
      draining: this.draining,
      retryQueueSize: this.retryQueue.size,
      isTimerActive: !!this.timer
    });

    // If we haven't had a successful operation in 2 minutes, we might be stuck
    if (timeSinceLastSuccess > 120000) {
      logger.error("[db-writer-v2] potential hang detected", {
        timeSinceLastSuccess,
        lastDrainTime: this.lastDrainTime,
        draining: this.draining,
        retryQueueSize: this.retryQueue.size
      });

      // Try to recover by restarting the drain cycle
      this.recoverFromHang();
    }
  }

  private checkForDeadlocks(): void {
    const now = Date.now();

    // Check if we've been in draining state for too long
    if (this.draining && (now - this.lastDrainTime > this.DEADLOCK_TIMEOUT)) {
      logger.error("[db-writer-v2] deadlock detected - stuck in draining state", {
        drainDuration: now - this.lastDrainTime,
        retryQueueSize: this.retryQueue.size,
        workerId: process.env.WORKER_ID || 'unknown'
      });

      // Force recovery
      this.recoverFromDeadlock();
    }
  }

  private recoverFromHang(): void {
    logger.warn("[db-writer-v2] attempting recovery from hang", {
      workerId: process.env.WORKER_ID || 'unknown'
    });

    try {
      // Reset draining state
      this.draining = false;

      // Clear and restart timer
      if (this.timer) {
        clearInterval(this.timer);
      }
      this.timer = setInterval(() => void this.drain(), this.DRAIN_INTERVAL);

      // Reset timestamps
      this.lastDrainTime = Date.now();
      this.lastSuccessfulOperation = Date.now();

      logger.info("[db-writer-v2] recovery completed");
    } catch (error) {
      logger.error("[db-writer-v2] recovery failed", {
        error: formatError(error)
      });
    }
  }

  private recoverFromDeadlock(): void {
    logger.error("[db-writer-v2] forcing recovery from deadlock", {
      workerId: process.env.WORKER_ID || 'unknown'
    });

    try {
      // Force reset all states
      this.draining = false;

      // Clear all timers
      if (this.timer) {
        clearInterval(this.timer);
        this.timer = null;
      }

      // Restart everything
      this.timer = setInterval(() => void this.drain(), this.DRAIN_INTERVAL);
      this.lastDrainTime = Date.now();
      this.lastSuccessfulOperation = Date.now();

      logger.error("[db-writer-v2] forced deadlock recovery completed");
    } catch (error) {
      logger.error("[db-writer-v2] forced deadlock recovery failed", {
        error: formatError(error)
      });
    }
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;
    this.lastDrainTime = Date.now();

    const drainStartTime = Date.now();
    let processedAny = false;

    try {
      // Process retry queue first
      const retryProcessed = await this.processRetryQueue();
      if (retryProcessed > 0) processedAny = true;

      // Get bootstrap status to determine processing strategy
      const bootstrapStatus = await BootstrapCoordinator.getBootstrapStatus();

      logger.debug("[db-writer-v2] drain cycle", {
        bootstrapStatus,
        retryQueueSize: this.retryQueue.size,
        workerId: process.env.WORKER_ID || 'unknown',
        drainStartTime
      });

      if (bootstrapStatus.complete) {
        // Normal operation: process all queues
        logger.debug("[db-writer-v2] processing all queues (post-bootstrap)");
        const queueProcessed = await this.processAllQueues();
        if (queueProcessed > 0) processedAny = true;
      } else {
        // Bootstrap mode: strict ordering with barriers
        logger.debug("[db-writer-v2] processing in bootstrap mode", {
          stage: this.getCurrentBootstrapStage(bootstrapStatus)
        });
        const queueProcessed = await this.processBootstrapMode(bootstrapStatus);
        if (queueProcessed > 0) processedAny = true;
      }

      // Update success timestamp if we processed anything
      if (processedAny) {
        this.lastSuccessfulOperation = Date.now();
      }

      const drainDuration = Date.now() - drainStartTime;
      logger.debug("[db-writer-v2] drain cycle completed", {
        duration: drainDuration,
        processedAny,
        workerId: process.env.WORKER_ID || 'unknown'
      });

    } catch (error) {
      logger.error("[db-writer-v2] drain failed", {
        error: formatError(error),
        duration: Date.now() - drainStartTime,
        workerId: process.env.WORKER_ID || 'unknown'
      });
    } finally {
      this.draining = false;
    }
  }

  private getCurrentBootstrapStage(bootstrapStatus: any): string {
    if (!bootstrapStatus.events) return "events";
    if (!bootstrapStatus.markets) return "markets";
    if (!bootstrapStatus.outcomes) return "outcomes";
    return "complete";
  }

  private async processRetryQueue(): Promise<number> {
    if (this.retryQueue.size === 0) return 0;

    const now = Date.now();
    const readyJobs: { key: string; job: RetryableJob }[] = [];

    for (const [jobId, job] of this.retryQueue.entries()) {
      if (job.nextRetryAt <= now) {
        readyJobs.push({ key: jobId, job });
        this.retryQueue.delete(jobId);
      }
    }

    if (readyJobs.length === 0) return 0;

    logger.info("[db-writer-v2] processing retry queue", {
      retryJobs: readyJobs.length,
      remainingInQueue: this.retryQueue.size
    });

    let processedCount = 0;
    for (const { key, job } of readyJobs) {
      const success = await this.retryJob(job);
      if (success) {
        await this.removePersistedRetry(key);
        processedCount++;
      }
    }
    return processedCount;
  }

  private async retryJob(job: RetryableJob): Promise<boolean> {
    try {
      switch (job.kind) {
        case "event":
          await this.processEventBatch([job as UpdateJob<NormalizedEvent>]);
          break;
        case "market":
          await this.processMarketBatch([job as UpdateJob<NormalizedMarket>]);
          break;
        case "outcome":
          await this.processOutcomeBatch([job as UpdateJob<NormalizedOutcome>]);
          break;
        case "tick":
          await this.processTickBatch([job as UpdateJob<NormalizedTick>]);
          break;
      }

      logger.info("[db-writer-v2] retry successful", {
        kind: job.kind,
        attempts: job.attempts,
        reason: job.reason
      });

      return true;
    } catch (error) {
      await this.scheduleRetryJob(
        job,
        job.reason || "retry failure",
        job.attempts ?? 0
      );
      logger.error("[db-writer-v2] retry failed", {
        kind: job.kind,
        attemptsTried: job.attempts,
        error: formatError(error)
      });
      return false;
    }
  }

  private async processBootstrapMode(bootstrapStatus: any): Promise<number> {
    logger.debug("[db-writer-v2] processing in bootstrap mode", bootstrapStatus);

    // Process events until barrier is reached and queue is empty
    if (!bootstrapStatus.events) {
      return await this.processEventsUntilBarrier();
    }

    // Process markets until events are done and markets barrier is reached
    if (!bootstrapStatus.markets) {
      const eventsEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("events");
      if (eventsEmpty) {
        return await this.processMarketsUntilBarrier();
      }
      return 0;
    }

    // Process outcomes until markets are done and outcomes barrier is reached
    if (!bootstrapStatus.outcomes) {
      const marketsEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("markets");
      if (marketsEmpty) {
        return await this.processOutcomesUntilBarrier();
      }
      return 0;
    }

    // Bootstrap complete, process ticks
    return await this.processTicks();
  }

  private async processAllQueues(): Promise<number> {
    logger.debug("[db-writer-v2] processing all queues");

    // Process in priority order, but allow some interleaving for throughput
    const pipeline = [
      { kind: "event" as UpdateKind, batchSize: settings.dbWriterBatchSize.events },
      { kind: "market" as UpdateKind, batchSize: settings.dbWriterBatchSize.markets },
      { kind: "outcome" as UpdateKind, batchSize: settings.dbWriterBatchSize.outcomes },
      { kind: "tick" as UpdateKind, batchSize: settings.dbWriterBatchSize.ticks },
    ];

    let totalProcessed = 0;
    for (const stage of pipeline) {
      const processed = await this.processQueue(stage.kind, stage.batchSize);
      totalProcessed += processed;
      if (processed > 0) {
        heartbeatMonitor.beat(WORKERS.dbWriter, {
          stage: stage.kind,
          processed,
        });
      }
    }
    return totalProcessed;
  }

  private async processEventsUntilBarrier(): Promise<number> {
    const processed = await this.processQueue("event", settings.dbWriterBatchSize.events);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "event",
        processed,
      });
      return processed;
    }

    // No more events, check if queue is empty
    const queueEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("events");
    if (queueEmpty) {
      // Set barrier to mark events stage complete
      await BootstrapCoordinator.setStageBarrier("events", processed);
      logger.info("[db-writer-v2] events stage barrier set");
    }
    return processed;
  }

  private async processMarketsUntilBarrier(): Promise<number> {
    const processed = await this.processQueue("market", settings.dbWriterBatchSize.markets);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "market",
        processed,
      });
      return processed;
    }

    // No more markets, set barrier
    await BootstrapCoordinator.setStageBarrier("markets", processed);
    logger.info("[db-writer-v2] markets stage barrier set");
    return processed;
  }

  private async processOutcomesUntilBarrier(): Promise<number> {
    const processed = await this.processQueue("outcome", settings.dbWriterBatchSize.outcomes);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "outcome",
        processed,
      });
      return processed;
    }

    // No more outcomes, set barrier
    await BootstrapCoordinator.setStageBarrier("outcomes", processed);
    logger.info("[db-writer-v2] outcomes stage barrier set");
    return processed;
  }

  private async processTicks(): Promise<number> {
    const processed = await this.processQueue("tick", settings.dbWriterBatchSize.ticks);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "tick",
        processed,
      });
    }
    return processed;
  }

  private async processQueue(
    kind: UpdateKind,
    batchSize: number
  ): Promise<number> {
    let total = 0;
    let batchCount = 0;

    while (true) {
      const jobs: UpdateJob<any>[] = [];

      // Collect a batch
      for (let i = 0; i < batchSize; i++) {
        const job = await pullNextUpdate(kind);
        if (!job) break;
        jobs.push(job);
      }

      if (!jobs.length) break;

      batchCount++;
      let batchProcessed = 0;

      try {
        switch (kind) {
          case "event":
            batchProcessed = await this.processEventBatch(jobs as UpdateJob<NormalizedEvent>[]);
            break;
          case "market":
            batchProcessed = await this.processMarketBatch(jobs as UpdateJob<NormalizedMarket>[]);
            break;
          case "outcome":
            batchProcessed = await this.processOutcomeBatch(jobs as UpdateJob<NormalizedOutcome>[]);
            break;
          case "tick":
            batchProcessed = await this.processTickBatch(jobs as UpdateJob<NormalizedTick>[]);
            break;
        }

        total += batchProcessed;

      } catch (error) {
        logger.error(`[db-writer-v2] batch processing failed for ${kind}`, {
          batchSize: jobs.length,
          batchNumber: batchCount,
          error: formatError(error)
        });

        // Add failed jobs to retry queue
        for (const job of jobs) {
          await this.scheduleRetryJob(job, "batch failure", job.attempts ?? 0);
        }
      }

      // Safety limit to prevent infinite loops
      if (batchCount >= 10) {
        logger.warn(`[db-writer-v2] batch limit reached for ${kind}`, {
          batchesProcessed: batchCount,
          totalProcessed: total
        });
        break;
      }
    }

    if (total > 0) {
      logger.debug(`[db-writer-v2] ${kind} queue processed`, {
        total,
        batches: batchCount
      });
    }

    return total;
  }

  private async processEventBatch(
    jobs: UpdateJob<NormalizedEvent>[]
  ): Promise<number> {
    if (!jobs.length) return 0;

    // Deduplicate by polymarket_id
    const dedup = new Map<string, NormalizedEvent>();
    for (const job of jobs) {
      dedup.set(job.payload.polymarket_id, job.payload);
    }
    const unique = Array.from(dedup.values());

    const now = new Date();
    const values = unique.map((e) => ({
      polymarketId: e.polymarket_id,
      slug: e.slug,
      title: e.title,
      description: e.description,
      category: e.category,
      subcategory: null,
      liquidity: e.liquidity?.toString() ?? "0",
      volume24h: e.volume?.toString() ?? "0",
      volumeTotal: e.volume?.toString() ?? "0",
      active: e.is_active,
      closed: e.closed,
      archived: e.archived,
      restricted: e.restricted,
      relevanceScore: "0",
      startDate: e.start_date ? new Date(e.start_date) : null,
      endDate: e.end_date ? new Date(e.end_date) : null,
      lastIngestedAt: now,
      updatedAt: now,
    }));

    await dbInstance
      .insert(events)
      .values(values)
      .onConflictDoUpdate({
        target: events.polymarketId,
        set: {
          title: sql`excluded.title`,
          slug: sql`excluded.slug`,
          description: sql`excluded.description`,
          category: sql`excluded.category`,
          liquidity: sql`excluded.liquidity`,
          volume24h: sql`excluded.volume_24h`,
          volumeTotal: sql`excluded.volume_total`,
          active: sql`excluded.active`,
          closed: sql`excluded.closed`,
          archived: sql`excluded.archived`,
          restricted: sql`excluded.restricted`,
          startDate: sql`excluded.start_date`,
          endDate: sql`excluded.end_date`,
          lastIngestedAt: sql`excluded.last_ingested_at`,
          updatedAt: sql`excluded.updated_at`,
        },
      });

    return unique.length;
  }

  private async processMarketBatch(
    jobs: UpdateJob<NormalizedMarket>[]
  ): Promise<number> {
    if (!jobs.length) return 0;

    const eventIds = Array.from(
      new Set(jobs.map((j) => j.payload.event_polymarket_id)),
    );

    // Batch fetch parent events with retry logic
    const parentRows = await dbInstance
      .select({
        id: events.id,
        polymarketId: events.polymarketId,
      })
      .from(events)
      .where(inArray(events.polymarketId, eventIds));

    const parentMap = new Map(parentRows.map((r) => [r.polymarketId, r.id]));
    const now = new Date();

    const ready: { m: NormalizedMarket; eventId: string }[] = [];
    let missingParents = 0;

    for (const job of jobs) {
      const market = job.payload;
      const parentId = parentMap.get(market.event_polymarket_id);
      if (!parentId) {
        missingParents++;
        await this.scheduleRetryJob(
          job,
          `missing parent event ${market.event_polymarket_id}`,
          job.attempts ?? 0
        );
        continue;
      }
      ready.push({ m: market, eventId: parentId });
    }

    if (missingParents > 0) {
      logger.warn("[db-writer-v2] markets waiting on parent events", {
        missingCount: missingParents
      });
    }

    if (!ready.length) return 0;

    const values = ready.map(({ m, eventId }) => ({
      polymarketId: m.polymarket_id,
      eventId: eventId,
      question: m.question,
      slug: m.slug,
      description: m.description,
      liquidity: m.liquidity?.toString() ?? "0",
      volume24h: m.volume?.toString() ?? "0",
      volumeTotal: m.volume?.toString() ?? "0",
      currentPrice: m.current_price?.toString() ?? null,
      lastTradePrice: m.last_trade_price?.toString() ?? null,
      bestBid: m.best_bid?.toString() ?? null,
      bestAsk: m.best_ask?.toString() ?? null,
      status: m.status,
      resolvedAt: m.resolved_at ? new Date(m.resolved_at) : null,
      active: m.is_active,
      closed: m.closed,
      archived: m.archived,
      restricted: m.restricted,
      approved: m.approved,
      relevanceScore: m.relevance_score?.toString() ?? "0",
      lastIngestedAt: now,
      updatedAt: now,
    }));

    await dbInstance
      .insert(markets)
      .values(values)
      .onConflictDoUpdate({
        target: markets.polymarketId,
        set: {
          eventId: sql`excluded.event_id`,
          question: sql`excluded.question`,
          slug: sql`excluded.slug`,
          description: sql`excluded.description`,
          liquidity: sql`excluded.liquidity`,
          volume24h: sql`excluded.volume_24h`,
          volumeTotal: sql`excluded.volume_total`,
          currentPrice: sql`excluded.current_price`,
          lastTradePrice: sql`excluded.last_trade_price`,
          bestBid: sql`excluded.best_bid`,
          bestAsk: sql`excluded.best_ask`,
          status: sql`excluded.status`,
          resolvedAt: sql`excluded.resolved_at`,
          active: sql`excluded.active`,
          closed: sql`excluded.closed`,
          archived: sql`excluded.archived`,
          restricted: sql`excluded.restricted`,
          approved: sql`excluded.approved`,
          relevanceScore: sql`excluded.relevance_score`,
          lastIngestedAt: sql`excluded.last_ingested_at`,
          updatedAt: sql`excluded.updated_at`,
        },
      });

    return ready.length;
  }

  private async processOutcomeBatch(
    jobs: UpdateJob<NormalizedOutcome>[]
  ): Promise<number> {
    if (!jobs.length) return 0;

    const marketIds = Array.from(
      new Set(jobs.map((j) => j.payload.market_polymarket_id)),
    );

    const parentRows = await dbInstance
      .select({
        id: markets.id,
        polymarketId: markets.polymarketId,
      })
      .from(markets)
      .where(inArray(markets.polymarketId, marketIds));

    const parentMap = new Map(parentRows.map((r) => [r.polymarketId, r.id]));
    const now = new Date();

    const ready: { o: NormalizedOutcome; marketId: string }[] = [];
    let missingParents = 0;

    for (const job of jobs) {
      const outcome = job.payload;
      const parentId = parentMap.get(outcome.market_polymarket_id);
      if (!parentId) {
        missingParents++;
        await this.scheduleRetryJob(
          job,
          `missing parent market ${outcome.market_polymarket_id}`,
          job.attempts ?? 0
        );
        continue;
      }
      ready.push({ o: outcome, marketId: parentId });
    }

    if (missingParents > 0) {
      logger.warn("[db-writer-v2] outcomes waiting on parent markets", {
        missingCount: missingParents
      });
    }

    if (!ready.length) return 0;

    const values = ready.map(({ o, marketId }) => ({
      polymarketId: o.polymarket_id,
      marketId: marketId,
      title: o.title,
      description: o.description,
      price: o.price?.toString() ?? "0",
      probability: o.probability?.toString() ?? "0",
      volume: o.volume?.toString() ?? "0",
      status: o.status,
      displayOrder: o.display_order,
      updatedAt: now,
    }));

    await dbInstance
      .insert(outcomes)
      .values(values)
      .onConflictDoUpdate({
        target: outcomes.polymarketId,
        set: {
          title: sql`excluded.title`,
          description: sql`excluded.description`,
          price: sql`excluded.price`,
          probability: sql`excluded.probability`,
          volume: sql`excluded.volume`,
          status: sql`excluded.status`,
          displayOrder: sql`excluded.display_order`,
          updatedAt: sql`excluded.updated_at`,
        },
      });

    return ready.length;
  }

  private async processTickBatch(
    jobs: UpdateJob<NormalizedTick>[]
  ): Promise<number> {
    if (!jobs.length) return 0;

    const marketIds = Array.from(
      new Set(jobs.map((j) => j.payload.market_polymarket_id)),
    );

    const parentRows = await dbInstance
      .select({
        id: markets.id,
        polymarketId: markets.polymarketId,
      })
      .from(markets)
      .where(inArray(markets.polymarketId, marketIds));

    const parentMap = new Map(parentRows.map((r) => [r.polymarketId, r.id]));
    const batch = [];

    for (const job of jobs) {
      const tick = job.payload;
      const marketId = parentMap.get(tick.market_polymarket_id);
      if (!marketId) {
        await this.scheduleRetryJob(
          job,
          `missing parent market for tick ${tick.market_polymarket_id}`,
          job.attempts ?? 0
        );
        continue;
      }

      batch.push({
        marketId,
        price: tick.price?.toString() ?? null,
        bestBid: tick.best_bid?.toString() ?? null,
        bestAsk: tick.best_ask?.toString() ?? null,
        lastTradePrice: tick.last_trade_price?.toString() ?? null,
        liquidity: tick.liquidity?.toString() ?? null,
        volume24h: tick.volume_24h?.toString() ?? null,
        updatedAt: tick.captured_at ? new Date(tick.captured_at) : new Date(),
      });
    }

    if (!batch.length) return 0;

    await dbInstance.insert(marketPricesRealtime).values(batch);
    return batch.length;
  }

  getRetryQueueSize(): number {
    return this.retryQueue.size;
  }

  getMode(): "bootstrap" | "normal" {
    // This would be determined by checking bootstrap coordinator
    return "normal"; // Simplified for now
  }
}

export const dbWriterWorkerV2 = new DbWriterWorkerV2();
