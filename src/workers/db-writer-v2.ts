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
}

export class DbWriterWorkerV2 {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;
  private retryQueue = new Map<string, RetryableJob>();

  // Configuration
  private readonly DRAIN_INTERVAL = 1000; // Increased from 200ms
  private readonly MAX_RETRY_ATTEMPTS = 5;
  private readonly RETRY_BACKOFF_MS = [5000, 15000, 45000, 120000, 300000];

  start(): void {
    if (this.timer) return;
    logger.info("[db-writer-v2] starting with improved batch processing");
    this.timer = setInterval(() => void this.drain(), this.DRAIN_INTERVAL);
    void this.drain();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    logger.info("[db-writer-v2] stopped");
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    try {
      // Process retry queue first
      await this.processRetryQueue();

      // Get bootstrap status to determine processing strategy
      const bootstrapStatus = await BootstrapCoordinator.getBootstrapStatus();

      if (bootstrapStatus.complete) {
        // Normal operation: process all queues
        await this.processAllQueues();
      } else {
        // Bootstrap mode: strict ordering with barriers
        await this.processBootstrapMode(bootstrapStatus);
      }

    } catch (error) {
      logger.error("[db-writer-v2] drain failed", { error: formatError(error) });
    } finally {
      this.draining = false;
    }
  }

  private async processRetryQueue(): Promise<void> {
    if (this.retryQueue.size === 0) return;

    const now = Date.now();
    const readyJobs: RetryableJob[] = [];

    for (const [jobId, job] of this.retryQueue.entries()) {
      if (job.nextRetryAt <= now) {
        readyJobs.push(job);
        this.retryQueue.delete(jobId);
      }
    }

    if (readyJobs.length === 0) return;

    logger.info("[db-writer-v2] processing retry queue", {
      retryJobs: readyJobs.length,
      remainingInQueue: this.retryQueue.size
    });

    for (const job of readyJobs) {
      await this.retryJob(job);
    }
  }

  private async retryJob(job: RetryableJob): Promise<void> {
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
        attempts: job.attempts + 1
      });

    } catch (error) {
      const nextAttempt = job.attempts + 1;
      if (nextAttempt >= this.MAX_RETRY_ATTEMPTS) {
        logger.error("[db-writer-v2] retry exhausted, dropping job", {
          kind: job.kind,
          attempts: nextAttempt,
          error: formatError(error)
        });
      } else {
        const backoffMs = this.RETRY_BACKOFF_MS[Math.min(nextAttempt - 1, this.RETRY_BACKOFF_MS.length - 1)] || 300000;
        const retryJob: RetryableJob = {
          ...job,
          attempts: nextAttempt,
          lastAttempt: Date.now(),
          nextRetryAt: Date.now() + backoffMs
        };

        const jobId = `${job.kind}_${job.payload.polymarket_id || Date.now()}_${Math.random()}`;
        this.retryQueue.set(jobId, retryJob);

        logger.warn("[db-writer-v2] job scheduled for retry", {
          kind: job.kind,
          attempt: nextAttempt,
          nextRetryIn: backoffMs
        });
      }
    }
  }

  private async processBootstrapMode(bootstrapStatus: any): Promise<void> {
    logger.debug("[db-writer-v2] processing in bootstrap mode", bootstrapStatus);

    // Process events until barrier is reached and queue is empty
    if (!bootstrapStatus.events) {
      await this.processEventsUntilBarrier();
      return;
    }

    // Process markets until events are done and markets barrier is reached
    if (!bootstrapStatus.markets) {
      const eventsEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("events");
      if (eventsEmpty) {
        await this.processMarketsUntilBarrier();
      }
      return;
    }

    // Process outcomes until markets are done and outcomes barrier is reached
    if (!bootstrapStatus.outcomes) {
      const marketsEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("markets");
      if (marketsEmpty) {
        await this.processOutcomesUntilBarrier();
      }
      return;
    }

    // Bootstrap complete, process ticks
    await this.processTicks();
  }

  private async processAllQueues(): Promise<void> {
    logger.debug("[db-writer-v2] processing all queues");

    // Process in priority order, but allow some interleaving for throughput
    const pipeline = [
      { kind: "event" as UpdateKind, batchSize: settings.dbWriterBatchSize.events },
      { kind: "market" as UpdateKind, batchSize: settings.dbWriterBatchSize.markets },
      { kind: "outcome" as UpdateKind, batchSize: settings.dbWriterBatchSize.outcomes },
      { kind: "tick" as UpdateKind, batchSize: settings.dbWriterBatchSize.ticks },
    ];

    for (const stage of pipeline) {
      const processed = await this.processQueue(stage.kind, stage.batchSize);
      if (processed > 0) {
        heartbeatMonitor.beat(WORKERS.dbWriter, {
          stage: stage.kind,
          processed,
        });
      }
    }
  }

  private async processEventsUntilBarrier(): Promise<void> {
    const processed = await this.processQueue("event", settings.dbWriterBatchSize.events);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "event",
        processed,
      });
      return;
    }

    // No more events, check if queue is empty
    const queueEmpty = await BootstrapCoordinator.areQueuesEmptyForStage("events");
    if (queueEmpty) {
      // Set barrier to mark events stage complete
      await BootstrapCoordinator.setStageBarrier("events", processed);
      logger.info("[db-writer-v2] events stage barrier set");
    }
  }

  private async processMarketsUntilBarrier(): Promise<void> {
    const processed = await this.processQueue("market", settings.dbWriterBatchSize.markets);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "market",
        processed,
      });
      return;
    }

    // No more markets, set barrier
    await BootstrapCoordinator.setStageBarrier("markets", processed);
    logger.info("[db-writer-v2] markets stage barrier set");
  }

  private async processOutcomesUntilBarrier(): Promise<void> {
    const processed = await this.processQueue("outcome", settings.dbWriterBatchSize.outcomes);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "outcome",
        processed,
      });
      return;
    }

    // No more outcomes, set barrier
    await BootstrapCoordinator.setStageBarrier("outcomes", processed);
    logger.info("[db-writer-v2] outcomes stage barrier set");
  }

  private async processTicks(): Promise<void> {
    const processed = await this.processQueue("tick", settings.dbWriterBatchSize.ticks);
    if (processed > 0) {
      heartbeatMonitor.beat(WORKERS.dbWriter, {
        stage: "tick",
        processed,
      });
    }
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
          const retryJob: RetryableJob = {
            ...job,
            attempts: 0,
            lastAttempt: Date.now(),
            nextRetryAt: Date.now() + (this.RETRY_BACKOFF_MS[0] || 5000)
          };

          const jobId = `${job.kind}_${job.payload.polymarket_id || Date.now()}_${Math.random()}`;
          this.retryQueue.set(jobId, retryJob);
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

    const payloads = jobs.map((j) => j.payload);
    const eventIds = Array.from(
      new Set(payloads.map((m) => m.event_polymarket_id)),
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
    const missingParents: NormalizedMarket[] = [];

    for (const m of payloads) {
      const parentId = parentMap.get(m.event_polymarket_id);
      if (!parentId) {
        missingParents.push(m);
      } else {
        ready.push({ m, eventId: parentId });
      }
    }

    // Log missing parents but don't fail the batch
    if (missingParents.length > 0) {
      logger.warn("[db-writer-v2] markets with missing parent events", {
        missingCount: missingParents.length,
        missingEvents: missingParents.slice(0, 3).map(m => m.event_polymarket_id)
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

    const payloads = jobs.map((j) => j.payload);
    const marketIds = Array.from(
      new Set(payloads.map((o) => o.market_polymarket_id)),
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
    const missingParents: NormalizedOutcome[] = [];

    for (const o of payloads) {
      const parentId = parentMap.get(o.market_polymarket_id);
      if (!parentId) missingParents.push(o);
      else ready.push({ o, marketId: parentId });
    }

    // Log missing parents
    if (missingParents.length > 0) {
      logger.warn("[db-writer-v2] outcomes with missing parent markets", {
        missingCount: missingParents.length,
        missingMarkets: missingParents.slice(0, 3).map(o => o.market_polymarket_id)
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

    const payloads = jobs.map((j) => j.payload);
    const marketIds = Array.from(
      new Set(payloads.map((t) => t.market_polymarket_id)),
    );

    const parentRows = await dbInstance
      .select({
        id: markets.id,
        polymarketId: markets.polymarketId,
      })
      .from(markets)
      .where(inArray(markets.polymarketId, marketIds));

    const parentMap = new Map(parentRows.map((r) => [r.polymarketId, r.id]));
    const batch = payloads
      .map((t) => {
        const mId = parentMap.get(t.market_polymarket_id);
        if (!mId) return null;
        return {
          marketId: mId,
          price: t.price?.toString() ?? null,
          bestBid: t.best_bid?.toString() ?? null,
          bestAsk: t.best_ask?.toString() ?? null,
          lastTradePrice: t.last_trade_price?.toString() ?? null,
          liquidity: t.liquidity?.toString() ?? null,
          volume24h: t.volume_24h?.toString() ?? null,
          updatedAt: t.captured_at ? new Date(t.captured_at) : new Date(),
        };
      })
      .filter(Boolean) as any[];

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