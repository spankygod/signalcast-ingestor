import { eq, sql, inArray } from "drizzle-orm";
import logger, { formatError } from "../lib/logger";
import { settings } from "../config/settings";
import { polymarketClient } from "../config/polymarket";
import { pullNextUpdate, pushUpdate, UpdateJob, UpdateKind } from "../queues/updates.queue";
import { db, events, markets, outcomes, marketPricesRealtime } from "../lib/db";
import {
  NormalizedEvent,
  NormalizedMarket,
  NormalizedOutcome,
  normalizeEvent,
  normalizeMarket
} from "../utils/normalizeMarket";
import { NormalizedTick } from "../utils/normalizeTick";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";

// Debug configuration
const DEBUG_SQL_QUERIES = process.env.DEBUG_SQL_QUERIES === 'true';
const DEBUG_QUERY_TIMEOUT = Number(process.env.DEBUG_QUERY_TIMEOUT || 30000); // 30 seconds
const DEBUG_DETAILED_TIMING = process.env.DEBUG_DETAILED_TIMING === 'true';

type PipelineStage = {
  kind: UpdateKind;
  handler: (jobs: UpdateJob<any>[]) => Promise<number>;
  batchSize: number;
};

export class DbWriterWorker {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;
  private readonly maxJobAttempts = Number(process.env.JOB_MAX_ATTEMPTS || 3);

  // Debug tracking
  private lastProgressTimestamp = Date.now();
  private currentOperation: string = 'idle';
  private operationStartTime: number = Date.now();

  // Helper method to time database operations with detailed logging
  private async timedDbOperation<T>(
    operationName: string,
    operation: () => Promise<T>,
    context?: Record<string, any>
  ): Promise<T> {
    const startTime = Date.now();
    this.currentOperation = operationName;
    this.operationStartTime = startTime;

    logger.debug(`db-writer starting operation: ${operationName}`, {
      operation: operationName,
      startTime,
      ...context
    });

    try {
      // Set up timeout warning
      const timeoutWarning = setTimeout(() => {
        const elapsed = Date.now() - startTime;
        logger.warn(`db-writer operation taking too long: ${operationName}`, {
          operation: operationName,
          elapsed: `${elapsed}ms`,
          context,
          operationId: startTime
        });
      }, DEBUG_QUERY_TIMEOUT / 2); // Warn at half the timeout

      const result = await Promise.race([
        operation(),
        new Promise<never>((_, reject) =>
          setTimeout(() => {
            const elapsed = Date.now() - startTime;
            logger.error(`db-writer operation timeout: ${operationName}`, {
              operation: operationName,
              elapsed: `${elapsed}ms`,
              context,
              operationId: startTime
            });
            reject(new Error(`Database operation timeout: ${operationName} after ${elapsed}ms`));
          }, DEBUG_QUERY_TIMEOUT)
        )
      ]);

      clearTimeout(timeoutWarning);
      const duration = Date.now() - startTime;
      this.lastProgressTimestamp = Date.now();
      this.currentOperation = 'idle';

      logger.debug(`db-writer completed operation: ${operationName}`, {
        operation: operationName,
        duration: `${duration}ms`,
        success: true,
        ...context
      });

      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      this.currentOperation = 'error';

      logger.error(`db-writer operation failed: ${operationName}`, {
        operation: operationName,
        duration: `${duration}ms`,
        error: formatError(error),
        ...context
      });

      throw error;
    }
  }

  start(): void {
    if (this.timer) return;
    logger.info("db-writer starting", {
      debugConfig: {
        sqlQueries: DEBUG_SQL_QUERIES,
        queryTimeout: `${DEBUG_QUERY_TIMEOUT}ms`,
        detailedTiming: DEBUG_DETAILED_TIMING
      }
    });
    this.timer = setInterval(() => {
      void this.drain();
    }, settings.queueDrainIntervalMs);
    void this.drain();
  }

  // Method to get current debug status
  getDebugStatus() {
    return {
      currentOperation: this.currentOperation,
      operationStartTime: new Date(this.operationStartTime).toISOString(),
      lastProgressTimestamp: new Date(this.lastProgressTimestamp).toISOString(),
      timeSinceLastProgress: Date.now() - this.lastProgressTimestamp,
      debugConfig: {
        sqlQueries: DEBUG_SQL_QUERIES,
        queryTimeout: DEBUG_QUERY_TIMEOUT,
        detailedTiming: DEBUG_DETAILED_TIMING
      }
    };
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    // keep-alive heartbeat even if queues are empty
    heartbeatMonitor.beat(WORKERS.dbWriter, {
      state: "draining",
      currentOperation: this.currentOperation,
      lastProgress: Date.now() - this.lastProgressTimestamp
    });

    try {
      let totalProcessed = 0;
      const pipeline: PipelineStage[] = [
        {
          kind: "event",
          batchSize: 15, // on Pro you can try 15 later
          handler: (jobs) => this.processEventJobs(jobs as UpdateJob<NormalizedEvent>[])
        },
        {
          kind: "market",
          batchSize: 25,
          handler: (jobs) => this.processMarketJobsBatch(jobs as UpdateJob<NormalizedMarket>[])
        },
        {
          kind: "outcome",
          batchSize: 50,
          handler: (jobs) => this.processOutcomeJobsBatch(jobs as UpdateJob<NormalizedOutcome>[])
        },
        {
          kind: "tick",
          batchSize: 100,
          handler: (jobs) => this.processTickJobsBatch(jobs as UpdateJob<NormalizedTick>[])
        }
      ];

      for (const stage of pipeline) {
        logger.info(`db-writer starting pipeline stage: ${stage.kind}`, {
          stage: stage.kind,
          batchSize: stage.batchSize,
          totalProcessedSoFar: totalProcessed,
          lastProgressTime: new Date(this.lastProgressTimestamp).toISOString()
        });

        const stageStart = Date.now();

        // Wrap the entire stage processing in timeout detection
        const stageProcessed = await this.timedDbOperation(
          `process-${stage.kind}-stage`,
          () => this.processQueue(stage),
          { stage: stage.kind, batchSize: stage.batchSize }
        );

        const stageDuration = Date.now() - stageStart;

        totalProcessed += stageProcessed;

        logger.info(`db-writer completed pipeline stage: ${stage.kind}`, {
          stage: stage.kind,
          stageProcessed,
          stageDuration: `${stageDuration}ms`,
          totalProcessedSoFar: totalProcessed,
          avgPerJob: stageProcessed > 0 ? `${Math.round(stageDuration / stageProcessed)}ms/job` : 'N/A'
        });

        // Update heartbeat after each stage
        heartbeatMonitor.beat(WORKERS.dbWriter, {
          kind: stage.kind,
          processed: totalProcessed,
          currentOperation: this.currentOperation
        });
      }

      if (totalProcessed === 0) {
        // beat + mark idle so heartbeat monitor never thinks we're dead
        heartbeatMonitor.beat(WORKERS.dbWriter, {
          state: "idle",
          currentOperation: this.currentOperation
        });
        heartbeatMonitor.markIdle(WORKERS.dbWriter);
      }
    } catch (error) {
      logger.error("db-writer failed to drain queues", {
        error: formatError(error),
        currentOperation: this.currentOperation,
        operationDuration: Date.now() - this.operationStartTime,
        lastProgressTime: new Date(this.lastProgressTimestamp).toISOString()
      });
      throw error;
    } finally {
      this.draining = false;
    }
  }

  private async processQueue(stage: PipelineStage): Promise<number> {
    let processed = 0;
    let batchCount = 0;

    logger.debug(`db-writer starting ${stage.kind} queue processing`, {
      batchSize: stage.batchSize,
      initialProcessed: processed
    });

    while (true) {
      batchCount++;
      logger.debug(`db-writer pulling ${stage.kind} jobs - batch ${batchCount}`, {
        batchNumber: batchCount,
        processedSoFar: processed,
        batchSize: stage.batchSize
      });

      const jobs = await this.pullJobs(stage.kind, stage.batchSize);
      if (jobs.length === 0) {
        logger.debug(`db-writer no more ${stage.kind} jobs available`, {
          batchNumber: batchCount,
          totalProcessed: processed,
          totalBatches: batchCount - 1
        });
        break;
      }

      logger.debug(`db-writer processing ${stage.kind} batch`, {
        batchNumber: batchCount,
        jobCount: jobs.length,
        processedSoFar: processed,
        jobIdRange: jobs.length > 0 ? `${jobs[0]?.queuedAt}-${jobs[jobs.length - 1]?.queuedAt}` : 'empty'
      });

      try {
        const handled = await stage.handler(jobs);
        logger.debug(`db-writer completed ${stage.kind} batch`, {
          batchNumber: batchCount,
          jobCount: jobs.length,
          handledCount: handled,
          processedSoFar: processed,
          newTotal: processed + handled
        });

        if (handled > 0) {
          processed += handled;
          heartbeatMonitor.beat(WORKERS.dbWriter, {
            kind: stage.kind,
            processed,
            debugInfo: this.getDebugStatus()
          });
        }
      } catch (error) {
        logger.error(`db-writer failed to process ${stage.kind} batch`, {
          batchNumber: batchCount,
          jobCount: jobs.length,
          error: formatError(error),
          processedSoFar: processed,
          jobs: jobs.map(j => ({ kind: j.kind, attempts: j.attempts, queuedAt: j.queuedAt }))
        });
        throw error;
      }
    }

    logger.info(`db-writer completed ${stage.kind} queue processing`, {
      totalBatches: batchCount - 1,
      totalProcessed: processed,
      averageJobsPerBatch: batchCount > 1 ? processed / (batchCount - 1) : 0
    });

    return processed;
  }

  private async pullJobs(kind: UpdateKind, max: number): Promise<Array<UpdateJob<any>>> {
    const jobs: Array<UpdateJob<any>> = [];
    for (let i = 0; i < max; i++) {
      const job = await pullNextUpdate(kind);
      if (!job) break;
      jobs.push(job);
    }
    return jobs;
  }

  // ---------------------------
  // EVENTS (bulk upsert)
  // ---------------------------

  private async processEventJobs(jobs: UpdateJob<NormalizedEvent>[]): Promise<number> {
    if (jobs.length === 0) return 0;

    const polymarketIds = jobs.map(j => j.payload.polymarket_id);
    logger.debug('db-writer starting event job processing', {
      jobCount: jobs.length,
      jobIds: jobs.map(j => j.queuedAt),
      polymarketIds,
      timestamp: new Date().toISOString()
    });

    try {
      await this.timedDbOperation(
        'upsert-events-batch',
        () => this.upsertEvents(jobs.map((job) => job.payload)),
        {
          jobCount: jobs.length,
          polymarketIds,
          firstJobId: jobs[0]?.queuedAt,
          lastJobId: jobs[jobs.length - 1]?.queuedAt
        }
      );

      logger.debug('db-writer completed event job processing', {
        jobCount: jobs.length,
        processedIds: polymarketIds,
        timestamp: new Date().toISOString()
      });
      return jobs.length;
    } catch (error) {
      logger.error('db-writer failed to process event jobs', {
        jobCount: jobs.length,
        error: formatError(error),
        jobIds: jobs.map(j => j.queuedAt),
        polymarketIds,
        currentOperation: this.currentOperation,
        operationDuration: Date.now() - this.operationStartTime,
        timestamp: new Date().toISOString()
      });
      throw error;
    }
  }

  private async upsertEvents(batch: NormalizedEvent[]): Promise<void> {
    if (batch.length === 0) return;

    const uniqueBatch = this.dedupeBatch(batch, (event) => event.polymarket_id, 'events');

    logger.debug('db-writer starting event batch upsert', {
      originalBatchSize: batch.length,
      uniqueBatchSize: uniqueBatch.length,
      polymarketIds: uniqueBatch.map(e => e.polymarket_id),
      timestamp: new Date().toISOString()
    });

    const now = new Date();
    const values = uniqueBatch.map((event, index) => ({
      polymarketId: event.polymarket_id,
      title: event.title,
      slug: event.slug,
      description: event.description,
      category: event.category,
      liquidity: this.decimal(event.liquidity) ?? "0",
      volume24h: this.decimal(event.volume) ?? "0",
      volumeTotal: this.decimal(event.volume) ?? "0",
      active: event.is_active,
      closed: event.closed,
      archived: event.archived,
      restricted: event.restricted,
      startDate: this.toDate(event.start_date),
      endDate: this.toDate(event.end_date),
      lastIngestedAt: this.toDate(event.last_ingested_at) ?? now,
      updatedAt: now
    }));

    logger.debug('db-writer executing events database upsert', {
      recordCount: values.length,
      targetConflictColumn: 'polymarketId',
      samplePolymarketIds: values.slice(0, 3).map(v => v.polymarketId),
      sqlOperation: 'INSERT ... ON CONFLICT DO UPDATE'
    });

    if (DEBUG_SQL_QUERIES) {
      logger.info('db-writer SQL DEBUG - Events upsert query', {
        table: 'events',
        operation: 'INSERT ON CONFLICT DO UPDATE',
        conflictTarget: 'polymarket_id',
        recordCount: values.length,
        sampleData: values.slice(0, 2).map(v => ({
          polymarketId: v.polymarketId,
          title: v.title?.substring(0, 50) + '...',
          active: v.active
        }))
      });
    }

    try {
      const result = await this.timedDbOperation(
        'events-bulk-upsert',
        async () => {
          const queryResult = await db
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
                updatedAt: sql`excluded.updated_at`
              }
            });

          if (DEBUG_SQL_QUERIES) {
            logger.info('db-writer SQL DEBUG - Events upsert completed', {
              result: 'success',
              recordCount: values.length
            });
          }

          return queryResult;
        },
        {
          recordCount: values.length,
          operation: 'INSERT ON CONFLICT DO UPDATE',
          table: 'events',
          conflictTarget: 'polymarketId'
        }
      );

      logger.debug('db-writer completed events database upsert', {
        recordCount: values.length,
        polymarketIds: values.map(v => v.polymarketId)
      });

    } catch (error) {
      logger.error('db-writer events database upsert failed', {
        recordCount: values.length,
        error: formatError(error),
        errorType: error?.constructor?.name,
        errorCode: (error as any)?.code,
        errorMessage: (error as Error)?.message,
        polymarketIds: values.map(v => v.polymarketId),
        sqlOperation: 'INSERT ON CONFLICT DO UPDATE',
        timestamp: new Date().toISOString()
      });

      // Enhanced constraint violation detection
      if (this.isConstraintViolationError(error)) {
        logger.warn('db-writer detected constraint violation in events batch', {
          recordCount: values.length,
          error: (error as Error).message,
          errorCode: (error as any)?.code,
          polymarketIds: values.map(v => v.polymarketId),
          action: 'processing individually',
          timestamp: new Date().toISOString()
        });

        // Fallback: process events individually to handle duplicates
        await this.upsertEventsIndividually(batch);
        return;
      }

      throw error; // Re-throw non-constraint errors
    }
  }

  // ---------------------------
  // MARKETS (bulk: FK resolve + upsert)
  // ---------------------------

  private async processMarketJobsBatch(
    jobs: UpdateJob<NormalizedMarket>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const marketsPayload = jobs.map((j) => j.payload);
    const eventPolymarketIds = Array.from(
      new Set(
        marketsPayload
          .map((m) => m.event_polymarket_id)
          .filter((v): v is string => typeof v === "string" && v.length > 0)
      )
    );

    if (eventPolymarketIds.length === 0) {
      logger.warn("db-writer markets batch missing event ids");
      return 0;
    }

    const eventRows = await db
      .select({ polymarketId: events.polymarketId, id: events.id })
      .from(events)
      .where(inArray(events.polymarketId, eventPolymarketIds));

    const eventIdByPolymarket = new Map<string, string>();
    for (const row of eventRows) {
      eventIdByPolymarket.set(row.polymarketId, row.id);
    }

    const ready: { market: NormalizedMarket; eventId: string }[] = [];
    const deferred: UpdateJob<NormalizedMarket>[] = [];

    for (const job of jobs) {
      const m = job.payload;
      const eventId = m.event_polymarket_id
        ? eventIdByPolymarket.get(m.event_polymarket_id)
        : undefined;

      if (!eventId) {
        deferred.push(job);
      } else {
        ready.push({ market: m, eventId });
      }
    }

    if (ready.length > 0) {
      await this.upsertMarketsBatch(ready);
    }

    // handle missing parents similar to before, but in batch
    for (const job of deferred) {
      const handled = await this.handleMissingEventForMarket(job.payload, job);
      if (!handled) {
        // if not handled, we just drop; logs already emitted
      }
    }

    return ready.length;
  }

  private async upsertMarketsBatch(
    batch: { market: NormalizedMarket; eventId: string }[]
  ): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();

    const values = batch.map(({ market, eventId }) => ({
      polymarketId: market.polymarket_id,
      eventId,
      question: market.question,
      slug: market.slug,
      description: market.description,
      liquidity: this.decimal(market.liquidity) ?? "0",
      volume24h: this.decimal(market.volume) ?? "0",
      volumeTotal: this.decimal(market.volume) ?? "0",
      currentPrice: this.decimal(market.current_price),
      lastTradePrice: this.decimal(market.last_trade_price),
      bestBid: this.decimal(market.best_bid),
      bestAsk: this.decimal(market.best_ask),
      status: market.status,
      resolvedAt: this.toDate(market.resolved_at),
      active: market.is_active,
      closed: market.closed,
      archived: market.archived,
      restricted: market.restricted,
      approved: market.approved,
      relevanceScore: market.relevance_score,
      lastIngestedAt: now,
      updatedAt: now
    }));

    await db
      .insert(markets)
      .values(values)
      .onConflictDoUpdate({
        target: markets.polymarketId,
        set: {
          ...values[0], // shape only; drizzle ignores values beyond type
          updatedAt: new Date(),
          lastIngestedAt: new Date()
        }
      });
  }

  // ---------------------------
  // OUTCOMES (bulk)
  // ---------------------------

  private async processOutcomeJobsBatch(
    jobs: UpdateJob<NormalizedOutcome>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const payloads = jobs.map((j) => j.payload);
    const marketPolymarketIds = Array.from(
      new Set(
        payloads
          .map((o) => o.market_polymarket_id)
          .filter((v): v is string => typeof v === "string" && v.length > 0)
      )
    );

    if (marketPolymarketIds.length === 0) {
      logger.warn("db-writer outcomes batch missing market ids");
      return 0;
    }

    const marketRows = await db
      .select({ polymarketId: markets.polymarketId, id: markets.id })
      .from(markets)
      .where(inArray(markets.polymarketId, marketPolymarketIds));

    const marketIdByPolymarket = new Map<string, string>();
    for (const row of marketRows) {
      if (row.id && row.polymarketId) {
        marketIdByPolymarket.set(row.polymarketId, row.id);
      }
    }

    const ready: { outcome: NormalizedOutcome; marketId: string }[] = [];
    const deferred: UpdateJob<NormalizedOutcome>[] = [];

    for (const job of jobs) {
      const o = job.payload;
      const marketId = o.market_polymarket_id
        ? marketIdByPolymarket.get(o.market_polymarket_id)
        : undefined;

      if (!marketId) {
        deferred.push(job);
      } else {
        ready.push({ outcome: o, marketId });
      }
    }

    if (ready.length > 0) {
      await this.upsertOutcomesBatch(ready);
    }

    for (const job of deferred) {
      const handled = await this.handleMissingMarketForOutcome(job.payload, job);
      if (!handled) {
        // dropped; already logged
      }
    }

    return ready.length;
  }

  private async upsertOutcomesBatch(
    batch: { outcome: NormalizedOutcome; marketId: string }[]
  ): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();

    const values = batch.map(({ outcome, marketId }) => ({
      polymarketId: outcome.polymarket_id,
      marketId,
      title: outcome.title,
      description: outcome.description,
      price: this.decimal(outcome.price) ?? "0",
      probability: this.decimal(outcome.probability) ?? "0",
      volume: this.decimal(outcome.volume) ?? "0",
      status: outcome.status,
      displayOrder: outcome.display_order,
      updatedAt: now
    }));

    await db
      .insert(outcomes)
      .values(values)
      .onConflictDoUpdate({
        target: outcomes.polymarketId,
        set: {
          ...values[0],
          updatedAt: new Date()
        }
      });
  }

  // ---------------------------
  // TICKS (bulk insert)
  // ---------------------------

  private async processTickJobsBatch(
    jobs: UpdateJob<NormalizedTick>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const payloads = jobs.map((j) => j.payload);
    const marketPolymarketIds = Array.from(
      new Set(
        payloads
          .map((t) => t.market_polymarket_id)
          .filter((v): v is string => typeof v === "string" && v.length > 0)
      )
    );

    if (marketPolymarketIds.length === 0) {
      logger.warn("db-writer ticks batch missing market ids");
      return 0;
    }

    const marketRows = await db
      .select({ polymarketId: markets.polymarketId, id: markets.id })
      .from(markets)
      .where(inArray(markets.polymarketId, marketPolymarketIds));

    const marketIdByPolymarket = new Map<string, string>();
    for (const row of marketRows) {
      if (row.id && row.polymarketId) {
        marketIdByPolymarket.set(row.polymarketId, row.id);
      }
    }

    const ready: { tick: NormalizedTick; marketId: string }[] = [];
    const deferred: UpdateJob<NormalizedTick>[] = [];

    for (const job of jobs) {
      const t = job.payload;
      const marketId = t.market_polymarket_id
        ? marketIdByPolymarket.get(t.market_polymarket_id)
        : undefined;

      if (!marketId) {
        deferred.push(job);
      } else {
        ready.push({ tick: t, marketId });
      }
    }

    if (ready.length > 0) {
      await this.insertTicksBatch(ready);
    }

    for (const job of deferred) {
      const handled = await this.handleMissingMarketForTick(job.payload, job);
      if (!handled) {
        // dropped; already logged
      }
    }

    return ready.length;
  }

  private async insertTicksBatch(
    batch: { tick: NormalizedTick; marketId: string }[]
  ): Promise<void> {
    if (batch.length === 0) return;

    const values = batch.map(({ tick, marketId }) => ({
      marketId,
      price: this.decimal(tick.price),
      bestBid: this.decimal(tick.best_bid),
      bestAsk: this.decimal(tick.best_ask),
      lastTradePrice: this.decimal(tick.last_trade_price),
      liquidity: this.decimal(tick.liquidity),
      volume24h: this.decimal(tick.volume_24h),
      updatedAt: this.toDate(tick.captured_at) ?? new Date()
    }));

    await db.insert(marketPricesRealtime).values(values);
  }

  // ---------------------------
  // Parent-missing handling (unchanged logic, just reused)
  // ---------------------------

  private async handleMissingEventForMarket(
    market: NormalizedMarket,
    job: UpdateJob<NormalizedMarket>
  ): Promise<boolean> {
    const fetched = await this.fetchAndEnqueueEvent(market.event_polymarket_id);
    const requeued = await this.requeueJob("market", job);

    if (requeued) {
      logger.debug("db-writer requeued market awaiting parent event", {
        polymarketId: market.polymarket_id,
        eventPolymarketId: market.event_polymarket_id,
        fetched
      });
      return false;
    }

    logger.warn("db-writer dropping market after missing parent event", {
      polymarketId: market.polymarket_id,
      eventPolymarketId: market.event_polymarket_id,
      fetched
    });
    return true;
  }

  private async handleMissingMarketForOutcome(
    outcome: NormalizedOutcome,
    job: UpdateJob<NormalizedOutcome>
  ): Promise<boolean> {
    const fetched = await this.fetchAndEnqueueMarket(
      outcome.market_polymarket_id
    );
    const requeued = await this.requeueJob("outcome", job);

    if (requeued) {
      logger.debug("db-writer requeued outcome awaiting parent market", {
        polymarketId: outcome.polymarket_id,
        marketPolymarketId: outcome.market_polymarket_id,
        fetched
      });
      return false;
    }

    logger.warn("db-writer dropping outcome after missing parent market", {
      polymarketId: outcome.polymarket_id,
      marketPolymarketId: outcome.market_polymarket_id,
      fetched
    });
    return true;
  }

  private async handleMissingMarketForTick(
    tick: NormalizedTick,
    job: UpdateJob<NormalizedTick>
  ): Promise<boolean> {
    const fetched = await this.fetchAndEnqueueMarket(
      tick.market_polymarket_id
    );
    const requeued = fetched ? await this.requeueJob("tick", job) : false;

    if (requeued) {
      logger.debug("db-writer requeued tick awaiting parent market", {
        marketPolymarketId: tick.market_polymarket_id
      });
      return false;
    }

    logger.debug("db-writer skipping tick for unknown market", {
      polymarketId: tick.market_polymarket_id
    });
    return true;
  }

  private async requeueJob<T>(
    kind: UpdateKind,
    job: UpdateJob<T>
  ): Promise<boolean> {
    const attempts = job.attempts ?? 0;
    if (attempts >= this.maxJobAttempts) {
      logger.warn("db-writer max attempts exceeded, dropping job", {
        kind,
        attempts,
        queuedAt: job.queuedAt
      });
      return false;
    }

    await pushUpdate(kind, job.payload, attempts + 1);
    return true;
  }

  private async fetchAndEnqueueEvent(polymarketId: string): Promise<boolean> {
    try {
      const event = await polymarketClient.getEvent(polymarketId);
      if (!event) {
        logger.warn("db-writer event lookup returned no data", { polymarketId });
        return false;
      }

      await pushUpdate("event", normalizeEvent(event));
      return true;
    } catch (error) {
      logger.warn("db-writer failed to fetch event from API", {
        polymarketId,
        error: formatError(error)
      });
      return false;
    }
  }

  private async fetchAndEnqueueMarket(polymarketId: string): Promise<boolean> {
    try {
      const market = await polymarketClient.getMarket(polymarketId);
      if (!market) {
        logger.warn("db-writer market lookup returned no data", { polymarketId });
        return false;
      }

      const eventId = market.event?.id ?? market.eventId ?? market.id;
      if (market.event) {
        await pushUpdate("event", normalizeEvent(market.event));
      }
      await pushUpdate("market", normalizeMarket(market, { id: eventId }));
      return true;
    } catch (error) {
      logger.warn("db-writer failed to fetch market from API", {
        polymarketId,
        error: formatError(error)
      });
      return false;
    }
  }

  private decimal(value?: number | null): string | null {
    if (value === undefined || value === null || Number.isNaN(value)) {
      return null;
    }
    return value.toString();
  }

  private toDate(value: Date | string | null | undefined): Date | null {
    if (!value) return null;
    if (value instanceof Date) return value;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private dedupeBatch<T>(
    items: T[],
    keySelector: (item: T) => string,
    context: string
  ): T[] {
    const map = new Map<string, T>();
    for (const item of items) {
      const key = keySelector(item);
      if (!key) continue;
      map.set(key, item);
    }

    if (map.size !== items.length) {
      logger.debug(`db-writer deduped ${context} batch`, {
        original: items.length,
        unique: map.size
      });
    }

    return Array.from(map.values());
  }

  // ---------------------------
  // ERROR HANDLING HELPERS
  // ---------------------------

  private isConstraintViolationError(error: any): boolean {
    // PostgreSQL constraint violation codes:
    // 23505 - unique_violation
    // 21000 - "ON CONFLICT DO UPDATE command cannot affect row a second time"
    return (
      error?.code === '23505' ||
      error?.code === '21000' ||
      error?.message?.includes('ON CONFLICT DO UPDATE command cannot affect row a second time') ||
      error?.message?.includes('duplicate key') ||
      error?.message?.includes('unique constraint')
    );
  }

  private async upsertEventsIndividually(events: NormalizedEvent[]): Promise<void> {
    logger.debug('db-writer processing events individually due to constraint violation', {
      eventCount: events.length,
      polymarketIds: events.map(e => e.polymarket_id),
      timestamp: new Date().toISOString()
    });

    let processedCount = 0;
    let skippedCount = 0;
    let errorCount = 0;

    for (let i = 0; i < events.length; i++) {
      const event = events[i];

      if (!event) {
        logger.warn('db-writer encountered null event in individual processing', {
          eventIndex: i + 1,
          totalEvents: events.length
        });
        continue;
      }

      try {
        logger.debug('db-writer processing individual event', {
          eventIndex: i + 1,
          totalEvents: events.length,
          polymarketId: event.polymarket_id,
          title: event.title?.substring(0, 50) + '...'
        });

        // Process single event with timeout to prevent hanging
        await this.timedDbOperation(
          'upsert-single-event',
          () => this.upsertSingleEvent(event),
          {
            polymarketId: event.polymarket_id,
            eventIndex: i + 1,
            totalEvents: events.length
          }
        );

        processedCount++;
        logger.debug('db-writer successfully processed individual event', {
          polymarketId: event.polymarket_id,
          processedCount,
          totalEvents: events.length
        });

      } catch (error) {
        if (this.isConstraintViolationError(error)) {
          logger.debug('db-writer skipping duplicate event', {
            polymarketId: event.polymarket_id,
            eventIndex: i + 1,
            errorCode: (error as any)?.code,
            errorMessage: (error as Error)?.message
          });
          skippedCount++;
          continue; // Skip duplicates
        }

        errorCount++;
        logger.error('db-writer failed to process individual event', {
          polymarketId: event.polymarket_id,
          eventIndex: i + 1,
          error: formatError(error),
          errorType: error?.constructor?.name,
          errorCode: (error as any)?.code,
          errorMessage: (error as Error)?.message,
          processedCount,
          skippedCount,
          errorCount,
          totalEvents: events.length,
          timestamp: new Date().toISOString()
        });

        // Continue processing other events instead of failing the entire batch
        continue;
      }
    }

    logger.info('db-writer completed individual event processing', {
      totalEvents: events.length,
      processedCount,
      skippedCount,
      errorCount,
      timestamp: new Date().toISOString()
    });

    // If we have too many errors, log a warning
    if (errorCount > 0 && errorCount > events.length * 0.5) {
      logger.warn('db-writer high error rate in individual event processing', {
        errorRate: `${Math.round((errorCount / events.length) * 100)}%`,
        errorCount,
        totalEvents: events.length
      });
    }
  }

  // New method to process a single event (used in individual processing)
  private async upsertSingleEvent(event: NormalizedEvent): Promise<void> {
    const now = new Date();
    const values = [{
      polymarketId: event.polymarket_id,
      title: event.title,
      slug: event.slug,
      description: event.description,
      category: event.category,
      liquidity: this.decimal(event.liquidity) ?? "0",
      volume24h: this.decimal(event.volume) ?? "0",
      volumeTotal: this.decimal(event.volume) ?? "0",
      active: event.is_active,
      closed: event.closed,
      archived: event.archived,
      restricted: event.restricted,
      startDate: this.toDate(event.start_date),
      endDate: this.toDate(event.end_date),
      lastIngestedAt: this.toDate(event.last_ingested_at) ?? now,
      updatedAt: now
    }];

    if (DEBUG_SQL_QUERIES) {
      logger.info('db-writer SQL DEBUG - Single event upsert', {
        polymarketId: event.polymarket_id,
        title: event.title?.substring(0, 50) + '...',
        operation: 'INSERT ON CONFLICT DO UPDATE (single record)'
      });
    }

    try {
      await db
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
            updatedAt: sql`excluded.updated_at`
          }
        });

      if (DEBUG_SQL_QUERIES) {
        logger.info('db-writer SQL DEBUG - Single event upsert completed', {
          polymarketId: event.polymarket_id,
          result: 'success'
        });
      }
    } catch (error) {
      if (DEBUG_SQL_QUERIES) {
        logger.error('db-writer SQL DEBUG - Single event upsert failed', {
          polymarketId: event.polymarket_id,
          error: formatError(error),
          errorCode: (error as any)?.code
        });
      }
      throw error;
    }
  }
}

export const dbWriterWorker = new DbWriterWorker();
