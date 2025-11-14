import { eq, sql } from "drizzle-orm";
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
import { sleep } from "../lib/retry";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";

type PipelineStage = {
  kind: UpdateKind;
  handler: (jobs: UpdateJob<any>[]) => Promise<number>;
  batchSize: number;
};

export class DbWriterWorker {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;
  private readonly maxJobAttempts = Number(process.env.JOB_MAX_ATTEMPTS || 3);

  start(): void {
    if (this.timer) return;
    logger.info('db-writer starting');
    this.timer = setInterval(() => {
      void this.drain();
    }, settings.queueDrainIntervalMs);
    void this.drain();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  private async drain(): Promise<void> {
    heartbeatMonitor.beat(WORKERS.dbWriter, { state: 'alive' });
    if (this.draining) return;
    this.draining = true;
    heartbeatMonitor.beat(WORKERS.dbWriter, { state: 'draining' });

    try {
      let totalProcessed = 0;
      const pipeline: PipelineStage[] = [
        {
          kind: 'event',
          batchSize: Math.max(1, settings.eventBatchSize),
          handler: (jobs) => this.processEventJobs(jobs as UpdateJob<NormalizedEvent>[])
        },
        {
          kind: 'market',
          batchSize: 50,
          handler: (jobs) => this.processMarketJobs(jobs as UpdateJob<NormalizedMarket>[])
        },
        {
          kind: 'outcome',
          batchSize: 100,
          handler: (jobs) => this.processOutcomeJobs(jobs as UpdateJob<NormalizedOutcome>[])
        },
        {
          kind: 'tick',
          batchSize: 1,
          handler: (jobs) => this.processTickJobs(jobs as UpdateJob<NormalizedTick>[])
        }
      ];

      for (const stage of pipeline) {
        totalProcessed += await this.processQueue(stage);
      }

      if (totalProcessed === 0) {
        heartbeatMonitor.markIdle(WORKERS.dbWriter);
      }
    } catch (error) {
      logger.error('db-writer failed to drain queues', { error: formatError(error) });
    } finally {
      this.draining = false;
    }
  }

  private async processQueue(stage: PipelineStage): Promise<number> {
    let processed = 0;

    while (true) {
      const jobs = await this.pullJobs(stage.kind, stage.batchSize);
      if (jobs.length === 0) break;

      const handled = await stage.handler(jobs);
      if (handled > 0) {
        processed += handled;
        heartbeatMonitor.beat(WORKERS.dbWriter, { kind: stage.kind, processed });
      }
    }

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

  private async processEventJobs(jobs: UpdateJob<NormalizedEvent>[]): Promise<number> {
    if (jobs.length === 0) return 0;
    await this.upsertEvents(jobs.map(job => job.payload));
    return jobs.length;
  }

  private async processMarketJobs(jobs: UpdateJob<NormalizedMarket>[]): Promise<number> {
    let processed = 0;
    for (const job of jobs) {
      if (await this.processMarketJob(job)) {
        processed += 1;
      }
    }
    return processed;
  }

  private async processMarketJob(job: UpdateJob<NormalizedMarket>): Promise<boolean> {
    const market = job.payload;
    const eventId = await this.resolveEventId(market.event_polymarket_id);
    if (!eventId) {
      return await this.handleMissingEventForMarket(market, job);
    }

    await this.upsertMarket(market, eventId);
    return true;
  }

  private async processOutcomeJobs(jobs: UpdateJob<NormalizedOutcome>[]): Promise<number> {
    let processed = 0;
    for (const job of jobs) {
      if (await this.processOutcomeJob(job)) {
        processed += 1;
      }
    }
    return processed;
  }

  private async processOutcomeJob(job: UpdateJob<NormalizedOutcome>): Promise<boolean> {
    const outcome = job.payload;
    const marketId = await this.resolveMarketId(outcome.market_polymarket_id);
    if (!marketId) {
      return await this.handleMissingMarketForOutcome(outcome, job);
    }

    await this.upsertOutcome(outcome, marketId);
    return true;
  }

  private async processTickJobs(jobs: UpdateJob<NormalizedTick>[]): Promise<number> {
    let processed = 0;
    for (const job of jobs) {
      if (await this.processTickJob(job)) {
        processed += 1;
      }
    }
    return processed;
  }

  private async processTickJob(job: UpdateJob<NormalizedTick>): Promise<boolean> {
    const tick = job.payload;
    const marketId = await this.resolveMarketId(tick.market_polymarket_id);
    if (!marketId) {
      return await this.handleMissingMarketForTick(tick, job);
    }

    await this.insertTick(tick, marketId);
    return true;
  }

  private async upsertEvents(batch: NormalizedEvent[]): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();
    const values = batch.map((event) => ({
      polymarketId: event.polymarket_id,
      title: event.title,
      slug: event.slug,
      description: event.description,
      category: event.category,
      liquidity: this.decimal(event.liquidity) ?? '0',
      volume24h: this.decimal(event.volume) ?? '0',
      volumeTotal: this.decimal(event.volume) ?? '0',
      active: event.is_active,
      closed: event.closed,
      archived: event.archived,
      restricted: event.restricted,
      startDate: this.toDate(event.start_date),
      endDate: this.toDate(event.end_date),
      lastIngestedAt: this.toDate(event.last_ingested_at) ?? now,
      updatedAt: now
    }));

    await this.withRateLimitHandling(
      () =>
        db.insert(events)
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
      }),
      'events-upsert'
    );
  }

  private async upsertMarket(market: NormalizedMarket, eventId: string): Promise<void> {
    const values = {
      polymarketId: market.polymarket_id,
      eventId,
      question: market.question,
      slug: market.slug,
      description: market.description,
      liquidity: this.decimal(market.liquidity) ?? '0',
      volume24h: this.decimal(market.volume) ?? '0',
      volumeTotal: this.decimal(market.volume) ?? '0',
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
      lastIngestedAt: new Date(),
      updatedAt: new Date()
    };

    await this.withRateLimitHandling(
      () =>
        db.insert(markets)
        .values(values)
        .onConflictDoUpdate({
          target: markets.polymarketId,
          set: {
            ...values,
            updatedAt: new Date(),
            lastIngestedAt: new Date()
          }
        }),
      'markets-upsert'
    );
  }

  private async upsertOutcome(outcome: NormalizedOutcome, marketId: string): Promise<void> {
    const values = {
      polymarketId: outcome.polymarket_id,
      marketId,
      title: outcome.title,
      description: outcome.description,
      price: this.decimal(outcome.price) ?? '0',
      probability: this.decimal(outcome.probability) ?? '0',
      volume: this.decimal(outcome.volume) ?? '0',
      status: outcome.status,
      displayOrder: outcome.display_order,
      updatedAt: new Date()
    };

    await this.withRateLimitHandling(
      () =>
        db.insert(outcomes)
        .values(values)
        .onConflictDoUpdate({
          target: outcomes.polymarketId,
          set: {
            ...values,
            updatedAt: new Date()
          }
        }),
      'outcomes-upsert'
    );
  }

  private async insertTick(tick: NormalizedTick, marketId: string): Promise<void> {
    await this.withRateLimitHandling(
      () =>
        db.insert(marketPricesRealtime).values({
          marketId,
          price: this.decimal(tick.price),
          bestBid: this.decimal(tick.best_bid),
          bestAsk: this.decimal(tick.best_ask),
          lastTradePrice: this.decimal(tick.last_trade_price),
          liquidity: this.decimal(tick.liquidity),
          volume24h: this.decimal(tick.volume_24h),
          updatedAt: this.toDate(tick.captured_at) ?? new Date()
        }),
      'ticks-insert'
    );
  }

  private async handleMissingEventForMarket(
    market: NormalizedMarket,
    job: UpdateJob<NormalizedMarket>
  ): Promise<boolean> {
    const fetched = await this.fetchAndEnqueueEvent(market.event_polymarket_id);
    const requeued = await this.requeueJob('market', job);

    if (requeued) {
      logger.debug('db-writer requeued market awaiting parent event', {
        polymarketId: market.polymarket_id,
        eventPolymarketId: market.event_polymarket_id,
        fetched
      });
      return false;
    }

    logger.warn('db-writer dropping market after missing parent event', {
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
    const fetched = await this.fetchAndEnqueueMarket(outcome.market_polymarket_id);
    const requeued = await this.requeueJob('outcome', job);

    if (requeued) {
      logger.debug('db-writer requeued outcome awaiting parent market', {
        polymarketId: outcome.polymarket_id,
        marketPolymarketId: outcome.market_polymarket_id,
        fetched
      });
      return false;
    }

    logger.warn('db-writer dropping outcome after missing parent market', {
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
    const fetched = await this.fetchAndEnqueueMarket(tick.market_polymarket_id);
    const requeued = fetched ? await this.requeueJob('tick', job) : false;

    if (requeued) {
      logger.debug('db-writer requeued tick awaiting parent market', {
        marketPolymarketId: tick.market_polymarket_id
      });
      return false;
    }

    logger.debug('db-writer skipping tick for unknown market', { polymarketId: tick.market_polymarket_id });
    return true;
  }

  private async requeueJob<T>(kind: UpdateKind, job: UpdateJob<T>): Promise<boolean> {
    const attempts = job.attempts ?? 0;
    if (attempts >= this.maxJobAttempts) {
      logger.warn('db-writer max attempts exceeded, dropping job', {
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
        logger.warn('db-writer event lookup returned no data', { polymarketId });
        return false;
      }

      await pushUpdate('event', normalizeEvent(event));
      return true;
    } catch (error) {
      logger.warn('db-writer failed to fetch event from API', {
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
        logger.warn('db-writer market lookup returned no data', { polymarketId });
        return false;
      }

      const eventId = market.event?.id ?? market.eventId ?? market.id;
      if (market.event) {
        await pushUpdate('event', normalizeEvent(market.event));
      }
      await pushUpdate('market', normalizeMarket(market, { id: eventId }));
      return true;
    } catch (error) {
      logger.warn('db-writer failed to fetch market from API', {
        polymarketId,
        error: formatError(error)
      });
      return false;
    }
  }

  private async resolveEventId(polymarketId: string): Promise<string | null> {
    const result = await db
      .select({ id: events.id })
      .from(events)
      .where(eq(events.polymarketId, polymarketId))
      .limit(1);

    return result[0]?.id ?? null;
  }

  private async resolveMarketId(polymarketId: string): Promise<string | null> {
    const result = await db
      .select({ id: markets.id })
      .from(markets)
      .where(eq(markets.polymarketId, polymarketId))
      .limit(1);

    return result[0]?.id ?? null;
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

  private async withRateLimitHandling<T>(operation: () => Promise<T>, context: string): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      if (this.isRateLimitError(error)) {
        const delay = Number(process.env.DB_RATE_LIMIT_BACKOFF_MS || 500);
        logger.warn('db-writer rate limit detected, backing off', {
          context,
          delay,
          error: formatError(error)
        });
        await sleep(delay);
      }
      throw error;
    }
  }

  private isRateLimitError(error: unknown): boolean {
    if (!error) return false;
    const message = error instanceof Error ? error.message : String(error);
    return message.toLowerCase().includes('too many concurrent writes');
  }
}

export const dbWriterWorker = new DbWriterWorker();
