import { eq, sql, inArray } from "drizzle-orm";
import logger, { formatError } from "../lib/logger";
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

export class DbWriterWorker {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;

  start(): void {
    if (this.timer) return;
    logger.info("db-writer starting [PRO TIER]");
    this.timer = setInterval(() => void this.drain(), 200);
    void this.drain();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  //---------------------------------------------------------------------
  // Main Pipeline
  //---------------------------------------------------------------------

  private async drain() {
    if (this.draining) return;
    this.draining = true;

    try {
      const pipeline = [
        { kind: "event" as UpdateKind, batchSize: 20, handler: this.processEventBatch.bind(this) },
        { kind: "market" as UpdateKind, batchSize: 20, handler: this.processMarketBatch.bind(this) },
        { kind: "outcome" as UpdateKind, batchSize: 50, handler: this.processOutcomeBatch.bind(this) },
        { kind: "tick" as UpdateKind, batchSize: 100, handler: this.processTickBatch.bind(this) }
      ];

      for (const stage of pipeline) {
        const processed = await this.processQueue(stage.kind, stage.batchSize, stage.handler);
        if (processed > 0) {
          heartbeatMonitor.beat(WORKERS.dbWriter, { stage: stage.kind, processed });
        }
      }
    } catch (error) {
      logger.error("db-writer drain failed", { error: formatError(error) });
    } finally {
      this.draining = false;
    }
  }

  private async processQueue(
    kind: UpdateKind,
    batchSize: number,
    handler: (jobs: UpdateJob<any>[]) => Promise<number>
  ): Promise<number> {
    let total = 0;

    while (true) {
      const jobs = await this.pullJobs(kind, batchSize);
      if (jobs.length === 0) break;
      total += await handler(jobs);
    }

    return total;
  }

  private async pullJobs(kind: UpdateKind, max: number) {
    const jobs: UpdateJob<any>[] = [];
    for (let i = 0; i < max; i++) {
      const job = await pullNextUpdate(kind);
      if (!job) break;
      jobs.push(job);
    }
    return jobs;
  }

  //---------------------------------------------------------------------
  // EVENT BATCH UPSERT
  //---------------------------------------------------------------------

  private async processEventBatch(jobs: UpdateJob<NormalizedEvent>[]): Promise<number> {
    if (jobs.length === 0) return 0;
    const deduped = new Map<string, NormalizedEvent>();
    for (const payload of jobs.map(j => j.payload)) {
      deduped.set(payload.polymarket_id, payload);
    }

    const uniqueEvents = Array.from(deduped.values());
    const now = new Date();
    const values = uniqueEvents.map(e => ({
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
      updatedAt: now
    }));

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

    return uniqueEvents.length;
  }

  //---------------------------------------------------------------------
  // MARKET BATCH UPSERT
  //---------------------------------------------------------------------

  private async processMarketBatch(jobs: UpdateJob<NormalizedMarket>[]): Promise<number> {
    if (jobs.length === 0) return 0;

    const payloads = jobs.map(j => j.payload);

    // Resolve parents in bulk
    const eventIds = Array.from(new Set(payloads.map(m => m.event_polymarket_id)));
    const eventRows = await db
      .select({ id: events.id, polymarketId: events.polymarketId })
      .from(events)
      .where(inArray(events.polymarketId, eventIds));

    const eventMap = new Map(eventRows.map((r: { id: string; polymarketId: string }) => [r.polymarketId, r.id]));

    const ready: { m: NormalizedMarket; eventId: string }[] = [];
    const deferred: UpdateJob<NormalizedMarket>[] = [];

    for (const job of jobs) {
      const m = job.payload;
      const eventId = eventMap.get(m.event_polymarket_id);
      if (!eventId) deferred.push(job);
      else ready.push({ m, eventId });
    }

    if (ready.length > 0) {
      const now = new Date();
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
        updatedAt: now
      }));

      await db
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
            updatedAt: sql`excluded.updated_at`
          }
        });
    }

    // Requeue parents missing
    for (const job of deferred) {
      await pushUpdate("market", job.payload, (job.attempts ?? 0) + 1);
    }

    return ready.length;
  }

  //---------------------------------------------------------------------
  // OUTCOME BATCH UPSERT
  //---------------------------------------------------------------------

  private async processOutcomeBatch(jobs: UpdateJob<NormalizedOutcome>[]): Promise<number> {
    if (jobs.length === 0) return 0;
    const payloads = jobs.map(j => j.payload);

    const marketIds = Array.from(new Set(payloads.map(o => o.market_polymarket_id)));

    const marketRows = await db
      .select({ id: markets.id, polymarketId: markets.polymarketId })
      .from(markets)
      .where(inArray(markets.polymarketId, marketIds));

    const marketMap = new Map(marketRows.map((r: { id: string; polymarketId: string }) => [r.polymarketId, r.id]));

    const ready: { o: NormalizedOutcome; marketId: string }[] = [];
    const deferred: UpdateJob<NormalizedOutcome>[] = [];

    for (const job of jobs) {
      const o = job.payload;
      const parentId = marketMap.get(o.market_polymarket_id);
      if (!parentId) deferred.push(job);
      else ready.push({ o, marketId: parentId });
    }

    if (ready.length > 0) {
      const now = new Date();
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
        updatedAt: now
      }));

      await db
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
            updatedAt: sql`excluded.updated_at`
          }
        });
    }

    for (const job of deferred) {
      await pushUpdate("outcome", job.payload, (job.attempts ?? 0) + 1);
    }

    return ready.length;
  }

  //---------------------------------------------------------------------
  // TICKS BATCH INSERT (NO UPSERT)
  //---------------------------------------------------------------------

  private async processTickBatch(jobs: UpdateJob<NormalizedTick>[]): Promise<number> {
    if (jobs.length === 0) return 0;

    const payloads = jobs.map(j => j.payload);
    const marketIds = Array.from(new Set(payloads.map(t => t.market_polymarket_id)));

    const marketRows = await db
      .select({ id: markets.id, polymarketId: markets.polymarketId })
      .from(markets)
      .where(inArray(markets.polymarketId, marketIds));

    const map = new Map(marketRows.map((r: { id: string; polymarketId: string }) => [r.polymarketId, r.id]));

    const batch = payloads
      .map(t => {
        const mId = map.get(t.market_polymarket_id);
        if (!mId) return null;
        return {
          marketId: mId,
          price: t.price?.toString() ?? null,
          bestBid: t.best_bid?.toString() ?? null,
          bestAsk: t.best_ask?.toString() ?? null,
          lastTradePrice: t.last_trade_price?.toString() ?? null,
          liquidity: t.liquidity?.toString() ?? null,
          volume24h: t.volume_24h?.toString() ?? null,
          updatedAt: t.captured_at ? new Date(t.captured_at) : new Date()
        };
      })
      .filter(Boolean) as any[];

    if (batch.length > 0) {
      await db.insert(marketPricesRealtime).values(batch);
    }

    return batch.length;
  }
}

export const dbWriterWorker = new DbWriterWorker();
