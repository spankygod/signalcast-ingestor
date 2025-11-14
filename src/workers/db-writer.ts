import { eq, sql } from "drizzle-orm";
import logger, { formatError } from "../lib/logger";
import { polymarketClient } from "../config/polymarket";
import {
  pullNextUpdate,
  requeueUpdate,
  UpdateJob,
  UpdateKind
} from "../queues/updates.queue";
import {
  db,
  events,
  markets,
  outcomes,
  marketPricesRealtime
} from "../lib/db";
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
import { bootstrap } from "../lib/bootstrap";
import { pushUpdate } from "../queues/updates.queue";

const JOB_MAX_ATTEMPTS = Number(process.env.JOB_MAX_ATTEMPTS || 3);

export class DbWriterWorker {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;

  private readonly PIPELINE = [
    { kind: "event", batchSize: 50, handler: this.processEventBatch.bind(this) },
    { kind: "market", batchSize: 50, handler: this.processMarketBatch.bind(this) },
    { kind: "outcome", batchSize: 100, handler: this.processOutcomeBatch.bind(this) },
    { kind: "tick", batchSize: 300, handler: this.processTickBatch.bind(this) }
  ];

  start(): void {
    if (this.timer) return;
    logger.info("db-writer starting [PRO TIER STABLE]");
    // drain ~5x per second
    this.timer = setInterval(() => {
      void this.drain();
    }, 200);
    void this.drain();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  // -------------------------------------------------------------------
  // Main pipeline - STRICT ORDER: events -> markets -> outcomes -> ticks
  // -------------------------------------------------------------------

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    try {
      let totalProcessed = 0;

      for (const stage of this.PIPELINE) {
        let processed;
        do {
          processed = await this.processQueue(stage.kind as UpdateKind, stage.batchSize, stage.handler);
          totalProcessed += processed;
          heartbeatMonitor.beat(WORKERS.dbWriter, {
            stage: stage.kind,
            processed: processed
          });
        } while (processed > 0);
      }

      if (totalProcessed === 0) {
        heartbeatMonitor.beat(WORKERS.dbWriter, { state: "idle" });
        heartbeatMonitor.markIdle(WORKERS.dbWriter);
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

      try {
        const handled = await handler(jobs);
        total += handled;
      } catch (error) {
        logger.error(`db-writer failed processing ${kind} batch`, {
          error: formatError(error),
          kind
        });
        // break to avoid infinite error loop
        break;
      }
    }

    return total;
  }

  private async pullJobs(
    kind: UpdateKind,
    max: number
  ): Promise<UpdateJob<any>[]> {
    const jobs: UpdateJob<any>[] = [];
    for (let i = 0; i < max; i++) {
      const job = await pullNextUpdate(kind);
      if (!job) break;
      jobs.push(job);
    }
    return jobs;
  }

  // -------------------------------------------------------------------
  // EVENTS — bulk upsert, parent of everything
  // -------------------------------------------------------------------

  private async processEventBatch(
    jobs: UpdateJob<NormalizedEvent>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    // dedupe by polymarket_id
    const map = new Map<string, NormalizedEvent>();
    for (const job of jobs) {
      if (!job.payload?.polymarket_id) continue;
      map.set(job.payload.polymarket_id, job.payload);
    }

    const eventsList = Array.from(map.values());
    if (eventsList.length === 0) return 0;

    await this.upsertEvents(eventsList);
    return eventsList.length;
  }

  private async upsertEvents(batch: NormalizedEvent[]): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();

    const values = batch.map((e) => ({
      polymarketId: e.polymarket_id,
      slug: e.slug,
      title: e.title,
      description: e.description,
      category: e.category,
      subcategory: null as string | null, // not used yet
      liquidity: e.liquidity != null ? e.liquidity.toString() : "0",
      volume24h: e.volume != null ? e.volume.toString() : "0",
      volumeTotal: e.volume != null ? e.volume.toString() : "0",
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
  }

  // -------------------------------------------------------------------
  // MARKETS — ensure event exists, then bulk upsert
  // -------------------------------------------------------------------

  private async processMarketBatch(
    jobs: UpdateJob<NormalizedMarket>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const batch: { market: NormalizedMarket; eventId: string }[] = [];

    for (const job of jobs) {
      const m = job.payload;
      if (!m?.polymarket_id || !m.event_polymarket_id) continue;

      const eventId = await this.ensureEventExists(m.event_polymarket_id);
      if (!eventId) {
        // can't guarantee parent, skip or requeue this market
        logger.warn("skipping market with missing parent event", {
          eventPolymarketId: m.event_polymarket_id
        });

        // ONLY requeue if bootstrap is done
        if (await bootstrap.isDone("events_done")) {
          await requeueUpdate("market", job);
        }
        continue;
      }

      batch.push({ market: m, eventId });
    }

    if (batch.length === 0) return 0;

    await this.upsertMarkets(batch);
    return batch.length;
  }

  private async upsertMarkets(
    batch: { market: NormalizedMarket; eventId: string }[]
  ): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();

    const values = batch.map(({ market, eventId }) => ({
      polymarketId: market.polymarket_id,
      eventId: eventId,
      question: market.question,
      slug: market.slug,
      description: market.description,
      liquidity: market.liquidity != null ? market.liquidity.toString() : "0",
      volume24h: market.volume != null ? market.volume.toString() : "0",
      volumeTotal: market.volume != null ? market.volume.toString() : "0",
      currentPrice:
        market.current_price != null ? market.current_price.toString() : null,
      lastTradePrice:
        market.last_trade_price != null
          ? market.last_trade_price.toString()
          : null,
      bestBid: market.best_bid != null ? market.best_bid.toString() : null,
      bestAsk: market.best_ask != null ? market.best_ask.toString() : null,
      status: market.status,
      resolvedAt: market.resolved_at ? new Date(market.resolved_at) : null,
      active: market.is_active,
      closed: market.closed,
      archived: market.archived,
      restricted: market.restricted,
      approved: market.approved,
      relevanceScore:
        market.relevance_score != null
          ? market.relevance_score.toString()
          : "0",
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

  // -------------------------------------------------------------------
  // OUTCOMES — ensure market exists, then bulk upsert
  // -------------------------------------------------------------------

  private async processOutcomeBatch(
    jobs: UpdateJob<NormalizedOutcome>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const batch: { outcome: NormalizedOutcome; marketId: string }[] = [];

    for (const job of jobs) {
      const o = job.payload;
      if (!o?.polymarket_id || !o.market_polymarket_id) continue;

      const marketId = await this.ensureMarketExists(o.market_polymarket_id);
      if (!marketId) {
        logger.warn("skipping outcome with missing parent market", {
          outcomePolymarketId: o.polymarket_id,
          marketPolymarketId: o.market_polymarket_id
        });

        // ONLY requeue if bootstrap is done
        if (await bootstrap.isDone("events_done")) {
          await requeueUpdate("outcome", job);
        }
        continue;
      }

      batch.push({ outcome: o, marketId });
    }

    if (batch.length === 0) return 0;

    await this.upsertOutcomes(batch);
    return batch.length;
  }

  private async upsertOutcomes(
    batch: { outcome: NormalizedOutcome; marketId: string }[]
  ): Promise<void> {
    if (batch.length === 0) return;
    const now = new Date();

    const values = batch.map(({ outcome, marketId }) => ({
      polymarketId: outcome.polymarket_id,
      marketId: marketId,
      title: outcome.title,
      description: outcome.description,
      price: outcome.price != null ? outcome.price.toString() : "0",
      probability:
        outcome.probability != null ? outcome.probability.toString() : "0",
      volume: outcome.volume != null ? outcome.volume.toString() : "0",
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

  // -------------------------------------------------------------------
  // TICKS — ensure market exists, then insert-only
  // -------------------------------------------------------------------

  private async processTickBatch(
    jobs: UpdateJob<NormalizedTick>[]
  ): Promise<number> {
    if (jobs.length === 0) return 0;

    const rows: {
      marketId: string;
      price: string | null;
      bestBid: string | null;
      bestAsk: string | null;
      lastTradePrice: string | null;
      liquidity: string | null;
      volume24h: string | null;
      updatedAt: Date;
    }[] = [];

    for (const job of jobs) {
      const t = job.payload;
      if (!t?.market_polymarket_id) continue;

      const marketId = await this.ensureMarketExists(t.market_polymarket_id);
      if (!marketId) {
        logger.debug("db-writer skipping tick for unknown market", {
          marketPolymarketId: t.market_polymarket_id
        });
        continue;
      }

      rows.push({
        marketId: marketId,
        price: t.price != null ? t.price.toString() : null,
        bestBid: t.best_bid != null ? t.best_bid.toString() : null,
        bestAsk: t.best_ask != null ? t.best_ask.toString() : null,
        lastTradePrice:
          t.last_trade_price != null ? t.last_trade_price.toString() : null,
        liquidity: t.liquidity != null ? t.liquidity.toString() : null,
        volume24h:
          t.volume_24h != null ? t.volume_24h.toString() : null,
        updatedAt: t.captured_at ? new Date(t.captured_at) : new Date()
      });
    }

    if (rows.length === 0) return 0;

    await db.insert(marketPricesRealtime).values(rows);
    return rows.length;
  }

  // -------------------------------------------------------------------
  // PARENT HELPERS — ensure event/market exist (DB first, API fallback)
  // -------------------------------------------------------------------

  private async ensureEventExists(
    eventPolymarketId: string
  ): Promise<string | null> {
    if (!eventPolymarketId) return null;

    // 1) Try DB first
    const existing = await db
      .select({ id: events.id })
      .from(events)
      .where(eq(events.polymarketId, eventPolymarketId))
      .limit(1);

    if (existing[0]?.id) return existing[0].id;

    // 2) Fallback: fetch from API + normalize + upsert
    try {
      const apiEvent = await polymarketClient.getEvent(eventPolymarketId);
      if (!apiEvent) {
        logger.warn("db-writer ensureEventExists: API returned no event", {
          eventPolymarketId
        });
        return null;
      }

      const normalized = normalizeEvent(apiEvent);
      await this.upsertEvents([normalized]);

      const after = await db
        .select({ id: events.id })
        .from(events)
        .where(eq(events.polymarketId, eventPolymarketId))
        .limit(1);

      return after[0]?.id ?? null;
    } catch (error) {
      logger.warn("db-writer ensureEventExists: failed to fetch event", {
        eventPolymarketId,
        error: formatError(error)
      });
      return null;
    }
  }

  private async ensureMarketExists(
    marketPolymarketId: string
  ): Promise<string | null> {
    if (!marketPolymarketId) return null;

    // 1) Try DB first
    const existing = await db
      .select({ id: markets.id })
      .from(markets)
      .where(eq(markets.polymarketId, marketPolymarketId))
      .limit(1);

    if (existing[0]?.id) return existing[0].id;

    // 2) Fallback: fetch from API, ensure event, then upsert market
    try {
      const apiMarket = await polymarketClient.getMarket(marketPolymarketId);
      if (!apiMarket) {
        logger.warn("db-writer ensureMarketExists: API returned no market", {
          marketPolymarketId
        });
        return null;
      }

      const eventPolymarketId =
        apiMarket.event?.id ?? apiMarket.eventId ?? apiMarket.id;
      let eventId: string | null = null;

      if (eventPolymarketId) {
        // if API gives embedded event, normalize + upsert directly
        if (apiMarket.event) {
          const normalizedEvent = normalizeEvent(apiMarket.event);
          await this.upsertEvents([normalizedEvent]);
        }
        eventId = await this.ensureEventExists(eventPolymarketId);
      }

      if (!eventId) {
        logger.warn(
          "db-writer ensureMarketExists: unable to resolve parent event",
          { marketPolymarketId, eventPolymarketId }
        );
        return null;
      }

      const normalizedMarket = normalizeMarket(apiMarket, {
        id: eventPolymarketId
      }) as NormalizedMarket;

      await this.upsertMarkets([{ market: normalizedMarket, eventId }]);

      const after = await db
        .select({ id: markets.id })
        .from(markets)
        .where(eq(markets.polymarketId, marketPolymarketId))
        .limit(1);

      return after[0]?.id ?? null;
    } catch (error) {
      logger.warn("db-writer ensureMarketExists: failed to fetch market", {
        marketPolymarketId,
        error: formatError(error)
      });
      return null;
    }
  }
}

export const dbWriterWorker = new DbWriterWorker();
