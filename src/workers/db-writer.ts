import { eq } from "drizzle-orm";
import logger from "../lib/logger";
import { settings } from "../config/settings";
import { pullNextUpdate, UpdateJob } from "../queues/updates.queue";
import { db, events, markets, outcomes, marketPricesRealtime } from "../lib/db";
import { NormalizedEvent, NormalizedMarket, NormalizedOutcome } from "../utils/normalizeMarket";
import { NormalizedTick } from "../utils/normalizeTick";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";

export class DbWriterWorker {
  private timer: NodeJS.Timeout | null = null;
  private draining = false;

  start(): void {
    if (this.timer) return;
    logger.info('db-writer', 'starting');
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
    if (this.draining) return;
    this.draining = true;

    let processed = 0;
    heartbeatMonitor.beat(WORKERS.dbWriter, { processed });

    try {
      while (processed < settings.maxConcurrentJobs) {
        const job = await pullNextUpdate();
        if (!job) break;
        await this.handleJob(job);
        processed += 1;
        heartbeatMonitor.beat(WORKERS.dbWriter, { processed });
      }

      if (processed === 0) {
        heartbeatMonitor.markIdle(WORKERS.dbWriter);
      }
    } catch (error) {
      logger.error('db-writer', 'failed to drain queue', error);
    } finally {
      this.draining = false;
    }
  }

  private async handleJob(job: UpdateJob<any>): Promise<void> {
    switch (job.kind) {
      case 'event':
        await this.upsertEvent(job.payload as NormalizedEvent);
        break;
      case 'market':
        await this.upsertMarket(job.payload as NormalizedMarket);
        break;
      case 'outcome':
        await this.upsertOutcome(job.payload as NormalizedOutcome);
        break;
      case 'tick':
        await this.insertTick(job.payload as NormalizedTick);
        break;
      default:
        logger.warning('db-writer', 'received unknown job kind', { kind: job.kind });
    }
  }

  private async upsertEvent(event: NormalizedEvent): Promise<void> {
    const values = {
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
      startDate: event.start_date,
      endDate: event.end_date,
      lastIngestedAt: event.last_ingested_at,
      updatedAt: new Date()
    };

    await db.insert(events)
      .values(values)
      .onConflictDoUpdate({
        target: events.polymarketId,
        set: {
          ...values,
          updatedAt: new Date()
        }
      });
  }

  private async upsertMarket(market: NormalizedMarket): Promise<void> {
    const eventId = await this.resolveEventId(market.event_polymarket_id);
    if (!eventId) {
      logger.warning('db-writer', 'missing parent event for market', { polymarketId: market.event_polymarket_id });
      return;
    }

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
      resolvedAt: market.resolved_at,
      active: market.is_active,
      closed: market.closed,
      archived: market.archived,
      restricted: market.restricted,
      approved: market.approved,
      relevanceScore: market.relevance_score,
      lastIngestedAt: new Date(),
      updatedAt: new Date()
    };

    await db.insert(markets)
      .values(values)
      .onConflictDoUpdate({
        target: markets.polymarketId,
        set: {
          ...values,
          updatedAt: new Date(),
          lastIngestedAt: new Date()
        }
      });
  }

  private async upsertOutcome(outcome: NormalizedOutcome): Promise<void> {
    const marketId = await this.resolveMarketId(outcome.market_polymarket_id);
    if (!marketId) {
      logger.warning('db-writer', 'missing parent market for outcome', { polymarketId: outcome.market_polymarket_id });
      return;
    }

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

    await db.insert(outcomes)
      .values(values)
      .onConflictDoUpdate({
        target: outcomes.polymarketId,
        set: {
          ...values,
          updatedAt: new Date()
        }
      });
  }

  private async insertTick(tick: NormalizedTick): Promise<void> {
    const marketId = await this.resolveMarketId(tick.market_polymarket_id);
    if (!marketId) {
      logger.debug('db-writer', 'skipping tick for unknown market', { polymarketId: tick.market_polymarket_id });
      return;
    }

    await db.insert(marketPricesRealtime).values({
      marketId,
      price: this.decimal(tick.price),
      bestBid: this.decimal(tick.best_bid),
      bestAsk: this.decimal(tick.best_ask),
      lastTradePrice: this.decimal(tick.last_trade_price),
      liquidity: this.decimal(tick.liquidity),
      volume24h: this.decimal(tick.volume_24h),
      updatedAt: tick.captured_at
    });
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
}

export const dbWriterWorker = new DbWriterWorker();
