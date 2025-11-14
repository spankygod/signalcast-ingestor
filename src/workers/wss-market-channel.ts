import WebSocket from "ws";
import logger, { formatError } from "../lib/logger";
import { polymarketConfig, PolymarketMarket, polymarketClient } from "../config/polymarket";
import { pushUpdate } from "../queues/updates.queue";
import { normalizeTick, NormalizedTick } from "../utils/normalizeTick";
import { sleep } from "../lib/retry";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS, QUEUES } from "../utils/constants";
import { redisAvailable, redis } from "../lib/redis";
import { settings } from "../config/settings";
import { politicsFilter } from "../utils/politicsFilter";
import { bootstrap } from "../lib/bootstrap";

const WORKER_NAME = WORKERS.marketChannel;

// How many asset IDs per subscription message to avoid huge payloads
const MAX_ASSETS_PER_SUBSCRIPTION = 400;

// How often we allow a "tick" heartbeat (ms)
const TICK_HEARTBEAT_INTERVAL_MS = 2000;

const authPayload =
  process.env.POLYMARKET_API_KEY &&
  process.env.POLYMARKET_SECRET &&
  process.env.POLYMARKET_PASSPHRASE
    ? {
        apiKey: process.env.POLYMARKET_API_KEY,
        secret: process.env.POLYMARKET_SECRET,
        passphrase: process.env.POLYMARKET_PASSPHRASE
      }
    : null;

export interface MarketChannelSubscription extends Record<string, unknown> {
  type: "market" | "user";
  assets_ids?: string[];
  markets?: string[];
  auth?: Record<string, string>;
}

export class MarketChannelWorker {
  private socket: WebSocket | null = null;
  private reconnectDelay = 5_000;
  private shouldRun = false;
  private assetIds: string[] = [];
  private pingTimer: NodeJS.Timeout | null = null;

  private initializing = false;
  private lastTickHeartbeatAt = 0;

  constructor() {}

  start(): void {
    if (this.shouldRun) return;
    this.shouldRun = true;
    heartbeatMonitor.beat(WORKER_NAME, { state: "initializing" });
    void this.initializeAndConnect();
  }

  stop(): void {
    this.shouldRun = false;
    this.stopPing();

    if (this.socket) {
      this.socket.removeAllListeners();
      try {
        this.socket.close();
      } catch {
        // ignore
      }
      // Forcefully terminate if still hanging
      try {
        this.socket.terminate();
      } catch {
        // ignore
      }
      this.socket = null;
    }

    heartbeatMonitor.markIdle(WORKER_NAME, { state: "stopped" });
  }

  private async initializeAndConnect(): Promise<void> {
    if (this.initializing) return;
    this.initializing = true;

    try {
      await this.refreshAssetIds();
    } catch (error) {
      logger.error(`${WORKER_NAME} failed to load asset ids`, { error: formatError(error) });
    } finally {
      this.initializing = false;
    }

    if (!this.shouldRun) return;
    this.connect();
  }

  private connect(): void {
    if (!this.shouldRun) return;

    heartbeatMonitor.beat(WORKER_NAME, { state: "connecting" });

    const url = `${polymarketConfig.wsBaseUrl}${process.env.POLYMARKET_WS_PATH || "/prices"}`;
    logger.info(`${WORKER_NAME} connecting`, { url });

    this.socket = new WebSocket(url);

    this.socket.on("open", () => {
      logger.info(`${WORKER_NAME} connected`);
      this.reconnectDelay = 5_000;
      heartbeatMonitor.beat(WORKER_NAME, { state: "connected" });

      // On every (re)connect, refresh asset IDs then subscribe
      void this.refreshAssetIdsAndSubscribe();
      this.startPing();
    });

    this.socket.on("message", async (raw) => {
      try {
        const text = raw.toString();

        // Legacy string ping/pong from server (just in case)
        if (text === "PING") {
          this.send({ type: "pong" });
          heartbeatMonitor.beat(WORKER_NAME, { state: "pong" });
          return;
        }
        if (text === "PONG") {
          return;
        }

        const message = JSON.parse(text);

        // JSON ping from Polymarket
        if (message?.type === "ping") {
          this.send({ type: "pong" });
          heartbeatMonitor.beat(WORKER_NAME, { state: "pong" });
          return;
        }

        // Error payload from server (e.g. auth/format error)
        if (message?.error) {
          logger.error(`${WORKER_NAME} received error from server`, {
            error: message.error,
            data: message.data ?? null
          });
          heartbeatMonitor.beat(WORKER_NAME, { state: "error" });
          return;
        }

        if (message?.type !== "channel_data" || !message?.data) {
          return;
        }

        const marketPayload = this.extractMarket(message);
        if (!marketPayload?.id) return;

        const tick = normalizeTick(this.toMarket(marketPayload));

        if (await this.shouldEnqueueTick()) {
          await pushUpdate("tick", tick);
        } else {
          this.logTick(tick);
        }

        this.beatTick();
      } catch (error) {
        logger.warn(`${WORKER_NAME} failed to process tick`, { error: formatError(error) });
      }
    });

    this.socket.on("error", (error) => {
      logger.error(`${WORKER_NAME} socket error`, { error: formatError(error) });
    });

    this.socket.on("close", async (code, reason) => {
      logger.warn(`${WORKER_NAME} connection closed`, {
        code,
        reason: reason?.toString()
      });
      heartbeatMonitor.markIdle(WORKER_NAME, { state: "disconnected" });
      this.stopPing();

      if (!this.shouldRun) return;

      await sleep(this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 60_000);
      heartbeatMonitor.beat(WORKER_NAME, { state: "reconnecting" });
      this.connect();
    });
  }

  private extractMarket(message: any): Partial<PolymarketMarket> | null {
    if (!message) return null;

    const payload = message.data ?? message;

    // Prefer explicit market key
    const maybeMarket = payload.market ?? payload.data?.market ?? payload;

    // Be strict: ensure it looks like a market
    if (!maybeMarket || typeof maybeMarket !== "object") return null;
    if (!maybeMarket.id) return null;

    return maybeMarket as Partial<PolymarketMarket>;
  }

  private toMarket(payload: Partial<PolymarketMarket>): PolymarketMarket {
    return {
      id: payload.id!,
      question: payload.question || "",
      description: payload.description ?? null,
      slug: payload.slug || payload.id!,
      startDate: payload.startDate || null,
      endDate: payload.endDate || null,
      liquidity: Number(payload.liquidity ?? 0),
      volume: Number(payload.volume ?? 0),
      createdAt: payload.createdAt || new Date().toISOString(),
      isActive: payload.isActive ?? true,
      closed: payload.closed ?? false,
      archived: payload.archived ?? false,
      restricted: payload.restricted ?? false,
      approved: payload.approved ?? true,
      currentPrice: payload.currentPrice ?? null,
      lastTradePrice: payload.lastTradePrice ?? null,
      bestBid: payload.bestBid ?? null,
      bestAsk: payload.bestAsk ?? null,
      spread: payload.spread ?? null,
      status: payload.status || "active",
      resolvedAt: payload.resolvedAt || null,
      outcomes: payload.outcomes || [],
      category: payload.category,
      tags: payload.tags,
      marketMakerAddress: payload.marketMakerAddress,
      imageUrl: payload.imageUrl,
      event: payload.event
    };
  }

private async shouldEnqueueTick(): Promise<boolean> {
  // If bootstrap not done, don’t enqueue ticks yet
  const eventsDone = await bootstrap.isDone("events_done");
  const marketsDone = await bootstrap.isDone("markets_done");
  if (!eventsDone || !marketsDone) return false;

  if (!settings.redisEnabled || !redisAvailable) return false;

  try {
    const backlog = await redis.llen(QUEUES.ticks);
    if (backlog >= settings.tickQueueBacklogThreshold) {
      logger.warn(
        `${WORKER_NAME} dropping ticks due to backlog`,
        { backlog },
      );
      return false;
    }
  } catch (e) {
    logger.warn(`${WORKER_NAME} failed to read tick backlog`, {
      error: formatError(e),
    });
  }

  return true;
}

  private logTick(tick: NormalizedTick): void {
    console.log(`[${WORKER_NAME}]`, "tick (redis disabled)", {
      market: tick.market_polymarket_id,
      price: tick.price,
      bestBid: tick.best_bid,
      bestAsk: tick.best_ask,
      lastTradePrice: tick.last_trade_price,
      capturedAt: tick.captured_at
    });
  }

  private beatTick(): void {
    const now = Date.now();
    if (now - this.lastTickHeartbeatAt < TICK_HEARTBEAT_INTERVAL_MS) return;
    this.lastTickHeartbeatAt = now;

    heartbeatMonitor.beat(WORKER_NAME, { state: "tick" });
  }

  private async refreshAssetIdsAndSubscribe(): Promise<void> {
    try {
      await this.refreshAssetIds();
      this.subscribe();
    } catch (error) {
      logger.error(`${WORKER_NAME} failed to refresh asset ids on connect`, {
        error: formatError(error)
      });
    }
  }

  private subscribe(): void {
    if (!this.assetIds.length) {
      logger.warn(`${WORKER_NAME} no asset ids available, skipping subscription`);
      return;
    }

    // Batch subscriptions to avoid huge payloads
    const chunks: string[][] = [];
    for (let i = 0; i < this.assetIds.length; i += MAX_ASSETS_PER_SUBSCRIPTION) {
      chunks.push(this.assetIds.slice(i, i + MAX_ASSETS_PER_SUBSCRIPTION));
    }

    for (const chunk of chunks) {
      const subscription: MarketChannelSubscription = {
        type: "market",
        assets_ids: chunk,
        auth: authPayload ?? undefined
      };
      this.send(subscription);
    }

    logger.info(`${WORKER_NAME} sent market subscriptions`, {
      assetCount: this.assetIds.length,
      batchCount: chunks.length
    });
  }

  private send(payload: Record<string, unknown> | string): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      logger.warn(`${WORKER_NAME} cannot send payload - socket not ready`);
      return;
    }

    if (typeof payload === "string") {
      this.socket.send(payload);
      return;
    }

    this.socket.send(JSON.stringify(payload));
  }

  private startPing(): void {
    if (this.pingTimer) return;

    const interval = Number(process.env.POLYMARKET_WS_PING_INTERVAL_MS || 10_000);

    this.pingTimer = setInterval(() => {
      // Prefer JSON ping; server already handles this pattern
      this.send({ type: "ping" });
    }, interval);
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private async refreshAssetIds(): Promise<void> {
    const fetched = await this.fetchAssetIds();

    if (fetched.length > 0) {
      this.assetIds = fetched;
      logger.info(`${WORKER_NAME} loaded asset ids`, { count: fetched.length });
      return;
    }

    const fallback = this.readAssetIdsFromEnv();
    this.assetIds = fallback;
    logger.warn(`${WORKER_NAME} fetched zero asset ids; using env fallback`, {
      fallbackCount: fallback.length
    });
  }

  private readAssetIdsFromEnv(): string[] {
    return (process.env.POLYMARKET_WS_ASSET_IDS || "")
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
  }

  private async fetchAssetIds(): Promise<string[]> {
    const limit = 100;
    let offset = 0;
    const assets = new Set<string>();
    let pageCount = 0;

    while (this.shouldRun) {
      let markets: PolymarketMarket[] = [];

      try {
        markets = await polymarketClient.listMarkets({
          limit,
          offset,
          closed: false,
          order: "createdAt",
          ascending: false
        });
      } catch (error) {
        logger.error(`${WORKER_NAME} failed to list markets`, {
          error: formatError(error),
          offset,
          limit
        });
        break;
      }

      if (!markets.length) {
        break;
      }

      const beforeSize = assets.size;

      const filtered = politicsFilter.filterMarkets(markets);
      for (const market of filtered) {
        for (const id of this.extractAssetIds(market)) {
          assets.add(id);
        }
      }

      pageCount += 1;
      offset += limit;

      // Stop if we didn't gain any new asset IDs from this page
      if (assets.size === beforeSize) {
        logger.warn(`${WORKER_NAME} pagination yielded no new asset IDs, stopping`, {
          pageCount,
          offset
        });
        break;
      }

      // Stop on "natural" last page
      if (markets.length < limit) {
        break;
      }

      // Hard safety guard: prevent unbounded loops
      if (pageCount >= 1000) {
        logger.warn(`${WORKER_NAME} reached max pagination pages when fetching markets`, {
          pageCount,
          offset
        });
        break;
      }
    }

    return Array.from(assets);
  }

  private extractAssetIds(market: PolymarketMarket): string[] {
    const tokens: unknown = (market as any).clobTokenIds;
    if (!tokens) return [];

    if (Array.isArray(tokens)) {
      return tokens.filter(
        (id): id is string => typeof id === "string" && id.length > 0
      );
    }

    if (typeof tokens === "string") {
      try {
        const parsed = JSON.parse(tokens);
        if (Array.isArray(parsed)) {
          return parsed.filter(
            (id): id is string => typeof id === "string" && id.length > 0
          );
        }
      } catch {
        logger.warn(`${WORKER_NAME} failed to parse clobTokenIds`, { marketId: market.id });
      }
    }

    return [];
  }
}

export const marketChannelWorker = new MarketChannelWorker();
