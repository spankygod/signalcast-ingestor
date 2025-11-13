import WebSocket from "ws";
import logger, { formatError } from "../lib/logger";
import { polymarketConfig, PolymarketMarket } from "../config/polymarket";
import { pushUpdate } from "../queues/updates.queue";
import { normalizeTick, NormalizedTick } from "../utils/normalizeTick";
import { sleep } from "../lib/retry";
import { heartbeatMonitor } from "./heartbeat";
import { WORKERS } from "../utils/constants";
import { redisAvailable } from "../lib/redis";
import { settings } from "../config/settings";

const WORKER_NAME = WORKERS.marketChannel;

export interface MarketChannelSubscription {
  type: 'subscribe' | 'unsubscribe';
  channel: string;
  markets?: string[];
  assets?: string[];
  [key: string]: unknown;
}

export class MarketChannelWorker {
  private socket: WebSocket | null = null;
  private reconnectDelay = 5_000;
  private shouldRun = false;
  private readonly subscriptions: MarketChannelSubscription[];

  constructor(subscriptions: MarketChannelSubscription[] = []) {
    // Default subscription to all market prices if none provided
    this.subscriptions = subscriptions.length > 0
      ? subscriptions
      : [{
          type: 'subscribe',
          channel: 'prices'
        }];
  }

  start(): void {
    if (this.shouldRun) return;
    this.shouldRun = true;
    heartbeatMonitor.beat(WORKER_NAME, { state: 'connecting' });
    this.connect();
  }

  stop(): void {
    this.shouldRun = false;
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.close();
      this.socket = null;
    }
    heartbeatMonitor.markIdle(WORKER_NAME, { state: 'stopped' });
  }

  private connect(): void {
    if (!this.shouldRun) return;

    const url = `${polymarketConfig.wsBaseUrl}${process.env.POLYMARKET_WS_PATH || '/prices'}`;
    logger.info(`${WORKER_NAME} connecting`, { url });

    this.socket = new WebSocket(url);

    this.socket.on('open', () => {
      logger.info(`${WORKER_NAME} connected`);
      this.reconnectDelay = 5_000;
      heartbeatMonitor.beat(WORKER_NAME, { state: 'connected' });
      this.sendSubscriptions();
    });

    this.socket.on('message', async (raw) => {
      try {
        const message = JSON.parse(raw.toString());

        if (message?.type === 'ping') {
          this.send({ type: 'pong' });
          heartbeatMonitor.beat(WORKER_NAME, { state: 'pong' });
          return;
        }

        if (message?.type !== 'channel_data' || !message?.data) {
          return;
        }

        const marketPayload = this.extractMarket(message);
        if (!marketPayload?.id) return;

        const tick = normalizeTick(this.toMarket(marketPayload));
        if (this.shouldEnqueueTick()) {
          await pushUpdate('tick', tick);
        } else {
          this.logTick(tick);
        }
        heartbeatMonitor.beat(WORKER_NAME, { state: 'tick' });
      } catch (error) {
        logger.warn(`${WORKER_NAME} failed to process tick`, { error: formatError(error) });
      }
    });

    this.socket.on('error', (error) => {
      logger.error(`${WORKER_NAME} socket error`, { error: formatError(error) });
    });

    this.socket.on('close', async () => {
      logger.warn(`${WORKER_NAME} connection closed`);
      heartbeatMonitor.markIdle(WORKER_NAME, { state: 'disconnected' });
      if (!this.shouldRun) return;
      await sleep(this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 60_000);
      heartbeatMonitor.beat(WORKER_NAME, { state: 'reconnecting' });
      this.connect();
    });
  }

  private extractMarket(message: any): Partial<PolymarketMarket> | null {
    if (!message) return null;
    const payload = message.data ?? message;
    if (payload.market) return payload.market;
    if (payload.data?.market) return payload.data.market;
    return payload;
  }

  private toMarket(payload: Partial<PolymarketMarket>): PolymarketMarket {
    return {
      id: payload.id!,
      question: payload.question || '',
      description: payload.description || null,
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
      status: payload.status || 'active',
      resolvedAt: payload.resolvedAt || null,
      outcomes: payload.outcomes || [],
      category: payload.category,
      tags: payload.tags,
      marketMakerAddress: payload.marketMakerAddress,
      imageUrl: payload.imageUrl,
      event: payload.event
    };
  }

  private shouldEnqueueTick(): boolean {
    return settings.redisEnabled && redisAvailable;
  }

  private logTick(tick: NormalizedTick): void {
    console.log(`[${WORKER_NAME}]`, 'tick (redis disabled)', {
      market: tick.market_polymarket_id,
      price: tick.price,
      bestBid: tick.best_bid,
      bestAsk: tick.best_ask,
      lastTradePrice: tick.last_trade_price,
      capturedAt: tick.captured_at
    });
  }

  private sendSubscriptions(): void {
    logger.info(`${WORKER_NAME} sending subscriptions`, {
      count: this.subscriptions.length,
      subscriptions: this.subscriptions
    });

    for (const subscription of this.subscriptions) {
      this.send(subscription);
      logger.debug(`${WORKER_NAME} sent subscription`, { subscription });
    }
  }

  private send(payload: Record<string, unknown>): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      logger.warn(`${WORKER_NAME} cannot send payload - socket not ready`);
      return;
    }

    this.socket.send(JSON.stringify(payload));
  }
}

export const marketChannelWorker = new MarketChannelWorker();
