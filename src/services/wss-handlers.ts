/**
 * Polymarket WebSocket Handlers
 * Manages WebSocket connections for real-time data streaming
 */

import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { Logger, LoggerFactory } from '../lib/logger';
import { retryUtils } from '../lib/retry';
import { TicksQueue } from '../queues';
import {
  WS_CONFIG,
  WS_MESSAGE_TYPES,
  WS_CHANNELS,
  WS_FILTERS,
  ERROR_HANDLING,
  DATA_MAPPING,
} from '../config/polymarket';
import { TickQueueMessage } from '../queues/types';

/**
 * WebSocket message interface
 */
export interface WsMessage {
  type: string;
  channel?: string;
  data?: any;
  timestamp?: number;
  id?: string;
  error?: string;
}

/**
 * WebSocket subscription interface
 */
export interface WsSubscription {
  id: string;
  channel: string;
  filters?: Record<string, any>;
  active: boolean;
  createdAt: number;
  lastMessageAt?: number;
  messageCount: number;
}

/**
 * Connection state
 */
export enum ConnectionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  RECONNECTING = 'reconnecting',
  ERROR = 'error',
  CLOSED = 'closed',
}

/**
 * WebSocket connection statistics
 */
export interface ConnectionStats {
  state: ConnectionState;
  connectedAt?: number;
  lastMessageAt?: number;
  reconnectAttempts: number;
  messagesReceived: number;
  messagesSent: number;
  bytesReceived: number;
  bytesSent: number;
  subscriptions: number;
  errors: number;
  lastError?: string;
  uptime?: number;
}

/**
 * Real-time tick data
 */
export interface RealtimeTick {
  marketId: string;
  price: number;
  bestBid?: number;
  bestAsk?: number;
  lastTradePrice?: number;
  liquidity: number;
  volume24h?: number;
  priceChange?: number;
  priceChangePercent?: number;
  timestamp: number;
  sequenceId?: number;
  tradeId?: string;
  tradeSize?: number;
  side?: 'buy' | 'sell';
  tickType: 'price_update' | 'trade' | 'liquidity_change' | 'market_state';
  marketState: 'open' | 'closed' | 'suspended' | 'resolved';
}

/**
 * WebSocket connection handler class
 */
export class PolymarketWebSocketHandler extends EventEmitter {
  private logger: Logger;
  private ws: WebSocket | null = null;
  private url: string;
  private connectionState: ConnectionState;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private pingTimer: NodeJS.Timeout | null = null;
  private pongTimer: NodeJS.Timeout | null = null;
  private subscriptions: Map<string, WsSubscription>;
  private messageQueue: WsMessage[];
  private tickQueue: TicksQueue;
  private stats: ConnectionStats;
  private backoffMs: number;
  private maxReconnectAttempts: number;
  private reconnectAttempts: number;

  constructor(private readonly workerId: string = process.pid.toString()) {
    super();
    this.logger = LoggerFactory.getLogger(`wss-handler-${this.workerId}`, {
      category: 'WEBSOCKET',
      workerId: this.workerId,
    });

    this.url = WS_CONFIG.URL;
    this.connectionState = ConnectionState.DISCONNECTED;
    this.subscriptions = new Map();
    this.messageQueue = [];
    this.tickQueue = new TicksQueue();
    this.backoffMs = WS_CONFIG.RECONNECT_INTERVAL_MS;
    this.maxReconnectAttempts = WS_CONFIG.MAX_RECONNECT_ATTEMPTS;
    this.reconnectAttempts = 0;

    this.stats = {
      state: ConnectionState.DISCONNECTED,
      reconnectAttempts: 0,
      messagesReceived: 0,
      messagesSent: 0,
      bytesReceived: 0,
      bytesSent: 0,
      subscriptions: 0,
      errors: 0,
    };

    this.setupEventHandlers();

    this.logger.info('WebSocket handler initialized', {
      url: this.url,
      workerId: this.workerId,
    });
  }

  /**
   * Initialize WebSocket connection
   */
  async initialize(): Promise<void> {
    this.logger.info('Initializing WebSocket connection');
    await this.connect();
  }

  /**
   * Connect to WebSocket server
   */
  async connect(): Promise<void> {
    if (this.connectionState === ConnectionState.CONNECTED ||
        this.connectionState === ConnectionState.CONNECTING) {
      return;
    }

    this.setState(ConnectionState.CONNECTING);

    return await this.logger.timed('websocket-connect', this.logger.getLevel(), async () => {
      return await retryUtils.withExponentialBackoff(
        async () => {
          return new Promise<void>((resolve, reject) => {
            try {
              this.logger.debug('Connecting to WebSocket', { url: this.url });

              this.ws = new WebSocket(this.url, {
                headers: {
                  'User-Agent': 'signalcast-ingestor/1.0.0',
                  'Origin': 'https://signalcast.io',
                },
                perMessageDeflate: false, // Disable compression for better performance
              });

              const connectTimeout = setTimeout(() => {
                this.ws?.terminate();
                reject(new Error('WebSocket connection timeout'));
              }, 15000);

              this.ws.on('open', () => {
                clearTimeout(connectTimeout);
                this.logger.info('WebSocket connection established');
                this.setState(ConnectionState.CONNECTED);
                this.reconnectAttempts = 0;
                this.backoffMs = WS_CONFIG.RECONNECT_INTERVAL_MS;
                this.stats.connectedAt = Date.now();

                // Start ping/pong
                this.startPingInterval();

                // Resubscribe to previous subscriptions
                this.resubscribeAll();

                // Process queued messages
                this.processMessageQueue();

                this.emit('connected');
                resolve();
              });

              this.ws.on('error', (error) => {
                clearTimeout(connectTimeout);
                this.logger.error('WebSocket connection error', { error });
                this.setState(ConnectionState.ERROR);
                this.stats.lastError = error.message;
                this.stats.errors++;
                this.emit('error', error);
                reject(error);
              });

              this.ws.on('message', async (data) => {
                try {
                  await this.handleMessage(data);
                } catch (error) {
                  this.logger.error('Error handling WebSocket message', {
                    error: error as Error,
                  });
                }
              });

              this.ws.on('close', (code, reason) => {
                clearTimeout(connectTimeout);
                this.logger.warn('WebSocket connection closed', { code, reason: reason.toString() });
                this.setState(ConnectionState.DISCONNECTED);
                this.emit('disconnected', { code, reason });

                if (code !== 1000) { // Not a normal closure
                  this.scheduleReconnect();
                }
              });

            } catch (error) {
              reject(error);
            }
          });
        },
        3, // maxAttempts
        1000, // initialDelay
        8000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Disconnect from WebSocket server
   */
  disconnect(): void {
    this.logger.info('Disconnecting WebSocket');

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }

    if (this.pongTimer) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }

    if (this.ws) {
      this.ws.close(1000, 'Normal shutdown');
      this.ws = null;
    }

    this.setState(ConnectionState.CLOSED);
    this.emit('disconnected');
  }

  /**
   * Subscribe to a channel
   */
  subscribe(channel: string, filters?: Record<string, any>): string {
    const subscriptionId = `sub_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const subscription: WsSubscription = {
      id: subscriptionId,
      channel,
      filters,
      active: true,
      createdAt: Date.now(),
      messageCount: 0,
    };

    this.subscriptions.set(subscriptionId, subscription);
    this.stats.subscriptions = this.subscriptions.size;

    const message: WsMessage = {
      type: WS_MESSAGE_TYPES.SUBSCRIBE,
      channel,
      data: filters,
      id: subscriptionId,
      timestamp: Date.now(),
    };

    this.sendMessage(message);

    this.logger.info('Subscribed to channel', {
      subscriptionId,
      channel,
      filters,
    });

    return subscriptionId;
  }

  /**
   * Unsubscribe from a channel
   */
  unsubscribe(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }

    subscription.active = false;

    const message: WsMessage = {
      type: WS_MESSAGE_TYPES.UNSUBSCRIBE,
      channel: subscription.channel,
      id: subscriptionId,
      timestamp: Date.now(),
    };

    this.sendMessage(message);
    this.subscriptions.delete(subscriptionId);
    this.stats.subscriptions = this.subscriptions.size;

    this.logger.info('Unsubscribed from channel', {
      subscriptionId,
      channel: subscription.channel,
    });
  }

  /**
   * Send message to WebSocket
   */
  private sendMessage(message: WsMessage): void {
    if (!this.ws || this.connectionState !== ConnectionState.CONNECTED) {
      // Queue message for later
      this.messageQueue.push(message);
      return;
    }

    try {
      const messageStr = JSON.stringify(message);
      this.ws.send(messageStr);
      this.stats.messagesSent++;
      this.stats.bytesSent += Buffer.byteLength(messageStr);

      this.logger.debug('Message sent', {
        type: message.type,
        channel: message.channel,
        id: message.id,
      });
    } catch (error) {
      this.logger.error('Failed to send message', {
        error: error as Error,
        message,
      });
    }
  }

  /**
   * Setup WebSocket event handlers
   */
  private setupEventHandlers(): void {
    this.on('connected', () => {
      this.logger.info('WebSocket connected event');
    });

    this.on('disconnected', (data?: { code: number; reason: string }) => {
      this.logger.warn('WebSocket disconnected event', data);
    });

    this.on('error', (error: Error) => {
      this.logger.error('WebSocket error event', { error });
    });
  }

  /**
   * Handle incoming WebSocket message
   */
  private async handleMessage(data: WebSocket.Data): Promise<void> {
    try {
      const messageStr = data.toString();
      const message: WsMessage = JSON.parse(messageStr);

      this.stats.messagesReceived++;
      this.stats.bytesReceived += data.byteLength;
      this.stats.lastMessageAt = Date.now();

      this.logger.debug('Message received', {
        type: message.type,
        channel: message.channel,
        id: message.id,
      });

      // Update subscription stats
      if (message.channel) {
        for (const subscription of this.subscriptions.values()) {
          if (subscription.channel === message.channel && subscription.active) {
            subscription.lastMessageAt = Date.now();
            subscription.messageCount++;
          }
        }
      }

      // Handle different message types
      switch (message.type) {
        case WS_MESSAGE_TYPES.DATA:
          await this.handleDataMessage(message);
          break;

        case WS_MESSAGE_TYPES.SUBSCRIPTION_ACK:
          this.handleSubscriptionAck(message);
          break;

        case WS_MESSAGE_TYPES.UNSUBSCRIPTION_ACK:
          this.handleUnsubscriptionAck(message);
          break;

        case WS_MESSAGE_TYPES.PONG:
          this.handlePong(message);
          break;

        case WS_MESSAGE_TYPES.ERROR:
          this.handleErrorMessage(message);
          break;

        case WS_MESSAGE_TYPES.AUTH_SUCCESS:
          this.logger.info('WebSocket authentication successful');
          break;

        case WS_MESSAGE_TYPES.AUTH_FAILURE:
          this.logger.error('WebSocket authentication failed', { error: message.error });
          break;

        default:
          this.logger.debug('Unknown message type', { type: message.type });
      }

      this.emit('message', message);
    } catch (error) {
      this.logger.error('Failed to handle message', {
        error: error as Error,
        data: data.toString(),
      });
    }
  }

  /**
   * Handle data messages containing real-time updates
   */
  private async handleDataMessage(message: WsMessage): Promise<void> {
    if (!message.data) {
      return;
    }

    try {
      // Process different data types
      if (message.channel === WS_CHANNELS.PRICE_UPDATES ||
          message.channel === WS_CHANNELS.MARKET_DATA) {
        await this.processPriceUpdate(message.data);
      } else if (message.channel === WS_CHANNELS.TRADES) {
        await this.processTradeData(message.data);
      } else if (message.channel === WS_CHANNELS.TICKER) {
        await this.processTickerData(message.data);
      }

      this.emit('data', message);
    } catch (error) {
      this.logger.error('Failed to process data message', {
        error: error as Error,
        message,
      });
    }
  }

  /**
   * Process price update data
   */
  private async processPriceUpdate(data: any): Promise<void> {
    const tick = this.normalizeTickData(data);
    if (tick) {
      await this.queueTick(tick);
    }
  }

  /**
   * Process trade data
   */
  private async processTradeData(data: any): Promise<void> {
    const tick = this.normalizeTickData({
      ...data,
      tickType: 'trade',
    });
    if (tick) {
      await this.queueTick(tick);
    }
  }

  /**
   * Process ticker data
   */
  private async processTickerData(data: any): Promise<void> {
    const tick = this.normalizeTickData({
      ...data,
      tickType: 'price_update',
    });
    if (tick) {
      await this.queueTick(tick);
    }
  }

  /**
   * Normalize tick data to our format
   */
  private normalizeTickData(data: any): RealtimeTick | null {
    if (!data.marketId) {
      return null;
    }

    return {
      marketId: data.marketId,
      price: Number(data.price) || 0,
      bestBid: data.bestBid ? Number(data.bestBid) : undefined,
      bestAsk: data.bestAsk ? Number(data.bestAsk) : undefined,
      lastTradePrice: data.lastTradePrice ? Number(data.lastTradePrice) : undefined,
      liquidity: Number(data.liquidity) || 0,
      volume24h: data.volume24h ? Number(data.volume24h) : undefined,
      priceChange: data.priceChange ? Number(data.priceChange) : undefined,
      priceChangePercent: data.priceChangePercent ? Number(data.priceChangePercent) : undefined,
      timestamp: data.timestamp || Date.now(),
      sequenceId: data.sequenceId,
      tradeId: data.tradeId,
      tradeSize: data.tradeSize ? Number(data.tradeSize) : undefined,
      side: data.side,
      tickType: data.tickType || 'price_update',
      marketState: data.marketState || 'open',
    };
  }

  /**
   * Queue tick data for processing
   */
  private async queueTick(tick: RealtimeTick): Promise<void> {
    try {
      const tickMessage: TickQueueMessage = {
        id: `tick_${tick.marketId}_${tick.timestamp}_${Math.random().toString(36).substr(2, 9)}`,
        timestamp: new Date(tick.timestamp).toISOString(),
        marketId: tick.marketId,
        price: tick.price,
        bestBid: tick.bestBid,
        bestAsk: tick.bestAsk,
        liquidity: tick.liquidity,
        volume24h: tick.volume24h,
        priceChange: tick.priceChange,
        priceChangePercent: tick.priceChangePercent,
        sequenceId: tick.sequenceId,
        tradeId: tick.tradeId,
        tradeSize: tick.tradeSize,
        side: tick.side,
        tickType: tick.tickType,
        marketState: tick.marketState,
        source: 'polymarket-wss',
        version: '1.0.0',
      };

      await this.tickQueue.addMessage(tickMessage);

      this.logger.debug('Tick queued', {
        marketId: tick.marketId,
        price: tick.price,
        tickType: tick.tickType,
      });
    } catch (error) {
      this.logger.error('Failed to queue tick', {
        error: error as Error,
        tick,
      });
    }
  }

  /**
   * Handle subscription acknowledgment
   */
  private handleSubscriptionAck(message: WsMessage): void {
    this.logger.info('Subscription acknowledged', { id: message.id });
    this.emit('subscription_ack', message);
  }

  /**
   * Handle unsubscription acknowledgment
   */
  private handleUnsubscriptionAck(message: WsMessage): void {
    this.logger.info('Unsubscription acknowledged', { id: message.id });
    this.emit('unsubscription_ack', message);
  }

  /**
   * Handle pong message
   */
  private handlePong(message: WsMessage): void {
    if (this.pongTimer) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }
    this.logger.debug('Pong received');
    this.emit('pong', message);
  }

  /**
   * Handle error message
   */
  private handleErrorMessage(message: WsMessage): void {
    this.logger.error('WebSocket error message', { error: message.error });
    this.stats.lastError = message.error;
    this.stats.errors++;
    this.emit('server_error', message);
  }

  /**
   * Start ping interval to keep connection alive
   */
  private startPingInterval(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
    }

    this.pingTimer = setInterval(() => {
      if (this.connectionState === ConnectionState.CONNECTED) {
        this.sendMessage({
          type: WS_MESSAGE_TYPES.PING,
          timestamp: Date.now(),
        });

        // Set pong timeout
        this.pongTimer = setTimeout(() => {
          this.logger.error('Pong timeout, closing connection');
          this.ws?.terminate();
        }, WS_CONFIG.PONG_TIMEOUT_MS);
      }
    }, WS_CONFIG.PING_INTERVAL_MS);
  }

  /**
   * Schedule reconnection attempt
   */
  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.logger.error('Max reconnection attempts reached, giving up');
      this.setState(ConnectionState.ERROR);
      this.emit('max_reconnect_attempts_reached');
      return;
    }

    this.reconnectAttempts++;
    this.setState(ConnectionState.RECONNECTING);

    const delay = Math.min(
      this.backoffMs * Math.pow(2, this.reconnectAttempts - 1),
      30000 // Max 30 seconds
    );

    this.logger.info(`Scheduling reconnection attempt ${this.reconnectAttempts}`, {
      delay,
      attempts: this.reconnectAttempts,
      maxAttempts: this.maxReconnectAttempts,
    });

    this.reconnectTimer = setTimeout(async () => {
      try {
        await this.connect();
      } catch (error) {
        this.logger.error('Reconnection failed', {
          error: error as Error,
          attempt: this.reconnectAttempts,
        });
      }
    }, delay);
  }

  /**
   * Resubscribe to all active subscriptions
   */
  private resubscribeAll(): void {
    this.logger.info('Resubscribing to all channels', {
      count: this.subscriptions.size,
    });

    for (const subscription of this.subscriptions.values()) {
      if (subscription.active) {
        const message: WsMessage = {
          type: WS_MESSAGE_TYPES.SUBSCRIBE,
          channel: subscription.channel,
          data: subscription.filters,
          id: subscription.id,
          timestamp: Date.now(),
        };

        this.sendMessage(message);
      }
    }
  }

  /**
   * Process queued messages
   */
  private processMessageQueue(): void {
    if (this.messageQueue.length === 0) {
      return;
    }

    this.logger.info('Processing message queue', {
      count: this.messageQueue.length,
    });

    const messages = [...this.messageQueue];
    this.messageQueue = [];

    for (const message of messages) {
      this.sendMessage(message);
    }
  }

  /**
   * Set connection state
   */
  private setState(state: ConnectionState): void {
    if (this.connectionState !== state) {
      const oldState = this.connectionState;
      this.connectionState = state;
      this.stats.state = state;

      this.logger.info('Connection state changed', {
        from: oldState,
        to: state,
      });

      this.emit('state_change', { from: oldState, to: state });
    }
  }

  /**
   * Get current connection statistics
   */
  getStats(): ConnectionStats {
    return {
      ...this.stats,
      uptime: this.stats.connectedAt ? Date.now() - this.stats.connectedAt : 0,
    } as ConnectionStats;
  }

  /**
   * Get active subscriptions
   */
  getSubscriptions(): WsSubscription[] {
    return Array.from(this.subscriptions.values());
  }

  /**
   * Check if connection is healthy
   */
  isHealthy(): boolean {
    return this.connectionState === ConnectionState.CONNECTED &&
           (!this.stats.lastMessageAt || (Date.now() - this.stats.lastMessageAt) < 60000); // Last message within 1 minute
  }

  /**
   * Cleanup resources
   */
  cleanup(): void {
    this.disconnect();
    this.removeAllListeners();
    this.subscriptions.clear();
    this.messageQueue = [];
    this.logger.info('WebSocket handler cleaned up');
  }
}

/**
 * WebSocket manager for handling multiple connections
 */
export class PolymarketWebSocketManager {
  private handlers: Map<string, PolymarketWebSocketHandler>;
  private logger: Logger;

  constructor() {
    this.handlers = new Map();
    this.logger = LoggerFactory.getLogger('wss-manager', {
      category: 'WEBSOCKET',
    });
  }

  /**
   * Create new WebSocket handler
   */
  createHandler(workerId: string): PolymarketWebSocketHandler {
    if (this.handlers.has(workerId)) {
      throw new Error(`WebSocket handler already exists for worker: ${workerId}`);
    }

    const handler = new PolymarketWebSocketHandler(workerId);
    this.handlers.set(workerId, handler);

    this.logger.info('WebSocket handler created', { workerId });

    return handler;
  }

  /**
   * Get handler by worker ID
   */
  getHandler(workerId: string): PolymarketWebSocketHandler | undefined {
    return this.handlers.get(workerId);
  }

  /**
   * Remove handler
   */
  removeHandler(workerId: string): void {
    const handler = this.handlers.get(workerId);
    if (handler) {
      handler.cleanup();
      this.handlers.delete(workerId);
      this.logger.info('WebSocket handler removed', { workerId });
    }
  }

  /**
   * Get all handlers
   */
  getAllHandlers(): PolymarketWebSocketHandler[] {
    return Array.from(this.handlers.values());
  }

  /**
   * Get aggregated statistics
   */
  getAggregatedStats(): {
    totalHandlers: number;
    connectedHandlers: number;
    totalSubscriptions: number;
    totalMessagesReceived: number;
    totalMessagesSent: number;
  } {
    const handlers = this.getAllHandlers();

    return {
      totalHandlers: handlers.length,
      connectedHandlers: handlers.filter(h => h.isHealthy()).length,
      totalSubscriptions: handlers.reduce((sum, h) => sum + h.getSubscriptions().length, 0),
      totalMessagesReceived: handlers.reduce((sum, h) => sum + h.getStats().messagesReceived, 0),
      totalMessagesSent: handlers.reduce((sum, h) => sum + h.getStats().messagesSent, 0),
    };
  }

  /**
   * Cleanup all handlers
   */
  cleanup(): void {
    for (const handler of this.handlers.values()) {
      handler.cleanup();
    }
    this.handlers.clear();
    this.logger.info('WebSocket manager cleaned up');
  }
}

/**
 * Create and initialize WebSocket handler
 */
export async function createWebSocketHandler(workerId?: string): Promise<PolymarketWebSocketHandler> {
  const handler = new PolymarketWebSocketHandler(workerId);
  await handler.initialize();
  return handler;
}

/**
 * Export types and utilities
 */
export type {
  WsMessage,
  WsSubscription,
  RealtimeTick,
  ConnectionStats,
};

export default PolymarketWebSocketHandler;