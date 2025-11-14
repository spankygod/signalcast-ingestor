/**
 * Market Channel WebSocket Handler
 * Handles real-time market data from Polymarket WebSocket API
 * Processes price updates, trades, and market state changes
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { TicksQueue } from '../queues';
import { type TickQueueMessage, queueMessageGuards } from '../queues/types';
import { normalizeTick } from '../utils/normalizeTick';
import {
  PolymarketWebSocketHandler,
  type RealtimeTick,
  type WsSubscription,
  type ConnectionStats,
  type WsMessage
} from '../services/wss-handlers';
import { WS_CONFIG, WS_CHANNELS } from '../config/polymarket';

/**
 * Market channel configuration
 */
interface MarketChannelConfig {
  maxSubscriptions: number;
  subscriptionTimeoutMs: number;
  tickProcessingTimeoutMs: number;
  maxRetries: number;
  retryDelayMs: number;
  enableTickDeduplication: boolean;
  deduplicationWindowMs: number;
  deduplicationCacheMaxSize: number;
  deduplicationCleanupIntervalMs: number;
  enableTickAggregation: boolean;
  aggregationWindowMs: number;
  enablePersistedSubscriptions: boolean;
  healthCheckIntervalMs: number;
}

/**
 * Market subscription filters
 */
interface MarketSubscriptionFilter {
  marketIds?: string[];
  eventIds?: string[];
  categories?: string[];
  minLiquidity?: number;
  minVolume?: number;
  activeOnly?: boolean;
}

/**
 * Tick deduplication entry
 */
interface TickDeduplicationEntry {
  marketId: string;
  timestamp: number;
  price: number;
  sequenceId?: number;
  tickType: string;
}

/**
 * Market channel statistics
 */
interface MarketChannelStats {
  totalConnections: number;
  activeConnections: number;
  totalSubscriptions: number;
  activeSubscriptions: number;
  ticksReceived: number;
  ticksProcessed: number;
  ticksQueued: number;
  duplicateTicksFiltered: number;
  errorsCount: number;
  reconnections: number;
  uptime: number;
  lastTickAt?: string;
  lastSubscriptionAt?: string;
  averageProcessingTime: number;
  tickTypes: Record<string, number>;
  connectionStats: ConnectionStats;
}

/**
 * Market channel WebSocket handler class
 */
export class MarketChannelWebSocketHandler {
  private logger: Logger;
  private config: MarketChannelConfig;
  private wsHandler: PolymarketWebSocketHandler;
  private ticksQueue: TicksQueue;
  private isRunning: boolean = false;
  private startTime: number;
  private stats: MarketChannelStats;
  private deduplicationCache: Map<string, TickDeduplicationEntry> = new Map();
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private deduplicationCleanupTimer: NodeJS.Timeout | null = null;
  private lastDeduplicationCleanup: number = 0;

  constructor(
    ticksQueue: TicksQueue,
    wsHandler?: PolymarketWebSocketHandler,
    config?: Partial<MarketChannelConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('wss-market-channel', process.pid.toString());
    this.ticksQueue = ticksQueue;
    this.wsHandler = wsHandler || new PolymarketWebSocketHandler();
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      maxSubscriptions: 1000,
      subscriptionTimeoutMs: 30000,
      tickProcessingTimeoutMs: 5000,
      maxRetries: 3,
      retryDelayMs: 1000,
      enableTickDeduplication: true,
      deduplicationWindowMs: 1000, // 1 second
      deduplicationCacheMaxSize: 10000, // Maximum 10,000 entries in cache
      deduplicationCleanupIntervalMs: 30000, // Clean up every 30 seconds
      enableTickAggregation: false,
      aggregationWindowMs: 60000, // 1 minute
      enablePersistedSubscriptions: true,
      healthCheckIntervalMs: 30000,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      totalSubscriptions: 0,
      activeSubscriptions: 0,
      ticksReceived: 0,
      ticksProcessed: 0,
      ticksQueued: 0,
      duplicateTicksFiltered: 0,
      errorsCount: 0,
      reconnections: 0,
      uptime: 0,
      averageProcessingTime: 0,
      tickTypes: {},
      connectionStats: this.wsHandler.getStats(),
    };

    this.setupWebSocketEventHandlers();
    this.setupGracefulShutdown();
    this.logger.info('Market channel WebSocket handler initialized', {
      config: this.config,
    });
  }

  /**
   * Start the market channel handler
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Market channel WebSocket handler is already running');
      return;
    }

    this.logger.info('Starting market channel WebSocket handler');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Initialize WebSocket handler
      await this.wsHandler.initialize();

      // Subscribe to default channels
      await this.subscribeToDefaultChannels();

      // Start health monitoring
      this.startHealthMonitoring();

      // Start deduplication cleanup timer
      this.startDeduplicationCleanup();

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('Market channel WebSocket handler started successfully');

    } catch (error) {
      this.logger.error('Failed to start market channel WebSocket handler', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the market channel handler
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Market channel WebSocket handler is not running');
      return;
    }

    this.logger.info('Stopping market channel WebSocket handler');
    this.isRunning = false;

    try {
      // Stop health monitoring
      if (this.healthCheckTimer) {
        clearInterval(this.healthCheckTimer);
        this.healthCheckTimer = null;
      }

      // Stop deduplication cleanup timer
      if (this.deduplicationCleanupTimer) {
        clearInterval(this.deduplicationCleanupTimer);
        this.deduplicationCleanupTimer = null;
      }

      // Disconnect WebSocket
      this.wsHandler.disconnect();

      // Clean up caches
      this.deduplicationCache.clear();

      this.logger.info('Market channel WebSocket handler stopped', {
        finalStats: this.getStats(),
      });

    } catch (error) {
      this.logger.error('Error during shutdown', {
        error: error as Error,
      });
    }
  }

  /**
   * Subscribe to default channels
   */
  private async subscribeToDefaultChannels(): Promise<void> {
    try {
      // Subscribe to market data updates
      const marketDataSubscription = this.wsHandler.subscribe(WS_CHANNELS.MARKET_DATA, {
        active: true,
      });

      // Subscribe to price updates
      const priceUpdatesSubscription = this.wsHandler.subscribe(WS_CHANNELS.PRICE_UPDATES, {
        min_liquidity: 1000, // Only markets with significant liquidity
      });

      // Subscribe to trade data
      const tradesSubscription = this.wsHandler.subscribe(WS_CHANNELS.TRADES, {
        min_size: 100, // Only significant trades
      });

      // Subscribe to ticker data for overview
      const tickerSubscription = this.wsHandler.subscribe(WS_CHANNELS.TICKER, {
        categories: ['sports', 'politics', 'business', 'technology'],
      });

      this.stats.totalSubscriptions += 4;
      this.stats.activeSubscriptions = 4;
      this.stats.lastSubscriptionAt = new Date().toISOString();

      this.logger.info('Subscribed to default market channels', {
        subscriptions: [marketDataSubscription, priceUpdatesSubscription, tradesSubscription, tickerSubscription],
      });

    } catch (error) {
      this.logger.error('Failed to subscribe to default channels', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Subscribe to specific markets
   */
  async subscribeToMarkets(filter: MarketSubscriptionFilter): Promise<string[]> {
    const subscriptionIds: string[] = [];

    try {
      // Build subscription filters
      const filters: Record<string, any> = {};

      if (filter.marketIds && filter.marketIds.length > 0) {
        filters['market_ids'] = filter.marketIds;
      }

      if (filter.eventIds && filter.eventIds.length > 0) {
        filters['event_ids'] = filter.eventIds;
      }

      if (filter.categories && filter.categories.length > 0) {
        filters['categories'] = filter.categories;
      }

      if (filter.minLiquidity) {
        filters['min_liquidity'] = filter.minLiquidity;
      }

      if (filter.minVolume) {
        filters['min_volume'] = filter.minVolume;
      }

      if (filter.activeOnly !== undefined) {
        filters['active_only'] = filter.activeOnly;
      }

      // Subscribe to relevant channels
      const channels = [WS_CHANNELS.MARKET_DATA, WS_CHANNELS.PRICE_UPDATES, WS_CHANNELS.TRADES];

      for (const channel of channels) {
        try {
          const subscriptionId = this.wsHandler.subscribe(channel, filters);
          subscriptionIds.push(subscriptionId);
          this.stats.totalSubscriptions++;
        } catch (error) {
          this.logger.error('Failed to subscribe to channel', {
            channel,
            error: error as Error,
          });
        }
      }

      this.stats.activeSubscriptions = this.wsHandler.getSubscriptions().length;
      this.stats.lastSubscriptionAt = new Date().toISOString();

      this.logger.info('Subscribed to markets', {
        filter,
        subscriptionIds,
        totalSubscriptions: this.stats.totalSubscriptions,
      });

    } catch (error) {
      this.logger.error('Failed to subscribe to markets', {
        filter,
        error: error as Error,
      });
      throw error;
    }

    return subscriptionIds;
  }

  /**
   * Unsubscribe from specific markets
   */
  async unsubscribeFromMarkets(subscriptionIds: string[]): Promise<void> {
    for (const subscriptionId of subscriptionIds) {
      try {
        this.wsHandler.unsubscribe(subscriptionId);
        this.stats.totalSubscriptions--;
      } catch (error) {
        this.logger.error('Failed to unsubscribe', {
          subscriptionId,
          error: error as Error,
        });
      }
    }

    this.stats.activeSubscriptions = this.wsHandler.getSubscriptions().length;

    this.logger.info('Unsubscribed from markets', {
      subscriptionIds,
      activeSubscriptions: this.stats.activeSubscriptions,
    });
  }

  /**
   * Setup WebSocket event handlers
   */
  private setupWebSocketEventHandlers(): void {
    this.wsHandler.on('data', async (message: WsMessage) => {
      if (this.isRunning) {
        await this.handleWebSocketMessage(message);
      }
    });

    this.wsHandler.on('error', (error: Error) => {
      this.logger.error('WebSocket error', {
        error: error.message,
        stack: error.stack,
      });
      this.stats.errorsCount++;
    });

    this.wsHandler.on('disconnected', () => {
      this.logger.warn('WebSocket disconnected');
      this.stats.activeConnections = Math.max(0, this.stats.activeConnections - 1);
      this.stats.reconnections++;
    });

    this.wsHandler.on('connected', () => {
      this.logger.info('WebSocket connected');
      this.stats.activeConnections++;
      this.stats.totalConnections++;
    });

    this.wsHandler.on('subscription_ack', (message: WsMessage) => {
      this.logger.debug('Subscription acknowledged', {
        subscriptionId: message.id,
        channel: message.channel,
      });
    });
  }

  /**
   * Handle WebSocket message
   */
  private async handleWebSocketMessage(message: WsMessage): Promise<void> {
    const processingStartTime = Date.now();

    try {
      this.stats.ticksReceived++;

      if (!message.data) {
        return;
      }

      // Process different message types
      if (message.channel === WS_CHANNELS.PRICE_UPDATES || message.channel === WS_CHANNELS.MARKET_DATA) {
        await this.processPriceUpdate(message.data);
      } else if (message.channel === WS_CHANNELS.TRADES) {
        await this.processTradeData(message.data);
      } else if (message.channel === WS_CHANNELS.TICKER) {
        await this.processTickerData(message.data);
      }

      // Update processing statistics
      const processingTime = Date.now() - processingStartTime;
      this.updateProcessingStats(processingTime);

    } catch (error) {
      this.stats.errorsCount++;
      this.logger.error('Failed to handle WebSocket message', {
        error: error as Error,
        message,
      });
    }
  }

  /**
   * Process price update data
   */
  private async processPriceUpdate(data: any): Promise<void> {
    const tick = this.normalizeTickData(data, 'price_update');
    if (!tick) {
      return;
    }

    await this.processTick(tick);
  }

  /**
   * Process trade data
   */
  private async processTradeData(data: any): Promise<void> {
    const tick = this.normalizeTickData(data, 'trade');
    if (!tick) {
      return;
    }

    await this.processTick(tick);
  }

  /**
   * Process ticker data
   */
  private async processTickerData(data: any): Promise<void> {
    const tick = this.normalizeTickData(data, 'price_update');
    if (!tick) {
      return;
    }

    await this.processTick(tick);
  }

  /**
   * Normalize tick data
   */
  private normalizeTickData(data: any, tickType: string): RealtimeTick | null {
    if (!data.marketId) {
      this.logger.debug('Tick missing market ID', { data });
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
      tickType: tickType as any,
      marketState: data.marketState || 'open',
    };
  }

  /**
   * Process tick data
   */
  private async processTick(tick: RealtimeTick): Promise<void> {
    // Apply deduplication if enabled
    if (this.config.enableTickDeduplication && this.isDuplicateTick(tick)) {
      this.stats.duplicateTicksFiltered++;
      return;
    }

    // Normalize tick
    const { tick: normalizedTick, validation } = normalizeTick({
      marketId: tick.marketId,
      price: tick.price,
      bestBid: tick.bestBid,
      bestAsk: tick.bestAsk,
      lastTradePrice: tick.lastTradePrice,
      liquidity: tick.liquidity,
      volume24h: tick.volume24h,
      priceChange: tick.priceChange,
      priceChangePercent: tick.priceChangePercent,
      timestamp: new Date(tick.timestamp).toISOString(),
      sequenceId: tick.sequenceId,
      tradeId: tick.tradeId,
      tradeSize: tick.tradeSize,
      side: tick.side,
      tickType: tick.tickType,
      marketState: tick.marketState,
    });

    if (!validation.isValid) {
      this.logger.debug('Tick validation failed', {
        marketId: tick.marketId,
        errors: validation.errors,
      });
      return;
    }

    // Create queue message
    const tickMessage: TickQueueMessage = {
      id: `tick_${tick.marketId}_${tick.timestamp}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: normalizedTick.timestamp,
      marketId: normalizedTick.marketId,
      price: normalizedTick.price,
      bestBid: normalizedTick.bestBid,
      bestAsk: normalizedTick.bestAsk,
      liquidity: normalizedTick.liquidity,
      volume24h: normalizedTick.volume24h,
      priceChange: normalizedTick.priceChange,
      priceChangePercent: normalizedTick.priceChangePercent,
      sequenceId: normalizedTick.sequenceId,
      tradeId: normalizedTick.tradeId,
      tradeSize: normalizedTick.tradeSize,
      side: normalizedTick.side,
      tickType: normalizedTick.tickType,
      marketState: normalizedTick.marketState,
      source: 'polymarket-wss-market-channel',
      version: '1.0.0',
    };

    // Validate message
    if (!queueMessageGuards.isTickMessage(tickMessage)) {
      this.logger.debug('Tick message validation failed', {
        marketId: tick.marketId,
      });
      return;
    }

    // Queue the tick
    await this.ticksQueue.addMessage(tickMessage);

    this.stats.ticksProcessed++;
    this.stats.ticksQueued++;
    this.stats.lastTickAt = new Date().toISOString();

    // Update tick type statistics
    this.stats.tickTypes[tick.tickType] = (this.stats.tickTypes[tick.tickType] || 0) + 1;

    // Add to deduplication cache if enabled
    if (this.config.enableTickDeduplication) {
      this.addToDeduplicationCache(tick);
    }

    this.logger.debug('Tick processed and queued', {
      marketId: tick.marketId,
      price: tick.price,
      tickType: tick.tickType,
    });
  }

  /**
   * Check if tick is a duplicate
   */
  private isDuplicateTick(tick: RealtimeTick): boolean {
    const key = `${tick.marketId}_${tick.tickType}`;
    const now = Date.now();

    const existing = this.deduplicationCache.get(key);
    if (!existing) {
      return false;
    }

    // Check if within deduplication window
    if (now - existing.timestamp > this.config.deduplicationWindowMs) {
      return false;
    }

    // Check if same price and sequence
    return existing.price === tick.price && existing.sequenceId === tick.sequenceId;
  }

  /**
   * Add tick to deduplication cache with size management
   */
  private addToDeduplicationCache(tick: RealtimeTick): void {
    const key = `${tick.marketId}_${tick.tickType}`;
    const now = Date.now();

    this.deduplicationCache.set(key, {
      marketId: tick.marketId,
      timestamp: now,
      price: tick.price,
      sequenceId: tick.sequenceId,
      tickType: tick.tickType,
    });

    // Check cache size and evict if necessary
    if (this.deduplicationCache.size > this.config.deduplicationCacheMaxSize) {
      this.evictOldestCacheEntries();
    }

    // Clean up old entries periodically (not on every insert)
    if (now - this.lastDeduplicationCleanup > this.config.deduplicationCleanupIntervalMs) {
      this.cleanupDeduplicationCache();
    }
  }

  /**
   * Evict oldest cache entries to maintain size limit
   */
  private evictOldestCacheEntries(): void {
    const entriesToEvict = Math.floor(this.config.deduplicationCacheMaxSize * 0.2); // Remove 20%
    const sortedEntries = Array.from(this.deduplicationCache.entries())
      .sort(([, a], [, b]) => a.timestamp - b.timestamp);

    for (let i = 0; i < entriesToEvict && i < sortedEntries.length; i++) {
      this.deduplicationCache.delete(sortedEntries[i][0]);
    }

    this.logger.debug(`Evicted ${entriesToEvict} entries from deduplication cache`, {
      cacheSize: this.deduplicationCache.size,
      maxSize: this.config.deduplicationCacheMaxSize,
    });
  }

  /**
   * Clean up old deduplication entries efficiently
   */
  private cleanupDeduplicationCache(): void {
    const now = Date.now();
    const cutoff = now - this.config.deduplicationWindowMs;
    const initialSize = this.deduplicationCache.size;
    let evictedCount = 0;

    // Batch delete expired entries
    const keysToDelete: string[] = [];
    for (const [key, entry] of this.deduplicationCache) {
      if (entry.timestamp < cutoff) {
        keysToDelete.push(key);
        if (keysToDelete.length >= 1000) { // Process in batches of 1000
          break;
        }
      }
    }

    // Delete the batch
    for (const key of keysToDelete) {
      this.deduplicationCache.delete(key);
      evictedCount++;
    }

    this.lastDeduplicationCleanup = now;

    if (evictedCount > 0) {
      this.logger.debug(`Cleaned up ${evictedCount} expired entries from deduplication cache`, {
        initialSize,
        finalSize: this.deduplicationCache.size,
        cutoff: new Date(cutoff).toISOString(),
      });
    }
  }

  /**
   * Start deduplication cleanup timer
   */
  private startDeduplicationCleanup(): void {
    if (this.config.enableTickDeduplication) {
      this.deduplicationCleanupTimer = setInterval(() => {
        if (this.isRunning) {
          this.cleanupDeduplicationCache();
        }
      }, this.config.deduplicationCleanupIntervalMs);
    }
  }

  /**
   * Update processing statistics
   */
  private updateProcessingStats(processingTime: number): void {
    const totalTime = this.stats.averageProcessingTime * this.stats.ticksProcessed + processingTime;
    this.stats.averageProcessingTime = totalTime / (this.stats.ticksProcessed + 1);
  }

  /**
   * Start health monitoring
   */
  private startHealthMonitoring(): void {
    this.healthCheckTimer = setInterval(async () => {
      if (this.isRunning) {
        await this.performHealthCheck();
      }
    }, this.config.healthCheckIntervalMs);
  }

  /**
   * Perform health check
   */
  private async performHealthCheck(): Promise<void> {
    try {
      const connectionStats = this.wsHandler.getStats();
      this.stats.connectionStats = connectionStats;

      const isHealthy = this.wsHandler.isHealthy();

      if (!isHealthy) {
        this.logger.warn('WebSocket connection unhealthy', {
          connectionStats,
        });

        // Attempt to reconnect if needed
        if (connectionStats.state === 'disconnected' || connectionStats.state === 'error') {
          this.logger.info('Attempting to reconnect WebSocket');
          await this.wsHandler.connect();
        }
      }

      this.logger.debug('Health check completed', {
        connectionStats,
        isHealthy,
        deduplicationCacheSize: this.deduplicationCache.size,
      });

    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
      this.stats.errorsCount++;
    }
  }

  /**
   * Get current statistics
   */
  getStats(): MarketChannelStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
      connectionStats: this.wsHandler.getStats(),
    };
  }

  /**
   * Check if handler is healthy
   */
  isHealthy(): boolean {
    const wsHealthy = this.wsHandler.isHealthy();
    const recentlyProcessed = this.stats.lastTickAt
      ? Date.now() - new Date(this.stats.lastTickAt).getTime() < 60000
      : false;

    return this.isRunning && wsHealthy && (recentlyProcessed || this.stats.ticksReceived === 0);
  }

  /**
   * Get active subscriptions
   */
  getActiveSubscriptions(): WsSubscription[] {
    return this.wsHandler.getSubscriptions();
  }

  /**
   * Get WebSocket connection statistics
   */
  getConnectionStats(): ConnectionStats {
    return this.wsHandler.getStats();
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down market channel handler`);
      try {
        await this.stop();
      } catch (error) {
        this.logger.error('Error during shutdown', {
          error: error as Error,
        });
      }
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }
}

/**
 * Create and initialize market channel WebSocket handler
 */
export async function createMarketChannelHandler(
  ticksQueue: TicksQueue,
  wsHandler?: PolymarketWebSocketHandler,
  config?: Partial<MarketChannelConfig>
): Promise<MarketChannelWebSocketHandler> {
  const handler = new MarketChannelWebSocketHandler(ticksQueue, wsHandler, config);

  // Send ready signal if running under PM2
  if (process.send) {
    process.send('ready');
  }

  return handler;
}

/**
 * Standalone worker entry point
 */
export async function runMarketChannelHandler(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('market-channel-main', {
      category: LogCategory.WEBSOCKET,
    });

    logger.info('Initializing market channel WebSocket handler standalone');

    // Initialize dependencies
    const ticksQueue = new TicksQueue();
    const wsHandler = new PolymarketWebSocketHandler();

    // Create and start handler
    const handler = await createMarketChannelHandler(ticksQueue, wsHandler);
    await handler.start();

    logger.info('Market channel WebSocket handler running standalone');

    // Keep process alive
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught exception', { error });
      process.exit(1);
    });

    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled rejection', { reason, promise });
      process.exit(1);
    });

  } catch (error) {
    console.error('Failed to start market channel handler:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runMarketChannelHandler().catch((error) => {
    console.error('Market channel handler failed:', error);
    process.exit(1);
  });
}

export default MarketChannelWebSocketHandler;