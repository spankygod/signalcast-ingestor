/**
 * Ticks queue processor for SignalCast system
 * Handles high-frequency real-time tick data processing with performance optimizations
 */

import { BaseQueue } from './base-queue';
import {
  TickQueueMessage,
  QueueProcessor,
  QueueProcessingContext,
  QueueProcessingResult,
  QueueProcessingStatus,
  BatchProcessingConfig,
  queueMessageGuards
} from './types';
import { Logger, LogCategory } from '../lib/logger';
import { database } from '../lib/db';
import { redisUtils } from '../lib/redis';
import { DEFAULT_QUEUE_CONFIGS } from './types';

/**
 * Tick aggregation window for batching similar ticks
 */
interface TickAggregationWindow {
  marketId: string;
  ticks: TickQueueMessage[];
  windowStart: number;
  lastUpdate: number;
  price: number;
  liquidity: number;
  volume24h: number;
}

/**
 * Ticks queue processor implementation with high-performance optimizations
 */
export class TicksQueueProcessor implements QueueProcessor<TickQueueMessage> {
  private logger: Logger;
  private db: any;
  private aggregationWindows: Map<string, TickAggregationWindow> = new Map();
  private readonly AGGREGATION_WINDOW_MS = 1000; // 1 second aggregation window
  private readonly MAX_TICKS_PER_WINDOW = 100;

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'ticks-processor' });
    this.db = database;

    // Start cleanup interval for aggregation windows
    setInterval(() => this.cleanupAggregationWindows(), 5000);
  }

  /**
   * Process tick message with high-performance optimizations
   */
  async process(
    message: TickQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.debug(`Processing tick message`, {
        marketId: message.marketId,
        price: message.price,
        tickType: message.tickType,
        sequenceId: message.sequenceId,
        correlationId: message.correlationId,
      });

      // For high-frequency data, use aggregation for price updates
      if (message.tickType === 'price_update') {
        return await this.processAggregatedTick(message, context, startTime);
      } else {
        return await this.processDirectTick(message, context, startTime);
      }
    } catch (error) {
      this.logger.error(`Failed to process tick message`, {
        error: error as Error,
        marketId: message.marketId,
        tickType: message.tickType,
        correlationId: message.correlationId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'TICK_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            marketId: message.marketId,
            tickType: message.tickType,
          },
        }],
        metadata: {
          processingTime: Date.now() - startTime,
        },
        shouldRetry: this.shouldRetry(error as Error),
      };
    }
  }

  /**
   * Process tick with aggregation for performance
   */
  private async processAggregatedTick(
    message: TickQueueMessage,
    context: QueueProcessingContext,
    startTime: number
  ): Promise<QueueProcessingResult> {
    const window = this.getOrCreateAggregationWindow(message);

    // Add tick to aggregation window
    window.ticks.push(message);
    window.lastUpdate = Date.now();
    window.price = message.price;
    window.liquidity = message.liquidity;
    window.volume24h = message.volume24h || window.volume24h;

    // Check if window should be flushed
    if (this.shouldFlushWindow(window)) {
      await this.flushAggregationWindow(window);
    }

    return {
      success: true,
      status: QueueProcessingStatus.COMPLETED,
      processedItems: 1,
      metadata: {
        action: 'aggregated',
        marketId: message.marketId,
        aggregatedTicks: window.ticks.length,
        processingTime: Date.now() - startTime,
        tickType: message.tickType,
      },
    };
  }

  /**
   * Process tick directly without aggregation
   */
  private async processDirectTick(
    message: TickQueueMessage,
    context: QueueProcessingContext,
    startTime: number
  ): Promise<QueueProcessingResult> {
    // Verify market exists (with cache lookup for performance)
    const marketExists = await this.verifyMarketExistsCached(message.marketId);
    if (!marketExists) {
      throw new Error(`Market ${message.marketId} does not exist for tick`);
    }

    // Store tick in database
    await this.storeTick(message);

    // Update real-time price data
    await this.updateRealTimePrice(message);

    // Update market cache
    await this.updateMarketCache(message);

    this.logger.debug(`Processed direct tick`, {
      marketId: message.marketId,
      tickType: message.tickType,
      price: message.price,
    });

    return {
      success: true,
      status: QueueProcessingStatus.COMPLETED,
      processedItems: 1,
      metadata: {
        action: 'direct',
        marketId: message.marketId,
        tickType: message.tickType,
        processingTime: Date.now() - startTime,
      },
    };
  }

  /**
   * Validate tick message
   */
  async validate(message: TickQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.marketId || typeof message.price !== 'number' || !message.tickType || !message.marketState) {
        this.logger.warn(`Tick message missing required fields`, {
          marketId: message.marketId,
          price: message.price,
          tickType: message.tickType,
          marketState: message.marketState,
        });
        return false;
      }

      // Validate market ID format
      if (!/^0x[a-fA-F0-9]{64}$/.test(message.marketId)) {
        this.logger.warn(`Invalid market ID format`, {
          marketId: message.marketId,
        });
        return false;
      }

      // Validate price range
      if (message.price < 0 || message.price > 1) {
        this.logger.warn(`Invalid price range`, {
          price: message.price,
        });
        return false;
      }

      // Validate tick type
      const validTickTypes = ['price_update', 'trade', 'liquidity_change', 'market_state'];
      if (!validTickTypes.includes(message.tickType)) {
        this.logger.warn(`Invalid tick type`, {
          tickType: message.tickType,
        });
        return false;
      }

      // Validate market state
      const validMarketStates = ['open', 'closed', 'suspended', 'resolved'];
      if (!validMarketStates.includes(message.marketState)) {
        this.logger.warn(`Invalid market state`, {
          marketState: message.marketState,
        });
        return false;
      }

      // Validate optional numeric fields
      if (message.bestBid !== undefined && (message.bestBid < 0 || message.bestBid > 1)) {
        this.logger.warn(`Invalid best bid price`, {
          bestBid: message.bestBid,
        });
        return false;
      }

      if (message.bestAsk !== undefined && (message.bestAsk < 0 || message.bestAsk > 1)) {
        this.logger.warn(`Invalid best ask price`, {
          bestAsk: message.bestAsk,
        });
        return false;
      }

      if (message.liquidity < 0) {
        this.logger.warn(`Invalid liquidity value`, {
          liquidity: message.liquidity,
        });
        return false;
      }

      if (message.volume24h !== undefined && message.volume24h < 0) {
        this.logger.warn(`Invalid volume 24h value`, {
          volume24h: message.volume24h,
        });
        return false;
      }

      // Validate trade-specific fields
      if (message.tickType === 'trade') {
        if (!message.tradeId) {
          this.logger.warn(`Trade tick missing trade ID`);
          return false;
        }
        if (message.tradeSize !== undefined && message.tradeSize <= 0) {
          this.logger.warn(`Invalid trade size`, {
            tradeSize: message.tradeSize,
          });
          return false;
        }
        if (message.side && !['buy', 'sell'].includes(message.side)) {
          this.logger.warn(`Invalid trade side`, {
            side: message.side,
          });
          return false;
        }
      }

      // Validate timestamp
      const timestamp = new Date(message.timestamp);
      if (isNaN(timestamp.getTime())) {
        this.logger.warn(`Invalid timestamp`, {
          timestamp: message.timestamp,
        });
        return false;
      }

      // Check for stale data (older than 5 minutes)
      const now = new Date();
      const fiveMinutesAgo = new Date(now.getTime() - 5 * 60 * 1000);
      if (timestamp < fiveMinutesAgo) {
        this.logger.warn(`Stale tick data detected`, {
          timestamp: message.timestamp,
          ageMinutes: (now.getTime() - timestamp.getTime()) / (1000 * 60),
        });
        return false;
      }

      // Check for future data (more than 1 minute in future)
      const oneMinuteFromNow = new Date(now.getTime() + 60 * 1000);
      if (timestamp > oneMinuteFromNow) {
        this.logger.warn(`Future tick data detected`, {
          timestamp: message.timestamp,
          futureMinutes: (timestamp.getTime() - now.getTime()) / (1000 * 60),
        });
        return false;
      }

      return true;
    } catch (error) {
      this.logger.error(`Error during tick message validation`, {
        error: error as Error,
        marketId: message.marketId,
      });
      return false;
    }
  }

  /**
   * Transform tick message if needed
   */
  async transform(message: TickQueueMessage): Promise<TickQueueMessage> {
    try {
      // Normalize timestamps
      const transformed = { ...message };

      if (!transformed.timestamp.includes('Z')) {
        transformed.timestamp = new Date(transformed.timestamp).toISOString();
      }

      // Normalize tick type and market state
      transformed.tickType = transformed.tickType.toLowerCase() as any;
      transformed.marketState = transformed.marketState.toLowerCase() as any;

      // Ensure numeric fields are properly formatted with high precision
      transformed.price = parseFloat(transformed.price.toFixed(8));
      transformed.liquidity = parseFloat(transformed.liquidity.toFixed(8));

      if (transformed.bestBid !== undefined) {
        transformed.bestBid = parseFloat(transformed.bestBid.toFixed(8));
      }
      if (transformed.bestAsk !== undefined) {
        transformed.bestAsk = parseFloat(transformed.bestAsk.toFixed(8));
      }
      if (transformed.volume24h !== undefined) {
        transformed.volume24h = parseFloat(transformed.volume24h.toFixed(8));
      }
      if (transformed.priceChange !== undefined) {
        transformed.priceChange = parseFloat(transformed.priceChange.toFixed(8));
      }
      if (transformed.priceChangePercent !== undefined) {
        transformed.priceChangePercent = parseFloat(transformed.priceChangePercent.toFixed(4));
      }
      if (transformed.tradeSize !== undefined) {
        transformed.tradeSize = parseFloat(transformed.tradeSize.toFixed(8));
      }

      // Normalize trade side if present
      if (transformed.side) {
        transformed.side = transformed.side.toLowerCase() as any;
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming tick message`, {
        error: error as Error,
        marketId: message.marketId,
      });
      throw error;
    }
  }

  /**
   * Called before processing
   */
  async onBeforeProcess(
    message: TickQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    // Minimal logging for high-frequency ticks
    if (message.tickType !== 'price_update' || context.attempt === 1) {
      this.logger.debug(`Starting tick processing`, {
        marketId: message.marketId,
        tickType: message.tickType,
        attempt: context.attempt,
      });
    }

    // Update processing metrics in Redis (use increment for performance)
    await redisUtils.increment('ticks_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: TickQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      // Minimal logging for high-frequency ticks
      if (message.tickType !== 'price_update') {
        this.logger.debug(`Tick processing completed`, {
          marketId: message.marketId,
          tickType: message.tickType,
          action: result.metadata?.action,
        });
      }

      // Update success metrics
      await redisUtils.increment('ticks_processed_count');

      // Update real-time cache for critical ticks
      if (message.tickType === 'trade' || message.tickType === 'market_state') {
        await this.updateRealTimeTickCache(message);
      }
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: TickQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Tick processing error`, {
      error: error,
      marketId: message.marketId,
      tickType: message.tickType,
      attempt: context.attempt,
    });

    // Update error metrics
    await redisUtils.increment('ticks_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: TickQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying tick processing`, {
      marketId: message.marketId,
      tickType: message.tickType,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('ticks_retry_count');
  }

  /**
   * Called when message goes to dead letter queue
   */
  async onDeadLetter(
    message: TickQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Tick message sent to dead letter queue`, {
      marketId: message.marketId,
      tickType: message.tickType,
      error: error,
      attempts: context.attempt,
    });

    // Update dead letter metrics
    await redisUtils.increment('ticks_deadletter_count');
  }

  /**
   * Get or create aggregation window for market
   */
  private getOrCreateAggregationWindow(message: TickQueueMessage): TickAggregationWindow {
    const existing = this.aggregationWindows.get(message.marketId);

    if (existing && Date.now() - existing.windowStart < this.AGGREGATION_WINDOW_MS) {
      return existing;
    }

    // Create new window
    const window: TickAggregationWindow = {
      marketId: message.marketId,
      ticks: [],
      windowStart: Date.now(),
      lastUpdate: Date.now(),
      price: message.price,
      liquidity: message.liquidity,
      volume24h: message.volume24h || 0,
    };

    this.aggregationWindows.set(message.marketId, window);
    return window;
  }

  /**
   * Check if aggregation window should be flushed
   */
  private shouldFlushWindow(window: TickAggregationWindow): boolean {
    const timeSinceStart = Date.now() - window.windowStart;
    return timeSinceStart >= this.AGGREGATION_WINDOW_MS || window.ticks.length >= this.MAX_TICKS_PER_WINDOW;
  }

  /**
   * Flush aggregation window to database
   */
  private async flushAggregationWindow(window: TickAggregationWindow): Promise<void> {
    if (window.ticks.length === 0) return;

    try {
      // Get the latest tick as representative
      const latestTick = window.ticks[window.ticks.length - 1];

      // Verify market exists
      const marketExists = await this.verifyMarketExistsCached(latestTick.marketId);
      if (!marketExists) {
        this.logger.warn(`Market ${latestTick.marketId} does not exist for aggregated tick`);
        return;
      }

      // Store aggregated tick
      await this.storeAggregatedTick(window, latestTick);

      // Update real-time price data
      await this.updateRealTimePrice(latestTick);

      // Update market cache
      await this.updateMarketCache(latestTick);

      this.logger.debug(`Flushed aggregated tick window`, {
        marketId: window.marketId,
        tickCount: window.ticks.length,
        windowDuration: Date.now() - window.windowStart,
      });

      // Clear the window
      this.aggregationWindows.delete(window.marketId);
    } catch (error) {
      this.logger.error(`Failed to flush aggregation window`, {
        error: error as Error,
        marketId: window.marketId,
        tickCount: window.ticks.length,
      });
    }
  }

  /**
   * Clean up old aggregation windows
   */
  private cleanupAggregationWindows(): void {
    const now = Date.now();
    const cutoff = now - this.AGGREGATION_WINDOW_MS * 2; // Keep windows for 2x the window time

    for (const [marketId, window] of this.aggregationWindows.entries()) {
      if (window.lastUpdate < cutoff) {
        // Force flush old windows
        if (window.ticks.length > 0) {
          this.flushAggregationWindow(window).catch(error => {
            this.logger.error(`Error flushing old aggregation window`, {
              error,
              marketId,
            });
          });
        } else {
          this.aggregationWindows.delete(marketId);
        }
      }
    }
  }

  /**
   * Verify market exists with caching
   */
  private async verifyMarketExistsCached(marketId: string): Promise<boolean> {
    try {
      // Check cache first
      const cacheKey = redisUtils.getCacheKey('market_exists', marketId);
      const cached = await redisUtils.get(cacheKey);

      if (cached) {
        return cached === 'true';
      }

      // Query database
      const query = `SELECT id FROM markets WHERE polymarket_id = $1 LIMIT 1`;
      const result = await this.db.query(query, [marketId]);
      const exists = result.rows.length > 0;

      // Cache result for 30 seconds
      await redisUtils.setWithExpiry(cacheKey, exists.toString(), 30);

      return exists;
    } catch (error) {
      this.logger.error(`Failed to verify market existence`, {
        error: error as Error,
        marketId,
      });
      return false;
    }
  }

  /**
   * Store individual tick
   */
  private async storeTick(message: TickQueueMessage): Promise<void> {
    try {
      const query = `
        INSERT INTO market_ticks (
          market_id, price, best_bid, best_ask, liquidity, volume_24h,
          price_change, price_change_percent, timestamp, sequence_id,
          trade_id, trade_size, side, tick_type, market_state
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        )
      `;

      const values = [
        message.marketId,
        message.price,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume24h || null,
        message.priceChange || null,
        message.priceChangePercent || null,
        message.timestamp,
        message.sequenceId || null,
        message.tradeId || null,
        message.tradeSize || null,
        message.side || null,
        message.tickType,
        message.marketState,
      ];

      await this.db.query(query);
    } catch (error) {
      this.logger.error(`Failed to store tick`, {
        error: error as Error,
        marketId: message.marketId,
        tickType: message.tickType,
      });
      throw error;
    }
  }

  /**
   * Store aggregated tick data
   */
  private async storeAggregatedTick(window: TickAggregationWindow, latestTick: TickQueueMessage): Promise<void> {
    try {
      // Store only the latest tick with aggregated information
      const aggregatedMessage = {
        ...latestTick,
        priceChange: window.ticks.length > 1 ? latestTick.price - window.ticks[0].price : 0,
        // Add metadata about aggregation
        aggregatedCount: window.ticks.length,
        aggregationWindowStart: new Date(window.windowStart).toISOString(),
      };

      await this.storeTick(aggregatedMessage as TickQueueMessage);
    } catch (error) {
      this.logger.error(`Failed to store aggregated tick`, {
        error: error as Error,
        marketId: window.marketId,
        tickCount: window.ticks.length,
      });
      throw error;
    }
  }

  /**
   * Update real-time price data
   */
  private async updateRealTimePrice(message: TickQueueMessage): Promise<void> {
    try {
      const query = `
        UPDATE market_prices_realtime SET
          current_price = $2,
          best_bid = $3,
          best_ask = $4,
          liquidity = $5,
          volume_24h = $6,
          last_updated = $7,
          price_change = $8,
          price_change_percent = $9
        WHERE market_id = $1
      `;

      // Get previous price for change calculation
      const previousQuery = `
        SELECT current_price FROM market_prices_realtime
        WHERE market_id = $1 ORDER BY last_updated DESC LIMIT 1
      `;
      const previousResult = await this.db.query(previousQuery, [message.marketId]);
      const previousPrice = previousResult.rows[0]?.current_price || message.price;

      const priceChange = message.price - previousPrice;
      const priceChangePercent = previousPrice > 0 ? (priceChange / previousPrice) * 100 : 0;

      const values = [
        message.marketId,
        message.price,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume24h || 0,
        new Date().toISOString(),
        priceChange,
        priceChangePercent,
      ];

      const result = await this.db.query(query, values);

      // If no rows were updated, insert new record
      if (result.rowCount === 0) {
        const insertQuery = `
          INSERT INTO market_prices_realtime (
            market_id, current_price, best_bid, best_ask, liquidity,
            volume_24h, last_updated, price_change, price_change_percent
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `;
        await this.db.query(insertQuery, values);
      }
    } catch (error) {
      this.logger.error(`Failed to update real-time price`, {
        error: error as Error,
        marketId: message.marketId,
      });
      // Don't throw - real-time price failures shouldn't stop tick processing
    }
  }

  /**
   * Update market cache
   */
  private async updateMarketCache(message: TickQueueMessage): Promise<void> {
    try {
      const cacheKey = redisUtils.getCacheKey('market_tick', message.marketId);
      const cacheData = {
        price: message.price,
        bestBid: message.bestBid,
        bestAsk: message.bestAsk,
        liquidity: message.liquidity,
        volume24h: message.volume24h,
        marketState: message.marketState,
        lastTick: message.timestamp,
        tickType: message.tickType,
      };

      await redisUtils.hSet(cacheKey, 'data', JSON.stringify(cacheData));

      // Set short TTL for tick cache (30 seconds)
      // Note: This would need to be implemented in the Redis client
    } catch (error) {
      this.logger.warn(`Failed to update market cache`, {
        error: error as Error,
        marketId: message.marketId,
      });
      // Don't throw - caching failures shouldn't stop processing
    }
  }

  /**
   * Update real-time tick cache for critical ticks
   */
  private async updateRealTimeTickCache(message: TickQueueMessage): Promise<void> {
    try {
      const cacheKey = redisUtils.getCacheKey('realtime_tick', message.marketId);
      const cacheData = {
        ...message,
        cachedAt: new Date().toISOString(),
      };

      await redisUtils.hSet(cacheKey, 'data', JSON.stringify(cacheData));
    } catch (error) {
      this.logger.warn(`Failed to update real-time tick cache`, {
        error: error as Error,
        marketId: message.marketId,
      });
    }
  }

  /**
   * Determine if error is retryable
   */
  private shouldRetry(error: Error): boolean {
    const message = error.message.toLowerCase();

    // Don't retry validation errors
    if (message.includes('validation') || message.includes('invalid')) {
      return false;
    }

    // Don't retry foreign key errors (market doesn't exist)
    if (message.includes('foreign key') || message.includes('does not exist')) {
      return false;
    }

    // Retry database connection errors
    if (message.includes('connection') || message.includes('timeout')) {
      return true;
    }

    // Retry transient errors
    if (message.includes('temporarily unavailable') || message.includes('try again')) {
      return true;
    }

    // For high-frequency ticks, be more aggressive about retries
    return true;
  }
}

/**
 * Ticks queue implementation with high-performance optimizations
 */
export class TicksQueue extends BaseQueue<TickQueueMessage> {
  private processor: TicksQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.ticks>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.ticks, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new TicksQueueProcessor(this.logger);
  }

  /**
   * Get the ticks processor
   */
  protected getProcessor(): QueueProcessor<TickQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is a tick message
   */
  protected deserializeMessage(streamMessage: any): TickQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isTickMessage(message)) {
      throw new Error(`Invalid tick message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to ticks
   */
  async getTickStatistics(): Promise<{
    totalTicks: number;
    ticksByType: Record<string, number>;
    averagePriceChange: number;
    highFrequencyMarkets: Array<{
      marketId: string;
      tickCount: number;
      lastActivity: string;
    }>;
    recentActivity: {
      lastMinute: number;
      lastHour: number;
      lastDay: number;
    };
    aggregationStats: {
      activeWindows: number;
      avgTicksPerWindow: number;
      totalAggregated: number;
    };
  }> {
    try {
      // This would typically query the database for statistics
      // For now, return a placeholder implementation
      return {
        totalTicks: 0,
        ticksByType: {},
        averagePriceChange: 0,
        highFrequencyMarkets: [],
        recentActivity: {
          lastMinute: 0,
          lastHour: 0,
          lastDay: 0,
        },
        aggregationStats: {
          activeWindows: 0,
          avgTicksPerWindow: 0,
          totalAggregated: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get tick statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Force flush all aggregation windows
   */
  async flushAllAggregationWindows(): Promise<void> {
    // This would be exposed for external control during shutdown
    // Implementation would call processor.cleanupAggregationWindows()
    this.logger.info(`Flushing all tick aggregation windows`);
  }
}