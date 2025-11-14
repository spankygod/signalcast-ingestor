/**
 * Markets queue processor for SignalCast system
 * Handles processing of polymarket market data with database operations and validation
 */

import { BaseQueue } from './base-queue';
import {
  MarketQueueMessage,
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
 * Markets queue processor implementation
 */
export class MarketsQueueProcessor implements QueueProcessor<MarketQueueMessage> {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'markets-processor' });
    this.db = database;
  }

  /**
   * Process market message
   */
  async process(
    message: MarketQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.debug(`Processing market message`, {
        marketId: message.polymarketId,
        eventId: message.eventId,
        question: message.question.substring(0, 100),
        correlationId: message.correlationId,
      });

      // Verify event exists
      const eventExists = await this.verifyEventExists(message.eventId);
      if (!eventExists) {
        throw new Error(`Event ${message.eventId} does not exist for market ${message.polymarketId}`);
      }

      // Check if market already exists (idempotency)
      const existingMarket = await this.findExistingMarket(message.polymarketId);

      if (existingMarket) {
        // Update existing market
        await this.updateMarket(existingMarket.id, message);

        this.logger.info(`Updated existing market`, {
          marketId: existingMarket.id,
          polymarketId: message.polymarketId,
          eventId: message.eventId,
        });

        // Update real-time price data if changed
        await this.updateRealTimePrice(existingMarket.id, message);

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'updated',
            marketId: existingMarket.id,
            polymarketId: message.polymarketId,
            eventId: message.eventId,
            processingTime: Date.now() - startTime,
          },
        };
      } else {
        // Create new market
        const marketId = await this.createMarket(message);

        this.logger.info(`Created new market`, {
          marketId,
          polymarketId: message.polymarketId,
          eventId: message.eventId,
        });

        // Initialize real-time price data
        await this.initializeRealTimePrice(marketId, message);

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'created',
            marketId,
            polymarketId: message.polymarketId,
            eventId: message.eventId,
            processingTime: Date.now() - startTime,
          },
        };
      }
    } catch (error) {
      this.logger.error(`Failed to process market message`, {
        error: error as Error,
        marketId: message.polymarketId,
        eventId: message.eventId,
        correlationId: message.correlationId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'MARKET_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            marketId: message.polymarketId,
            eventId: message.eventId,
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
   * Validate market message
   */
  async validate(message: MarketQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.polymarketId || !message.eventId || !message.question) {
        this.logger.warn(`Market message missing required fields`, {
          polymarketId: message.polymarketId,
          eventId: message.eventId,
          question: message.question ? 'present' : 'missing',
        });
        return false;
      }

      // Validate polymarket ID format
      if (!/^0x[a-fA-F0-9]{64}$/.test(message.polymarketId)) {
        this.logger.warn(`Invalid polymarket ID format`, {
          polymarketId: message.polymarketId,
        });
        return false;
      }

      // Validate event ID format
      if (!/^0x[a-fA-F0-9]{64}$/.test(message.eventId)) {
        this.logger.warn(`Invalid event ID format`, {
          eventId: message.eventId,
        });
        return false;
      }

      // Validate question length
      if (message.question.length < 5 || message.question.length > 1000) {
        this.logger.warn(`Invalid question length`, {
          questionLength: message.question.length,
        });
        return false;
      }

      // Validate price ranges
      if (message.currentPrice < 0 || message.currentPrice > 1) {
        this.logger.warn(`Invalid current price`, {
          currentPrice: message.currentPrice,
        });
        return false;
      }

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

      // Validate spread
      if (message.bestBid !== undefined && message.bestAsk !== undefined) {
        const spread = message.bestAsk - message.bestBid;
        if (spread < 0) {
          this.logger.warn(`Invalid spread (ask < bid)`, {
            bestBid: message.bestBid,
            bestAsk: message.bestAsk,
          });
          return false;
        }
      }

      // Validate business logic
      if (message.liquidity < 0) {
        this.logger.warn(`Invalid liquidity value`, {
          liquidity: message.liquidity,
        });
        return false;
      }

      if (message.volume24h !== undefined && message.volume24h < 0) {
        this.logger.warn(`Invalid volume value`, {
          volume24h: message.volume24h,
        });
        return false;
      }

      // Validate dates
      if (message.startDate) {
        const startDate = new Date(message.startDate);
        if (isNaN(startDate.getTime())) {
          this.logger.warn(`Invalid start date`, {
            startDate: message.startDate,
          });
          return false;
        }
      }

      if (message.endDate) {
        const endDate = new Date(message.endDate);
        if (isNaN(endDate.getTime())) {
          this.logger.warn(`Invalid end date`, {
            endDate: message.endDate,
          });
          return false;
        }
      }

      // Validate timestamps
      const createdAt = new Date(message.createdAt);
      const updatedAt = new Date(message.updatedAt);
      if (isNaN(createdAt.getTime()) || isNaN(updatedAt.getTime())) {
        this.logger.warn(`Invalid timestamps`, {
          createdAt: message.createdAt,
          updatedAt: message.updatedAt,
        });
        return false;
      }

      return true;
    } catch (error) {
      this.logger.error(`Error during market message validation`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      return false;
    }
  }

  /**
   * Transform market message if needed
   */
  async transform(message: MarketQueueMessage): Promise<MarketQueueMessage> {
    try {
      // Normalize timestamps
      const transformed = { ...message };

      if (transformed.createdAt && !transformed.createdAt.includes('Z')) {
        transformed.createdAt = new Date(transformed.createdAt).toISOString();
      }
      if (transformed.updatedAt && !transformed.updatedAt.includes('Z')) {
        transformed.updatedAt = new Date(transformed.updatedAt).toISOString();
      }
      if (transformed.startDate && !transformed.startDate.includes('Z')) {
        transformed.startDate = new Date(transformed.startDate).toISOString();
      }
      if (transformed.endDate && !transformed.endDate.includes('Z')) {
        transformed.endDate = new Date(transformed.endDate).toISOString();
      }

      // Ensure numeric fields are properly formatted
      transformed.currentPrice = parseFloat(transformed.currentPrice.toFixed(8));
      transformed.liquidity = parseFloat(transformed.liquidity.toFixed(8));

      if (transformed.bestBid !== undefined) {
        transformed.bestBid = parseFloat(transformed.bestBid.toFixed(8));
      }
      if (transformed.bestAsk !== undefined) {
        transformed.bestAsk = parseFloat(transformed.bestAsk.toFixed(8));
      }
      if (transformed.spread !== undefined) {
        transformed.spread = parseFloat(transformed.spread.toFixed(8));
      }
      if (transformed.volume24h !== undefined) {
        transformed.volume24h = parseFloat(transformed.volume24h.toFixed(8));
      }

      // Calculate spread if not provided
      if (transformed.bestBid !== undefined && transformed.bestAsk !== undefined && !transformed.spread) {
        transformed.spread = parseFloat((transformed.bestAsk - transformed.bestBid).toFixed(8));
      }

      // Process outcome prices
      if (transformed.outcomePrices) {
        const processedPrices: Record<string, number> = {};
        for (const [outcome, price] of Object.entries(transformed.outcomePrices)) {
          if (typeof price === 'number' && price >= 0 && price <= 1) {
            processedPrices[outcome] = parseFloat(price.toFixed(8));
          }
        }
        transformed.outcomePrices = processedPrices;
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming market message`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Called before processing
   */
  async onBeforeProcess(
    message: MarketQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.debug(`Starting market processing`, {
      polymarketId: message.polymarketId,
      eventId: message.eventId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update processing metrics in Redis
    await redisUtils.increment('markets_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: MarketQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      this.logger.debug(`Market processing completed successfully`, {
        polymarketId: message.polymarketId,
        action: result.metadata?.action,
        processingTime: result.metadata?.processingTime,
      });

      // Update success metrics
      await redisUtils.increment('markets_processed_count');

      // Cache market data for quick access
      await this.cacheMarketData(message, result.metadata?.marketId);
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: MarketQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Market processing error`, {
      error: error,
      polymarketId: message.polymarketId,
      eventId: message.eventId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update error metrics
    await redisUtils.increment('markets_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: MarketQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying market processing`, {
      polymarketId: message.polymarketId,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('markets_retry_count');
  }

  /**
   * Called when message goes to dead letter queue
   */
  async onDeadLetter(
    message: MarketQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Market message sent to dead letter queue`, {
      polymarketId: message.polymarketId,
      eventId: message.eventId,
      error: error,
      attempts: context.attempt,
    });

    // Update dead letter metrics
    await redisUtils.increment('markets_deadletter_count');
  }

  /**
   * Verify event exists in database
   */
  private async verifyEventExists(eventId: string): Promise<boolean> {
    try {
      const query = `
        SELECT id FROM events WHERE polymarket_id = $1 LIMIT 1
      `;

      const result = await this.db.query(query, [eventId]);
      return result.rows.length > 0;
    } catch (error) {
      this.logger.error(`Failed to verify event existence`, {
        error: error as Error,
        eventId,
      });
      throw error;
    }
  }

  /**
   * Find existing market by polymarket ID
   */
  private async findExistingMarket(polymarketId: string): Promise<any> {
    try {
      const query = `
        SELECT id, polymarket_id, event_id, question, current_price, updated_at
        FROM markets
        WHERE polymarket_id = $1
        LIMIT 1
      `;

      const result = await this.db.query(query, [polymarketId]);
      return result.rows[0] || null;
    } catch (error) {
      this.logger.error(`Failed to find existing market`, {
        error: error as Error,
        polymarketId,
      });
      throw error;
    }
  }

  /**
   * Create new market in database
   */
  private async createMarket(message: MarketQueueMessage): Promise<string> {
    try {
      const query = `
        INSERT INTO markets (
          polymarket_id, event_id, question, description, liquidity, current_price,
          best_bid, best_ask, spread, volume_24h, active, closed, resolved,
          resolution, start_date, end_date, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
        ) RETURNING id
      `;

      const values = [
        message.polymarketId,
        message.eventId,
        message.question,
        message.description || null,
        message.liquidity,
        message.currentPrice,
        message.bestBid || null,
        message.bestAsk || null,
        message.spread || null,
        message.volume24h || null,
        message.active,
        message.closed,
        message.resolved,
        message.resolution || null,
        message.startDate || null,
        message.endDate || null,
        message.createdAt,
        message.updatedAt,
      ];

      const result = await this.db.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      this.logger.error(`Failed to create market`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Update existing market in database
   */
  private async updateMarket(marketId: string, message: MarketQueueMessage): Promise<void> {
    try {
      const query = `
        UPDATE markets SET
          question = $2,
          description = $3,
          liquidity = $4,
          current_price = $5,
          best_bid = $6,
          best_ask = $7,
          spread = $8,
          volume_24h = $9,
          active = $10,
          closed = $11,
          resolved = $12,
          resolution = $13,
          start_date = $14,
          end_date = $15,
          updated_at = $16
        WHERE id = $1
      `;

      const values = [
        marketId,
        message.question,
        message.description || null,
        message.liquidity,
        message.currentPrice,
        message.bestBid || null,
        message.bestAsk || null,
        message.spread || null,
        message.volume24h || null,
        message.active,
        message.closed,
        message.resolved,
        message.resolution || null,
        message.startDate || null,
        message.endDate || null,
        message.updatedAt,
      ];

      await this.db.query(query, values);
    } catch (error) {
      this.logger.error(`Failed to update market`, {
        error: error as Error,
        marketId,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Initialize real-time price data for new market
   */
  private async initializeRealTimePrice(marketId: string, message: MarketQueueMessage): Promise<void> {
    try {
      const query = `
        INSERT INTO market_prices_realtime (
          market_id, current_price, best_bid, best_ask, liquidity,
          volume_24h, last_updated, price_change, price_change_percent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `;

      const values = [
        marketId,
        message.currentPrice,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume24h || 0,
        new Date().toISOString(),
        0, // Initial price change
        0, // Initial price change percent
      ];

      await this.db.query(query, values);
    } catch (error) {
      this.logger.error(`Failed to initialize real-time price`, {
        error: error as Error,
        marketId,
        polymarketId: message.polymarketId,
      });
      // Don't throw - real-time price failures shouldn't stop market creation
    }
  }

  /**
   * Update real-time price data for existing market
   */
  private async updateRealTimePrice(marketId: string, message: MarketQueueMessage): Promise<void> {
    try {
      // Get previous price to calculate change
      const previousPriceQuery = `
        SELECT current_price FROM market_prices_realtime WHERE market_id = $1 ORDER BY last_updated DESC LIMIT 1
      `;
      const previousResult = await this.db.query(previousPriceQuery, [marketId]);
      const previousPrice = previousResult.rows[0]?.current_price || message.currentPrice;

      const priceChange = message.currentPrice - previousPrice;
      const priceChangePercent = previousPrice > 0 ? (priceChange / previousPrice) * 100 : 0;

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

      const values = [
        marketId,
        message.currentPrice,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume24h || 0,
        new Date().toISOString(),
        priceChange,
        priceChangePercent,
      ];

      const result = await this.db.query(query);

      // If no rows were updated, insert new record
      if (result.rowCount === 0) {
        await this.initializeRealTimePrice(marketId, message);
      }
    } catch (error) {
      this.logger.error(`Failed to update real-time price`, {
        error: error as Error,
        marketId,
        polymarketId: message.polymarketId,
      });
      // Don't throw - real-time price failures shouldn't stop market updates
    }
  }

  /**
   * Cache market data in Redis
   */
  private async cacheMarketData(message: MarketQueueMessage, marketId?: string): Promise<void> {
    try {
      if (!marketId) return;

      const cacheKey = redisUtils.getCacheKey('market', message.polymarketId);
      const cacheData = {
        id: marketId,
        polymarketId: message.polymarketId,
        eventId: message.eventId,
        question: message.question,
        currentPrice: message.currentPrice,
        active: message.active,
        closed: message.closed,
        resolved: message.resolved,
        cachedAt: new Date().toISOString(),
      };

      await redisUtils.hSet(cacheKey, 'data', JSON.stringify(cacheData));

      // Set TTL for cache (5 minutes)
      // Note: This would need to be implemented in the Redis client
    } catch (error) {
      this.logger.warn(`Failed to cache market data`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      // Don't throw - caching failures shouldn't stop processing
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

    // Don't retry foreign key errors (event doesn't exist)
    if (message.includes('foreign key') || message.includes('does not exist')) {
      return false;
    }

    // Don't retry duplicate key errors
    if (message.includes('duplicate') || message.includes('unique constraint')) {
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

    // Default to retry for unknown errors
    return true;
  }
}

/**
 * Markets queue implementation
 */
export class MarketsQueue extends BaseQueue<MarketQueueMessage> {
  private processor: MarketsQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.markets>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.markets, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new MarketsQueueProcessor(this.logger);
  }

  /**
   * Get the markets processor
   */
  protected getProcessor(): QueueProcessor<MarketQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is a market message
   */
  protected deserializeMessage(streamMessage: any): MarketQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isMarketMessage(message)) {
      throw new Error(`Invalid market message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to markets
   */
  async getMarketStatistics(): Promise<{
    totalMarkets: number;
    activeMarkets: number;
    closedMarkets: number;
    resolvedMarkets: number;
    marketsByEvent: Record<string, number>;
    averageLiquidity: number;
    averageVolume: number;
    recentActivity: {
      created: number;
      updated: number;
      resolved: number;
    };
  }> {
    try {
      // This would typically query the database for statistics
      // For now, return a placeholder implementation
      return {
        totalMarkets: 0,
        activeMarkets: 0,
        closedMarkets: 0,
        resolvedMarkets: 0,
        marketsByEvent: {},
        averageLiquidity: 0,
        averageVolume: 0,
        recentActivity: {
          created: 0,
          updated: 0,
          resolved: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get market statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }
}