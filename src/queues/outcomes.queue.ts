/**
 * Outcomes queue processor for SignalCast system
 * Handles processing of polymarket outcome data with database operations and validation
 */

import { BaseQueue } from './base-queue';
import {
  OutcomeQueueMessage,
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
 * Outcomes queue processor implementation
 */
export class OutcomesQueueProcessor implements QueueProcessor<OutcomeQueueMessage> {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'outcomes-processor' });
    this.db = database;
  }

  /**
   * Process outcome message
   */
  async process(
    message: OutcomeQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.debug(`Processing outcome message`, {
        outcomeId: message.polymarketId,
        marketId: message.marketId,
        eventId: message.eventId,
        title: message.title,
        correlationId: message.correlationId,
      });

      // Verify market exists
      const marketExists = await this.verifyMarketExists(message.marketId);
      if (!marketExists) {
        throw new Error(`Market ${message.marketId} does not exist for outcome ${message.polymarketId}`);
      }

      // Check if outcome already exists (idempotency)
      const existingOutcome = await this.findExistingOutcome(message.polymarketId);

      if (existingOutcome) {
        // Update existing outcome
        await this.updateOutcome(existingOutcome.id, message);

        this.logger.info(`Updated existing outcome`, {
          outcomeId: existingOutcome.id,
          polymarketId: message.polymarketId,
          marketId: message.marketId,
          title: message.title,
        });

        // Update real-time price data if changed
        await this.updateRealTimePrice(existingOutcome.id, message);

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'updated',
            outcomeId: existingOutcome.id,
            polymarketId: message.polymarketId,
            marketId: message.marketId,
            processingTime: Date.now() - startTime,
          },
        };
      } else {
        // Create new outcome
        const outcomeId = await this.createOutcome(message);

        this.logger.info(`Created new outcome`, {
          outcomeId,
          polymarketId: message.polymarketId,
          marketId: message.marketId,
          title: message.title,
        });

        // Initialize real-time price data
        await this.initializeRealTimePrice(outcomeId, message);

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'created',
            outcomeId,
            polymarketId: message.polymarketId,
            marketId: message.marketId,
            processingTime: Date.now() - startTime,
          },
        };
      }
    } catch (error) {
      this.logger.error(`Failed to process outcome message`, {
        error: error as Error,
        outcomeId: message.polymarketId,
        marketId: message.marketId,
        correlationId: message.correlationId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'OUTCOME_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            outcomeId: message.polymarketId,
            marketId: message.marketId,
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
   * Validate outcome message
   */
  async validate(message: OutcomeQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.polymarketId || !message.marketId || !message.eventId || !message.title) {
        this.logger.warn(`Outcome message missing required fields`, {
          polymarketId: message.polymarketId,
          marketId: message.marketId,
          eventId: message.eventId,
          title: message.title ? 'present' : 'missing',
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

      // Validate market ID format
      if (!/^0x[a-fA-F0-9]{64}$/.test(message.marketId)) {
        this.logger.warn(`Invalid market ID format`, {
          marketId: message.marketId,
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

      // Validate title length
      if (message.title.length < 1 || message.title.length > 200) {
        this.logger.warn(`Invalid title length`, {
          titleLength: message.title.length,
        });
        return false;
      }

      // Validate status
      const validStatuses = ['active', 'resolved', 'cancelled', 'suspended'];
      if (!validStatuses.includes(message.status)) {
        this.logger.warn(`Invalid status`, {
          status: message.status,
        });
        return false;
      }

      // Validate resolution if present
      if (message.resolution) {
        const validResolutions = ['yes', 'no', 'invalid'];
        if (!validResolutions.includes(message.resolution)) {
          this.logger.warn(`Invalid resolution`, {
            resolution: message.resolution,
          });
          return false;
        }
      }

      // Validate probability range
      if (message.probability < 0 || message.probability > 1) {
        this.logger.warn(`Invalid probability`, {
          probability: message.probability,
        });
        return false;
      }

      // Validate implied probability if present
      if (message.impliedProbability !== undefined) {
        if (message.impliedProbability < 0 || message.impliedProbability > 1) {
          this.logger.warn(`Invalid implied probability`, {
            impliedProbability: message.impliedProbability,
          });
          return false;
        }
      }

      // Validate price range
      if (message.price < 0 || message.price > 1) {
        this.logger.warn(`Invalid price`, {
          price: message.price,
        });
        return false;
      }

      // Validate bid/ask prices if present
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

      // Validate business logic
      if (message.liquidity < 0) {
        this.logger.warn(`Invalid liquidity value`, {
          liquidity: message.liquidity,
        });
        return false;
      }

      if (message.volume !== undefined && message.volume < 0) {
        this.logger.warn(`Invalid volume value`, {
          volume: message.volume,
        });
        return false;
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
      this.logger.error(`Error during outcome message validation`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      return false;
    }
  }

  /**
   * Transform outcome message if needed
   */
  async transform(message: OutcomeQueueMessage): Promise<OutcomeQueueMessage> {
    try {
      // Normalize timestamps
      const transformed = { ...message };

      if (transformed.createdAt && !transformed.createdAt.includes('Z')) {
        transformed.createdAt = new Date(transformed.createdAt).toISOString();
      }
      if (transformed.updatedAt && !transformed.updatedAt.includes('Z')) {
        transformed.updatedAt = new Date(transformed.updatedAt).toISOString();
      }

      // Normalize status
      transformed.status = transformed.status.toLowerCase() as any;

      // Normalize resolution if present
      if (transformed.resolution) {
        transformed.resolution = transformed.resolution.toLowerCase() as any;
      }

      // Ensure numeric fields are properly formatted
      transformed.price = parseFloat(transformed.price.toFixed(8));
      transformed.probability = parseFloat(transformed.probability.toFixed(8));
      transformed.liquidity = parseFloat(transformed.liquidity.toFixed(8));

      if (transformed.impliedProbability !== undefined) {
        transformed.impliedProbability = parseFloat(transformed.impliedProbability.toFixed(8));
      }
      if (transformed.bestBid !== undefined) {
        transformed.bestBid = parseFloat(transformed.bestBid.toFixed(8));
      }
      if (transformed.bestAsk !== undefined) {
        transformed.bestAsk = parseFloat(transformed.bestAsk.toFixed(8));
      }
      if (transformed.volume !== undefined) {
        transformed.volume = parseFloat(transformed.volume.toFixed(8));
      }

      // Calculate and validate odds if not provided
      if (!transformed.odds && transformed.price > 0) {
        transformed.odds = this.calculateOdds(transformed.price);
      } else if (transformed.odds) {
        // Validate and format odds
        transformed.odds = this.validateAndFormatOdds(transformed.odds, transformed.price);
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming outcome message`, {
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
    message: OutcomeQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.debug(`Starting outcome processing`, {
      polymarketId: message.polymarketId,
      marketId: message.marketId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update processing metrics in Redis
    await redisUtils.increment('outcomes_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: OutcomeQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      this.logger.debug(`Outcome processing completed successfully`, {
        polymarketId: message.polymarketId,
        action: result.metadata?.action,
        processingTime: result.metadata?.processingTime,
      });

      // Update success metrics
      await redisUtils.increment('outcomes_processed_count');

      // Cache outcome data for quick access
      await this.cacheOutcomeData(message, result.metadata?.outcomeId);

      // Update market outcome count cache
      await this.updateMarketOutcomeCount(message.marketId);
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: OutcomeQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Outcome processing error`, {
      error: error,
      polymarketId: message.polymarketId,
      marketId: message.marketId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update error metrics
    await redisUtils.increment('outcomes_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: OutcomeQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying outcome processing`, {
      polymarketId: message.polymarketId,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('outcomes_retry_count');
  }

  /**
   * Called when message goes to dead letter queue
   */
  async onDeadLetter(
    message: OutcomeQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Outcome message sent to dead letter queue`, {
      polymarketId: message.polymarketId,
      marketId: message.marketId,
      error: error,
      attempts: context.attempt,
    });

    // Update dead letter metrics
    await redisUtils.increment('outcomes_deadletter_count');
  }

  /**
   * Verify market exists in database
   */
  private async verifyMarketExists(marketId: string): Promise<boolean> {
    try {
      const query = `
        SELECT id FROM markets WHERE polymarket_id = $1 LIMIT 1
      `;

      const result = await this.db.query(query, [marketId]);
      return result.rows.length > 0;
    } catch (error) {
      this.logger.error(`Failed to verify market existence`, {
        error: error as Error,
        marketId,
      });
      throw error;
    }
  }

  /**
   * Find existing outcome by polymarket ID
   */
  private async findExistingOutcome(polymarketId: string): Promise<any> {
    try {
      const query = `
        SELECT id, polymarket_id, market_id, title, price, probability, status, updated_at
        FROM outcomes
        WHERE polymarket_id = $1
        LIMIT 1
      `;

      const result = await this.db.query(query, [polymarketId]);
      return result.rows[0] || null;
    } catch (error) {
      this.logger.error(`Failed to find existing outcome`, {
        error: error as Error,
        polymarketId,
      });
      throw error;
    }
  }

  /**
   * Create new outcome in database
   */
  private async createOutcome(message: OutcomeQueueMessage): Promise<string> {
    try {
      const query = `
        INSERT INTO outcomes (
          polymarket_id, market_id, event_id, title, description, price, probability,
          implied_probability, best_bid, best_ask, liquidity, volume, status,
          resolution, odds_decimal, odds_fractional, odds_american, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
        ) RETURNING id
      `;

      const values = [
        message.polymarketId,
        message.marketId,
        message.eventId,
        message.title,
        message.description || null,
        message.price,
        message.probability,
        message.impliedProbability || null,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume || null,
        message.status,
        message.resolution || null,
        message.odds?.decimal || null,
        message.odds?.fractional || null,
        message.odds?.american || null,
        message.createdAt,
        message.updatedAt,
      ];

      const result = await this.db.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      this.logger.error(`Failed to create outcome`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Update existing outcome in database
   */
  private async updateOutcome(outcomeId: string, message: OutcomeQueueMessage): Promise<void> {
    try {
      const query = `
        UPDATE outcomes SET
          title = $2,
          description = $3,
          price = $4,
          probability = $5,
          implied_probability = $6,
          best_bid = $7,
          best_ask = $8,
          liquidity = $9,
          volume = $10,
          status = $11,
          resolution = $12,
          odds_decimal = $13,
          odds_fractional = $14,
          odds_american = $15,
          updated_at = $16
        WHERE id = $1
      `;

      const values = [
        outcomeId,
        message.title,
        message.description || null,
        message.price,
        message.probability,
        message.impliedProbability || null,
        message.bestBid || null,
        message.bestAsk || null,
        message.liquidity,
        message.volume || null,
        message.status,
        message.resolution || null,
        message.odds?.decimal || null,
        message.odds?.fractional || null,
        message.odds?.american || null,
        message.updatedAt,
      ];

      await this.db.query(query, values);
    } catch (error) {
      this.logger.error(`Failed to update outcome`, {
        error: error as Error,
        outcomeId,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Initialize real-time price data for new outcome
   */
  private async initializeRealTimePrice(outcomeId: string, message: OutcomeQueueMessage): Promise<void> {
    try {
      const query = `
        INSERT INTO outcome_prices_realtime (
          outcome_id, current_price, best_bid, best_ask, probability,
          liquidity, volume, last_updated, price_change, price_change_percent
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `;

      const values = [
        outcomeId,
        message.price,
        message.bestBid || null,
        message.bestAsk || null,
        message.probability,
        message.liquidity,
        message.volume || 0,
        new Date().toISOString(),
        0, // Initial price change
        0, // Initial price change percent
      ];

      await this.db.query(query, values);
    } catch (error) {
      this.logger.error(`Failed to initialize outcome real-time price`, {
        error: error as Error,
        outcomeId,
        polymarketId: message.polymarketId,
      });
      // Don't throw - real-time price failures shouldn't stop outcome creation
    }
  }

  /**
   * Update real-time price data for existing outcome
   */
  private async updateRealTimePrice(outcomeId: string, message: OutcomeQueueMessage): Promise<void> {
    try {
      // Get previous price to calculate change
      const previousPriceQuery = `
        SELECT current_price FROM outcome_prices_realtime
        WHERE outcome_id = $1
        ORDER BY last_updated DESC
        LIMIT 1
      `;
      const previousResult = await this.db.query(previousPriceQuery, [outcomeId]);
      const previousPrice = previousResult.rows[0]?.current_price || message.price;

      const priceChange = message.price - previousPrice;
      const priceChangePercent = previousPrice > 0 ? (priceChange / previousPrice) * 100 : 0;

      const query = `
        UPDATE outcome_prices_realtime SET
          current_price = $2,
          best_bid = $3,
          best_ask = $4,
          probability = $5,
          liquidity = $6,
          volume = $7,
          last_updated = $8,
          price_change = $9,
          price_change_percent = $10
        WHERE outcome_id = $1
      `;

      const values = [
        outcomeId,
        message.price,
        message.bestBid || null,
        message.bestAsk || null,
        message.probability,
        message.liquidity,
        message.volume || 0,
        new Date().toISOString(),
        priceChange,
        priceChangePercent,
      ];

      const result = await this.db.query(query);

      // If no rows were updated, insert new record
      if (result.rowCount === 0) {
        await this.initializeRealTimePrice(outcomeId, message);
      }
    } catch (error) {
      this.logger.error(`Failed to update outcome real-time price`, {
        error: error as Error,
        outcomeId,
        polymarketId: message.polymarketId,
      });
      // Don't throw - real-time price failures shouldn't stop outcome updates
    }
  }

  /**
   * Calculate odds from price
   */
  private calculateOdds(price: number): {
    decimal: number;
    fractional: string;
    american: number;
  } {
    const decimal = price > 0 ? 1 / price : 0;
    const fractional = decimal > 1 ? `${Math.round(decimal - 1)}/1` : '1/1';
    const american = decimal >= 2 ? Math.round((decimal - 1) * 100) : Math.round(-100 / (decimal - 1));

    return {
      decimal: parseFloat(decimal.toFixed(2)),
      fractional,
      american,
    };
  }

  /**
   * Validate and format odds
   */
  private validateAndFormatOdds(
    odds: { decimal: number; fractional: string; american: number },
    price: number
  ): { decimal: number; fractional: string; american: number } {
    // Recalculate odds if they seem invalid
    const calculatedOdds = this.calculateOdds(price);

    if (odds.decimal <= 0 || !isFinite(odds.decimal)) {
      odds.decimal = calculatedOdds.decimal;
    } else {
      odds.decimal = parseFloat(odds.decimal.toFixed(2));
    }

    if (!odds.fractional || !/^\d+\/\d+$/.test(odds.fractional)) {
      odds.fractional = calculatedOdds.fractional;
    }

    if (!isFinite(odds.american) || Math.abs(odds.american) > 1000000) {
      odds.american = calculatedOdds.american;
    }

    return odds;
  }

  /**
   * Cache outcome data in Redis
   */
  private async cacheOutcomeData(message: OutcomeQueueMessage, outcomeId?: string): Promise<void> {
    try {
      if (!outcomeId) return;

      const cacheKey = redisUtils.getCacheKey('outcome', message.polymarketId);
      const cacheData = {
        id: outcomeId,
        polymarketId: message.polymarketId,
        marketId: message.marketId,
        title: message.title,
        price: message.price,
        probability: message.probability,
        status: message.status,
        cachedAt: new Date().toISOString(),
      };

      await redisUtils.hSet(cacheKey, 'data', JSON.stringify(cacheData));
    } catch (error) {
      this.logger.warn(`Failed to cache outcome data`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      // Don't throw - caching failures shouldn't stop processing
    }
  }

  /**
   * Update market outcome count cache
   */
  private async updateMarketOutcomeCount(marketId: string): Promise<void> {
    try {
      const cacheKey = redisUtils.getCacheKey('market_outcomes', marketId);

      // Increment the outcome count for this market
      await redisUtils.increment(cacheKey);
    } catch (error) {
      this.logger.warn(`Failed to update market outcome count`, {
        error: error as Error,
        marketId,
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

    // Don't retry foreign key errors (market doesn't exist)
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
 * Outcomes queue implementation
 */
export class OutcomesQueue extends BaseQueue<OutcomeQueueMessage> {
  private processor: OutcomesQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.outcomes>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.outcomes, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new OutcomesQueueProcessor(this.logger);
  }

  /**
   * Get the outcomes processor
   */
  protected getProcessor(): QueueProcessor<OutcomeQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is an outcome message
   */
  protected deserializeMessage(streamMessage: any): OutcomeQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isOutcomeMessage(message)) {
      throw new Error(`Invalid outcome message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to outcomes
   */
  async getOutcomeStatistics(): Promise<{
    totalOutcomes: number;
    activeOutcomes: number;
    resolvedOutcomes: number;
    cancelledOutcomes: number;
    suspendedOutcomes: number;
    averageProbability: number;
    averagePrice: number;
    outcomesByMarket: Record<string, number>;
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
        totalOutcomes: 0,
        activeOutcomes: 0,
        resolvedOutcomes: 0,
        cancelledOutcomes: 0,
        suspendedOutcomes: 0,
        averageProbability: 0,
        averagePrice: 0,
        outcomesByMarket: {},
        recentActivity: {
          created: 0,
          updated: 0,
          resolved: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get outcome statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }
}