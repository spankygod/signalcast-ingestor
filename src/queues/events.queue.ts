/**
 * Events queue processor for SignalCast system
 * Handles processing of polymarket event data with database operations and validation
 */

import { BaseQueue } from './base-queue';
import {
  EventQueueMessage,
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
 * Events queue processor implementation
 */
export class EventsQueueProcessor implements QueueProcessor<EventQueueMessage> {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'events-processor' });
    this.db = database;
  }

  /**
   * Process event message
   */
  async process(
    message: EventQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.debug(`Processing event message`, {
        eventId: message.polymarketId,
        slug: message.slug,
        correlationId: message.correlationId,
      });

      // Check if event already exists (idempotency)
      const existingEvent = await this.findExistingEvent(message.polymarketId);

      if (existingEvent) {
        // Update existing event
        await this.updateEvent(existingEvent.id, message);

        this.logger.info(`Updated existing event`, {
          eventId: existingEvent.id,
          polymarketId: message.polymarketId,
          slug: message.slug,
        });

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'updated',
            eventId: existingEvent.id,
            polymarketId: message.polymarketId,
            processingTime: Date.now() - startTime,
          },
        };
      } else {
        // Create new event
        const eventId = await this.createEvent(message);

        this.logger.info(`Created new event`, {
          eventId,
          polymarketId: message.polymarketId,
          slug: message.slug,
        });

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 1,
          metadata: {
            action: 'created',
            eventId,
            polymarketId: message.polymarketId,
            processingTime: Date.now() - startTime,
          },
        };
      }
    } catch (error) {
      this.logger.error(`Failed to process event message`, {
        error: error as Error,
        eventId: message.polymarketId,
        slug: message.slug,
        correlationId: message.correlationId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'EVENT_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            eventId: message.polymarketId,
            slug: message.slug,
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
   * Validate event message
   */
  async validate(message: EventQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.polymarketId || !message.slug || !message.title || !message.category) {
        this.logger.warn(`Event message missing required fields`, {
          polymarketId: message.polymarketId,
          slug: message.slug,
          title: message.title,
          category: message.category as any,
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

      // Validate slug format
      if (!/^[a-z0-9-]+$/.test(message.slug)) {
        this.logger.warn(`Invalid slug format`, {
          slug: message.slug,
        });
        return false;
      }

      // Validate category
      const validCategories = ['politics', 'sports', 'entertainment', 'business', 'technology', 'science', 'other'];
      if (!validCategories.includes(message.category.toLowerCase())) {
        this.logger.warn(`Invalid event category`, {
          category: message.category as any,
        });
        return false;
      }

      // Validate dates
      if (message.resolutionDate) {
        const resolutionDate = new Date(message.resolutionDate);
        if (isNaN(resolutionDate.getTime())) {
          this.logger.warn(`Invalid resolution date`, {
            resolutionDate: message.resolutionDate,
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

      return true;
    } catch (error) {
      this.logger.error(`Error during event message validation`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      return false;
    }
  }

  /**
   * Transform event message if needed
   */
  async transform(message: EventQueueMessage): Promise<EventQueueMessage> {
    try {
      // Normalize category to lowercase
      const transformed = {
        ...message,
        category: message.category.toLowerCase(),
        subcategory: message.subcategory?.toLowerCase(),
      };

      // Normalize timestamps
      if (transformed.createdAt && !transformed.createdAt.includes('Z')) {
        transformed.createdAt = new Date(transformed.createdAt).toISOString();
      }
      if (transformed.updatedAt && !transformed.updatedAt.includes('Z')) {
        transformed.updatedAt = new Date(transformed.updatedAt).toISOString();
      }
      if (transformed.resolutionDate && !transformed.resolutionDate.includes('Z')) {
        transformed.resolutionDate = new Date(transformed.resolutionDate).toISOString();
      }

      // Ensure numeric fields are properly formatted
      transformed.liquidity = parseFloat(transformed.liquidity.toFixed(8));
      if (transformed.volume !== undefined) {
        transformed.volume = parseFloat(transformed.volume.toFixed(8));
      }

      // Process tags array
      if (transformed.tags && Array.isArray(transformed.tags)) {
        transformed.tags = transformed.tags
          .filter(tag => typeof tag === 'string' && tag.trim().length > 0)
          .map(tag => tag.trim().toLowerCase());
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming event message`, {
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
    message: EventQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.debug(`Starting event processing`, {
      polymarketId: message.polymarketId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update processing metrics in Redis
    await redisUtils.increment('events_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: EventQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      this.logger.debug(`Event processing completed successfully`, {
        polymarketId: message.polymarketId,
        action: result.metadata?.action,
        processingTime: result.metadata?.processingTime,
      });

      // Update success metrics
      await redisUtils.increment('events_processed_count');

      // Cache event data for quick access
      await this.cacheEventData(message, result.metadata?.eventId);
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: EventQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Event processing error`, {
      error: error,
      polymarketId: message.polymarketId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update error metrics
    await redisUtils.increment('events_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: EventQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying event processing`, {
      polymarketId: message.polymarketId,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('events_retry_count');
  }

  /**
   * Called when message goes to dead letter queue
   */
  async onDeadLetter(
    message: EventQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Event message sent to dead letter queue`, {
      polymarketId: message.polymarketId,
      slug: message.slug,
      error: error,
      attempts: context.attempt,
    });

    // Update dead letter metrics
    await redisUtils.increment('events_deadletter_count');
  }

  /**
   * Find existing event by polymarket ID
   */
  private async findExistingEvent(polymarketId: string): Promise<any> {
    try {
      const query = `
        SELECT id, polymarket_id, slug, title, category, updated_at
        FROM events
        WHERE polymarket_id = $1
        LIMIT 1
      `;

      const result = await this.db.query(query, [polymarketId]);
      return result.rows[0] || null;
    } catch (error) {
      this.logger.error(`Failed to find existing event`, {
        error: error as Error,
        polymarketId,
      });
      throw error;
    }
  }

  /**
   * Create new event in database
   */
  private async createEvent(message: EventQueueMessage): Promise<string> {
    try {
      const query = `
        INSERT INTO events (
          polymarket_id, slug, title, description, category, subcategory,
          liquidity, volume, active, closed, resolution_date, image_url,
          tags, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        ) RETURNING id
      `;

      const values = [
        message.polymarketId,
        message.slug,
        message.title,
        message.description || null,
        message.category,
        message.subcategory || null,
        message.liquidity,
        message.volume || null,
        message.active,
        message.closed,
        message.resolutionDate || null,
        message.imageUrl || null,
        message.tags ? JSON.stringify(message.tags) : null,
        message.createdAt,
        message.updatedAt,
      ];

      const result = await this.db.query(query, values);
      return result.rows[0].id;
    } catch (error) {
      this.logger.error(`Failed to create event`, {
        error: error as Error,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Update existing event in database
   */
  private async updateEvent(eventId: string, message: EventQueueMessage): Promise<void> {
    try {
      const query = `
        UPDATE events SET
          slug = $2,
          title = $3,
          description = $4,
          category = $5,
          subcategory = $6,
          liquidity = $7,
          volume = $8,
          active = $9,
          closed = $10,
          resolution_date = $11,
          image_url = $12,
          tags = $13,
          updated_at = $14
        WHERE id = $1
      `;

      const values = [
        eventId,
        message.slug,
        message.title,
        message.description || null,
        message.category,
        message.subcategory || null,
        message.liquidity,
        message.volume || null,
        message.active,
        message.closed,
        message.resolutionDate || null,
        message.imageUrl || null,
        message.tags ? JSON.stringify(message.tags) : null,
        message.updatedAt,
      ];

      await this.db.query(query, values);
    } catch (error) {
      this.logger.error(`Failed to update event`, {
        error: error as Error,
        eventId,
        polymarketId: message.polymarketId,
      });
      throw error;
    }
  }

  /**
   * Cache event data in Redis
   */
  private async cacheEventData(message: EventQueueMessage, eventId?: string): Promise<void> {
    try {
      if (!eventId) return;

      const cacheKey = redisUtils.getCacheKey('event', message.polymarketId);
      const cacheData = {
        id: eventId,
        polymarketId: message.polymarketId,
        slug: message.slug,
        title: message.title,
        category: message.category,
        active: message.active,
        closed: message.closed,
        cachedAt: new Date().toISOString(),
      };

      await redisUtils.hSet(cacheKey, 'data', JSON.stringify(cacheData));

      // Set TTL for cache (5 minutes)
      // Note: This would need to be implemented in the Redis client
      // await this.redis.expire(cacheKey, 300);
    } catch (error) {
      this.logger.warn(`Failed to cache event data`, {
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
 * Events queue implementation
 */
export class EventsQueue extends BaseQueue<EventQueueMessage> {
  private processor: EventsQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.events>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.events, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new EventsQueueProcessor(this.logger);
  }

  /**
   * Get the events processor
   */
  protected getProcessor(): QueueProcessor<EventQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is an event message
   */
  protected deserializeMessage(streamMessage: any): EventQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isEventMessage(message)) {
      throw new Error(`Invalid event message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to events
   */
  async getEventStatistics(): Promise<{
    totalEvents: number;
    activeEvents: number;
    closedEvents: number;
    eventsByCategory: Record<string, number>;
    averageLiquidity: number;
    recentActivity: {
      created: number;
      updated: number;
    };
  }> {
    try {
      // This would typically query the database for statistics
      // For now, return a placeholder implementation
      return {
        totalEvents: 0,
        activeEvents: 0,
        closedEvents: 0,
        eventsByCategory: {},
        averageLiquidity: 0,
        recentActivity: {
          created: 0,
          updated: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get event statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }
}