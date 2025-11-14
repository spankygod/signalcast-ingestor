/**
 * Updates queue processor for SignalCast system
 * Handles general update processing including batch operations, system maintenance, and error corrections
 */

import { BaseQueue } from './base-queue';
import {
  UpdateQueueMessage,
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
 * Update operation handlers
 */
interface UpdateHandler {
  canHandle(updateType: string): boolean;
  handle(message: UpdateQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult>;
}

/**
 * Market creation update handler
 */
class MarketCreationHandler implements UpdateHandler {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger, db: any) {
    this.logger = logger.child({ component: 'market-creation-handler' });
    this.db = db;
  }

  canHandle(updateType: string): boolean {
    return updateType === 'market_created';
  }

  async handle(message: UpdateQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult> {
    try {
      const marketData = message.changes;

      // Validate required market fields
      if (!marketData.polymarketId || !marketData.eventId || !marketData.question) {
        throw new Error('Market creation update missing required fields');
      }

      // Check if market already exists
      const existingQuery = `SELECT id FROM markets WHERE polymarket_id = $1 LIMIT 1`;
      const existingResult = await this.db.query(existingQuery, [marketData.polymarketId]);

      if (existingResult.rows.length > 0) {
        this.logger.info(`Market already exists, skipping creation`, {
          polymarketId: marketData.polymarketId,
        });

        return {
          success: true,
          status: QueueProcessingStatus.COMPLETED,
          processedItems: 0,
          metadata: {
            action: 'skipped_duplicate',
            polymarketId: marketData.polymarketId,
          },
        };
      }

      // Create market
      const insertQuery = `
        INSERT INTO markets (
          polymarket_id, event_id, question, description, liquidity,
          current_price, active, closed, resolved, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING id
      `;

      const values = [
        marketData.polymarketId,
        marketData.eventId,
        marketData.question,
        marketData.description || null,
        marketData.liquidity || 0,
        marketData.currentPrice || 0.5,
        marketData.active !== false,
        marketData.closed || false,
        marketData.resolved || false,
        marketData.createdAt || new Date().toISOString(),
        marketData.updatedAt || new Date().toISOString(),
      ];

      const result = await this.db.query(insertQuery, values);

      this.logger.info(`Created market from update`, {
        marketId: result.rows[0].id,
        polymarketId: marketData.polymarketId,
        question: marketData.question,
      });

      return {
        success: true,
        status: QueueProcessingStatus.COMPLETED,
        processedItems: 1,
        metadata: {
          action: 'created_market',
          marketId: result.rows[0].id,
          polymarketId: marketData.polymarketId,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to handle market creation update`, {
        error: error as Error,
        entityId: message.entityId,
      });
      throw error;
    }
  }
}

/**
 * Market resolution update handler
 */
class MarketResolutionHandler implements UpdateHandler {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger, db: any) {
    this.logger = logger.child({ component: 'market-resolution-handler' });
    this.db = db;
  }

  canHandle(updateType: string): boolean {
    return updateType === 'market_resolved';
  }

  async handle(message: UpdateQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult> {
    try {
      const resolutionData = message.changes;

      if (!resolutionData.resolution) {
        throw new Error('Market resolution update missing resolution data');
      }

      // Find market by polymarket ID or internal ID
      let marketQuery = `SELECT id, polymarket_id FROM markets WHERE `;
      let queryParams: any[] = [];

      if (message.entityId.startsWith('0x')) {
        marketQuery += `polymarket_id = $1`;
        queryParams.push(message.entityId);
      } else {
        marketQuery += `id = $1`;
        queryParams.push(message.entityId);
      }

      const marketResult = await this.db.query(marketQuery, queryParams);

      if (marketResult.rows.length === 0) {
        throw new Error(`Market not found: ${message.entityId}`);
      }

      const market = marketResult.rows[0];

      // Update market resolution
      const updateQuery = `
        UPDATE markets SET
          resolved = true,
          closed = true,
          active = false,
          resolution = $2,
          resolved_at = $3,
          updated_at = $4
        WHERE id = $1
      `;

      await this.db.query(updateQuery, [
        market.id,
        resolutionData.resolution,
        resolutionData.resolvedAt || new Date().toISOString(),
        new Date().toISOString(),
      ]);

      // Update outcomes for this market
      if (resolutionData.outcomeResolutions) {
        await this.updateOutcomeResolutions(market.id, resolutionData.outcomeResolutions);
      }

      this.logger.info(`Resolved market`, {
        marketId: market.id,
        polymarketId: market.polymarketId,
        resolution: resolutionData.resolution,
      });

      return {
        success: true,
        status: QueueProcessingStatus.COMPLETED,
        processedItems: 1,
        metadata: {
          action: 'resolved_market',
          marketId: market.id,
          polymarketId: market.polymarketId,
          resolution: resolutionData.resolution,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to handle market resolution update`, {
        error: error as Error,
        entityId: message.entityId,
      });
      throw error;
    }
  }

  private async updateOutcomeResolutions(marketId: string, outcomeResolutions: Record<string, any>): Promise<void> {
    for (const [outcomeId, resolution] of Object.entries(outcomeResolutions)) {
      const updateQuery = `
        UPDATE outcomes SET
          status = 'resolved',
          resolution = $2,
          updated_at = $3
        WHERE market_id = $1 AND (polymarket_id = $4 OR id = $4)
      `;

      await this.db.query(updateQuery, [
        marketId,
        resolution.resolution,
        new Date().toISOString(),
        outcomeId,
      ]);
    }
  }
}

/**
 * Batch import update handler
 */
class BatchImportHandler implements UpdateHandler {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger, db: any) {
    this.logger = logger.child({ component: 'batch-import-handler' });
    this.db = db;
  }

  canHandle(updateType: string): boolean {
    return updateType === 'batch_import';
  }

  async handle(message: UpdateQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult> {
    try {
      const batchData = message.changes;
      const { items = [], entityType } = batchData;

      if (!Array.isArray(items) || items.length === 0) {
        throw new Error('Batch import update contains no items');
      }

      let processedCount = 0;
      let errorCount = 0;
      const errors: Array<{ code: string; message: string; details?: any }> = [];

      this.logger.info(`Processing batch import`, {
        batchId: message.batchId,
        entityType,
        itemCount: items.length,
      });

      // Process items in batches for better performance
      const batchSize = 100;
      for (let i = 0; i < items.length; i += batchSize) {
        const batch = items.slice(i, i + batchSize);

        try {
          const batchResult = await this.processBatch(batch, entityType);
          processedCount += batchResult.processedCount;
          errorCount += batchResult.errorCount;
          errors.push(...batchResult.errors);
        } catch (error) {
          errorCount += batch.length;
          errors.push({
            code: 'BATCH_PROCESSING_ERROR',
            message: (error as Error).message,
            details: { batchIndex: i, batchSize: batch.length },
          });
        }
      }

      this.logger.info(`Batch import completed`, {
        batchId: message.batchId,
        entityType,
        processedCount,
        errorCount,
        totalItems: items.length,
      });

      return {
        success: errorCount === 0,
        status: errorCount === 0 ? QueueProcessingStatus.COMPLETED : QueueProcessingStatus.FAILED,
        processedItems: processedCount,
        errors: errors.length > 0 ? errors : undefined,
        metadata: {
          action: 'batch_import',
          batchId: message.batchId,
          entityType,
          totalItems: items.length,
          processedCount,
          errorCount,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to handle batch import update`, {
        error: error as Error,
        batchId: message.batchId,
      });
      throw error;
    }
  }

  private async processBatch(items: any[], entityType: string): Promise<{
    processedCount: number;
    errorCount: number;
    errors: Array<{ code: string; message: string; details?: any }>;
  }> {
    let processedCount = 0;
    let errorCount = 0;
    const errors: Array<{ code: string; message: string; details?: any }> = [];

    // Start transaction
    const client = await this.db.connect();

    try {
      await client.query('BEGIN');

      for (const item of items) {
        try {
          switch (entityType) {
            case 'events':
              await this.insertEvent(client, item);
              break;
            case 'markets':
              await this.insertMarket(client, item);
              break;
            case 'outcomes':
              await this.insertOutcome(client, item);
              break;
            default:
              throw new Error(`Unknown entity type in batch import: ${entityType}`);
          }
          processedCount++;
        } catch (error) {
          errorCount++;
          errors.push({
            code: 'ITEM_PROCESSING_ERROR',
            message: (error as Error).message,
            details: { entityType, item: item.id || item.polymarketId },
          });
        }
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    return { processedCount, errorCount, errors };
  }

  private async insertEvent(client: any, eventData: any): Promise<void> {
    const query = `
      INSERT INTO events (
        polymarket_id, slug, title, description, category, subcategory,
        liquidity, volume, active, closed, resolution_date, image_url,
        tags, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
      ) ON CONFLICT (polymarket_id) DO NOTHING
    `;

    await client.query(query, [
      eventData.polymarketId,
      eventData.slug,
      eventData.title,
      eventData.description || null,
      eventData.category,
      eventData.subcategory || null,
      eventData.liquidity || 0,
      eventData.volume || null,
      eventData.active !== false,
      eventData.closed || false,
      eventData.resolutionDate || null,
      eventData.imageUrl || null,
      eventData.tags ? JSON.stringify(eventData.tags) : null,
      eventData.createdAt || new Date().toISOString(),
      eventData.updatedAt || new Date().toISOString(),
    ]);
  }

  private async insertMarket(client: any, marketData: any): Promise<void> {
    const query = `
      INSERT INTO markets (
        polymarket_id, event_id, question, description, liquidity,
        current_price, active, closed, resolved, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      ) ON CONFLICT (polymarket_id) DO NOTHING
    `;

    await client.query(query, [
      marketData.polymarketId,
      marketData.eventId,
      marketData.question,
      marketData.description || null,
      marketData.liquidity || 0,
      marketData.currentPrice || 0.5,
      marketData.active !== false,
      marketData.closed || false,
      marketData.resolved || false,
      marketData.createdAt || new Date().toISOString(),
      marketData.updatedAt || new Date().toISOString(),
    ]);
  }

  private async insertOutcome(client: any, outcomeData: any): Promise<void> {
    const query = `
      INSERT INTO outcomes (
        polymarket_id, market_id, event_id, title, description, price,
        probability, liquidity, status, created_at, updated_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      ) ON CONFLICT (polymarket_id) DO NOTHING
    `;

    await client.query(query, [
      outcomeData.polymarketId,
      outcomeData.marketId,
      outcomeData.eventId,
      outcomeData.title,
      outcomeData.description || null,
      outcomeData.price || 0.5,
      outcomeData.probability || 0.5,
      outcomeData.liquidity || 0,
      outcomeData.status || 'active',
      outcomeData.createdAt || new Date().toISOString(),
      outcomeData.updatedAt || new Date().toISOString(),
    ]);
  }
}

/**
 * Error correction update handler
 */
class ErrorCorrectionHandler implements UpdateHandler {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger, db: any) {
    this.logger = logger.child({ component: 'error-correction-handler' });
    this.db = db;
  }

  canHandle(updateType: string): boolean {
    return updateType === 'error_correction';
  }

  async handle(message: UpdateQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult> {
    try {
      const correctionData = message.changes;
      const { entityType, corrections = [] } = correctionData;

      if (!entityType || !Array.isArray(corrections)) {
        throw new Error('Error correction update missing required fields');
      }

      let processedCount = 0;
      let errorCount = 0;
      const errors: Array<{ code: string; message: string; details?: any }> = [];

      this.logger.info(`Processing error correction`, {
        entityType,
        correctionCount: corrections.length,
        reason: message.reason,
      });

      for (const correction of corrections) {
        try {
          await this.applyCorrection(entityType, correction);
          processedCount++;
        } catch (error) {
          errorCount++;
          errors.push({
            code: 'CORRECTION_FAILED',
            message: (error as Error).message,
            details: { entityType, entityId: correction.entityId },
          });
        }
      }

      this.logger.info(`Error correction completed`, {
        entityType,
        processedCount,
        errorCount,
        totalCorrections: corrections.length,
      });

      return {
        success: errorCount === 0,
        status: errorCount === 0 ? QueueProcessingStatus.COMPLETED : QueueProcessingStatus.FAILED,
        processedItems: processedCount,
        errors: errors.length > 0 ? errors : undefined,
        metadata: {
          action: 'error_correction',
          entityType,
          totalCorrections: corrections.length,
          processedCount,
          errorCount,
          reason: message.reason,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to handle error correction update`, {
        error: error as Error,
        entityId: message.entityId,
      });
      throw error;
    }
  }

  private async applyCorrection(entityType: string, correction: any): Promise<void> {
    const { entityId, field, oldValue, newValue, reason } = correction;

    let tableName: string;
    let idField: string;

    switch (entityType) {
      case 'events':
        tableName = 'events';
        idField = 'polymarket_id';
        break;
      case 'markets':
        tableName = 'markets';
        idField = 'polymarket_id';
        break;
      case 'outcomes':
        tableName = 'outcomes';
        idField = 'polymarket_id';
        break;
      default:
        throw new Error(`Unknown entity type for correction: ${entityType}`);
    }

    // Verify current value matches expected old value
    const verifyQuery = `SELECT ${field} FROM ${tableName} WHERE ${idField} = $1`;
    const verifyResult = await this.db.query(verifyQuery, [entityId]);

    if (verifyResult.rows.length === 0) {
      throw new Error(`Entity not found: ${entityId}`);
    }

    const currentValue = verifyResult.rows[0][field];
    if (JSON.stringify(currentValue) !== JSON.stringify(oldValue)) {
      this.logger.warn(`Value mismatch during correction`, {
        entityId,
        field,
        expected: oldValue,
        actual: currentValue,
      });
    }

    // Apply correction
    const updateQuery = `UPDATE ${tableName} SET ${field} = $2, updated_at = $3 WHERE ${idField} = $1`;
    await this.db.query(updateQuery, [entityId, newValue, new Date().toISOString()]);

    // Log correction for audit
    const auditQuery = `
      INSERT INTO correction_audit (
        entity_type, entity_id, field, old_value, new_value, reason,
        corrected_by, corrected_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `;

    await this.db.query(auditQuery, [
      entityType,
      entityId,
      field,
      JSON.stringify(oldValue),
      JSON.stringify(newValue),
      reason || 'Error correction',
      'system',
      new Date().toISOString(),
    ]);
  }
}

/**
 * Updates queue processor
 */
export class UpdatesQueueProcessor implements QueueProcessor<UpdateQueueMessage> {
  private logger: Logger;
  private db: any;
  private handlers: UpdateHandler[];

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'updates-processor' });
    this.db = database;

    // Initialize update handlers
    this.handlers = [
      new MarketCreationHandler(this.logger, this.db),
      new MarketResolutionHandler(this.logger, this.db),
      new BatchImportHandler(this.logger, this.db),
      new ErrorCorrectionHandler(this.logger, this.db),
    ];
  }

  /**
   * Process update message
   */
  async process(
    message: UpdateQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.debug(`Processing update message`, {
        updateType: message.updateType,
        entityId: message.entityId,
        entityType: message.entityType,
        priority: message.priority,
        correlationId: message.correlationId,
      });

      // Find appropriate handler
      const handler = this.handlers.find(h => h.canHandle(message.updateType));

      if (!handler) {
        throw new Error(`No handler found for update type: ${message.updateType}`);
      }

      // Process based on priority
      if (message.priority === 'critical') {
        this.logger.warn(`Processing critical update`, {
          updateType: message.updateType,
          entityId: message.entityId,
          reason: message.reason,
        });
      }

      const result = await handler.handle(message, context);

      this.logger.info(`Update processing completed`, {
        updateType: message.updateType,
        entityId: message.entityId,
        success: result.success,
        processedItems: result.processedItems,
      });

      return {
        ...result,
        metadata: {
          ...result.metadata,
          processingTime: Date.now() - startTime,
          updateType: message.updateType,
          priority: message.priority,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to process update message`, {
        error: error as Error,
        updateType: message.updateType,
        entityId: message.entityId,
        correlationId: message.correlationId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'UPDATE_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            updateType: message.updateType,
            entityId: message.entityId,
            entityType: message.entityType,
          },
        }],
        metadata: {
          processingTime: Date.now() - startTime,
        },
        shouldRetry: this.shouldRetry(error as Error, message),
      };
    }
  }

  /**
   * Validate update message
   */
  async validate(message: UpdateQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.updateType || !message.entityId || !message.entityType) {
        this.logger.warn(`Update message missing required fields`, {
          updateType: message.updateType,
          entityId: message.entityId,
          entityType: message.entityType,
        });
        return false;
      }

      // Validate update type
      const validUpdateTypes = [
        'market_created', 'market_updated', 'market_closed', 'market_resolved',
        'outcome_added', 'outcome_updated', 'price_change', 'liquidity_change',
        'batch_import', 'system_maintenance', 'error_correction'
      ];

      if (!validUpdateTypes.includes(message.updateType)) {
        this.logger.warn(`Invalid update type`, {
          updateType: message.updateType,
        });
        return false;
      }

      // Validate entity type
      const validEntityTypes = ['event', 'market', 'outcome', 'tick', 'system'];
      if (!validEntityTypes.includes(message.entityType)) {
        this.logger.warn(`Invalid entity type`, {
          entityType: message.entityType,
        });
        return false;
      }

      // Validate priority
      const validPriorities = ['low', 'normal', 'high', 'critical'];
      if (!validPriorities.includes(message.priority)) {
        this.logger.warn(`Invalid priority`, {
          priority: message.priority,
        });
        return false;
      }

      // Validate changes object
      if (!message.changes || typeof message.changes !== 'object') {
        this.logger.warn(`Invalid changes object`, {
          changes: message.changes,
        });
        return false;
      }

      // Specific validations based on update type
      if (message.updateType === 'batch_import') {
        if (!Array.isArray(message.changes.items) || message.changes.items.length === 0) {
          this.logger.warn(`Batch import missing items array`);
          return false;
        }
      }

      if (message.updateType === 'market_resolved') {
        if (!message.changes.resolution) {
          this.logger.warn(`Market resolution missing resolution data`);
          return false;
        }
      }

      return true;
    } catch (error) {
      this.logger.error(`Error during update message validation`, {
        error: error as Error,
        updateType: message.updateType,
      });
      return false;
    }
  }

  /**
   * Transform update message if needed
   */
  async transform(message: UpdateQueueMessage): Promise<UpdateQueueMessage> {
    try {
      const transformed = { ...message };

      // Normalize update type and entity type
      transformed.updateType = transformed.updateType.toLowerCase() as any;
      transformed.entityType = transformed.entityType.toLowerCase() as any;
      transformed.priority = transformed.priority.toLowerCase() as any;

      // Ensure timestamp format
      if (transformed.timestamp && !transformed.timestamp.includes('Z')) {
        transformed.timestamp = new Date(transformed.timestamp).toISOString();
      }

      // Process changes based on update type
      if (transformed.updateType === 'batch_import' && transformed.changes.items) {
        // Normalize batch items
        transformed.changes.items = transformed.changes.items.map(item => {
          if (item.createdAt && !item.createdAt.includes('Z')) {
            item.createdAt = new Date(item.createdAt).toISOString();
          }
          if (item.updatedAt && !item.updatedAt.includes('Z')) {
            item.updatedAt = new Date(item.updatedAt).toISOString();
          }
          return item;
        });
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming update message`, {
        error: error as Error,
        updateType: message.updateType,
      });
      throw error;
    }
  }

  /**
   * Called before processing
   */
  async onBeforeProcess(
    message: UpdateQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.debug(`Starting update processing`, {
      updateType: message.updateType,
      entityId: message.entityId,
      priority: message.priority,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update processing metrics in Redis
    await redisUtils.increment('updates_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: UpdateQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      this.logger.debug(`Update processing completed successfully`, {
        updateType: message.updateType,
        action: result.metadata?.action,
        processedItems: result.processedItems,
        processingTime: result.metadata?.processingTime,
      });

      // Update success metrics
      await redisUtils.increment('updates_processed_count');

      // Update queue priority metrics
      if (message.priority === 'critical') {
        await redisUtils.increment('updates_critical_processed');
      }
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: UpdateQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Update processing error`, {
      error: error,
      updateType: message.updateType,
      entityId: message.entityId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update error metrics
    await redisUtils.increment('updates_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: UpdateQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying update processing`, {
      updateType: message.updateType,
      entityId: message.entityId,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('updates_retry_count');
  }

  /**
   * Called when message goes to dead letter queue
   */
  async onDeadLetter(
    message: UpdateQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Update message sent to dead letter queue`, {
      updateType: message.updateType,
      entityId: message.entityId,
      error: error,
      attempts: context.attempt,
    });

    // Update dead letter metrics
    await redisUtils.increment('updates_deadletter_count');
  }

  /**
   * Determine if error is retryable based on update type and priority
   */
  private shouldRetry(error: Error, message: UpdateQueueMessage): boolean {
    const errorMessage = error.message.toLowerCase();

    // Don't retry validation errors
    if (errorMessage.includes('validation') || errorMessage.includes('invalid')) {
      return false;
    }

    // Don't retry data integrity errors for critical updates
    if (message.priority === 'critical' && errorMessage.includes('duplicate')) {
      return false;
    }

    // Retry most errors for updates, especially batch operations
    if (message.updateType === 'batch_import') {
      return true;
    }

    // Retry database connection errors
    if (errorMessage.includes('connection') || errorMessage.includes('timeout')) {
      return true;
    }

    // Retry transient errors
    if (errorMessage.includes('temporarily unavailable') || errorMessage.includes('try again')) {
      return true;
    }

    // Default to retry for updates
    return true;
  }
}

/**
 * Updates queue implementation
 */
export class UpdatesQueue extends BaseQueue<UpdateQueueMessage> {
  private processor: UpdatesQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.updates>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.updates, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new UpdatesQueueProcessor(this.logger);
  }

  /**
   * Get the updates processor
   */
  protected getProcessor(): QueueProcessor<UpdateQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is an update message
   */
  protected deserializeMessage(streamMessage: any): UpdateQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isUpdateMessage(message)) {
      throw new Error(`Invalid update message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to updates
   */
  async getUpdateStatistics(): Promise<{
    totalUpdates: number;
    updatesByType: Record<string, number>;
    updatesByPriority: Record<string, number>;
    batchImportsProcessed: number;
    errorCorrectionsApplied: number;
    recentActivity: {
      lastHour: number;
      lastDay: number;
      lastWeek: number;
    };
  }> {
    try {
      // This would typically query the database for statistics
      // For now, return a placeholder implementation
      return {
        totalUpdates: 0,
        updatesByType: {},
        updatesByPriority: {},
        batchImportsProcessed: 0,
        errorCorrectionsApplied: 0,
        recentActivity: {
          lastHour: 0,
          lastDay: 0,
          lastWeek: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get update statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }
}