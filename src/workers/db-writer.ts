/**
 * Database Writer Worker
 * Processes messages from all queues and performs bulk UPSERT operations
 * CRITICAL: Uses bulk UPSERT with ON CONFLICT for all database operations
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { dbConnection, type BatchResult } from '../lib/db';
import { retryUtils } from '../lib/retry';
import { EventsQueue, MarketsQueue, OutcomesQueue, TicksQueue } from '../queues';
import { DeadletterQueue } from '../queues/dead-letter.queue';
import {
  type EventQueueMessage,
  type MarketQueueMessage,
  type OutcomeQueueMessage,
  type TickQueueMessage,
  type DeadletterQueueMessage,
  queueMessageGuards
} from '../queues/types';
import { normalizeEvent, type NormalizedEvent } from '../utils/normalizeEvent';
import { normalizeMarket, type NormalizedMarket } from '../utils/normalizeMarket';
import { normalizeTick, type NormalizedTick } from '../utils/normalizeTick';
import {
  config,
  features,
  REDIS_STREAMS,
  REDIS_CONSUMER_GROUPS
} from '../config';

/**
 * Database writer configuration
 */
interface DatabaseWriterConfig {
  batchSize: number;
  batchTimeoutMs: number;
  maxRetries: number;
  retryDelayMs: number;
  processingTimeoutMs: number;
  queueProcessingIntervalMs: number;
  enableDeadletter: boolean;
  idleSleepMs: number;
  healthCheckIntervalMs: number;
}

/**
 * Processing statistics
 */
interface ProcessingStats {
  totalMessages: number;
  successfulBatches: number;
  failedBatches: number;
  eventsProcessed: number;
  marketsProcessed: number;
  outcomesProcessed: number;
  ticksProcessed: number;
  errorsCount: number;
  deadletterMessages: number;
  averageProcessingTime: number;
  uptime: number;
  lastProcessedAt?: string;
  queueDepths: {
    events: number;
    markets: number;
    outcomes: number;
    ticks: number;
  };
  databaseOperations: {
    upserts: number;
    inserts: number;
    updates: number;
    errors: number;
  };
}

/**
 * Message batch for processing
 */
interface MessageBatch<T> {
  messages: T[];
  messageType: 'events' | 'markets' | 'outcomes' | 'ticks';
  receivedAt: number;
  queueName: string;
}

/**
 * Database writer worker class
 */
export class DatabaseWriterWorker {
  private logger: Logger;
  private config: DatabaseWriterConfig;
  private db: typeof dbConnection;
  private eventsQueue: EventsQueue;
  private marketsQueue: MarketsQueue;
  private outcomesQueue: OutcomesQueue;
  private ticksQueue: TicksQueue;
  private deadletterQueue: DeadletterQueue;

  private isRunning: boolean = false;
  private startTime: number;
  private stats: ProcessingStats;
  private processingTimers: Map<string, NodeJS.Timeout> = new Map();
  private healthCheckTimer: NodeJS.Timeout | null = null;

  constructor(
    eventsQueue: EventsQueue,
    marketsQueue: MarketsQueue,
    outcomesQueue: OutcomesQueue,
    ticksQueue: TicksQueue,
    config?: Partial<DatabaseWriterConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('db-writer', process.pid.toString());
    this.db = dbConnection;
    this.eventsQueue = eventsQueue;
    this.marketsQueue = marketsQueue;
    this.outcomesQueue = outcomesQueue;
    this.ticksQueue = ticksQueue;
    this.deadletterQueue = new DeadletterQueue();
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      batchSize: 100,
      batchTimeoutMs: 2000,
      maxRetries: 3,
      retryDelayMs: 1000,
      processingTimeoutMs: 30000,
      queueProcessingIntervalMs: 100,
      enableDeadletter: true,
      idleSleepMs: 1000,
      healthCheckIntervalMs: 30000,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalMessages: 0,
      successfulBatches: 0,
      failedBatches: 0,
      eventsProcessed: 0,
      marketsProcessed: 0,
      outcomesProcessed: 0,
      ticksProcessed: 0,
      errorsCount: 0,
      deadletterMessages: 0,
      averageProcessingTime: 0,
      uptime: 0,
      queueDepths: {
        events: 0,
        markets: 0,
        outcomes: 0,
        ticks: 0,
      },
      databaseOperations: {
        upserts: 0,
        inserts: 0,
        updates: 0,
        errors: 0,
      },
    };

    this.setupGracefulShutdown();
    this.logger.info('Database writer worker initialized', {
      config: this.config,
    });
  }

  /**
   * Start the database writer
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Database writer is already running');
      return;
    }

    this.logger.info('Starting database writer worker');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Test database connection
      const isHealthy = await this.db.healthCheck();
      if (!isHealthy) {
        throw new Error('Database health check failed');
      }

      // Start queue processors
      this.startQueueProcessors();

      // Start health monitoring
      this.startHealthMonitoring();

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('Database writer worker started successfully');

    } catch (error) {
      this.logger.error('Failed to start database writer worker', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the database writer
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Database writer is not running');
      return;
    }

    this.logger.info('Stopping database writer worker');
    this.isRunning = false;

    // Stop all timers
    this.processingTimers.forEach(timer => clearTimeout(timer));
    this.processingTimers.clear();

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    // Wait for current operations to complete
    await new Promise(resolve => setTimeout(resolve, 5000));

    this.logger.info('Database writer worker stopped', {
      finalStats: this.getStats(),
    });
  }

  /**
   * Start processing all queues
   */
  private startQueueProcessors(): void {
    // Events queue processor
    this.processingTimers.set('events', setInterval(async () => {
      if (this.isRunning) {
        await this.processEventsQueue();
      }
    }, this.config.queueProcessingIntervalMs));

    // Markets queue processor
    this.processingTimers.set('markets', setInterval(async () => {
      if (this.isRunning) {
        await this.processMarketsQueue();
      }
    }, this.config.queueProcessingIntervalMs));

    // Outcomes queue processor
    this.processingTimers.set('outcomes', setInterval(async () => {
      if (this.isRunning) {
        await this.processOutcomesQueue();
      }
    }, this.config.queueProcessingIntervalMs));

    // Ticks queue processor (higher frequency)
    this.processingTimers.set('ticks', setInterval(async () => {
      if (this.isRunning) {
        await this.processTicksQueue();
      }
    }, this.config.queueProcessingIntervalMs / 2)); // Process ticks more frequently

    this.logger.info('Queue processors started');
  }

  /**
   * Process events queue
   */
  private async processEventsQueue(): Promise<void> {
    const messages = await this.safeQueueRead<EventQueueMessage[]>(
      'events',
      () => this.eventsQueue.consumeMessages(this.config.batchSize)
    );

    if (!messages || messages.length === 0) {
      return;
    }

    this.logger.debug('Processing events batch', {
      messageCount: messages.length,
    });

    const batch: MessageBatch<EventQueueMessage> = {
      messages,
      messageType: 'events',
      receivedAt: Date.now(),
      queueName: 'events',
    };

    await this.safeBatchExecution(batch, 'events', () => this.processBatch(batch));
  }

  /**
   * Process markets queue
   */
  private async processMarketsQueue(): Promise<void> {
    const messages = await this.safeQueueRead<MarketQueueMessage[]>(
      'markets',
      () => this.marketsQueue.consumeMessages(this.config.batchSize)
    );

    if (!messages || messages.length === 0) {
      return;
    }

    this.logger.debug('Processing markets batch', {
      messageCount: messages.length,
    });

    const batch: MessageBatch<MarketQueueMessage> = {
      messages,
      messageType: 'markets',
      receivedAt: Date.now(),
      queueName: 'markets',
    };

    await this.safeBatchExecution(batch, 'markets', () => this.processBatch(batch));
  }

  /**
   * Process outcomes queue
   */
  private async processOutcomesQueue(): Promise<void> {
    const messages = await this.safeQueueRead<OutcomeQueueMessage[]>(
      'outcomes',
      () => this.outcomesQueue.consumeMessages(this.config.batchSize)
    );

    if (!messages || messages.length === 0) {
      return;
    }

    this.logger.debug('Processing outcomes batch', {
      messageCount: messages.length,
    });

    const batch: MessageBatch<OutcomeQueueMessage> = {
      messages,
      messageType: 'outcomes',
      receivedAt: Date.now(),
      queueName: 'outcomes',
    };

    await this.safeBatchExecution(batch, 'outcomes', () => this.processBatch(batch));
  }

  /**
   * Process ticks queue
   */
  private async processTicksQueue(): Promise<void> {
    const messages = await this.safeQueueRead<TickQueueMessage[]>(
      'ticks',
      () => this.ticksQueue.consumeMessages(this.config.batchSize * 2)
    );

    if (!messages || messages.length === 0) {
      return;
    }

    this.logger.debug('Processing ticks batch', {
      messageCount: messages.length,
    });

    const batch: MessageBatch<TickQueueMessage> = {
      messages,
      messageType: 'ticks',
      receivedAt: Date.now(),
      queueName: 'ticks',
    };

    await this.safeBatchExecution(batch, 'ticks', () => this.processBatch(batch));
  }

  /**
   * Execute queue read safely
   */
  private async safeQueueRead<T>(queueName: string, fn: () => Promise<T>): Promise<T | undefined> {
    try {
      return await fn();
    } catch (error) {
      this.logger.error(`Failed to read from ${queueName} queue`, {
        error: error as Error,
      });
      this.stats.errorsCount++;
      return undefined;
    }
  }

  /**
   * Execute batch handling safely with automatic dead-letter handling
   */
  private async safeBatchExecution(
    batch: MessageBatch<any>,
    operation: string,
    fn: () => Promise<void>
  ): Promise<void> {
    try {
      await fn();
    } catch (error) {
      await this.handleBatchFailure(batch, error as Error, operation);
    }
  }

  /**
   * Handle batch failures and forward to dead letter queue
   */
  private async handleBatchFailure(
    batch: MessageBatch<any>,
    error: Error,
    operation: string
  ): Promise<void> {
    this.logger.error(`Failed to process ${operation} batch`, {
      error,
      messageType: batch.messageType,
      queueName: batch.queueName,
    });

    this.stats.errorsCount++;
    this.stats.failedBatches++;

    if (!this.config.enableDeadletter || batch.messages.length === 0) {
      return;
    }

    await this.sendBatchToDeadLetter(batch, error);
  }

  /**
   * Send batch messages to dead letter queue
   */
  private async sendBatchToDeadLetter(batch: MessageBatch<any>, error: Error): Promise<void> {
    const streamName = this.getStreamNameForMessageType(batch.messageType);
    if (!streamName) {
      return;
    }

    const failureTimestamp = new Date().toISOString();
    const processingDuration = Date.now() - batch.receivedAt;

    for (const message of batch.messages) {
      const deadletterMessage: DeadletterQueueMessage = {
        id: `dlq_${batch.messageType}_${Date.now()}_${Math.random().toString(36).slice(2)}`,
        timestamp: failureTimestamp,
        originalStream: streamName,
        originalMessageId: (message as any).id ?? `unknown-${Date.now()}`,
        originalMessage: this.serializeForDeadLetter(message),
        error: error.message,
        errorCode: error.name,
        errorStack: error.stack,
        retryCount: 0,
        maxRetries: this.config.maxRetries,
        firstFailureTimestamp: failureTimestamp,
        lastFailureTimestamp: failureTimestamp,
        processingDuration,
        workerId: process.pid.toString(),
        consumerGroup: REDIS_CONSUMER_GROUPS.PRIMARY,
        severity: 'medium' as const,
      };

      try {
        await this.deadletterQueue.addMessage(deadletterMessage);
        this.stats.deadletterMessages++;
      } catch (deadLetterError) {
        this.logger.error('Failed to add message to dead letter queue', {
          error: deadLetterError as Error,
          originalMessageId: deadletterMessage.originalMessageId,
          streamName,
        });
      }
    }
  }

  private getStreamNameForMessageType(
    messageType: MessageBatch<any>['messageType']
  ): string | undefined {
    switch (messageType) {
      case 'events':
        return REDIS_STREAMS.EVENTS;
      case 'markets':
        return REDIS_STREAMS.MARKETS;
      case 'outcomes':
        return REDIS_STREAMS.OUTCOMES;
      case 'ticks':
        return REDIS_STREAMS.TICKS;
      default:
        return undefined;
    }
  }

  private serializeForDeadLetter(message: any): Record<string, string> {
    return Object.entries(message).reduce<Record<string, string>>((acc, [key, value]) => {
      if (value === null || value === undefined) {
        acc[key] = '';
      } else if (typeof value === 'string') {
        acc[key] = value;
      } else if (typeof value === 'number' || typeof value === 'boolean') {
        acc[key] = value.toString();
      } else {
        try {
          acc[key] = JSON.stringify(value);
        } catch {
          acc[key] = String(value);
        }
      }
      return acc;
    }, {});
  }

  /**
   * Process a batch of messages
   */
  private async processBatch<T>(batch: MessageBatch<T>): Promise<void> {
    const processingStartTime = Date.now();

    return this.logger.timed(`batch-process-${batch.messageType}`, LogCategory.WORKER, async () => {
      await retryUtils.withExponentialBackoff(
        async () => {
          let result: BatchResult;

          switch (batch.messageType) {
            case 'events':
              result = await this.processEventsBatch(batch as MessageBatch<EventQueueMessage>);
              this.stats.eventsProcessed += result.processed;
              break;

            case 'markets':
              result = await this.processMarketsBatch(batch as MessageBatch<MarketQueueMessage>);
              this.stats.marketsProcessed += result.processed;
              break;

            case 'outcomes':
              result = await this.processOutcomesBatch(batch as MessageBatch<OutcomeQueueMessage>);
              this.stats.outcomesProcessed += result.processed;
              break;

            case 'ticks':
              result = await this.processTicksBatch(batch as MessageBatch<TickQueueMessage>);
              this.stats.ticksProcessed += result.processed;
              break;

            default:
              throw new Error(`Unknown message type: ${batch.messageType}`);
          }

          // Update statistics
          this.stats.totalMessages += batch.messages.length;
          this.stats.successfulBatches++;
          this.stats.databaseOperations.upserts += result.inserted + result.updated;

          if (result.errors.length > 0) {
            this.stats.errorsCount += result.errors.length;
            await this.handleBatchErrors(batch, result.errors);
          }

          const processingTime = Date.now() - processingStartTime;
          this.updateAverageProcessingTime(processingTime);
          this.stats.lastProcessedAt = new Date().toISOString();

          this.logger.debug('Batch processed successfully', {
            messageType: batch.messageType,
            messageCount: batch.messages.length,
            processed: result.processed,
            inserted: result.inserted,
            updated: result.updated,
            errors: result.errors.length,
            processingTime,
          });

        },
        this.config.maxRetries,
        this.config.retryDelayMs,
        10000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Process events batch using bulk UPSERT
   */
  private async processEventsBatch(batch: MessageBatch<EventQueueMessage>): Promise<BatchResult> {
    // Validate and normalize events
    const validEvents = batch.messages.filter(message =>
      queueMessageGuards.isEventMessage(message)
    );

    if (validEvents.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    // Convert to database format
    const dbEvents = validEvents.map(message => ({
      polymarket_id: message.polymarketId,
      slug: message.slug,
      title: message.title,
      description: message.description,
      category: message.category,
      subcategory: message.subcategory,
      liquidity: message.liquidity,
      volume_24h: message.volume,
      active: message.active,
      closed: message.closed,
      archived: false,
      restricted: false,
      relevance_score: 0.5, // Would be calculated
      start_date: null,
      end_date: message.resolutionDate,
      last_ingested_at: new Date(),
      created_at: message.createdAt,
      updated_at: message.updatedAt,
    }));

    // Perform bulk UPSERT
    return await this.db.batchUpsertEvents(dbEvents);
  }

  /**
   * Process markets batch using bulk UPSERT
   */
  private async processMarketsBatch(batch: MessageBatch<MarketQueueMessage>): Promise<BatchResult> {
    // Validate and normalize markets
    const validMarkets = batch.messages.filter(message =>
      queueMessageGuards.isMarketMessage(message)
    );

    if (validMarkets.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    // Convert to database format
    const dbMarkets = validMarkets.map(message => ({
      polymarket_id: message.polymarketId,
      event_id: message.eventId,
      question: message.question,
      slug: message.slug || '', // Generate slug if missing
      description: message.description,
      liquidity: message.liquidity,
      volume_24h: message.volume24h,
      current_price: message.currentPrice,
      last_trade_price: message.bestBid,
      best_bid: message.bestBid,
      best_ask: message.bestAsk,
      active: message.active,
      closed: message.closed,
      archived: false,
      restricted: false,
      approved: true,
      status: message.resolved ? 'resolved' : 'active',
      resolved_at: message.resolved ? new Date() : null,
      relevance_score: 0.5, // Would be calculated
      start_date: message.startDate ? new Date(message.startDate) : null,
      end_date: message.endDate ? new Date(message.endDate) : null,
      last_ingested_at: new Date(),
      created_at: message.createdAt,
      updated_at: message.updatedAt,
    }));

    // Perform bulk UPSERT
    return await this.db.batchUpsertMarkets(dbMarkets);
  }

  /**
   * Process outcomes batch using bulk UPSERT
   */
  private async processOutcomesBatch(batch: MessageBatch<OutcomeQueueMessage>): Promise<BatchResult> {
    // Validate and normalize outcomes
    const validOutcomes = batch.messages.filter(message =>
      queueMessageGuards.isOutcomeMessage(message)
    );

    if (validOutcomes.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    // Convert to database format
    const dbOutcomes = validOutcomes.map(message => ({
      polymarket_id: message.polymarketId,
      market_id: message.marketId,
      title: message.title,
      description: message.description,
      price: message.price,
      probability: message.probability,
      volume: message.volume,
      status: message.status,
      display_order: 1, // Would be calculated
      created_at: message.createdAt,
      updated_at: message.updatedAt,
    }));

    // Perform bulk UPSERT
    return await this.db.batchUpsertOutcomes(dbOutcomes);
  }

  /**
   * Process ticks batch using bulk INSERT (append-only)
   */
  private async processTicksBatch(batch: MessageBatch<TickQueueMessage>): Promise<BatchResult> {
    // Validate and normalize ticks
    const validTicks = batch.messages.filter(message =>
      queueMessageGuards.isTickMessage(message)
    );

    if (validTicks.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    // Convert to database format
    const dbTicks = validTicks.map(message => ({
      market_id: message.marketId,
      price: message.price,
      best_bid: message.bestBid,
      best_ask: message.bestAsk,
      last_trade_price: message.bestBid,
      liquidity: message.liquidity,
      volume_24h: message.volume24h,
      updated_at: new Date(),
    }));

    // Perform bulk INSERT (ticks are append-only)
    return await this.db.batchInsertTicks(dbTicks);
  }

  /**
   * Handle batch errors by sending to dead letter queue
   */
  private async handleBatchErrors<T>(batch: MessageBatch<T>, errors: Array<{ data: any; error: Error }>): Promise<void> {
    if (!this.config.enableDeadletter || errors.length === 0) {
      return;
    }

    try {
      for (const error of errors) {
        const deadletterMessage = {
          id: `dlq_${batch.messageType}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          timestamp: new Date().toISOString(),
          originalStream: `sc:${batch.messageType}`,
          originalMessageId: error.data.id || 'unknown',
          originalMessage: error.data,
          error: error.error.message,
          errorCode: error.error.constructor.name,
          errorStack: error.error.stack,
          retryCount: 0,
          maxRetries: this.config.maxRetries,
          firstFailureTimestamp: new Date().toISOString(),
          lastFailureTimestamp: new Date().toISOString(),
          processingDuration: 0,
          workerId: process.pid.toString(),
          consumerGroup: 'signalcast',
          severity: 'medium' as const,
          action: 'retry' as const,
        };

        await this.deadletterQueue.addMessage(deadletterMessage);
        this.stats.deadletterMessages++;
      }

      this.logger.warn('Batch errors sent to dead letter queue', {
        messageType: batch.messageType,
        errorCount: errors.length,
      });

    } catch (dlqError) {
      this.logger.error('Failed to send errors to dead letter queue', {
        error: dlqError as Error,
      });
    }
  }

  /**
   * Update average processing time
   */
  private updateAverageProcessingTime(processingTime: number): void {
    const totalBatches = this.stats.successfulBatches + this.stats.failedBatches;
    const totalTime = this.stats.averageProcessingTime * (totalBatches - 1) + processingTime;
    this.stats.averageProcessingTime = totalTime / totalBatches;
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
      // Check database health
      const dbHealthy = await this.db.healthCheck();

      // Check queue depths (would need to be implemented in queue classes)
      this.stats.queueDepths = {
        events: 0, // Would be actual queue depth
        markets: 0,
        outcomes: 0,
        ticks: 0,
      };

      this.logger.debug('Health check completed', {
        databaseHealthy: dbHealthy,
        uptime: Date.now() - this.startTime,
        stats: this.stats,
      });

    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
    }
  }

  /**
   * Get current statistics
   */
  getStats(): ProcessingStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
    };
  }

  /**
   * Check if worker is healthy
   */
  isHealthy(): boolean {
    const now = Date.now();
    const timeSinceLastProcess = this.stats.lastProcessedAt
      ? now - new Date(this.stats.lastProcessedAt).getTime()
      : Infinity;

    // Consider healthy if processed something in last 30 seconds
    const recentProcessing = timeSinceLastProcess < 30000;

    // Consider healthy if error rate is below 10%
    const totalBatches = this.stats.successfulBatches + this.stats.failedBatches;
    const errorRate = totalBatches > 0
      ? this.stats.failedBatches / totalBatches
      : 0;
    const acceptableErrorRate = errorRate < 0.1;

    return this.isRunning && recentProcessing && acceptableErrorRate;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down database writer`);
      try {
        await this.stop();
        await this.db.close();
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
 * Create and initialize database writer worker
 */
export async function createDatabaseWriter(
  eventsQueue: EventsQueue,
  marketsQueue: MarketsQueue,
  outcomesQueue: OutcomesQueue,
  ticksQueue: TicksQueue,
  config?: Partial<DatabaseWriterConfig>
): Promise<DatabaseWriterWorker> {
  const worker = new DatabaseWriterWorker(
    eventsQueue,
    marketsQueue,
    outcomesQueue,
    ticksQueue,
    config
  );

  return worker;
}

/**
 * Standalone worker entry point
 */
export async function runDatabaseWriter(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('db-writer-main', {
      category: LogCategory.WORKER,
    });

    logger.info('Initializing database writer worker standalone');

    // Initialize queues
    const eventsQueue = new EventsQueue();
    const marketsQueue = new MarketsQueue();
    const outcomesQueue = new OutcomesQueue();
    const ticksQueue = new TicksQueue();

    // Create and start worker
    const worker = await createDatabaseWriter(
      eventsQueue,
      marketsQueue,
      outcomesQueue,
      ticksQueue
    );
    await worker.start();

    logger.info('Database writer worker running standalone');

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
    console.error('Failed to start database writer worker:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runDatabaseWriter().catch((error) => {
    console.error('Database writer worker failed:', error);
    process.exit(1);
  });
}

export default DatabaseWriterWorker;
