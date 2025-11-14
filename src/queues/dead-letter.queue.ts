/**
 * Dead letter queue processor for SignalCast system
 * Handles failed messages with retry logic, analysis, and recovery strategies
 */

import { BaseQueue } from './base-queue';
import {
  DeadletterQueueMessage,
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
 * Dead letter action types
 */
enum DeadLetterAction {
  RETRY = 'retry',
  IGNORE = 'ignore',
  MANUAL_REVIEW = 'manual_review',
  ARCHIVE = 'archive',
}

/**
 * Dead letter analysis result
 */
interface DeadLetterAnalysis {
  action: DeadLetterAction;
  reason: string;
  retryable: boolean;
  priority: 'low' | 'medium' | 'high' | 'critical';
  estimatedRetryDelay: number;
  recommendations: string[];
}

/**
 * Dead letter recovery strategies
 */
class DeadLetterRecoveryStrategies {
  private logger: Logger;
  private db: any;

  constructor(logger: Logger, db: any) {
    this.logger = logger.child({ component: 'deadletter-recovery' });
    this.db = db;
  }

  /**
   * Analyze dead letter message and determine recovery action
   */
  async analyze(message: DeadletterQueueMessage): Promise<DeadLetterAnalysis> {
    const error = message.error.toLowerCase();
    const originalStream = message.originalStream;

    this.logger.debug(`Analyzing dead letter message`, {
      originalStream,
      originalMessageId: message.originalMessageId,
      error: new Error(message.error),
      retryCount: message.retryCount,
    });

    // Analyze error type and determine action
    if (this.isValidationError(error)) {
      return {
        action: DeadLetterAction.IGNORE,
        reason: 'Validation error - message format is invalid',
        retryable: false,
        priority: 'low',
        estimatedRetryDelay: 0,
        recommendations: ['Fix message format at source', 'Update validation rules'],
      };
    }

    if (this.isDataIntegrityError(error)) {
      return {
        action: DeadLetterAction.MANUAL_REVIEW,
        reason: 'Data integrity error - requires manual investigation',
        retryable: false,
        priority: 'high',
        estimatedRetryDelay: 0,
        recommendations: ['Review data consistency', 'Check related records'],
      };
    }

    if (this.isDependencyError(error)) {
      if (message.retryCount < 3) {
        return {
          action: DeadLetterAction.RETRY,
          reason: 'Dependency error - service temporarily unavailable',
          retryable: true,
          priority: 'medium',
          estimatedRetryDelay: this.calculateRetryDelay(message.retryCount),
          recommendations: ['Check external service status', 'Verify network connectivity'],
        };
      } else {
        return {
          action: DeadLetterAction.MANUAL_REVIEW,
          reason: 'Repeated dependency errors - manual intervention required',
          retryable: false,
          priority: 'high',
          estimatedRetryDelay: 0,
          recommendations: ['Investigate external service issues', 'Consider circuit breaker status'],
        };
      }
    }

    if (this.isTransientError(error)) {
      return {
        action: DeadLetterAction.RETRY,
        reason: 'Transient error - likely temporary issue',
        retryable: true,
        priority: 'medium',
        estimatedRetryDelay: this.calculateRetryDelay(message.retryCount),
        recommendations: ['Monitor system health', 'Check resource availability'],
      };
    }

    if (this.isTimeoutError(error)) {
      if (message.retryCount < 5) {
        return {
          action: DeadLetterAction.RETRY,
          reason: 'Timeout error - operation took too long',
          retryable: true,
          priority: 'medium',
          estimatedRetryDelay: this.calculateRetryDelay(message.retryCount) * 2,
          recommendations: ['Increase timeout settings', 'Optimize operation performance'],
        };
      } else {
        return {
          action: DeadLetterAction.MANUAL_REVIEW,
          reason: 'Repeated timeouts - performance issue detected',
          retryable: false,
          priority: 'high',
          estimatedRetryDelay: 0,
          recommendations: ['Investigate performance bottlenecks', 'Review resource allocation'],
        };
      }
    }

    // Unknown error - default to manual review
    return {
      action: DeadLetterAction.MANUAL_REVIEW,
      reason: 'Unknown error type - requires investigation',
      retryable: false,
      priority: 'medium',
      estimatedRetryDelay: 0,
      recommendations: ['Analyze error pattern', 'Update error handling logic'],
    };
  }

  /**
   * Execute retry for dead letter message
   */
  async retry(message: DeadletterQueueMessage, context: QueueProcessingContext): Promise<QueueProcessingResult> {
    try {
      this.logger.info(`Retrying dead letter message`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        retryCount: message.retryCount + 1,
      });

      // Reconstruct original message
      const originalMessage = this.reconstructOriginalMessage(message);

      // Add message back to original stream
      const messageId = await this.resubmitMessage(message.originalStream, originalMessage);

      this.logger.info(`Message resubmitted to original queue`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        newMessageId: messageId,
      });

      return {
        success: true,
        status: QueueProcessingStatus.COMPLETED,
        processedItems: 1,
        metadata: {
          action: 'retried',
          originalStream: message.originalStream,
          originalMessageId: message.originalMessageId,
          newMessageId: messageId,
          retryCount: message.retryCount + 1,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to retry dead letter message`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'RETRY_FAILED',
          message: (error as Error).message,
          details: {
            originalStream: message.originalStream,
            originalMessageId: message.originalMessageId,
          },
        }],
      };
    }
  }

  /**
   * Archive dead letter message
   */
  async archive(message: DeadletterQueueMessage): Promise<QueueProcessingResult> {
    try {
      this.logger.info(`Archiving dead letter message`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        error: new Error(message.error),
        severity: message.severity,
      });

      // Store in database archive table
      await this.storeInArchive(message);

      // Update metrics
      await redisUtils.increment('deadletter_archived_count');

      return {
        success: true,
        status: QueueProcessingStatus.COMPLETED,
        processedItems: 1,
        metadata: {
          action: 'archived',
          originalStream: message.originalStream,
          originalMessageId: message.originalMessageId,
          archivedAt: new Date().toISOString(),
        },
      };
    } catch (error) {
      this.logger.error(`Failed to archive dead letter message`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'ARCHIVE_FAILED',
          message: (error as Error).message,
        }],
      };
    }
  }

  /**
   * Create manual review task
   */
  async createManualReview(message: DeadletterQueueMessage, analysis: DeadLetterAnalysis): Promise<QueueProcessingResult> {
    try {
      this.logger.info(`Creating manual review task`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        severity: message.severity,
        reason: analysis.reason,
      });

      // Store in manual review table
      await this.storeInManualReview(message, analysis);

      // Update metrics
      await redisUtils.increment('deadletter_manual_review_count');

      return {
        success: true,
        status: QueueProcessingStatus.COMPLETED,
        processedItems: 1,
        metadata: {
          action: 'manual_review_created',
          originalStream: message.originalStream,
          originalMessageId: message.originalMessageId,
          priority: analysis.priority,
          reason: analysis.reason,
          recommendations: analysis.recommendations,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to create manual review task`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'MANUAL_REVIEW_FAILED',
          message: (error as Error).message,
        }],
      };
    }
  }

  /**
   * Check if error is validation-related
   */
  private isValidationError(error: string): boolean {
    const validationPatterns = [
      'validation',
      'invalid format',
      'missing field',
      'schema validation',
      'malformed',
      'syntax error',
      'parse error',
    ];

    return validationPatterns.some(pattern => error.includes(pattern));
  }

  /**
   * Check if error is data integrity-related
   */
  private isDataIntegrityError(error: string): boolean {
    const integrityPatterns = [
      'foreign key',
      'constraint violation',
      'data integrity',
      'duplicate key',
      'referential integrity',
      'unique constraint',
    ];

    return integrityPatterns.some(pattern => error.includes(pattern));
  }

  /**
   * Check if error is dependency-related
   */
  private isDependencyError(error: string): boolean {
    const dependencyPatterns = [
      'connection refused',
      'service unavailable',
      'dependency',
      'external service',
      'api error',
      'rate limit',
      'service timeout',
    ];

    return dependencyPatterns.some(pattern => error.includes(pattern));
  }

  /**
   * Check if error is transient
   */
  private isTransientError(error: string): boolean {
    const transientPatterns = [
      'temporarily unavailable',
      'try again',
      'temporary',
      'momentary',
      'transient',
      'resource temporarily unavailable',
    ];

    return transientPatterns.some(pattern => error.includes(pattern));
  }

  /**
   * Check if error is timeout-related
   */
  private isTimeoutError(error: string): boolean {
    const timeoutPatterns = [
      'timeout',
      'timed out',
      'deadline exceeded',
      'operation timeout',
      'query timeout',
    ];

    return timeoutPatterns.some(pattern => error.includes(pattern));
  }

  /**
   * Calculate retry delay with exponential backoff
   */
  private calculateRetryDelay(retryCount: number): number {
    const baseDelay = 1000; // 1 second
    const maxDelay = 600000; // 10 minutes
    const delay = Math.min(baseDelay * Math.pow(2, retryCount), maxDelay);

    // Add jitter
    return delay + Math.random() * 1000;
  }

  /**
   * Reconstruct original message from dead letter data
   */
  private reconstructOriginalMessage(message: DeadletterQueueMessage): Record<string, string> {
    return {
      ...message.originalMessage,
      // Add metadata about the retry
      deadletter_retry: 'true',
      deadletter_retry_count: (message.retryCount + 1).toString(),
      deadletter_original_id: message.originalMessageId,
    };
  }

  /**
   * Resubmit message to original stream
   */
  private async resubmitMessage(streamName: string, messageData: Record<string, string>): Promise<string> {
    // This would use the Redis client to add message back to the stream
    const messageId = await redisUtils.addToStream(streamName, messageData);
    return messageId;
  }

  /**
   * Store message in archive table
   */
  private async storeInArchive(message: DeadletterQueueMessage): Promise<void> {
    const query = `
      INSERT INTO deadletter_archive (
        original_stream, original_message_id, original_message, error, error_code,
        error_stack, retry_count, max_retries, first_failure_timestamp,
        last_failure_timestamp, processing_duration, worker_id, consumer_group,
        severity, action, archived_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
      )
    `;

    const values = [
      message.originalStream,
      message.originalMessageId,
      JSON.stringify(message.originalMessage),
      message.error,
      message.errorCode || null,
      message.errorStack || null,
      message.retryCount,
      message.maxRetries,
      message.firstFailureTimestamp,
      message.lastFailureTimestamp,
      message.processingDuration || null,
      message.workerId,
      message.consumerGroup,
      message.severity,
      'archived',
      new Date().toISOString(),
    ];

    await this.db.query(query, values);
  }

  /**
   * Store message in manual review table
   */
  private async storeInManualReview(message: DeadletterQueueMessage, analysis: DeadLetterAnalysis): Promise<void> {
    const query = `
      INSERT INTO deadletter_manual_review (
        original_stream, original_message_id, original_message, error, error_code,
        error_stack, retry_count, max_retries, first_failure_timestamp,
        last_failure_timestamp, processing_duration, worker_id, consumer_group,
        severity, priority, action, reason, recommendations, created_at, status
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
      )
    `;

    const values = [
      message.originalStream,
      message.originalMessageId,
      JSON.stringify(message.originalMessage),
      message.error,
      message.errorCode || null,
      message.errorStack || null,
      message.retryCount,
      message.maxRetries,
      message.firstFailureTimestamp,
      message.lastFailureTimestamp,
      message.processingDuration || null,
      message.workerId,
      message.consumerGroup,
      message.severity,
      analysis.priority,
      analysis.action,
      analysis.reason,
      JSON.stringify(analysis.recommendations),
      new Date().toISOString(),
      'pending',
    ];

    await this.db.query(query, values);
  }
}

/**
 * Dead letter queue processor
 */
export class DeadletterQueueProcessor implements QueueProcessor<DeadletterQueueMessage> {
  private logger: Logger;
  private db: any;
  private recoveryStrategies: DeadLetterRecoveryStrategies;

  constructor(logger: Logger) {
    this.logger = logger.child({ component: 'deadletter-processor' });
    this.db = database;
    this.recoveryStrategies = new DeadLetterRecoveryStrategies(this.logger, this.db);
  }

  /**
   * Process dead letter message
   */
  async process(
    message: DeadletterQueueMessage,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const startTime = Date.now();

    try {
      this.logger.info(`Processing dead letter message`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        error: new Error(message.error),
        retryCount: message.retryCount,
        severity: message.severity,
        action: message.action,
      });

      // Analyze the dead letter message
      const analysis = await this.recoveryStrategies.analyze(message);

      this.logger.info(`Dead letter analysis completed`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        action: analysis.action,
        reason: analysis.reason,
        priority: analysis.priority,
      });

      let result: QueueProcessingResult;

      // Execute the determined action
      switch (analysis.action) {
        case DeadLetterAction.RETRY:
          result = await this.recoveryStrategies.retry(message, context);
          break;

        case DeadLetterAction.IGNORE:
          result = {
            success: true,
            status: QueueProcessingStatus.COMPLETED,
            processedItems: 1,
            metadata: {
              action: 'ignored',
              reason: analysis.reason,
              originalStream: message.originalStream,
              originalMessageId: message.originalMessageId,
            },
          };
          break;

        case DeadLetterAction.ARCHIVE:
          result = await this.recoveryStrategies.archive(message);
          break;

        case DeadLetterAction.MANUAL_REVIEW:
          result = await this.recoveryStrategies.createManualReview(message, analysis);
          break;

        default:
          throw new Error(`Unknown dead letter action: ${analysis.action}`);
      }

      return {
        ...result,
        metadata: {
          ...result.metadata,
          processingTime: Date.now() - startTime,
          analysis: {
            action: analysis.action,
            reason: analysis.reason,
            priority: analysis.priority,
            recommendations: analysis.recommendations,
          },
        },
      };
    } catch (error) {
      this.logger.error(`Failed to process dead letter message`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });

      return {
        success: false,
        status: QueueProcessingStatus.FAILED,
        errors: [{
          code: 'DEADLETTER_PROCESSING_FAILED',
          message: (error as Error).message,
          details: {
            originalStream: message.originalStream,
            originalMessageId: message.originalMessageId,
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
   * Validate dead letter message
   */
  async validate(message: DeadletterQueueMessage): Promise<boolean> {
    try {
      // Check required fields
      if (!message.originalStream || !message.originalMessageId || !message.error) {
        this.logger.warn(`Dead letter message missing required fields`, {
          originalStream: message.originalStream,
          originalMessageId: message.originalMessageId,
          hasError: !!message.error,
        });
        return false;
      }

      // Validate original stream name
      const validStreams = ['sc:events', 'sc:markets', 'sc:outcomes', 'sc:ticks', 'sc:updates'];
      if (!validStreams.includes(message.originalStream)) {
        this.logger.warn(`Invalid original stream`, {
          originalStream: message.originalStream,
        });
        return false;
      }

      // Validate severity
      const validSeverities = ['low', 'medium', 'high', 'critical'];
      if (!validSeverities.includes(message.severity)) {
        this.logger.warn(`Invalid severity`, {
          severity: message.severity,
        });
        return false;
      }

      // Validate action if present
      if (message.action) {
        const validActions = ['retry', 'ignore', 'manual_review', 'archive'];
        if (!validActions.includes(message.action)) {
          this.logger.warn(`Invalid action`, {
            action: message.action,
          });
          return false;
        }
      }

      // Validate retry counts
      if (message.retryCount < 0 || message.maxRetries <= 0) {
        this.logger.warn(`Invalid retry counts`, {
          retryCount: message.retryCount,
          maxRetries: message.maxRetries,
        });
        return false;
      }

      // Validate timestamps
      const firstFailure = new Date(message.firstFailureTimestamp);
      const lastFailure = new Date(message.lastFailureTimestamp);

      if (isNaN(firstFailure.getTime()) || isNaN(lastFailure.getTime())) {
        this.logger.warn(`Invalid failure timestamps`, {
          firstFailureTimestamp: message.firstFailureTimestamp,
          lastFailureTimestamp: message.lastFailureTimestamp,
        });
        return false;
      }

      // Check timestamp ordering
      if (lastFailure < firstFailure) {
        this.logger.warn(`Timestamp ordering error`, {
          firstFailureTimestamp: message.firstFailureTimestamp,
          lastFailureTimestamp: message.lastFailureTimestamp,
        });
        return false;
      }

      // Validate original message object
      if (!message.originalMessage || typeof message.originalMessage !== 'object') {
        this.logger.warn(`Invalid original message object`);
        return false;
      }

      return true;
    } catch (error) {
      this.logger.error(`Error during dead letter message validation`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });
      return false;
    }
  }

  /**
   * Transform dead letter message if needed
   */
  async transform(message: DeadletterQueueMessage): Promise<DeadletterQueueMessage> {
    try {
      const transformed = { ...message };

      // Normalize timestamps
      if (!transformed.firstFailureTimestamp.includes('Z')) {
        transformed.firstFailureTimestamp = new Date(transformed.firstFailureTimestamp).toISOString();
      }
      if (!transformed.lastFailureTimestamp.includes('Z')) {
        transformed.lastFailureTimestamp = new Date(transformed.lastFailureTimestamp).toISOString();
      }

      // Normalize severity and action
      transformed.severity = transformed.severity.toLowerCase() as any;
      if (transformed.action) {
        transformed.action = transformed.action.toLowerCase() as any;
      }

      return transformed;
    } catch (error) {
      this.logger.error(`Error transforming dead letter message`, {
        error: error as Error,
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
      });
      throw error;
    }
  }

  /**
   * Called before processing
   */
  async onBeforeProcess(
    message: DeadletterQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.info(`Starting dead letter processing`, {
      originalStream: message.originalStream,
      originalMessageId: message.originalMessageId,
      severity: message.severity,
      action: message.action,
      retryCount: message.retryCount,
      attempt: context.attempt,
    });

    // Update processing metrics in Redis
    await redisUtils.increment('deadletter_processing_count');
  }

  /**
   * Called after successful processing
   */
  async onAfterProcess(
    message: DeadletterQueueMessage,
    result: QueueProcessingResult,
    context: QueueProcessingContext
  ): Promise<void> {
    if (result.success) {
      this.logger.info(`Dead letter processing completed`, {
        originalStream: message.originalStream,
        originalMessageId: message.originalMessageId,
        action: result.metadata?.action,
        processingTime: result.metadata?.processingTime,
      });

      // Update success metrics
      await redisUtils.increment('deadletter_processed_count');

      // Update action-specific metrics
      const action = result.metadata?.action;
      if (action) {
        await redisUtils.increment(`deadletter_${action}_count`);
      }
    }
  }

  /**
   * Called when processing fails
   */
  async onError(
    error: Error,
    message: DeadletterQueueMessage,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Dead letter processing error`, {
      error: error,
      originalStream: message.originalStream,
      originalMessageId: message.originalMessageId,
      attempt: context.attempt,
      workerId: context.workerId,
    });

    // Update error metrics
    await redisUtils.increment('deadletter_error_count');
  }

  /**
   * Called before retry attempt
   */
  async onRetry(
    message: DeadletterQueueMessage,
    attempt: number,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.warn(`Retrying dead letter processing`, {
      originalStream: message.originalStream,
      originalMessageId: message.originalMessageId,
      attempt,
      error: error,
    });

    // Update retry metrics
    await redisUtils.increment('deadletter_retry_count');
  }

  /**
   * Called when message goes to dead letter queue (shouldn't happen for dead letter queue itself)
   */
  async onDeadLetter(
    message: DeadletterQueueMessage,
    error: Error,
    context: QueueProcessingContext
  ): Promise<void> {
    this.logger.error(`Dead letter message sent to dead letter queue (nested failure)`, {
      originalStream: message.originalStream,
      originalMessageId: message.originalMessageId,
      error: error,
      attempts: context.attempt,
    });

    // This is a critical failure - log as such
    await redisUtils.increment('deadletter_nested_failure_count');
  }

  /**
   * Determine if error is retryable
   */
  private shouldRetry(error: Error, message: DeadletterQueueMessage): boolean {
    const errorMessage = error.message.toLowerCase();

    // Don't retry validation errors for dead letter messages
    if (errorMessage.includes('validation') || errorMessage.includes('invalid')) {
      return false;
    }

    // Limit retry attempts for dead letter processing
    if (message.retryCount >= 5) {
      return false;
    }

    // Retry database connection errors
    if (errorMessage.includes('connection') || errorMessage.includes('timeout')) {
      return true;
    }

    // Retry transient errors
    if (errorMessage.includes('temporarily unavailable') || errorMessage.includes('try again')) {
      return true;
    }

    // Default to no retry for dead letter processing to avoid infinite loops
    return false;
  }
}

/**
 * Dead letter queue implementation
 */
export class DeadletterQueue extends BaseQueue<DeadletterQueueMessage> {
  private processor: DeadletterQueueProcessor;

  constructor(
    config?: Partial<typeof DEFAULT_QUEUE_CONFIGS.deadletter>,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const finalConfig = { ...DEFAULT_QUEUE_CONFIGS.deadletter, ...config };
    super(finalConfig, batchConfig, logger);
    this.processor = new DeadletterQueueProcessor(this.logger);
  }

  /**
   * Get the dead letter processor
   */
  protected getProcessor(): QueueProcessor<DeadletterQueueMessage> {
    return this.processor;
  }

  /**
   * Validate message is a dead letter message
   */
  protected deserializeMessage(streamMessage: any): DeadletterQueueMessage {
    const message = super.deserializeMessage(streamMessage);

    if (!queueMessageGuards.isDeadletterMessage(message)) {
      throw new Error(`Invalid dead letter message format: ${JSON.stringify(message)}`);
    }

    return message;
  }

  /**
   * Get queue statistics specific to dead letter
   */
  async getDeadletterStatistics(): Promise<{
    totalDeadletters: number;
    deadlettersByStream: Record<string, number>;
    deadlettersBySeverity: Record<string, number>;
    deadlettersByAction: Record<string, number>;
    averageRetryCount: number;
    oldestMessage: string;
    newestMessage: string;
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
        totalDeadletters: 0,
        deadlettersByStream: {},
        deadlettersBySeverity: {},
        deadlettersByAction: {},
        averageRetryCount: 0,
        oldestMessage: '',
        newestMessage: '',
        recentActivity: {
          lastHour: 0,
          lastDay: 0,
          lastWeek: 0,
        },
      };
    } catch (error) {
      this.logger.error(`Failed to get dead letter statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Get manual review tasks
   */
  async getManualReviewTasks(limit: number = 50, offset: number = 0): Promise<{
    tasks: Array<{
      id: number;
      originalStream: string;
      originalMessageId: string;
      error: string;
      severity: string;
      priority: string;
      reason: string;
      recommendations: string[];
      createdAt: string;
      status: string;
    }>;
    total: number;
  }> {
    try {
      // This would query the deadletter_manual_review table
      // For now, return a placeholder implementation
      return {
        tasks: [],
        total: 0,
      };
    } catch (error) {
      this.logger.error(`Failed to get manual review tasks`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Update manual review task status
   */
  async updateManualReviewTask(taskId: number, status: string, notes?: string): Promise<void> {
    try {
      // This would update the deadletter_manual_review table
      this.logger.info(`Updated manual review task`, {
        taskId,
        status,
        notes,
      });
    } catch (error) {
      this.logger.error(`Failed to update manual review task`, {
        error: error as Error,
        taskId,
        status,
      });
      throw error;
    }
  }
}