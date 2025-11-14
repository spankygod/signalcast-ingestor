/**
 * Base queue implementation with common functionality for all SignalCast queues
 * Provides core queue operations, error handling, metrics, and monitoring
 */

import {
  EnhancedRedisClient,
  redisConnection,
  RedisStreamError
} from '../lib/redis';
import type { StreamMessage } from '../lib/redis';
import { Logger, rootLogger } from '../lib/logger';
import { RetryManager, defaultRetryManager } from '../lib/retry';
import type {
  BaseQueueMessage,
  QueueConfig,
  QueueProcessor,
  QueueProcessingContext,
  QueueProcessingResult,
  QueueMetrics,
  QueueHealthStatus,
  BatchProcessingConfig,
  QueueWorkerState,
  QueueAlert,
} from './types';
import { QueueProcessingStatus, DEFAULT_QUEUE_CONFIGS } from './types';
import { REDIS_STREAMS, REDIS_CONSUMER_GROUPS } from '../config';

type AckEntry = {
  stream: string;
  id: string;
  idempotencyKey?: string;
};

/**
 * Base queue class that implements common queue functionality
 */
export abstract class BaseQueue<T extends BaseQueueMessage> {
  protected redis: EnhancedRedisClient;
  protected logger: Logger;
  protected retryManager: RetryManager;
  protected config: QueueConfig;
  protected batchConfig: BatchProcessingConfig | undefined;
  protected isRunning: boolean = false;
  protected workerId: string;
  protected consumerName: string;
  protected metrics: QueueMetrics;
  protected alerts: QueueAlert[] = [];
  protected processedMessages: Set<string> = new Set();
  protected processingMessages: Map<string, number> = new Map();
  protected startTime: number;
  protected lastHealthCheck: number = 0;
  protected shutdownTimeout: NodeJS.Timeout | null = null;
  protected metricsInterval: NodeJS.Timeout | undefined;
  protected healthCheckInterval: NodeJS.Timeout | undefined;

  constructor(
    config: QueueConfig,
    batchConfig?: BatchProcessingConfig,
    logger?: Logger
  ) {
    const defaultConfig =
      DEFAULT_QUEUE_CONFIGS[config.name as keyof typeof DEFAULT_QUEUE_CONFIGS];

    this.config = {
      ...(defaultConfig || {}),
      ...config,
    };
    this.batchConfig = batchConfig;
    this.redis = redisConnection;
    this.logger = logger || rootLogger.child({
      component: 'queue',
      queueName: this.config.name,
      streamName: this.config.streamName,
    });
    this.retryManager = defaultRetryManager;
    this.workerId = `queue-${this.config.name}-${process.pid}-${Date.now()}`;
    this.consumerName = `consumer-${this.config.name}-${process.pid}-${Date.now()}`;
    this.startTime = Date.now();
    this.metrics = this.initializeMetrics();
  }

  /**
   * Initialize the queue
   */
  async initialize(): Promise<void> {
    try {
      this.logger.info(`Initializing queue: ${this.config.name}`);

      // Create consumer group if it doesn't exist
      await this.redis.createConsumerGroup(
        this.config.streamName,
        this.config.consumerGroup,
        '0'
      );

      // Start metrics collection
      this.startMetricsCollection();

      // Start health checking
      this.startHealthChecking();

      this.logger.info(`Queue initialized successfully: ${this.config.name}`);
    } catch (error) {
      this.logger.error(`Failed to initialize queue ${this.config.name}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Start the queue consumer
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn(`Queue ${this.config.name} is already running`);
      return;
    }

    this.isRunning = true;
    this.logger.info(`Starting queue consumer: ${this.config.name}`);

    try {
      // Main processing loop
      while (this.isRunning) {
        try {
          await this.processMessages();
        } catch (error) {
          this.logger.error(`Error in processing loop for queue ${this.config.name}`, {
            error: error as Error,
          });

          // Wait before retrying
          await this.sleep(this.config.retryDelayMs);
        }
      }
    } catch (error) {
      this.logger.error(`Fatal error in queue ${this.config.name}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Stop the queue consumer
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.logger.info(`Stopping queue: ${this.config.name}`);
    this.isRunning = false;
    this.clearMonitoringIntervals();

    // Set shutdown timeout
    if (this.config.gracefulShutdownTimeoutMs) {
      this.shutdownTimeout = setTimeout(() => {
        this.logger.warn(`Force shutdown after timeout for queue ${this.config.name}`);
      }, this.config.gracefulShutdownTimeoutMs);
    }

    try {
      // Process any remaining messages
      if (this.batchConfig?.flushOnShutdown) {
        await this.flushRemainingMessages();
      }

      // Clean up resources
      await this.cleanup();

      if (this.shutdownTimeout) {
        clearTimeout(this.shutdownTimeout);
        this.shutdownTimeout = null;
      }

      this.logger.info(`Queue stopped successfully: ${this.config.name}`);
    } catch (error) {
      this.logger.error(`Error stopping queue ${this.config.name}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Add message to queue
   */
  async addMessage(message: T): Promise<string> {
    try {
      const serializedMessage = this.serializeMessage(message);
      const messageId = await this.redis.addToStream(
        this.config.streamName,
        serializedMessage,
        100000 // Max stream length
      );

      this.logger.debug(`Added message to queue ${this.config.name}`, {
        messageId,
        correlationId: message.correlationId,
      });

      // Update metrics
      this.metrics.totalMessages++;

      return messageId;
    } catch (error) {
      this.logger.error(`Failed to add message to queue ${this.config.name}`, {
        error: error as Error,
        message,
      });
      throw error;
    }
  }

  /**
   * Add multiple messages to queue
   */
  async addMessages(messages: T[]): Promise<string[]> {
    const serializedMessages = messages.map(msg => this.serializeMessage(msg));

    try {
      const messageIds = await this.redis.batchAddToStream(
        this.config.streamName,
        serializedMessages,
        100000
      );

      this.logger.debug(`Added ${messages.length} messages to queue ${this.config.name}`);

      // Update metrics
      this.metrics.totalMessages += messages.length;

      return messageIds;
    } catch (error) {
      this.logger.error(`Failed to add messages to queue ${this.config.name}`, {
        error: error as Error,
        messageCount: messages.length,
      });
      throw error;
    }
  }

  /**
   * Get queue metrics
   */
  async getMetrics(): Promise<QueueMetrics> {
    try {
      const streamInfo = await this.redis.getStreamInfo(this.config.streamName);

      this.metrics.queueDepth = streamInfo.length;
      this.metrics.consumerCount = streamInfo.groups.length;
      this.metrics.lastProcessedAt = new Date().toISOString();

      return { ...this.metrics };
    } catch (error) {
      this.logger.error(`Failed to get metrics for queue ${this.config.name}`, {
        error: error as Error,
      });
      return this.metrics;
    }
  }

  /**
   * Check queue health
   */
  async checkHealth(): Promise<QueueHealthStatus> {
    const now = Date.now();
    this.lastHealthCheck = now;

    try {
      const metrics = await this.getMetrics();
      const issues: Array<{
        severity: 'low' | 'medium' | 'high' | 'critical';
        type: string;
        message: string;
        timestamp: string;
      }> = [];

      // Check queue depth
      if (metrics.queueDepth > this.config.backpressureThreshold) {
        issues.push({
          severity: 'high',
          type: 'backpressure',
          message: `Queue depth ${metrics.queueDepth} exceeds threshold ${this.config.backpressureThreshold}`,
          timestamp: new Date().toISOString(),
        });
      }

      // Check error rate
      if (metrics.failedMessages > 0) {
        const errorRate = metrics.failedMessages / metrics.totalMessages;
        if (errorRate > 0.1) {
          issues.push({
            severity: 'high',
            type: 'high_error_rate',
            message: `Error rate ${(errorRate * 100).toFixed(2)}% exceeds threshold`,
            timestamp: new Date().toISOString(),
          });
        }
      }

      // Check circuit breaker state
      if (metrics.circuitBreakerState === 'open') {
        issues.push({
          severity: 'critical',
          type: 'circuit_breaker_open',
          message: 'Circuit breaker is open',
          timestamp: new Date().toISOString(),
        });
      }

      // Check memory usage
      if (metrics.memoryUsage > 1024 * 1024 * 1024) { // 1GB
        issues.push({
          severity: 'high',
          type: 'high_memory_usage',
          message: `Memory usage ${(metrics.memoryUsage / 1024 / 1024).toFixed(2)}MB exceeds threshold`,
          timestamp: new Date().toISOString(),
        });
      }

      // Determine overall status
      let status: 'healthy' | 'degraded' | 'unhealthy' | 'critical' = 'healthy';
      if (issues.some(i => i.severity === 'critical')) {
        status = 'critical';
      } else if (issues.some(i => i.severity === 'high')) {
        status = 'unhealthy';
      } else if (issues.some(i => i.severity === 'medium')) {
        status = 'degraded';
      }

      const recommendations = this.generateRecommendations(issues);

      return {
        queueName: this.config.name,
        status,
        lastCheck: new Date().toISOString(),
        issues,
        metrics,
        recommendations,
      };
    } catch (error) {
      return {
        queueName: this.config.name,
        status: 'critical',
        lastCheck: new Date().toISOString(),
        issues: [{
          severity: 'critical',
          type: 'health_check_failed',
          message: `Health check failed: ${(error as Error).message}`,
          timestamp: new Date().toISOString(),
        }],
        metrics: this.metrics,
        recommendations: ['Restart queue service', 'Check Redis connection'],
      };
    }
  }

  /**
   * Clear processed messages from idempotency cache
   */
  clearProcessedMessages(): void {
    this.processedMessages.clear();
  }

  /**
   * Get worker state
   */
  getWorkerState(): QueueWorkerState {
    return {
      workerId: this.workerId,
      queueName: this.config.name,
      status: this.isRunning ? 'processing' : 'idle',
      processedCount: this.metrics.processedMessages,
      errorCount: this.metrics.failedMessages,
      lastHeartbeat: new Date().toISOString(),
      memoryUsage: process.memoryUsage().heapUsed,
      cpuUsage: 0, // Would need to be implemented with proper CPU monitoring
      lastActivityTime: new Date().toISOString(),
      startupTime: new Date(this.startTime).toISOString(),
    };
  }

  /**
   * Abstract method to get the queue processor
   */
  protected abstract getProcessor(): QueueProcessor<T>;

  /**
   * Process messages from the queue
   */
  private async processMessages(): Promise<void> {
    const processor = this.getProcessor();

    try {
      const messages = await this.redis.readFromGroup(
        this.config.consumerGroup,
        this.consumerName,
        [this.config.streamName],
        {
          count: this.config.batchSize,
          blockMs: this.config.timeoutMs,
        }
      );

      if (messages.length === 0) {
        return; // No messages to process
      }

      this.logger.debug(`Processing ${messages.length} messages from queue ${this.config.name}`);

      if (this.batchConfig?.enabled) {
        await this.processBatch(messages, processor);
      } else {
        await this.processIndividual(messages, processor);
      }

      // Clean up old processed messages for idempotency
      this.cleanupProcessedMessages();
    } catch (error) {
      if (error instanceof RedisStreamError) {
        // Redis stream errors are expected and handled
        this.logger.debug(`Redis stream operation returned no data: ${error.message}`);
        return;
      }
      throw error;
    }
  }

  /**
   * Process messages individually
   */
  private async processIndividual(
    messages: StreamMessage[],
    processor: QueueProcessor<T>
  ): Promise<void> {
    const acknowledgements: AckEntry[] = [];

    for (const streamMessage of messages) {
      try {
        const { result, idempotencyKey } = await this.processSingleMessage(
          streamMessage,
          processor
        );

        if (result.success) {
          acknowledgements.push(this.createAckEntry(streamMessage, idempotencyKey));
        }
      } catch (error) {
        this.logger.error(`Failed to process message ${streamMessage.id}`, {
          error: error as Error,
        });

        // Move to dead letter queue
        await this.moveToDeadLetterQueue(streamMessage, error as Error);
      }
    }

    await this.flushAcknowledgements(acknowledgements);
  }

  /**
   * Process messages as a batch
   */
  private async processBatch(
    messages: StreamMessage[],
    processor: QueueProcessor<T>
  ): Promise<void> {
    const entries: Array<{ streamMessage: StreamMessage; message: T }> = [];

    for (const streamMessage of messages) {
      try {
        const message = this.deserializeMessage(streamMessage);
        entries.push({ streamMessage, message });
      } catch (error) {
        this.logger.error(`Failed to deserialize message ${streamMessage.id}`, {
          error: error as Error,
        });
        await this.moveToDeadLetterQueue(streamMessage, error as Error);
      }
    }

    if (entries.length === 0) {
      return;
    }

    const acknowledgements: AckEntry[] = [];

    for (const { message, streamMessage } of entries) {
      const idempotencyKey = this.getIdempotencyKey(message);
      if (this.isDuplicateMessage(idempotencyKey)) {
        this.logger.debug(`Skipping duplicate message: ${idempotencyKey}`);
        acknowledgements.push(this.createAckEntry(streamMessage, idempotencyKey));
        continue;
      }

      this.processingMessages.set(idempotencyKey, Date.now());

      try {
        const context = this.createProcessingContext(message.id, message.correlationId);
        const result = await this.processWithRetry(message, processor, context);

        if (result.success) {
          acknowledgements.push(this.createAckEntry(streamMessage, idempotencyKey));
          this.metrics.processedMessages++;
        } else {
          this.processingMessages.delete(idempotencyKey);
          this.metrics.failedMessages++;
        }
      } catch (error) {
        this.logger.error(`Batch processing failed for message ${streamMessage.id}`, {
          error: error as Error,
        });
        await this.moveToDeadLetterQueue(streamMessage, error as Error);
        this.metrics.failedMessages++;
        this.processingMessages.delete(idempotencyKey);
      }
    }

    await this.flushAcknowledgements(acknowledgements);
  }

  /**
   * Process a single message with retry logic
   */
  private async processWithRetry(
    message: T,
    processor: QueueProcessor<T>,
    context: QueueProcessingContext
  ): Promise<QueueProcessingResult> {
    const maxAttempts = this.config.maxRetries + 1;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const startTime = Date.now();

      try {
        context.attempt = attempt;
        context.startTime = startTime;

        // Call before processing hook
        if (processor.onBeforeProcess) {
          await processor.onBeforeProcess(message, context);
        }

        // Validate message if validator is provided
        if (processor.validate) {
          const isValid = await processor.validate(message);
          if (!isValid) {
            throw new Error(`Message validation failed for ${this.config.name} queue`);
          }
        }

        // Transform message if transformer is provided
        let processedMessage = message;
        if (processor.transform) {
          processedMessage = await processor.transform(message);
        }

        // Process the message
        const result = await processor.process(processedMessage, context);

        // Call after processing hook
        if (processor.onAfterProcess) {
          await processor.onAfterProcess(message, result, context);
        }

        const processingTime = Date.now() - startTime;
        this.updateProcessingMetrics(processingTime, true);

        return result;

      } catch (error) {
        const processingTime = Date.now() - startTime;
        this.updateProcessingMetrics(processingTime, false);

        if (attempt === maxAttempts) {
          // Final attempt failed
          if (processor.onError) {
            await processor.onError(error as Error, message, context);
          }
          throw error;
        }

        // Retry logic
        this.logger.warn(`Processing attempt ${attempt} failed for queue ${this.config.name}, retrying`, {
          error: error as Error,
          attempt,
          maxAttempts,
          messageId: message.id,
        });

        if (processor.onRetry) {
          await processor.onRetry(message, attempt, error as Error, context);
        }

        // Wait before retrying
        await this.sleep(this.config.retryDelayMs * attempt);
      }
    }

    // This should never be reached
    throw new Error(`Unexpected error in processWithRetry for queue ${this.config.name}`);
  }

  /**
   * Process a single message (non-batch)
   */
  private async processSingleMessage(
    streamMessage: StreamMessage,
    processor: QueueProcessor<T>
  ): Promise<{ result: QueueProcessingResult; idempotencyKey?: string }> {
    let idempotencyKey: string | undefined;

    try {
      const message = this.deserializeMessage(streamMessage);
      idempotencyKey = this.getIdempotencyKey(message);

      // Check idempotency
      if (this.isDuplicateMessage(idempotencyKey)) {
        this.logger.debug(`Skipping duplicate message: ${idempotencyKey}`);
        return {
          result: {
            success: true,
            status: QueueProcessingStatus.COMPLETED,
          },
          idempotencyKey,
        };
      }

      this.processingMessages.set(idempotencyKey, Date.now());

      const context = this.createProcessingContext(message.id, message.correlationId);

      const result = await this.processWithRetry(message, processor, context);

      if (result.success) {
        this.metrics.processedMessages++;
      } else {
        this.processingMessages.delete(idempotencyKey);
        this.metrics.failedMessages++;
      }

      return {
        result,
        idempotencyKey,
      };

    } catch (error) {
      if (idempotencyKey) {
        this.processingMessages.delete(idempotencyKey);
      }
      this.metrics.failedMessages++;
      throw error;
    }
  }

  /**
   * Build a processing context with optional correlation metadata.
   */
  private createProcessingContext(messageId: string, correlationId?: string): QueueProcessingContext {
    const context: QueueProcessingContext = {
      consumerName: this.consumerName,
      workerId: this.workerId,
      attempt: 0,
      startTime: Date.now(),
      messageId,
      streamName: this.config.streamName,
      consumerGroup: this.config.consumerGroup,
    };

    if (correlationId) {
      context.correlationId = correlationId;
    }

    return context;
  }

  /**
   * Build acknowledgement payloads while preserving optional ids.
   */
  private createAckEntry(streamMessage: StreamMessage, idempotencyKey?: string): AckEntry {
    const entry: AckEntry = {
      stream: streamMessage.stream,
      id: streamMessage.id,
    };

    if (idempotencyKey) {
      entry.idempotencyKey = idempotencyKey;
    }

    return entry;
  }

  /**
   * Move failed message to dead letter queue
   */
  private async moveToDeadLetterQueue(
    streamMessage: StreamMessage,
    error: Error
  ): Promise<void> {
    try {
      const metadata: Record<string, string> = {
        queue_name: this.config.name,
        worker_id: this.workerId,
        timestamp: new Date().toISOString(),
      };

      const errorCode = (error as any).code;
      if (errorCode) {
        metadata.error_code = String(errorCode);
      }

      if (error.stack) {
        metadata.error_stack = error.stack;
      }

      await this.redis.moveToDeadletter(
        streamMessage.stream,
        streamMessage.id,
        error.message,
        metadata
      );

      this.metrics.deadletterMessages++;
      this.logger.debug(`Moved message ${streamMessage.id} to dead letter queue`, {
        originalStream: streamMessage.stream,
        error,
      });
    } catch (deadLetterError) {
      this.logger.error(`Failed to move message to dead letter queue`, {
        error: deadLetterError as Error,
        originalMessage: streamMessage.id,
      });
    }
  }

  /**
   * Serialize message for Redis stream
   */
  private serializeMessage(message: T): Record<string, string> {
    const serialized: Record<string, string> = {};

    for (const [key, value] of Object.entries(message)) {
      if (value !== null && value !== undefined) {
        serialized[key] = typeof value === 'object'
          ? JSON.stringify(value)
          : String(value);
      }
    }

    return serialized;
  }

  /**
   * Deserialize message from Redis stream
   */
  protected deserializeMessage(streamMessage: StreamMessage): T {
    const message: Record<string, any> = { id: streamMessage.id };

    for (const [key, value] of Object.entries(streamMessage.fields)) {
      // Try to parse as JSON, fall back to string
      try {
        message[key] = JSON.parse(value);
      } catch {
        message[key] = value;
      }
    }

    return message as T;
  }

  /**
   * Get idempotency key for message
   */
  private getIdempotencyKey(message: T): string {
    return `${this.config.name}:${message.id}:${message.correlationId || 'no-correlation'}`;
  }

  /**
   * Initialize metrics
   */
  private initializeMetrics(): QueueMetrics {
    return {
      streamName: this.config.streamName,
      consumerGroup: this.config.consumerGroup,
      totalMessages: 0,
      processedMessages: 0,
      failedMessages: 0,
      deadletterMessages: 0,
      averageProcessingTime: 0,
      messagesPerSecond: 0,
      queueDepth: 0,
      consumerCount: 0,
      errorsPerSecond: 0,
      retryRate: 0,
      circuitBreakerState: 'closed',
      memoryUsage: 0,
      throughput: 0,
    };
  }

  /**
   * Update processing metrics
   */
  private updateProcessingMetrics(processingTime: number, success: boolean): void {
    const total = this.metrics.processedMessages + this.metrics.failedMessages;
    this.metrics.averageProcessingTime =
      (this.metrics.averageProcessingTime * total + processingTime) / (total + 1);

    if (success) {
      this.metrics.processedMessages++;
    } else {
      this.metrics.failedMessages++;
    }

    // Calculate throughput (messages per second)
    const uptime = (Date.now() - this.startTime) / 1000;
    this.metrics.messagesPerSecond = total / uptime;
    this.metrics.throughput = this.metrics.messagesPerSecond;
  }

  /**
   * Clean up old processed messages
   */
  private cleanupProcessedMessages(): void {
    if (this.processedMessages.size > 10000) {
      // Keep only the most recent 5000 messages
      const entries = Array.from(this.processedMessages);
      this.processedMessages.clear();
      entries.slice(-5000).forEach(key => this.processedMessages.add(key));
    }
  }

  /**
   * Determine whether a message has already been handled or is in-flight
   */
  private isDuplicateMessage(idempotencyKey: string): boolean {
    return this.processedMessages.has(idempotencyKey) || this.processingMessages.has(idempotencyKey);
  }

  /**
   * Mark a message as acknowledged and ready for deduplication tracking
   */
  private markMessageAcknowledged(idempotencyKey: string): void {
    this.processingMessages.delete(idempotencyKey);
    this.processedMessages.add(idempotencyKey);
  }

  /**
   * Revert in-flight tracking for messages whose acknowledgements failed
   */
  private revertInFlight(entries: AckEntry[]): void {
    for (const entry of entries) {
      if (!entry.idempotencyKey) {
        continue;
      }
      this.processingMessages.delete(entry.idempotencyKey);
      this.processedMessages.delete(entry.idempotencyKey);
    }
  }

  /**
   * Flush acknowledgements to Redis grouped by stream
   */
  private async flushAcknowledgements(entries: AckEntry[]): Promise<void> {
    if (entries.length === 0) {
      return;
    }

    const ackMap = new Map<string, string[]>();
    for (const entry of entries) {
      const streamName = entry.stream || this.config.streamName;
      if (!ackMap.has(streamName)) {
        ackMap.set(streamName, []);
      }
      ackMap.get(streamName)!.push(entry.id);
    }

    try {
      const ackPromises = Array.from(ackMap.entries()).map(([streamName, ids]) =>
        this.redis.acknowledgeMessages(streamName, this.config.consumerGroup, ids)
      );
      await Promise.all(ackPromises);

      for (const entry of entries) {
        if (entry.idempotencyKey) {
          this.markMessageAcknowledged(entry.idempotencyKey);
        }
      }
    } catch (error) {
      this.revertInFlight(entries);
      throw error;
    }
  }

  /**
   * Clear monitoring intervals to avoid leaks
   */
  private clearMonitoringIntervals(): void {
    if (this.metricsInterval) {
      clearInterval(this.metricsInterval);
      this.metricsInterval = undefined;
    }

    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = undefined;
    }
  }

  /**
   * Start metrics collection
   */
  private startMetricsCollection(): void {
    if (!this.config.metricsEnabled || this.metricsInterval) {
      return;
    }

    this.metricsInterval = setInterval(async () => {
      try {
        await this.getMetrics();
      } catch (error) {
        this.logger.error(`Failed to collect metrics for queue ${this.config.name}`, {
          error: error as Error,
        });
      }
    }, 60000); // Collect metrics every minute

    this.metricsInterval.unref?.();
  }

  /**
   * Start health checking
   */
  private startHealthChecking(): void {
    if (this.healthCheckInterval) {
      return;
    }

    this.healthCheckInterval = setInterval(async () => {
      try {
        const health = await this.checkHealth();
        if (health.status !== 'healthy') {
          this.logger.warn(`Queue health degraded: ${health.status}`, {
            queueName: this.config.name,
            issues: health.issues,
          });
        }
      } catch (error) {
        this.logger.error(`Health check failed for queue ${this.config.name}`, {
          error: error as Error,
        });
      }
    }, this.config.healthCheckIntervalMs);

    this.healthCheckInterval.unref?.();
  }

  /**
   * Generate recommendations based on issues
   */
  private generateRecommendations(issues: any[]): string[] {
    const recommendations: string[] = [];

    if (issues.some(i => i.type === 'backpressure')) {
      recommendations.push('Scale up consumers', 'Increase batch size', 'Optimize processing logic');
    }

    if (issues.some(i => i.type === 'high_error_rate')) {
      recommendations.push('Check message format', 'Review error handling', 'Monitor external dependencies');
    }

    if (issues.some(i => i.type === 'circuit_breaker_open')) {
      recommendations.push('Check service dependencies', 'Monitor system health', 'Consider manual reset');
    }

    if (issues.some(i => i.type === 'high_memory_usage')) {
      recommendations.push('Reduce batch size', 'Optimize memory usage', 'Consider scaling vertically');
    }

    return recommendations;
  }

  /**
   * Flush remaining messages during shutdown
   */
  private async flushRemainingMessages(): Promise<void> {
    this.logger.info(`Flushing remaining messages for queue ${this.config.name}`);

    try {
      const messages = await this.redis.readFromGroup(
        this.config.consumerGroup,
        this.consumerName,
        [this.config.streamName],
        {
          count: this.config.batchSize,
          blockMs: 1000, // Short timeout for shutdown
        }
      );

      if (messages.length > 0) {
        this.logger.info(`Processing ${messages.length} remaining messages during shutdown`);
        // Process remaining messages without timeout restrictions
        await this.processMessages();
      }
    } catch (error) {
      this.logger.error(`Error flushing remaining messages`, {
        error: error as Error,
      });
    }
  }

  /**
   * Clean up resources
   */
  private async cleanup(): Promise<void> {
    this.processedMessages.clear();
    this.processingMessages.clear();
    this.alerts.length = 0;
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
