/**
 * Type definitions and schemas for SignalCast queue system
 * Provides comprehensive TypeScript typing for all queue messages and operations
 */

import { StreamMessage } from '../lib/redis';

/**
 * Base queue message interface
 */
export interface BaseQueueMessage {
  id: string;
  timestamp: string;
  correlationId?: string;
  source?: string;
  version?: string;
  metadata?: Record<string, any>;
}

/**
 * Event data queue message schema
 */
export interface EventQueueMessage extends BaseQueueMessage {
  polymarketId: string;
  slug: string;
  title: string;
  description?: string;
  category: string;
  subcategory?: string;
  liquidity: number;
  volume?: number;
  active: boolean;
  closed: boolean;
  resolutionDate?: string;
  imageUrl?: string;
  tags?: string[];
  createdAt: string;
  updatedAt: string;
}

/**
 * Market data queue message schema
 */
export interface MarketQueueMessage extends BaseQueueMessage {
  polymarketId: string;
  eventId: string;
  question: string;
  description?: string;
  liquidity: number;
  currentPrice: number;
  bestBid?: number;
  bestAsk?: number;
  spread?: number;
  volume24h?: number;
  active: boolean;
  closed: boolean;
  resolved: boolean;
  resolution?: string;
  outcomePrices?: Record<string, number>;
  startDate?: string;
  endDate?: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Outcome data queue message schema
 */
export interface OutcomeQueueMessage extends BaseQueueMessage {
  polymarketId: string;
  marketId: string;
  eventId: string;
  title: string;
  description?: string;
  price: number;
  probability: number;
  impliedProbability?: number;
  bestBid?: number;
  bestAsk?: number;
  liquidity: number;
  volume?: number;
  status: 'active' | 'resolved' | 'cancelled' | 'suspended';
  resolution?: 'yes' | 'no' | 'invalid';
  odds?: {
    decimal: number;
    fractional: string;
    american: number;
  };
  createdAt: string;
  updatedAt: string;
}

/**
 * Tick data queue message schema (real-time price updates)
 */
export interface TickQueueMessage extends BaseQueueMessage {
  marketId: string;
  price: number;
  bestBid?: number;
  bestAsk?: number;
  liquidity: number;
  volume24h?: number;
  priceChange?: number;
  priceChangePercent?: number;
  timestamp: string; // High-precision timestamp for ticks
  sequenceId?: number;
  tradeId?: string;
  tradeSize?: number;
  side?: 'buy' | 'sell';
  tickType: 'price_update' | 'trade' | 'liquidity_change' | 'market_state';
  marketState: 'open' | 'closed' | 'suspended' | 'resolved';
}

/**
 * General update queue message schema
 */
export interface UpdateQueueMessage extends BaseQueueMessage {
  updateType: 'market_created' | 'market_updated' | 'market_closed' | 'market_resolved' |
             'outcome_added' | 'outcome_updated' | 'price_change' | 'liquidity_change' |
             'batch_import' | 'system_maintenance' | 'error_correction';
  entityId: string;
  entityType: 'event' | 'market' | 'outcome' | 'tick' | 'system';
  changes: Record<string, any>;
  reason?: string;
  batchId?: string;
  priority: 'low' | 'normal' | 'high' | 'critical';
}

/**
 * Dead letter queue message schema
 */
export interface DeadletterQueueMessage extends BaseQueueMessage {
  originalStream: string;
  originalMessageId: string;
  originalMessage: Record<string, string>;
  error: string;
  errorCode?: string;
  errorStack?: string;
  retryCount: number;
  maxRetries: number;
  firstFailureTimestamp: string;
  lastFailureTimestamp: string;
  processingDuration?: number;
  workerId: string;
  consumerGroup: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  action?: 'retry' | 'ignore' | 'manual_review' | 'archive';
}

/**
 * Queue processing status
 */
export enum QueueProcessingStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
  RETRYING = 'retrying',
  DEADLETTER = 'deadletter',
}

/**
 * Queue configuration interface
 */
export interface QueueConfig {
  name: string;
  streamName: string;
  consumerGroup: string;
  batchSize: number;
  timeoutMs: number;
  maxRetries: number;
  retryDelayMs: number;
  deadLetterEnabled: boolean;
  idempotencyWindowMs: number;
  backpressureThreshold: number;
  circuitBreakerThreshold: number;
  processingTimeoutMs: number;
  healthCheckIntervalMs: number;
  metricsEnabled: boolean;
  gracefulShutdownTimeoutMs?: number;
}

/**
 * Queue processor interface
 */
export interface QueueProcessor<T extends BaseQueueMessage> {
  process(message: T, context: QueueProcessingContext): Promise<QueueProcessingResult>;
  validate?(message: T): Promise<boolean>;
  transform?(message: T): Promise<T>;
  onBeforeProcess?(message: T, context: QueueProcessingContext): Promise<void>;
  onAfterProcess?(message: T, result: QueueProcessingResult, context: QueueProcessingContext): Promise<void>;
  onError?(error: Error, message: T, context: QueueProcessingContext): Promise<void>;
  onRetry?(message: T, attempt: number, error: Error, context: QueueProcessingContext): Promise<void>;
  onDeadLetter?(message: T, error: Error, context: QueueProcessingContext): Promise<void>;
}

/**
 * Queue processing context
 */
export interface QueueProcessingContext {
  consumerName: string;
  workerId: string;
  attempt: number;
  startTime: number;
  correlationId?: string;
  messageId: string;
  streamName: string;
  consumerGroup: string;
  metadata?: Record<string, any>;
}

/**
 * Queue processing result
 */
export interface QueueProcessingResult {
  success: boolean;
  status: QueueProcessingStatus;
  processedItems?: number;
  errors?: Array<{
    code: string;
    message: string;
    details?: any;
  }>;
  warnings?: Array<{
    code: string;
    message: string;
    details?: any;
  }>;
  metadata?: Record<string, any>;
  retryDelay?: number;
  shouldRetry?: boolean;
  shouldDeadLetter?: boolean;
  nextAttemptAt?: number;
}

/**
 * Queue metrics
 */
export interface QueueMetrics {
  streamName: string;
  consumerGroup: string;
  totalMessages: number;
  processedMessages: number;
  failedMessages: number;
  deadletterMessages: number;
  averageProcessingTime: number;
  messagesPerSecond: number;
  queueDepth: number;
  consumerCount: number;
  lastProcessedAt?: string;
  errorsPerSecond: number;
  retryRate: number;
  circuitBreakerState: 'closed' | 'open' | 'half_open';
  memoryUsage: number;
  throughput: number;
}

/**
 * Queue health status
 */
export interface QueueHealthStatus {
  queueName: string;
  status: 'healthy' | 'degraded' | 'unhealthy' | 'critical';
  lastCheck: string;
  issues: Array<{
    severity: 'low' | 'medium' | 'high' | 'critical';
    type: string;
    message: string;
    timestamp: string;
  }>;
  metrics: QueueMetrics;
  recommendations: string[];
}

/**
 * Batch processing configuration
 */
export interface BatchProcessingConfig {
  enabled: boolean;
  maxBatchSize: number;
  batchTimeoutMs: number;
  flushOnShutdown: boolean;
  partialBatchProcessing: boolean;
  aggregationStrategy: 'simple' | 'group_by_key' | 'time_window';
  aggregationKey?: string;
}

/**
 * Queue consumer configuration
 */
export interface QueueConsumerConfig {
  name: string;
  streamName: string;
  consumerGroup: string;
  processor: QueueProcessor<BaseQueueMessage>;
  config: QueueConfig;
  batchConfig?: BatchProcessingConfig;
  enabled: boolean;
  priority: number;
  maxConcurrency: number;
  backpressureHandling: 'drop' | 'buffer' | 'reject';
  gracefulShutdownTimeoutMs: number;
}

/**
 * Message transformation result
 */
export interface MessageTransformResult<T> {
  success: boolean;
  transformedMessage?: T;
  error?: string;
  warnings?: string[];
}

/**
 * Queue serialization options
 */
export interface QueueSerializationOptions {
  compressionEnabled: boolean;
  encryptionEnabled: boolean;
  schemaValidation: boolean;
  versionCompatibility: boolean;
}

/**
 * Queue monitoring alerts
 */
export interface QueueAlert {
  id: string;
  queueName: string;
  severity: 'info' | 'warning' | 'error' | 'critical';
  type: string;
  message: string;
  timestamp: string;
  resolved: boolean;
  resolvedAt?: string;
  metadata?: Record<string, any>;
}

/**
 * Queue performance thresholds
 */
export interface QueuePerformanceThresholds {
  maxProcessingTime: number;
  maxQueueDepth: number;
  maxErrorRate: number;
  maxRetryRate: number;
  minThroughput: number;
  maxMemoryUsage: number;
  maxCircuitBreakerOpenTime: number;
}

/**
 * Queue worker state
 */
export interface QueueWorkerState {
  workerId: string;
  queueName: string;
  status: 'idle' | 'processing' | 'error' | 'shutting_down';
  currentMessageId?: string;
  processingStartTime?: number;
  processedCount: number;
  errorCount: number;
  lastHeartbeat: string;
  memoryUsage: number;
  cpuUsage: number;
  lastActivityTime: string;
  startupTime: string;
}

/**
 * Queue manager configuration
 */
export interface QueueManagerConfig {
  redisUrl?: string;
  healthCheckInterval: number;
  metricsInterval: number;
  alertThresholds: QueuePerformanceThresholds;
  gracefulShutdownTimeout: number;
  circuitBreakerConfig: {
    failureThreshold: number;
    resetTimeout: number;
    monitoringPeriod: number;
  };
  backpressureConfig: {
    enabled: boolean;
    threshold: number;
    strategy: 'drop' | 'buffer' | 'reject';
    bufferSize: number;
  };
}

/**
 * Type guards for queue messages
 */
export const queueMessageGuards = {
  isEventMessage: (message: any): message is EventQueueMessage => {
    return message &&
           typeof message.polymarketId === 'string' &&
           typeof message.slug === 'string' &&
           typeof message.title === 'string' &&
           typeof message.category === 'string' &&
           typeof message.liquidity === 'number' &&
           typeof message.active === 'boolean' &&
           typeof message.closed === 'boolean';
  },

  isMarketMessage: (message: any): message is MarketQueueMessage => {
    return message &&
           typeof message.polymarketId === 'string' &&
           typeof message.eventId === 'string' &&
           typeof message.question === 'string' &&
           typeof message.liquidity === 'number' &&
           typeof message.currentPrice === 'number' &&
           typeof message.active === 'boolean' &&
           typeof message.closed === 'boolean';
  },

  isOutcomeMessage: (message: any): message is OutcomeQueueMessage => {
    return message &&
           typeof message.polymarketId === 'string' &&
           typeof message.marketId === 'string' &&
           typeof message.eventId === 'string' &&
           typeof message.title === 'string' &&
           typeof message.price === 'number' &&
           typeof message.probability === 'number' &&
           typeof message.status === 'string';
  },

  isTickMessage: (message: any): message is TickQueueMessage => {
    return message &&
           typeof message.marketId === 'string' &&
           typeof message.price === 'number' &&
           typeof message.liquidity === 'number' &&
           typeof message.tickType === 'string' &&
           typeof message.marketState === 'string';
  },

  isUpdateMessage: (message: any): message is UpdateQueueMessage => {
    return message &&
           typeof message.updateType === 'string' &&
           typeof message.entityId === 'string' &&
           typeof message.entityType === 'string' &&
           typeof message.priority === 'string';
  },

  isDeadletterMessage: (message: any): message is DeadletterQueueMessage => {
    return message &&
           typeof message.originalStream === 'string' &&
           typeof message.originalMessageId === 'string' &&
           typeof message.error === 'string' &&
           typeof message.retryCount === 'number' &&
           typeof message.maxRetries === 'number';
  }
};

/**
 * Default configurations for different queue types
 */
export const DEFAULT_QUEUE_CONFIGS = {
  events: {
    name: 'events',
    streamName: 'sc:events',
    consumerGroup: 'signalcast',
    batchSize: 100,
    timeoutMs: 2000,
    maxRetries: 3,
    retryDelayMs: 1000,
    deadLetterEnabled: true,
    idempotencyWindowMs: 300000, // 5 minutes
    backpressureThreshold: 10000,
    circuitBreakerThreshold: 5,
    processingTimeoutMs: 10000,
    healthCheckIntervalMs: 30000,
    metricsEnabled: true,
  } as QueueConfig,

  markets: {
    name: 'markets',
    streamName: 'sc:markets',
    consumerGroup: 'signalcast',
    batchSize: 100,
    timeoutMs: 2000,
    maxRetries: 3,
    retryDelayMs: 1000,
    deadLetterEnabled: true,
    idempotencyWindowMs: 300000,
    backpressureThreshold: 10000,
    circuitBreakerThreshold: 5,
    processingTimeoutMs: 10000,
    healthCheckIntervalMs: 30000,
    metricsEnabled: true,
  } as QueueConfig,

  outcomes: {
    name: 'outcomes',
    streamName: 'sc:outcomes',
    consumerGroup: 'signalcast',
    batchSize: 100,
    timeoutMs: 2000,
    maxRetries: 3,
    retryDelayMs: 1000,
    deadLetterEnabled: true,
    idempotencyWindowMs: 300000,
    backpressureThreshold: 10000,
    circuitBreakerThreshold: 5,
    processingTimeoutMs: 10000,
    healthCheckIntervalMs: 30000,
    metricsEnabled: true,
  } as QueueConfig,

  ticks: {
    name: 'ticks',
    streamName: 'sc:ticks',
    consumerGroup: 'signalcast',
    batchSize: 200, // Higher batch size for real-time data
    timeoutMs: 1000, // Lower timeout for real-time processing
    maxRetries: 2,
    retryDelayMs: 500,
    deadLetterEnabled: true,
    idempotencyWindowMs: 60000, // 1 minute for ticks
    backpressureThreshold: 50000,
    circuitBreakerThreshold: 10,
    processingTimeoutMs: 5000,
    healthCheckIntervalMs: 15000,
    metricsEnabled: true,
  } as QueueConfig,

  updates: {
    name: 'updates',
    streamName: 'sc:updates',
    consumerGroup: 'signalcast',
    batchSize: 50,
    timeoutMs: 3000,
    maxRetries: 5,
    retryDelayMs: 2000,
    deadLetterEnabled: true,
    idempotencyWindowMs: 600000, // 10 minutes
    backpressureThreshold: 5000,
    circuitBreakerThreshold: 3,
    processingTimeoutMs: 15000,
    healthCheckIntervalMs: 30000,
    metricsEnabled: true,
  } as QueueConfig,

  deadletter: {
    name: 'deadletter',
    streamName: 'sc:deadletter',
    consumerGroup: 'signalcast-deadletter',
    batchSize: 25,
    timeoutMs: 5000,
    maxRetries: 1,
    retryDelayMs: 5000,
    deadLetterEnabled: false,
    idempotencyWindowMs: 0,
    backpressureThreshold: 1000,
    circuitBreakerThreshold: 2,
    processingTimeoutMs: 30000,
    healthCheckIntervalMs: 60000,
    metricsEnabled: true,
  } as QueueConfig,
};
