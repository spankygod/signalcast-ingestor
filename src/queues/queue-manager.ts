/**
 * Queue Manager for SignalCast ingestion system
 * Orchestrates all queues, provides centralized management, monitoring, and control
 */

import {
  EventsQueue,
  MarketsQueue,
  OutcomesQueue,
  TicksQueue,
  UpdatesQueue,
  DeadletterQueue,
  QueueConfig,
  QueueHealthStatus,
  QueueMetrics,
  BatchProcessingConfig,
  DEFAULT_QUEUE_CONFIGS
} from './index';
import { Logger, LogCategory, rootLogger } from '../lib/logger';
import { redisUtils } from '../lib/redis';
import { RetryManager, defaultRetryManager } from '../lib/retry';

/**
 * Queue manager configuration
 */
export interface QueueManagerConfig {
  enabledQueues: string[];
  globalBatchConfig?: BatchProcessingConfig;
  healthCheckIntervalMs: number;
  metricsIntervalMs: number;
  alertThresholds: {
    maxErrorRate: number;
    maxQueueDepth: number;
    maxProcessingTime: number;
    minThroughput: number;
  };
  gracefulShutdownTimeoutMs: number;
  autoScaleEnabled: boolean;
  autoScaleConfig: {
    minWorkers: number;
    maxWorkers: number;
    scaleUpThreshold: number;
    scaleDownThreshold: number;
    checkIntervalMs: number;
  };
}

/**
 * Queue manager statistics
 */
export interface QueueManagerStats {
  totalQueues: number;
  activeQueues: number;
  totalMessages: number;
  processedMessages: number;
  failedMessages: number;
  deadletterMessages: number;
  averageProcessingTime: number;
  overallThroughput: number;
  systemHealth: 'healthy' | 'degraded' | 'unhealthy' | 'critical';
  lastHealthCheck: string;
  uptime: number;
  workerCount: number;
}

/**
 * Queue manager implementation
 */
export class QueueManager {
  private logger: Logger;
  private config: QueueManagerConfig;
  private queues: Map<string, any> = new Map();
  private retryManager: RetryManager;
  private isRunning: boolean = false;
  private startTime: number;
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private metricsInterval: NodeJS.Timeout | null = null;
  private autoScaleInterval: NodeJS.Timeout | null = null;
  private shutdownTimeout: NodeJS.Timeout | null = null;

  constructor(config: Partial<QueueManagerConfig> = {}) {
    this.logger = rootLogger.child({ component: 'queue-manager' });
    this.config = this.mergeConfig(config);
    this.retryManager = defaultRetryManager;
    this.startTime = Date.now();

    this.logger.info(`Queue manager initialized`, {
      enabledQueues: this.config.enabledQueues,
      autoScaleEnabled: this.config.autoScaleEnabled,
      healthCheckInterval: this.config.healthCheckIntervalMs,
    });
  }

  /**
   * Initialize all enabled queues
   */
  async initialize(): Promise<void> {
    try {
      this.logger.info(`Initializing queue manager with ${this.config.enabledQueues.length} enabled queues`);

      // Initialize individual queues
      for (const queueName of this.config.enabledQueues) {
        await this.initializeQueue(queueName);
      }

      // Start monitoring intervals
      this.startHealthChecking();
      this.startMetricsCollection();

      if (this.config.autoScaleEnabled) {
        this.startAutoScaling();
      }

      this.logger.info(`Queue manager initialization completed`, {
        initializedQueues: Array.from(this.queues.keys()),
      });
    } catch (error) {
      this.logger.error(`Queue manager initialization failed`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Start all enabled queues
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn(`Queue manager is already running`);
      return;
    }

    this.isRunning = true;
    this.logger.info(`Starting queue manager`);

    try {
      // Start all queues concurrently
      const startPromises = Array.from(this.queues.values()).map(queue => queue.start());
      await Promise.all(startPromises);

      this.logger.info(`All queues started successfully`);
    } catch (error) {
      this.logger.error(`Failed to start queues`, {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop all queues gracefully
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.logger.info(`Stopping queue manager`);
    this.isRunning = false;

    // Set shutdown timeout
    if (this.config.gracefulShutdownTimeoutMs) {
      this.shutdownTimeout = setTimeout(() => {
        this.logger.warn(`Force shutdown after timeout`);
      }, this.config.gracefulShutdownTimeoutMs);
    }

    try {
      // Stop all queues concurrently
      const stopPromises = Array.from(this.queues.values()).map(queue => queue.stop());
      await Promise.all(stopPromises);

      // Clean up intervals
      if (this.healthCheckInterval) {
        clearInterval(this.healthCheckInterval);
        this.healthCheckInterval = null;
      }

      if (this.metricsInterval) {
        clearInterval(this.metricsInterval);
        this.metricsInterval = null;
      }

      if (this.autoScaleInterval) {
        clearInterval(this.autoScaleInterval);
        this.autoScaleInterval = null;
      }

      if (this.shutdownTimeout) {
        clearTimeout(this.shutdownTimeout);
        this.shutdownTimeout = null;
      }

      this.logger.info(`Queue manager stopped successfully`);
    } catch (error) {
      this.logger.error(`Error during queue manager shutdown`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Get queue instance by name
   */
  getQueue<T>(queueName: string): T | undefined {
    return this.queues.get(queueName) as T;
  }

  /**
   * Get all queue instances
   */
  getAllQueues(): Map<string, any> {
    return new Map(this.queues);
  }

  /**
   * Get health status for all queues
   */
  async getHealthStatus(): Promise<Record<string, QueueHealthStatus>> {
    const healthStatus: Record<string, QueueHealthStatus> = {};

    for (const [queueName, queue] of this.queues) {
      try {
        healthStatus[queueName] = await queue.checkHealth();
      } catch (error) {
        this.logger.error(`Failed to get health status for queue ${queueName}`, {
          error: error as Error,
        });
        healthStatus[queueName] = {
          queueName,
          status: 'critical',
          lastCheck: new Date().toISOString(),
          issues: [{
            severity: 'critical',
            type: 'health_check_failed',
            message: (error as Error).message,
            timestamp: new Date().toISOString(),
          }],
          metrics: {
            streamName: '',
            consumerGroup: '',
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
          },
          recommendations: ['Restart queue', 'Check system resources'],
        };
      }
    }

    return healthStatus;
  }

  /**
   * Get comprehensive statistics
   */
  async getStatistics(): Promise<QueueManagerStats> {
    try {
      const healthStatus = await this.getHealthStatus();
      const queueMetrics = await this.getAllQueueMetrics();

      let totalMessages = 0;
      let processedMessages = 0;
      let failedMessages = 0;
      let deadletterMessages = 0;
      let totalProcessingTime = 0;
      let activeQueues = 0;

      for (const metrics of Object.values(queueMetrics)) {
        totalMessages += metrics.totalMessages;
        processedMessages += metrics.processedMessages;
        failedMessages += metrics.failedMessages;
        deadletterMessages += metrics.deadletterMessages;
        totalProcessingTime += metrics.averageProcessingTime;

        if (metrics.queueDepth > 0 || metrics.messagesPerSecond > 0) {
          activeQueues++;
        }
      }

      const averageProcessingTime = Object.values(queueMetrics).length > 0
        ? totalProcessingTime / Object.values(queueMetrics).length
        : 0;

      const uptime = Date.now() - this.startTime;
      const overallThroughput = uptime > 0 ? (processedMessages / (uptime / 1000)) : 0;

      // Determine system health
      const systemHealth = this.determineSystemHealth(healthStatus);

      return {
        totalQueues: this.queues.size,
        activeQueues,
        totalMessages,
        processedMessages,
        failedMessages,
        deadletterMessages,
        averageProcessingTime,
        overallThroughput,
        systemHealth,
        lastHealthCheck: new Date().toISOString(),
        uptime,
        workerCount: process.pid, // This would be enhanced in a real multi-worker system
      };
    } catch (error) {
      this.logger.error(`Failed to get queue manager statistics`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Add message to specific queue
   */
  async addMessage(queueName: string, message: any): Promise<string> {
    const queue = this.queues.get(queueName);
    if (!queue) {
      throw new Error(`Queue ${queueName} not found or not enabled`);
    }

    return await queue.addMessage(message);
  }

  /**
   * Add multiple messages to specific queue
   */
  async addMessages(queueName: string, messages: any[]): Promise<string[]> {
    const queue = this.queues.get(queueName);
    if (!queue) {
      throw new Error(`Queue ${queueName} not found or not enabled`);
    }

    return await queue.addMessages(messages);
  }

  /**
   * Get queue-specific statistics
   */
  async getQueueStatistics(queueName: string): Promise<any> {
    const queue = this.queues.get(queueName);
    if (!queue) {
      throw new Error(`Queue ${queueName} not found or not enabled`);
    }

    // Call the queue's specific statistics method if it exists
    if (typeof queue.getStatistics === 'function') {
      return await queue.getStatistics();
    } else {
      return await queue.getMetrics();
    }
  }

  /**
   * Enable/disable queue dynamically
   */
  async enableQueue(queueName: string): Promise<void> {
    if (!this.queues.has(queueName)) {
      await this.initializeQueue(queueName);
    }

    const queue = this.queues.get(queueName);
    if (this.isRunning) {
      await queue.start();
    }

    this.logger.info(`Queue ${queueName} enabled and started`);
  }

  async disableQueue(queueName: string): Promise<void> {
    const queue = this.queues.get(queueName);
    if (queue) {
      await queue.stop();
      this.logger.info(`Queue ${queueName} disabled and stopped`);
    }
  }

  /**
   * Scale queue workers (placeholder for autoscaling)
   */
  async scaleQueue(queueName: string, workerCount: number): Promise<void> {
    this.logger.info(`Scaling queue ${queueName} to ${workerCount} workers`);
    // This would implement actual worker scaling logic
    // For now, it's a placeholder that logs the request
  }

  /**
   * Initialize a specific queue
   */
  private async initializeQueue(queueName: string): Promise<void> {
    let queue: any;

    switch (queueName) {
      case 'events':
        queue = new EventsQueue(undefined, this.config.globalBatchConfig);
        break;
      case 'markets':
        queue = new MarketsQueue(undefined, this.config.globalBatchConfig);
        break;
      case 'outcomes':
        queue = new OutcomesQueue(undefined, this.config.globalBatchConfig);
        break;
      case 'ticks':
        queue = new TicksQueue(undefined, this.config.globalBatchConfig);
        break;
      case 'updates':
        queue = new UpdatesQueue(undefined, this.config.globalBatchConfig);
        break;
      case 'deadletter':
        queue = new DeadletterQueue(undefined, this.config.globalBatchConfig);
        break;
      default:
        throw new Error(`Unknown queue type: ${queueName}`);
    }

    await queue.initialize();
    this.queues.set(queueName, queue);

    this.logger.debug(`Queue ${queueName} initialized`);
  }

  /**
   * Get metrics for all queues
   */
  private async getAllQueueMetrics(): Promise<Record<string, QueueMetrics>> {
    const metrics: Record<string, QueueMetrics> = {};

    for (const [queueName, queue] of this.queues) {
      try {
        metrics[queueName] = await queue.getMetrics();
      } catch (error) {
        this.logger.error(`Failed to get metrics for queue ${queueName}`, {
          error: error as Error,
        });
      }
    }

    return metrics;
  }

  /**
   * Determine overall system health
   */
  private determineSystemHealth(healthStatus: Record<string, QueueHealthStatus>): 'healthy' | 'degraded' | 'unhealthy' | 'critical' {
    const statuses = Object.values(healthStatus).map(h => h.status);

    if (statuses.some(s => s === 'critical')) {
      return 'critical';
    }

    if (statuses.some(s => s === 'unhealthy')) {
      return 'unhealthy';
    }

    if (statuses.some(s => s === 'degraded')) {
      return 'degraded';
    }

    return 'healthy';
  }

  /**
   * Start health checking
   */
  private startHealthChecking(): void {
    this.healthCheckInterval = setInterval(async () => {
      try {
        const healthStatus = await this.getHealthStatus();
        const stats = await this.getStatistics();

        // Log health status if there are issues
        if (stats.systemHealth !== 'healthy') {
          this.logger.warn(`System health degraded`, {
            systemHealth: stats.systemHealth,
            totalQueues: stats.totalQueues,
            activeQueues: stats.activeQueues,
            failedMessages: stats.failedMessages,
          });
        }

        // Store health status in Redis for monitoring
        const healthKey = redisUtils.getMetricsKey('queue_manager_health');
        await redisUtils.hSet(healthKey, 'status', JSON.stringify({
          systemHealth: stats.systemHealth,
          timestamp: new Date().toISOString(),
          queueHealth: healthStatus,
        }));

      } catch (error) {
        this.logger.error(`Health check failed`, {
          error: error as Error,
        });
      }
    }, this.config.healthCheckIntervalMs);
  }

  /**
   * Start metrics collection
   */
  private startMetricsCollection(): void {
    this.metricsInterval = setInterval(async () => {
      try {
        const stats = await this.getStatistics();

        // Store metrics in Redis
        const metricsKey = redisUtils.getMetricsKey('queue_manager_stats');
        await redisUtils.hSet(metricsKey, 'stats', JSON.stringify(stats));

        // Update global counters
        await redisUtils.increment('queue_manager_uptime', this.config.metricsIntervalMs / 1000);

      } catch (error) {
        this.logger.error(`Metrics collection failed`, {
          error: error as Error,
        });
      }
    }, this.config.metricsIntervalMs);
  }

  /**
   * Start auto-scaling (placeholder implementation)
   */
  private startAutoScaling(): void {
    if (!this.config.autoScaleEnabled) {
      return;
    }

    this.autoScaleInterval = setInterval(async () => {
      try {
        const stats = await this.getStatistics();
        const queueMetrics = await this.getAllQueueMetrics();

        for (const [queueName, metrics] of Object.entries(queueMetrics)) {
          // Scale up logic
          if (metrics.queueDepth > this.config.autoScaleConfig.scaleUpThreshold) {
            this.logger.info(`Scale up trigger for queue ${queueName}`, {
              queueDepth: metrics.queueDepth,
              threshold: this.config.autoScaleConfig.scaleUpThreshold,
            });
            // Implement actual scaling logic here
          }

          // Scale down logic
          if (metrics.queueDepth < this.config.autoScaleConfig.scaleDownThreshold) {
            this.logger.info(`Scale down trigger for queue ${queueName}`, {
              queueDepth: metrics.queueDepth,
              threshold: this.config.autoScaleConfig.scaleDownThreshold,
            });
            // Implement actual scaling logic here
          }
        }
      } catch (error) {
        this.logger.error(`Auto-scaling check failed`, {
          error: error as Error,
        });
      }
    }, this.config.autoScaleConfig.checkIntervalMs);
  }

  /**
   * Merge configuration with defaults
   */
  private mergeConfig(config: Partial<QueueManagerConfig>): QueueManagerConfig {
    const defaultConfig: QueueManagerConfig = {
      enabledQueues: ['events', 'markets', 'outcomes', 'ticks', 'updates', 'deadletter'],
      globalBatchConfig: {
        enabled: true,
        maxBatchSize: 100,
        batchTimeoutMs: 2000,
        flushOnShutdown: true,
        partialBatchProcessing: false,
        aggregationStrategy: 'simple',
      },
      healthCheckIntervalMs: 30000, // 30 seconds
      metricsIntervalMs: 60000, // 1 minute
      alertThresholds: {
        maxErrorRate: 0.1, // 10%
        maxQueueDepth: 10000,
        maxProcessingTime: 5000, // 5 seconds
        minThroughput: 10, // messages per second
      },
      gracefulShutdownTimeoutMs: 30000, // 30 seconds
      autoScaleEnabled: false,
      autoScaleConfig: {
        minWorkers: 1,
        maxWorkers: 10,
        scaleUpThreshold: 1000,
        scaleDownThreshold: 100,
        checkIntervalMs: 60000, // 1 minute
      },
    };

    return { ...defaultConfig, ...config };
  }
}

/**
 * Default queue manager instance
 */
export const queueManager = new QueueManager();

/**
 * Initialize and start queue manager (convenience function)
 */
export async function initializeQueueManager(config?: Partial<QueueManagerConfig>): Promise<QueueManager> {
  const manager = new QueueManager(config);
  await manager.initialize();
  return manager;
}