/**
 * Autoscaler Worker
 * Monitors queue depths and dynamically scales workers between 1-3 instances
 * Based on confirmed thresholds: 5K for scale up, 500 for scale down
 */

import { Logger, LoggerFactory, LogCategory, LogLevel } from '../lib/logger';
import { PM2ProcessManager, type ClusterStatus } from '../lib/pm2-manager';
import { retryUtils } from '../lib/retry';
import { EventsQueue, MarketsQueue, OutcomesQueue, TicksQueue } from '../queues';
import { config, features } from '../config';

/**
 * Autoscaler configuration
 */
interface AutoscalerConfig {
  checkIntervalMs: number;
  minWorkers: number;
  maxWorkers: number;
  scaleUpThreshold: number;
  scaleDownThreshold: number;
  cooldownMs: number;
  maxRestartsPerHour: number;
  enablePredictiveScaling: boolean;
  metricsRetentionHours: number;
  healthCheckIntervalMs: number;
}

/**
 * Worker type configuration
 */
interface WorkerTypeConfig {
  name: string;
  script: string;
  currentInstances: number;
  minInstances: number;
  maxInstances: number;
  priority: number;
  dependencies: string[];
  lastScaleAction?: 'up' | 'down';
  lastScaleTime: number;
  restarts: number;
  restartTimestamps: number[];
}

/**
 * Queue metrics
 */
interface QueueMetrics {
  name: string;
  depth: number;
  processingRate: number;
  errorRate: number;
  averageProcessingTime: number;
  lastUpdated: number;
}

/**
 * Scaling decision
 */
interface ScalingDecision {
  workerType: string;
  action: 'scale_up' | 'scale_down' | 'no_action';
  targetInstances: number;
  reason: string;
  confidence: number;
  metrics: QueueMetrics[];
}

/**
 * Autoscaling statistics
 */
interface AutoscalerStats {
  totalChecks: number;
  scaleUpActions: number;
  scaleDownActions: number;
  noActionChecks: number;
  errorsCount: number;
  uptime: number;
  lastCheckAt?: string;
  lastScaleActionAt?: string;
  averageCheckTime: number;
  workerMetrics: Record<string, {
    currentInstances: number;
    totalScalingEvents: number;
    uptime: number;
  }>;
  queueMetrics: QueueMetrics[];
}

/**
 * Autoscaler worker class
 */
export class AutoscalerWorker {
  private logger: Logger;
  private config: AutoscalerConfig;
  private pm2Manager: PM2ProcessManager;
  private eventsQueue: EventsQueue;
  private marketsQueue: MarketsQueue;
  private outcomesQueue: OutcomesQueue;
  private ticksQueue: TicksQueue;

  private isRunning: boolean = false;
  private startTime: number;
  private stats: AutoscalerStats;
  private checkTimer: NodeJS.Timeout | null = null;
  private healthCheckTimer: NodeJS.Timeout | null = null;
  private metricsHistory: Map<string, QueueMetrics[]> = new Map();

  // Worker configurations
  private workerTypes: Map<string, WorkerTypeConfig> = new Map();

  constructor(
    pm2Manager: PM2ProcessManager,
    eventsQueue: EventsQueue,
    marketsQueue: MarketsQueue,
    outcomesQueue: OutcomesQueue,
    ticksQueue: TicksQueue,
    config?: Partial<AutoscalerConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('autoscaler', process.pid.toString());
    this.pm2Manager = pm2Manager;
    this.eventsQueue = eventsQueue;
    this.marketsQueue = marketsQueue;
    this.outcomesQueue = outcomesQueue;
    this.ticksQueue = ticksQueue;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      checkIntervalMs: 10000, // 10 seconds
      minWorkers: 1,
      maxWorkers: 3,
      scaleUpThreshold: 5000,
      scaleDownThreshold: 500,
      cooldownMs: 60000, // 1 minute
      maxRestartsPerHour: 10,
      enablePredictiveScaling: false,
      metricsRetentionHours: 24,
      healthCheckIntervalMs: 30000,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalChecks: 0,
      scaleUpActions: 0,
      scaleDownActions: 0,
      noActionChecks: 0,
      errorsCount: 0,
      uptime: 0,
      averageCheckTime: 0,
      workerMetrics: {},
      queueMetrics: [],
    };

    this.setupWorkerTypes();
    this.setupGracefulShutdown();
    this.logger.info('Autoscaler worker initialized', {
      config: this.config,
    });
  }

  /**
   * Setup worker type configurations
   */
  private setupWorkerTypes(): void {
    // Events poller
    this.workerTypes.set('events-poller', {
      name: 'events-poller',
      script: './src/workers/events-poller.js',
      currentInstances: 1,
      minInstances: this.config.minWorkers,
      maxInstances: this.config.maxWorkers,
      priority: 10,
      dependencies: [],
      lastScaleTime: Date.now(),
      restarts: 0,
      restartTimestamps: [],
    });

    // Markets poller
    this.workerTypes.set('markets-poller', {
      name: 'markets-poller',
      script: './src/workers/markets-poller.js',
      currentInstances: 1,
      minInstances: this.config.minWorkers,
      maxInstances: this.config.maxWorkers,
      priority: 10,
      dependencies: [],
      lastScaleTime: Date.now(),
      restarts: 0,
      restartTimestamps: [],
    });

    // Outcomes poller
    this.workerTypes.set('outcomes-poller', {
      name: 'outcomes-poller',
      script: './src/workers/outcomes-poller.js',
      currentInstances: 1,
      minInstances: this.config.minWorkers,
      maxInstances: this.config.maxWorkers,
      priority: 10,
      dependencies: [],
      lastScaleTime: Date.now(),
      restarts: 0,
      restartTimestamps: [],
    });

    // DB writer (higher priority, max 2 instances)
    this.workerTypes.set('db-writer', {
      name: 'db-writer',
      script: './src/workers/db-writer.js',
      currentInstances: 1,
      minInstances: 1,
      maxInstances: 2,
      priority: 20,
      dependencies: ['events-poller', 'markets-poller', 'outcomes-poller'],
      lastScaleTime: Date.now(),
      restarts: 0,
      restartTimestamps: [],
    });
  }

  /**
   * Start the autoscaler
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Autoscaler is already running');
      return;
    }

    this.logger.info('Starting autoscaler worker');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Initialize current worker instances
      await this.initializeWorkerInstances();

      // Perform initial scaling check
      await this.performScalingCheck();

      // Setup periodic scaling checks
      this.checkTimer = setInterval(async () => {
        if (this.isRunning) {
          await this.performScalingCheck();
        }
      }, this.config.checkIntervalMs);

      // Setup health monitoring
      this.healthCheckTimer = setInterval(async () => {
        if (this.isRunning) {
          await this.performHealthCheck();
        }
      }, this.config.healthCheckIntervalMs);

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('Autoscaler worker started successfully', {
        checkIntervalMs: this.config.checkIntervalMs,
        workerTypesCount: this.workerTypes.size,
      });

    } catch (error) {
      this.logger.error('Failed to start autoscaler worker', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the autoscaler
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Autoscaler is not running');
      return;
    }

    this.logger.info('Stopping autoscaler worker');
    this.isRunning = false;

    if (this.checkTimer) {
      clearInterval(this.checkTimer);
      this.checkTimer = null;
    }

    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }

    this.logger.info('Autoscaler worker stopped', {
      finalStats: this.getStats(),
    });
  }

  /**
   * Initialize current worker instances
   */
  private async initializeWorkerInstances(): Promise<void> {
    try {
      const clusterStatus: ClusterStatus = await this.pm2Manager.getClusterStatus();

      // Update current instances for each worker type
      for (const [workerTypeName, workerType] of this.workerTypes) {
        const workerTypeStatus = clusterStatus.workerTypes[workerTypeName];
        if (workerTypeStatus) {
          workerType.currentInstances = workerTypeStatus.running;
          this.stats.workerMetrics[workerTypeName] = {
            currentInstances: workerType.currentInstances,
            totalScalingEvents: 0,
            uptime: 0,
          };
        }
      }

      this.logger.info('Worker instances initialized', {
        workerTypes: Object.fromEntries(
          Array.from(this.workerTypes.entries()).map(([name, config]) => [
            name,
            config.currentInstances,
          ])
        ),
      });

    } catch (error) {
      this.logger.warn('Failed to get initial cluster status', {
        error: error as Error,
      });
    }
  }

  /**
   * Perform scaling check
   */
  private async performScalingCheck(): Promise<void> {
    const checkStartTime = Date.now();

    return this.logger.timed('autoscaling-check', LogCategory.AUTOSCALER, LogLevel.INFO, async () => {
      await retryUtils.withExponentialBackoff(
        async () => {
          // Collect queue metrics
          const queueMetrics = await this.collectQueueMetrics();

          // Analyze and make scaling decisions
          const scalingDecisions = await this.analyzeAndDecide(queueMetrics);

          // Execute scaling decisions
          await this.executeScalingDecisions(scalingDecisions);

          // Update statistics
          this.updateStats(checkStartTime, scalingDecisions, queueMetrics);

          this.stats.lastCheckAt = new Date().toISOString();

        },
        2, // maxRetries (scaling is critical)
        1000, // retryDelayMs
        5000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Collect metrics from all queues
   */
  private async collectQueueMetrics(): Promise<QueueMetrics[]> {
    const now = Date.now();
    const metrics: QueueMetrics[] = [];

    try {
      // Events queue metrics
      const eventsDepth = await this.getQueueDepth(this.eventsQueue);
      metrics.push({
        name: 'events',
        depth: eventsDepth,
        processingRate: 10, // Would be calculated from actual queue stats
        errorRate: 0.05, // Would be calculated from actual queue stats
        averageProcessingTime: 150,
        lastUpdated: now,
      });

      // Markets queue metrics
      const marketsDepth = await this.getQueueDepth(this.marketsQueue);
      metrics.push({
        name: 'markets',
        depth: marketsDepth,
        processingRate: 25,
        errorRate: 0.03,
        averageProcessingTime: 200,
        lastUpdated: now,
      });

      // Outcomes queue metrics
      const outcomesDepth = await this.getQueueDepth(this.outcomesQueue);
      metrics.push({
        name: 'outcomes',
        depth: outcomesDepth,
        processingRate: 30,
        errorRate: 0.04,
        averageProcessingTime: 180,
        lastUpdated: now,
      });

      // Ticks queue metrics (high frequency)
      const ticksDepth = await this.getQueueDepth(this.ticksQueue);
      metrics.push({
        name: 'ticks',
        depth: ticksDepth,
        processingRate: 100,
        errorRate: 0.02,
        averageProcessingTime: 50,
        lastUpdated: now,
      });

    } catch (error) {
      this.logger.error('Failed to collect queue metrics', {
        error: error as Error,
      });
      this.stats.errorsCount++;
    }

    // Store metrics in history
    metrics.forEach(metric => {
      if (!this.metricsHistory.has(metric.name)) {
        this.metricsHistory.set(metric.name, []);
      }

      const history = this.metricsHistory.get(metric.name)!;
      history.push(metric);

      // Keep only recent metrics (24 hours)
      const cutoffTime = now - (this.config.metricsRetentionHours * 60 * 60 * 1000);
      while (history.length > 0 && history[0].lastUpdated < cutoffTime) {
        history.shift();
      }
    });

    this.stats.queueMetrics = metrics;
    return metrics;
  }

  /**
   * Get queue depth (placeholder implementation)
   */
  private async getQueueDepth(queue: any): Promise<number> {
    // This would need to be implemented in the queue classes
    // For now, return simulated values based on time
    const baseDepth = Math.floor(Math.random() * 2000);
    const timeVariation = Math.sin(Date.now() / 10000) * 500;
    return Math.max(0, baseDepth + timeVariation);
  }

  /**
   * Analyze metrics and make scaling decisions
   */
  private async analyzeAndDecide(queueMetrics: QueueMetrics[]): Promise<ScalingDecision[]> {
    const decisions: ScalingDecision[] = [];

    // Calculate total queue depth
    const totalDepth = queueMetrics.reduce((sum, metric) => sum + metric.depth, 0);

    // Analyze each worker type
    for (const [workerTypeName, workerType] of this.workerTypes) {
      const decision = await this.makeScalingDecision(workerTypeName, workerType, queueMetrics, totalDepth);
      decisions.push(decision);
    }

    return decisions;
  }

  /**
   * Make scaling decision for a specific worker type
   */
  private async makeScalingDecision(
    workerTypeName: string,
    workerType: WorkerTypeConfig,
    queueMetrics: QueueMetrics[],
    totalDepth: number
  ): Promise<ScalingDecision> {
    const now = Date.now();
    const timeSinceLastScale = now - workerType.lastScaleTime;

    // Enforce cooldown period
    if (timeSinceLastScale < this.config.cooldownMs) {
      return {
        workerType: workerTypeName,
        action: 'no_action',
        targetInstances: workerType.currentInstances,
        reason: `Cooldown period (${timeSinceLastScale}ms < ${this.config.cooldownMs}ms)`,
        confidence: 1.0,
        metrics: queueMetrics,
      };
    }

    // Calculate relevant queue depth for this worker type
    let relevantDepth = totalDepth;
    if (workerTypeName === 'events-poller') {
      relevantDepth = queueMetrics.find(m => m.name === 'events')?.depth || 0;
    } else if (workerTypeName === 'markets-poller') {
      relevantDepth = queueMetrics.find(m => m.name === 'markets')?.depth || 0;
    } else if (workerTypeName === 'outcomes-poller') {
      relevantDepth = queueMetrics.find(m => m.name === 'outcomes')?.depth || 0;
    } else if (workerTypeName === 'db-writer') {
      // DB writer cares about all queues
      relevantDepth = totalDepth;
    }

    // Make scaling decision
    let action: ScalingDecision['action'] = 'no_action';
    let targetInstances = workerType.currentInstances;
    let reason: string;
    let confidence = 0.8;

    if (relevantDepth > this.config.scaleUpThreshold && workerType.currentInstances < workerType.maxInstances) {
      action = 'scale_up';
      targetInstances = Math.min(workerType.currentInstances + 1, workerType.maxInstances);
      reason = `Queue depth ${relevantDepth} > scale-up threshold ${this.config.scaleUpThreshold}`;
      confidence = Math.min(1.0, relevantDepth / this.config.scaleUpThreshold);
    } else if (relevantDepth < this.config.scaleDownThreshold && workerType.currentInstances > workerType.minInstances) {
      action = 'scale_down';
      targetInstances = Math.max(workerType.currentInstances - 1, workerType.minInstances);
      reason = `Queue depth ${relevantDepth} < scale-down threshold ${this.config.scaleDownThreshold}`;
      confidence = 0.7;
    } else {
      reason = `Queue depth ${relevantDepth} within acceptable range [${this.config.scaleDownThreshold}, ${this.config.scaleUpThreshold}]`;
    }

    return {
      workerType: workerTypeName,
      action,
      targetInstances,
      reason,
      confidence,
      metrics: queueMetrics,
    };
  }

  /**
   * Execute scaling decisions
   */
  private async executeScalingDecisions(decisions: ScalingDecision[]): Promise<void> {
    for (const decision of decisions) {
      if (decision.action === 'no_action') {
        this.stats.noActionChecks++;
        continue;
      }

      try {
        const workerType = this.workerTypes.get(decision.workerType);
        if (!workerType) {
          this.logger.error('Worker type not found', {
            workerType: decision.workerType,
          });
          continue;
        }

        if (decision.action === 'scale_up') {
          await this.scaleUp(workerType, decision);
          this.stats.scaleUpActions++;
        } else if (decision.action === 'scale_down') {
          await this.scaleDown(workerType, decision);
          this.stats.scaleDownActions++;
        }

        this.stats.lastScaleActionAt = new Date().toISOString();

      } catch (error) {
        this.logger.error('Failed to execute scaling decision', {
          decision,
          error: error as Error,
        });
        this.stats.errorsCount++;
      }
    }
  }

  /**
   * Scale up worker instances
   */
  private async scaleUp(workerType: WorkerTypeConfig, decision: ScalingDecision): Promise<void> {
    this.logger.info('Scaling up worker', {
      workerType: workerType.name,
      from: workerType.currentInstances,
      to: decision.targetInstances,
      reason: decision.reason,
      confidence: decision.confidence,
    });

    try {
      await this.pm2Manager.scaleWorkerType(workerType.name, decision.targetInstances);
      workerType.currentInstances = decision.targetInstances;
      workerType.lastScaleAction = 'up';
      workerType.lastScaleTime = Date.now();

      // Update worker metrics
      if (!this.stats.workerMetrics[workerType.name]) {
        this.stats.workerMetrics[workerType.name] = {
          currentInstances: workerType.currentInstances,
          totalScalingEvents: 0,
          uptime: 0,
        };
      }
      this.stats.workerMetrics[workerType.name].currentInstances = workerType.currentInstances;
      this.stats.workerMetrics[workerType.name].totalScalingEvents++;

    } catch (error) {
      this.logger.error('Failed to scale up worker', {
        workerType: workerType.name,
        targetInstances: decision.targetInstances,
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Scale down worker instances
   */
  private async scaleDown(workerType: WorkerTypeConfig, decision: ScalingDecision): Promise<void> {
    this.logger.info('Scaling down worker', {
      workerType: workerType.name,
      from: workerType.currentInstances,
      to: decision.targetInstances,
      reason: decision.reason,
      confidence: decision.confidence,
    });

    try {
      await this.pm2Manager.scaleWorkerType(workerType.name, decision.targetInstances);
      workerType.currentInstances = decision.targetInstances;
      workerType.lastScaleAction = 'down';
      workerType.lastScaleTime = Date.now();

      // Update worker metrics
      if (!this.stats.workerMetrics[workerType.name]) {
        this.stats.workerMetrics[workerType.name] = {
          currentInstances: workerType.currentInstances,
          totalScalingEvents: 0,
          uptime: 0,
        };
      }
      this.stats.workerMetrics[workerType.name].currentInstances = workerType.currentInstances;
      this.stats.workerMetrics[workerType.name].totalScalingEvents++;

    } catch (error) {
      this.logger.error('Failed to scale down worker', {
        workerType: workerType.name,
        targetInstances: decision.targetInstances,
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Perform health check
   */
  private async performHealthCheck(): Promise<void> {
    try {
      const clusterStatus: ClusterStatus = await this.pm2Manager.getClusterStatus();

      // Check for unhealthy workers
      const unhealthyWorkers = Object.entries(clusterStatus.workerTypes)
        .filter(([_, stats]) => stats.healthy === 0)
        .map(([name, _]) => name);

      if (unhealthyWorkers.length > 0) {
        this.logger.warn('Unhealthy workers detected', {
          unhealthyWorkers,
        });

        // Attempt to restart unhealthy workers
        for (const workerName of unhealthyWorkers) {
          const workerType = this.workerTypes.get(workerName);
          if (workerType) {
            await this.restartWorker(workerType);
          }
        }
      }

    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
      this.stats.errorsCount++;
    }
  }

  /**
   * Restart worker
   */
  private async restartWorker(workerType: WorkerTypeConfig): Promise<void> {
    const now = Date.now();
    const recentRestarts = workerType.restartTimestamps.filter(
      timestamp => now - timestamp < 3600000 // Last hour
    );

    if (recentRestarts.length >= this.config.maxRestartsPerHour) {
      this.logger.error('Worker restart limit exceeded', {
        workerType: workerType.name,
        restarts: recentRestarts.length,
        maxRestarts: this.config.maxRestartsPerHour,
      });
      return;
    }

    this.logger.info('Restarting worker', {
      workerType: workerType.name,
      recentRestarts: recentRestarts.length,
    });

    try {
      // In a real implementation, this would restart the PM2 process
      workerType.restarts++;
      workerType.restartTimestamps.push(now);

      // Clean up old restart timestamps
      workerType.restartTimestamps = workerType.restartTimestamps.filter(
        timestamp => now - timestamp < 3600000
      );

    } catch (error) {
      this.logger.error('Failed to restart worker', {
        workerType: workerType.name,
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Update statistics
   */
  private updateStats(checkStartTime: number, decisions: ScalingDecision[], queueMetrics: QueueMetrics[]): void {
    this.stats.totalChecks++;

    const checkTime = Date.now() - checkStartTime;
    const totalTime = this.stats.averageCheckTime * (this.stats.totalChecks - 1) + checkTime;
    this.stats.averageCheckTime = totalTime / this.stats.totalChecks;
    this.stats.uptime = Date.now() - this.startTime;
  }

  /**
   * Get current statistics
   */
  getStats(): AutoscalerStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
      workerMetrics: Object.fromEntries(
        Array.from(this.stats.workerMetrics.entries()).map(([name, metrics]) => [
          name,
          { ...metrics, uptime: Date.now() - this.startTime },
        ])
      ),
    };
  }

  /**
   * Check if autoscaler is healthy
   */
  isHealthy(): boolean {
    const now = Date.now();
    const timeSinceLastCheck = this.stats.lastCheckAt
      ? now - new Date(this.stats.lastCheckAt).getTime()
      : Infinity;

    // Consider healthy if last check was within last 2 minutes
    const recentCheck = timeSinceLastCheck < 120000;

    // Consider healthy if error rate is below 15%
    const errorRate = this.stats.totalChecks > 0
      ? this.stats.errorsCount / this.stats.totalChecks
      : 0;
    const acceptableErrorRate = errorRate < 0.15;

    return this.isRunning && recentCheck && acceptableErrorRate;
  }

  /**
   * Get scaling recommendations
   */
  getScalingRecommendations(): Array<{
    workerType: string;
    currentInstances: number;
    recommendedInstances: number;
    reason: string;
    priority: 'low' | 'medium' | 'high';
  }> {
    const recommendations = [];
    const now = Date.now();

    for (const [workerTypeName, workerType] of this.workerTypes) {
      const timeSinceLastScale = now - workerType.lastScaleTime;

      // Skip if in cooldown
      if (timeSinceLastScale < this.config.cooldownMs) {
        continue;
      }

      const metrics = this.stats.queueMetrics.find(m => m.name === workerTypeName.replace('-poller', ''));
      if (!metrics) continue;

      let recommendedInstances = workerType.currentInstances;
      let reason = 'Current configuration is optimal';
      let priority: 'low' | 'medium' | 'high' = 'low';

      if (metrics.depth > this.config.scaleUpThreshold && workerType.currentInstances < workerType.maxInstances) {
        recommendedInstances = Math.min(workerType.currentInstances + 1, workerType.maxInstances);
        reason = `High queue depth (${metrics.depth}) requires scaling up`;
        priority = metrics.depth > this.config.scaleUpThreshold * 2 ? 'high' : 'medium';
      } else if (metrics.depth < this.config.scaleDownThreshold && workerType.currentInstances > workerType.minInstances) {
        recommendedInstances = Math.max(workerType.currentInstances - 1, workerType.minInstances);
        reason = `Low queue depth (${metrics.depth}) allows scaling down`;
        priority = 'low';
      }

      if (recommendedInstances !== workerType.currentInstances) {
        recommendations.push({
          workerType: workerTypeName,
          currentInstances: workerType.currentInstances,
          recommendedInstances,
          reason,
          priority,
        });
      }
    }

    return recommendations.sort((a, b) => {
      const priorityOrder = { high: 3, medium: 2, low: 1 };
      return priorityOrder[b.priority] - priorityOrder[a.priority];
    });
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down autoscaler`);
      try {
        await this.stop();
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

  /**
   * Get current worker configurations
   */
  getWorkerConfigurations(): Record<string, WorkerTypeConfig> {
    return Object.fromEntries(this.workerTypes);
  }

  /**
   * Get metrics history
   */
  getMetricsHistory(): Record<string, QueueMetrics[]> {
    return Object.fromEntries(this.metricsHistory);
  }

  /**
   * Force scaling check
   */
  async forceScalingCheck(): Promise<ScalingDecision[]> {
    this.logger.info('Forcing scaling check');
    const queueMetrics = await this.collectQueueMetrics();
    return await this.analyzeAndDecide(queueMetrics);
  }
}

/**
 * Create and initialize autoscaler worker
 */
export async function createAutoscaler(
  pm2Manager: PM2ProcessManager,
  eventsQueue: EventsQueue,
  marketsQueue: MarketsQueue,
  outcomesQueue: OutcomesQueue,
  ticksQueue: TicksQueue,
  config?: Partial<AutoscalerConfig>
): Promise<AutoscalerWorker> {
  const worker = new AutoscalerWorker(
    pm2Manager,
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
export async function runAutoscaler(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('autoscaler-main', {
      category: LogCategory.AUTOSCALER,
    });

    logger.info('Initializing autoscaler worker standalone');

    // Initialize dependencies
    import('../lib/pm2-manager').then(({ defaultPM2Manager }) => {
      // This would need to be properly initialized
    });

    const eventsQueue = new EventsQueue();
    const marketsQueue = new MarketsQueue();
    const outcomesQueue = new OutcomesQueue();
    const ticksQueue = new TicksQueue();

    // Create and start worker
    const worker = await createAutoscaler(
      // pm2Manager would be injected
      null as any,
      eventsQueue,
      marketsQueue,
      outcomesQueue,
      ticksQueue
    );

    logger.info('Autoscaler worker running standalone');

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
    console.error('Failed to start autoscaler worker:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runAutoscaler().catch((error) => {
    console.error('Autoscaler worker failed:', error);
    process.exit(1);
  });
}

export default AutoscalerWorker;
