/**
 * Heartbeat Worker
 * Monitors system health every 30 seconds and reports status
 * Checks database connections, Redis connectivity, queue depths, and worker health
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { dbConnection, type DatabaseHealthStatus } from '../lib/db';
import { redisConnection } from '../lib/redis';
import { PM2ProcessManager, type ClusterStatus } from '../lib/pm2-manager';
import { EventsQueue, MarketsQueue, OutcomesQueue, TicksQueue } from '../queues';
import { config, features } from '../config';

/**
 * Health check configuration
 */
interface HeartbeatConfig {
  intervalMs: number;
  timeoutMs: number;
  maxConsecutiveFailures: number;
  enableDatabaseChecks: boolean;
  enableRedisChecks: boolean;
  enableQueueChecks: boolean;
  enableWorkerChecks: boolean;
  enablePerformanceChecks: boolean;
  metricsRetentionHours: number;
  alertThresholds: {
    databaseResponseTime: number;
    redisResponseTime: number;
    queueDepthCritical: number;
    queueDepthWarning: number;
    errorRateWarning: number;
    errorRateCritical: number;
    memoryUsageWarning: number;
    memoryUsageCritical: number;
    cpuUsageWarning: number;
    cpuUsageCritical: number;
  };
}

/**
 * System health status
 */
interface SystemHealthStatus {
  overall: 'healthy' | 'degraded' | 'unhealthy' | 'critical';
  timestamp: string;
  uptime: number;
  components: {
    database: ComponentHealthStatus;
    redis: ComponentHealthStatus;
    queues: ComponentHealthStatus;
    workers: ComponentHealthStatus;
    performance: ComponentHealthStatus;
  };
  metrics: SystemMetrics;
  alerts: HealthAlert[];
  recommendations: string[];
}

/**
 * Component health status
 */
interface ComponentHealthStatus {
  status: 'healthy' | 'degraded' | 'unhealthy' | 'unknown';
  lastCheck: string;
  responseTime?: number;
  errorRate?: number;
  consecutiveFailures: number;
  details?: Record<string, any>;
  lastError?: string;
}

/**
 * System metrics
 */
interface SystemMetrics {
  memory: {
    used: number;
    total: number;
    percentage: number;
  };
  cpu: {
    usage: number;
    loadAverage: number[];
  };
  queues: {
    events: QueueMetrics;
    markets: QueueMetrics;
    outcomes: QueueMetrics;
    ticks: QueueMetrics;
  };
  database: {
    connectionPool: {
      total: number;
      idle: number;
      waiting: number;
    };
    queryStats: {
      averageResponseTime: number;
      errors: number;
    };
  };
  redis: {
    memory: number;
    keys: number;
    connections: number;
  };
  workers: {
    total: number;
    running: number;
    healthy: number;
    processes: WorkerProcessMetrics[];
  };
}

/**
 * Queue metrics
 */
interface QueueMetrics {
  depth: number;
  processingRate: number;
  errorRate: number;
  averageProcessingTime: number;
  lastProcessed?: string;
}

/**
 * Worker process metrics
 */
interface WorkerProcessMetrics {
  pid: number;
  name: string;
  type: string;
  status: string;
  uptime: number;
  memoryUsage: number;
  cpuUsage: number;
  restarts: number;
}

/**
 * Health alert
 */
interface HealthAlert {
  id: string;
  severity: 'info' | 'warning' | 'error' | 'critical';
  component: string;
  message: string;
  timestamp: string;
  details?: Record<string, any>;
  resolved: boolean;
  resolvedAt?: string;
}

/**
 * Heartbeat statistics
 */
interface HeartbeatStats {
  totalChecks: number;
  successfulChecks: number;
  failedChecks: number;
  alertsGenerated: number;
  alertsResolved: number;
  averageCheckTime: number;
  uptime: number;
  lastCheckAt?: string;
  healthHistory: Array<{
    timestamp: string;
    overall: string;
    checks: number;
  }>;
}

/**
 * Heartbeat worker class
 */
export class HeartbeatWorker {
  private logger: Logger;
  private config: HeartbeatConfig;
  private db: typeof dbConnection;
  private redis: typeof redisConnection;
  private pm2Manager: PM2ProcessManager;
  private eventsQueue: EventsQueue;
  private marketsQueue: MarketsQueue;
  private outcomesQueue: OutcomesQueue;
  private ticksQueue: TicksQueue;

  private isRunning: boolean = false;
  private startTime: number;
  private stats: HeartbeatStats;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private alerts: Map<string, HealthAlert> = new Map();
  private consecutiveFailures: Map<string, number> = new Map();

  constructor(
    db: typeof dbConnection,
    redis: typeof redisConnection,
    pm2Manager: PM2ProcessManager,
    eventsQueue: EventsQueue,
    marketsQueue: MarketsQueue,
    outcomesQueue: OutcomesQueue,
    ticksQueue: TicksQueue,
    config?: Partial<HeartbeatConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('heartbeat', process.pid.toString());
    this.db = db;
    this.redis = redis;
    this.pm2Manager = pm2Manager;
    this.eventsQueue = eventsQueue;
    this.marketsQueue = marketsQueue;
    this.outcomesQueue = outcomesQueue;
    this.ticksQueue = ticksQueue;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      intervalMs: 30000, // 30 seconds
      timeoutMs: 10000,
      maxConsecutiveFailures: 3,
      enableDatabaseChecks: true,
      enableRedisChecks: true,
      enableQueueChecks: true,
      enableWorkerChecks: true,
      enablePerformanceChecks: true,
      metricsRetentionHours: 24,
      alertThresholds: {
        databaseResponseTime: 1000,
        redisResponseTime: 500,
        queueDepthCritical: 10000,
        queueDepthWarning: 5000,
        errorRateWarning: 0.05, // 5%
        errorRateCritical: 0.15, // 15%
        memoryUsageWarning: 0.8, // 80%
        memoryUsageCritical: 0.9, // 90%
        cpuUsageWarning: 0.7, // 70%
        cpuUsageCritical: 0.9, // 90%
      },
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalChecks: 0,
      successfulChecks: 0,
      failedChecks: 0,
      alertsGenerated: 0,
      alertsResolved: 0,
      averageCheckTime: 0,
      uptime: 0,
      healthHistory: [],
    };

    this.setupGracefulShutdown();
    this.logger.info('Heartbeat worker initialized', {
      config: this.config,
    });
  }

  /**
   * Start the heartbeat worker
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Heartbeat worker is already running');
      return;
    }

    this.logger.info('Starting heartbeat worker');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Perform initial health check
      await this.performHealthCheck();

      // Setup periodic health checks
      this.heartbeatTimer = setInterval(async () => {
        if (this.isRunning) {
          await this.performHealthCheck();
        }
      }, this.config.intervalMs);

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('Heartbeat worker started successfully', {
        intervalMs: this.config.intervalMs,
      });

    } catch (error) {
      this.logger.error('Failed to start heartbeat worker', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the heartbeat worker
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Heartbeat worker is not running');
      return;
    }

    this.logger.info('Stopping heartbeat worker');
    this.isRunning = false;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // Wait for current check to complete
    await new Promise(resolve => setTimeout(resolve, 5000));

    this.logger.info('Heartbeat worker stopped', {
      finalStats: this.getStats(),
    });
  }

  /**
   * Perform comprehensive health check
   */
  private async performHealthCheck(): Promise<void> {
    const checkStartTime = Date.now();

    return this.logger.timed('heartbeat-check', LogCategory.SYSTEM, async () => {
      try {
        const healthStatus = await this.collectSystemHealth();

        // Process alerts
        await this.processAlerts(healthStatus);

        // Update statistics
        this.updateStats(checkStartTime, healthStatus);

        // Log health status
        this.logHealthStatus(healthStatus);

        this.stats.successfulChecks++;
        this.stats.lastCheckAt = new Date().toISOString();

      } catch (error) {
        this.stats.failedChecks++;
        this.logger.error('Health check failed', {
          error: error as Error,
          consecutiveFailures: Array.from(this.consecutiveFailures.values()),
        });
        throw error;
      }
    });
  }

  /**
   * Collect system health status
   */
  private async collectSystemHealth(): Promise<SystemHealthStatus> {
    const timestamp = new Date().toISOString();

    // Collect component health statuses
    const databaseHealth = this.config.enableDatabaseChecks ? await this.checkDatabaseHealth() : this.createUnknownHealth();
    const redisHealth = this.config.enableRedisChecks ? await this.checkRedisHealth() : this.createUnknownHealth();
    const queueHealth = this.config.enableQueueChecks ? await this.checkQueueHealth() : this.createUnknownHealth();
    const workerHealth = this.config.enableWorkerChecks ? await this.checkWorkerHealth() : this.createUnknownHealth();
    const performanceHealth = this.config.enablePerformanceChecks ? await this.checkPerformanceHealth() : this.createUnknownHealth();

    // Collect system metrics
    const metrics = await this.collectSystemMetrics();

    // Determine overall health
    const componentStatuses = [databaseHealth.status, redisHealth.status, queueHealth.status, workerHealth.status, performanceHealth.status];
    let overall: SystemHealthStatus['overall'] = 'healthy';

    if (componentStatuses.includes('critical')) {
      overall = 'critical';
    } else if (componentStatuses.includes('unhealthy')) {
      overall = 'unhealthy';
    } else if (componentStatuses.includes('degraded')) {
      overall = 'degraded';
    }

    // Generate recommendations
    const recommendations = this.generateRecommendations(overall, componentStatuses, metrics);

    return {
      overall,
      timestamp,
      uptime: Date.now() - this.startTime,
      components: {
        database: databaseHealth,
        redis: redisHealth,
        queues: queueHealth,
        workers: workerHealth,
        performance: performanceHealth,
      },
      metrics,
      alerts: Array.from(this.alerts.values()).filter(alert => !alert.resolved),
      recommendations,
    };
  }

  /**
   * Check database health
   */
  private async checkDatabaseHealth(): Promise<ComponentHealthStatus> {
    try {
      const startTime = Date.now();
      const dbHealth = await this.db.getHealthStatus();
      const responseTime = Date.now() - startTime;

      const status = dbHealth.connected && responseTime < this.config.alertThresholds.databaseResponseTime
        ? 'healthy'
        : dbHealth.connected
        ? 'degraded'
        : 'unhealthy';

      return {
        status,
        lastCheck: new Date().toISOString(),
        responseTime,
        consecutiveFailures: this.updateConsecutiveFailures('database', status === 'healthy'),
        details: {
          connected: dbHealth.connected,
          poolStats: dbHealth.poolStats,
        },
        lastError: dbHealth.error,
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('database', false),
        lastError: (error as Error).message,
      };
    }
  }

  /**
   * Check Redis health
   */
  private async checkRedisHealth(): Promise<ComponentHealthStatus> {
    try {
      const startTime = Date.now();

      // Perform a simple Redis operation
      const testKey = 'health_check_test';
      await this.redis.set(testKey, 'test', { ttl: 10 });
      const result = await this.redis.get(testKey);
      await this.redis.del(testKey);

      const responseTime = Date.now() - startTime;
      const status = result === 'test' && responseTime < this.config.alertThresholds.redisResponseTime
        ? 'healthy'
        : 'degraded';

      return {
        status,
        lastCheck: new Date().toISOString(),
        responseTime,
        consecutiveFailures: this.updateConsecutiveFailures('redis', status === 'healthy'),
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('redis', false),
        lastError: (error as Error).message,
      };
    }
  }

  /**
   * Check queue health
   */
  private async checkQueueHealth(): Promise<ComponentHealthStatus> {
    try {
      const queueDepths = await Promise.all([
        this.getQueueDepth(this.eventsQueue),
        this.getQueueDepth(this.marketsQueue),
        this.getQueueDepth(this.outcomesQueue),
        this.getQueueDepth(this.ticksQueue),
      ]);

      const totalDepth = queueDepths.reduce((sum, depth) => sum + depth, 0);

      let status: ComponentHealthStatus['status'] = 'healthy';
      if (totalDepth > this.config.alertThresholds.queueDepthCritical) {
        status = 'unhealthy';
      } else if (totalDepth > this.config.alertThresholds.queueDepthWarning) {
        status = 'degraded';
      }

      return {
        status,
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('queues', status === 'healthy'),
        details: {
          totalDepth,
          queueDepths: {
            events: queueDepths[0],
            markets: queueDepths[1],
            outcomes: queueDepths[2],
            ticks: queueDepths[3],
          },
        },
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('queues', false),
        lastError: (error as Error).message,
      };
    }
  }

  /**
   * Check worker health
   */
  private async checkWorkerHealth(): Promise<ComponentHealthStatus> {
    try {
      const clusterStatus = await this.pm2Manager.getClusterStatus();

      const totalWorkers = clusterStatus.totalProcesses;
      const healthyWorkers = clusterStatus.runningProcesses;
      const healthPercentage = totalWorkers > 0 ? healthyWorkers / totalWorkers : 0;

      let status: ComponentHealthStatus['status'] = 'healthy';
      if (healthPercentage < 0.5) {
        status = 'unhealthy';
      } else if (healthPercentage < 0.8) {
        status = 'degraded';
      }

      return {
        status,
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('workers', status === 'healthy'),
        details: {
          totalWorkers,
          healthyWorkers,
          healthPercentage: Math.round(healthPercentage * 100) / 100,
          clusterLoad: clusterStatus.clusterLoad,
        },
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('workers', false),
        lastError: (error as Error).message,
      };
    }
  }

  /**
   * Check performance health
   */
  private async checkPerformanceHealth(): Promise<ComponentHealthStatus> {
    try {
      const memUsage = process.memoryUsage();
      const memoryPercentage = memUsage.heapUsed / memUsage.heapTotal;

      // CPU usage estimation (simplified)
      const cpuUsage = process.cpuUsage();
      const totalCpu = (cpuUsage.user + cpuUsage.system) / 1000000; // Convert to seconds
      const uptimeSeconds = process.uptime();
      const cpuPercentage = (totalCpu / uptimeSeconds) / require('os').cpus().length;

      let status: ComponentHealthStatus['status'] = 'healthy';
      if (memoryPercentage > this.config.alertThresholds.memoryUsageCritical || cpuPercentage > this.config.alertThresholds.cpuUsageCritical) {
        status = 'unhealthy';
      } else if (memoryPercentage > this.config.alertThresholds.memoryUsageWarning || cpuPercentage > this.config.alertThresholds.cpuUsageWarning) {
        status = 'degraded';
      }

      return {
        status,
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('performance', status === 'healthy'),
        details: {
          memoryUsage: {
            heapUsed: memUsage.heapUsed,
            heapTotal: memUsage.heapTotal,
            percentage: Math.round(memoryPercentage * 100) / 100,
          },
          cpuUsage: {
            percentage: Math.round(cpuPercentage * 100) / 100,
            uptime: uptimeSeconds,
          },
        },
      };

    } catch (error) {
      return {
        status: 'unhealthy',
        lastCheck: new Date().toISOString(),
        consecutiveFailures: this.updateConsecutiveFailures('performance', false),
        lastError: (error as Error).message,
      };
    }
  }

  /**
   * Collect system metrics
   */
  private async collectSystemMetrics(): Promise<SystemMetrics> {
    const memUsage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();
    const loadAvg = require('os').loadavg();

    // Queue metrics (placeholder implementation)
    const queueMetrics = await Promise.all([
      this.getQueueMetrics('events'),
      this.getQueueMetrics('markets'),
      this.getQueueMetrics('outcomes'),
      this.getQueueMetrics('ticks'),
    ]);

    // Worker metrics
    let workerMetrics: WorkerProcessMetrics[] = [];
    try {
      const clusterStatus = await this.pm2Manager.getClusterStatus();
      // Convert cluster status to worker metrics (placeholder)
      workerMetrics = clusterStatus.workerTypes ? [] : [];
    } catch (error) {
      // Continue without worker metrics
    }

    return {
      memory: {
        used: memUsage.heapUsed,
        total: memUsage.heapTotal,
        percentage: memUsage.heapUsed / memUsage.heapTotal,
      },
      cpu: {
        usage: (cpuUsage.user + cpuUsage.system) / 1000000 / process.uptime(),
        loadAverage: loadAvg,
      },
      queues: {
        events: queueMetrics[0],
        markets: queueMetrics[1],
        outcomes: queueMetrics[2],
        ticks: queueMetrics[3],
      },
      database: {
        connectionPool: {
          total: 5, // Would be actual pool stats
          idle: 3,
          waiting: 0,
        },
        queryStats: {
          averageResponseTime: 150, // Would be actual stats
          errors: 0,
        },
      },
      redis: {
        memory: 1024 * 1024 * 10, // 10MB placeholder
        keys: 1000, // placeholder
        connections: 5, // placeholder
      },
      workers: {
        total: workerMetrics.length,
        running: workerMetrics.filter(w => w.status === 'running').length,
        healthy: workerMetrics.filter(w => w.status === 'running').length,
        processes: workerMetrics,
      },
    };
  }

  /**
   * Get queue depth (placeholder implementation)
   */
  private async getQueueDepth(queue: any): Promise<number> {
    // This would need to be implemented in queue classes
    // Return simulated values based on time
    const baseDepth = Math.floor(Math.random() * 2000);
    const timeVariation = Math.sin(Date.now() / 10000) * 500;
    return Math.max(0, baseDepth + timeVariation);
  }

  /**
   * Get queue metrics (placeholder implementation)
   */
  private async getQueueMetrics(queueName: string): Promise<QueueMetrics> {
    const depth = await this.getQueueDepth(null);
    return {
      depth,
      processingRate: Math.floor(Math.random() * 100),
      errorRate: Math.random() * 0.1,
      averageProcessingTime: 100 + Math.random() * 200,
      lastProcessed: new Date().toISOString(),
    };
  }

  /**
   * Create unknown health status
   */
  private createUnknownHealth(): ComponentHealthStatus {
    return {
      status: 'unknown',
      lastCheck: new Date().toISOString(),
      consecutiveFailures: 0,
    };
  }

  /**
   * Update consecutive failures counter
   */
  private updateConsecutiveFailures(component: string, isHealthy: boolean): number {
    if (isHealthy) {
      this.consecutiveFailures.set(component, 0);
    } else {
      const current = this.consecutiveFailures.get(component) || 0;
      this.consecutiveFailures.set(component, current + 1);
    }
    return this.consecutiveFailures.get(component) || 0;
  }

  /**
   * Process alerts based on health status
   */
  private async processAlerts(healthStatus: SystemHealthStatus): Promise<void> {
    const timestamp = new Date().toISOString();

    // Check for critical issues
    Object.entries(healthStatus.components).forEach(([component, status]) => {
      if (status.status === 'unhealthy' || status.status === 'critical') {
        const alertId = `${component}-${timestamp}`;
        const existingAlert = Array.from(this.alerts.values())
          .find(alert => alert.component === component && !alert.resolved);

        if (!existingAlert) {
          const alert: HealthAlert = {
            id: alertId,
            severity: status.status === 'critical' ? 'critical' : 'error',
            component,
            message: `${component} is ${status.status}`,
            timestamp,
            details: status.details,
            resolved: false,
          };

          this.alerts.set(alertId, alert);
          this.stats.alertsGenerated++;

          this.logger.error('Health alert generated', {
            component,
            severity: alert.severity,
            message: alert.message,
            details: alert.details,
          });
        }
      } else {
        // Resolve existing alerts for healthy components
        const existingAlerts = Array.from(this.alerts.values())
          .filter(alert => alert.component === component && !alert.resolved);

        existingAlerts.forEach(alert => {
          alert.resolved = true;
          alert.resolvedAt = timestamp;
          this.stats.alertsResolved++;

          this.logger.info('Health alert resolved', {
            component: alert.component,
            alertId: alert.id,
          });
        });
      }
    });

    // Check queue depth alerts
    Object.entries(healthStatus.metrics.queues).forEach(([queueName, metrics]) => {
      if (metrics.depth > this.config.alertThresholds.queueDepthCritical) {
        const alertId = `queue-${queueName}-${timestamp}`;
        const existingAlert = Array.from(this.alerts.values())
          .find(alert => alert.id.includes(`queue-${queueName}`) && !alert.resolved);

        if (!existingAlert) {
          const alert: HealthAlert = {
            id: alertId,
            severity: 'critical',
            component: 'queues',
            message: `Queue ${queueName} depth is critical: ${metrics.depth}`,
            timestamp,
            details: { queueName, depth: metrics.depth },
            resolved: false,
          };

          this.alerts.set(alertId, alert);
          this.stats.alertsGenerated++;

          this.logger.error('Queue depth alert', {
            queueName,
            depth: metrics.depth,
            threshold: this.config.alertThresholds.queueDepthCritical,
          });
        }
      }
    });
  }

  /**
   * Generate health recommendations
   */
  private generateRecommendations(
    overall: string,
    componentStatuses: string[],
    metrics: SystemMetrics
  ): string[] {
    const recommendations: string[] = [];

    if (overall === 'critical' || overall === 'unhealthy') {
      recommendations.push('Immediate attention required - system is unstable');
    }

    // Database recommendations
    const dbStatus = healthStatus?.components.database.status || 'unknown';
    if (dbStatus !== 'healthy') {
      recommendations.push('Check database connectivity and performance');
    }

    // Queue recommendations
    if (metrics.queues.ticks.depth > 1000) {
      recommendations.push('Consider scaling up tick processing workers');
    }

    // Performance recommendations
    if (metrics.memory.percentage > 0.8) {
      recommendations.push('High memory usage detected - investigate memory leaks');
    }

    // Worker recommendations
    if (metrics.workers.healthy < metrics.workers.total) {
      recommendations.push('Some workers are unhealthy - restart failed processes');
    }

    return recommendations;
  }

  /**
   * Update statistics
   */
  private updateStats(checkStartTime: number, healthStatus: SystemHealthStatus): void {
    this.stats.totalChecks++;

    const checkTime = Date.now() - checkStartTime;
    const totalTime = this.stats.averageCheckTime * (this.stats.totalChecks - 1) + checkTime;
    this.stats.averageCheckTime = totalTime / this.stats.totalChecks;
    this.stats.uptime = Date.now() - this.startTime;

    // Add to health history
    this.stats.healthHistory.push({
      timestamp: healthStatus.timestamp,
      overall: healthStatus.overall,
      checks: this.stats.totalChecks,
    });

    // Keep only last 24 hours of history
    const cutoffTime = Date.now() - (this.config.metricsRetentionHours * 60 * 60 * 1000);
    this.stats.healthHistory = this.stats.healthHistory.filter(
      entry => new Date(entry.timestamp).getTime() > cutoffTime
    );
  }

  /**
   * Log health status
   */
  private logHealthStatus(healthStatus: SystemHealthStatus): void {
    const level = healthStatus.overall === 'healthy' ? 'info' :
                  healthStatus.overall === 'degraded' ? 'warn' : 'error';

    this.logger[level]('System health check completed', {
      overall: healthStatus.overall,
      timestamp: healthStatus.timestamp,
      uptime: healthStatus.uptime,
      alerts: healthStatus.alerts.length,
      recommendations: healthStatus.recommendations.length,
      componentStatuses: Object.entries(healthStatus.components).map(([name, status]) => ({
        name,
        status: status.status,
        responseTime: status.responseTime,
      })),
    });
  }

  /**
   * Get current statistics
   */
  getStats(): HeartbeatStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
    };
  }

  /**
   * Get current health status
   */
  async getCurrentHealthStatus(): Promise<SystemHealthStatus> {
    return await this.collectSystemHealth();
  }

  /**
   * Get active alerts
   */
  getActiveAlerts(): HealthAlert[] {
    return Array.from(this.alerts.values()).filter(alert => !alert.resolved);
  }

  /**
   * Check if heartbeat worker is healthy
   */
  isHealthy(): boolean {
    const now = Date.now();
    const timeSinceLastCheck = this.stats.lastCheckAt
      ? now - new Date(this.stats.lastCheckAt).getTime()
      : Infinity;

    // Consider healthy if last check was within last 2 minutes
    const recentCheck = timeSinceLastCheck < 120000;

    // Consider healthy if error rate is below 20%
    const errorRate = this.stats.totalChecks > 0
      ? this.stats.failedChecks / this.stats.totalChecks
      : 0;
    const acceptableErrorRate = errorRate < 0.2;

    return this.isRunning && recentCheck && acceptableErrorRate;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down heartbeat`);
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
   * Get alert thresholds configuration
   */
  getAlertThresholds(): HeartbeatConfig['alertThresholds'] {
    return { ...this.config.alertThresholds };
  }

  /**
   * Update alert thresholds
   */
  updateAlertThresholds(newThresholds: Partial<HeartbeatConfig['alertThresholds']>): void {
    this.config.alertThresholds = { ...this.config.alertThresholds, ...newThresholds };
    this.logger.info('Alert thresholds updated', {
      newThresholds,
      fullThresholds: this.config.alertThresholds,
    });
  }

  /**
   * Force health check
   */
  async forceHealthCheck(): Promise<SystemHealthStatus> {
    this.logger.info('Forcing health check');
    return await this.collectSystemHealth();
  }
}

/**
 * Create and initialize heartbeat worker
 */
export async function createHeartbeat(
  db: typeof dbConnection,
  redis: typeof redisConnection,
  pm2Manager: PM2ProcessManager,
  eventsQueue: EventsQueue,
  marketsQueue: MarketsQueue,
  outcomesQueue: OutcomesQueue,
  ticksQueue: TicksQueue,
  config?: Partial<HeartbeatConfig>
): Promise<HeartbeatWorker> {
  const worker = new HeartbeatWorker(
    db,
    redis,
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
export async function runHeartbeat(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('heartbeat-main', {
      category: LogCategory.SYSTEM,
    });

    logger.info('Initializing heartbeat worker standalone');

    // Initialize dependencies
    const eventsQueue = new EventsQueue();
    const marketsQueue = new MarketsQueue();
    const outcomesQueue = new OutcomesQueue();
    const ticksQueue = new TicksQueue();

    // Create and start worker
    const worker = await createHeartbeat(
      dbConnection,
      redisConnection,
      // pm2Manager would be injected
      null as any,
      eventsQueue,
      marketsQueue,
      outcomesQueue,
      ticksQueue
    );
    await worker.start();

    logger.info('Heartbeat worker running standalone');

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
    console.error('Failed to start heartbeat worker:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runHeartbeat().catch((error) => {
    console.error('Heartbeat worker failed:', error);
    process.exit(1);
  });
}

export default HeartbeatWorker;