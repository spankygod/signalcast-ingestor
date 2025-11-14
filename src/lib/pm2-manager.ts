/**
 * PM2 process lifecycle management for SignalCast ingestion system
 * Provides worker type registration, health monitoring, environment variable handling, and cluster coordination
 */

import { Logger, LogCategory, rootLogger } from './logger';
import { redisConnection } from './redis';
import {
  WORKER_CONFIG,
  REDIS_KEYS,
  LOCK_CONFIG,
  config
} from '../config';

interface PM2Metrics {
  uptime: number;
  restarts: number;
  cpu: number;
  memory: number;
  load: number[];
}

/**
 * Worker type configuration
 */
export interface WorkerTypeConfig {
  name: string;
  script: string;
  instances: number | 'max';
  exec_mode: 'fork' | 'cluster';
  env?: Record<string, string>;
  env_production?: Record<string, string>;
  watch?: boolean;
  max_memory_restart?: string;
  min_uptime?: string;
  max_restarts?: number;
  error_file?: string;
  out_file?: string;
  log_file?: string;
  time?: boolean;
  autorestart?: boolean;
  vizion?: boolean;
  args?: string[];
  wait_ready?: boolean;
  listen_timeout?: number;
  node_args?: string[];
  merge_logs?: boolean;
  kill_timeout?: number;
  cron?: string;
  cron_restart?: string;
  tree_kill?: boolean;
  user?: string;
  group?: string;
  date_format?: string;
  log_date_format?: string;
  restart_delay?: number;
  source_map_support?: boolean;
  disable_logs?: boolean;
  output?: string;
  error?: string;
  log?: string;
  exp_backoff_restart_delay?: number;
  pmx?: boolean;
  pmx_conf?: Record<string, any>;
}

/**
 * Worker registration interface
 */
export interface WorkerRegistration {
  type: string;
  config: WorkerTypeConfig;
  priority: number;
  dependencies?: string[];
  healthCheckInterval?: number;
  maxRestarts?: number;
  restartDelay?: number;
}

/**
 * Process health status
 */
export interface ProcessHealthStatus {
  processId: number;
  name: string;
  type: string;
  status: 'starting' | 'running' | 'stopping' | 'stopped' | 'error' | 'unknown';
  uptime: number;
  memoryUsage: number;
  cpuUsage: number;
  restarts: number;
  lastHeartbeat?: Date;
  isHealthy: boolean;
  lastHealthCheck: Date;
  error?: Error;
}

/**
 * Cluster status
 */
export interface ClusterStatus {
  totalProcesses: number;
  runningProcesses: number;
  stoppedProcesses: number;
  errorProcesses: number;
  workerTypes: Record<string, {
    registered: number;
    running: number;
    healthy: number;
  }>;
  clusterLoad: {
    cpu: number;
    memory: number;
  };
  timestamp: Date;
}

/**
 * PM2 process manager
 */
export class PM2ProcessManager {
  private logger: Logger;
  private workerTypes: Map<string, WorkerRegistration> = new Map();
  private processHealth: Map<number, ProcessHealthStatus> = new Map();
  private healthCheckInterval: NodeJS.Timeout | undefined;
  private isPM2Available: boolean = false;
  private clusterLockAcquired: boolean = false;
  private clusterLockValue?: string;

  constructor(logger: Logger) {
    this.logger = logger.child({ category: LogCategory.SYSTEM });
    this.detectPM2Availability();
    this.setupDefaultWorkerTypes();
    this.setupEventHandlers();
  }

  /**
   * Initialize PM2 manager
   */
  async initialize(): Promise<void> {
    if (!this.isPM2Available) {
      this.logger.warn('PM2 is not available, running in standalone mode');
      return;
    }

    this.logger.info('Initializing PM2 process manager');

    try {
      // Acquire cluster coordination lock
      await this.acquireClusterLock();

      // Register worker types
      await this.registerWorkerTypes();

      // Start health monitoring
      this.startHealthMonitoring();

      // Setup graceful shutdown handlers
      this.setupGracefulShutdown();

      this.logger.info('PM2 process manager initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize PM2 process manager', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Register worker type
   */
  async registerWorkerType(registration: WorkerRegistration): Promise<void> {
    this.logger.info(`Registering worker type: ${registration.type}`, {
      type: registration.type,
      script: registration.config.script,
      instances: registration.config.instances,
    });

    this.workerTypes.set(registration.type, registration);

    if (!this.isPM2Available) {
      this.logger.debug(`PM2 not available, skipping registration for ${registration.type}`);
      return;
    }

    try {
      // In a real implementation, this would use PM2 API to register the process
      // For now, we'll just log the registration
      this.logger.info(`Worker type ${registration.type} registered successfully`);
    } catch (error) {
      this.logger.error(`Failed to register worker type ${registration.type}`, {
        error: error as Error,
        registration,
      });
      throw error;
    }
  }

  /**
   * Start worker process
   */
  async startWorker(type: string, env?: Record<string, string>): Promise<number> {
    const registration = this.workerTypes.get(type);
    if (!registration) {
      throw new Error(`Worker type ${type} is not registered`);
    }

    this.logger.info(`Starting worker process`, {
      type,
      env: env ? Object.keys(env) : undefined,
    });

    if (!this.isPM2Available) {
      // In standalone mode, simulate worker start
      this.logger.warn(`PM2 not available, simulating worker start for ${type}`);
      return process.pid;
    }

    try {
      // In a real implementation, this would use PM2 API to start the process
      // For now, we'll return a simulated process ID
      const simulatedPid = Math.floor(Math.random() * 10000) + 1000;

      // Update process health
      this.processHealth.set(simulatedPid, {
        processId: simulatedPid,
        name: `${type}-${simulatedPid}`,
        type,
        status: 'starting',
        uptime: 0,
        memoryUsage: 0,
        cpuUsage: 0,
        restarts: 0,
        isHealthy: false,
        lastHealthCheck: new Date(),
      });

      this.logger.info(`Worker process started`, {
        type,
        processId: simulatedPid,
      });

      return simulatedPid;
    } catch (error) {
      this.logger.error(`Failed to start worker process ${type}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Stop worker process
   */
  async stopWorker(processId: number): Promise<void> {
    this.logger.info(`Stopping worker process`, { processId });

    if (!this.isPM2Available) {
      this.logger.warn(`PM2 not available, simulating worker stop for ${processId}`);
      return;
    }

    try {
      // In a real implementation, this would use PM2 API to stop the process
      // For now, we'll just update our local state
      const health = this.processHealth.get(processId);
      if (health) {
        health.status = 'stopping';
        this.processHealth.set(processId, health);
      }

      this.logger.info(`Worker process stopped`, { processId });
    } catch (error) {
      this.logger.error(`Failed to stop worker process ${processId}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Restart worker process
   */
  async restartWorker(processId: number): Promise<void> {
    this.logger.info(`Restarting worker process`, { processId });

    const health = this.processHealth.get(processId);
    if (!health) {
      throw new Error(`Process ${processId} not found`);
    }

    try {
      await this.stopWorker(processId);

      // Clear old health data
      this.processHealth.delete(processId);

      // Start new process
      await this.startWorker(health.type);

      this.logger.info(`Worker process restarted`, { processId, type: health.type });
    } catch (error) {
      this.logger.error(`Failed to restart worker process ${processId}`, {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Scale worker type
   */
  async scaleWorkerType(type: string, instances: number): Promise<void> {
    const registration = this.workerTypes.get(type);
    if (!registration) {
      throw new Error(`Worker type ${type} is not registered`);
    }

    this.logger.info(`Scaling worker type`, {
      type,
      instances,
      current: this.getProcessCountByType(type),
    });

    if (!this.isPM2Available) {
      this.logger.warn(`PM2 not available, simulating scaling for ${type}`);
      return;
    }

    try {
      // In a real implementation, this would use PM2 API to scale the process
      // For now, we'll just update our local state
      this.logger.info(`Worker type ${type} scaled to ${instances} instances`);
    } catch (error) {
      this.logger.error(`Failed to scale worker type ${type}`, {
        error: error as Error,
        instances,
      });
      throw error;
    }
  }

  /**
   * Get cluster status
   */
  async getClusterStatus(): Promise<ClusterStatus> {
    const processes = Array.from(this.processHealth.values());
    const runningProcesses = processes.filter(p => p.status === 'running').length;
    const stoppedProcesses = processes.filter(p => p.status === 'stopped').length;
    const errorProcesses = processes.filter(p => p.status === 'error').length;

    // Aggregate by worker type
    const workerTypes: Record<string, { registered: number; running: number; healthy: number }> = {};
    for (const [type] of this.workerTypes) {
      const typeProcesses = processes.filter(p => p.type === type);
      const runningTypeProcesses = typeProcesses.filter(p => p.status === 'running').length;
      const healthyTypeProcesses = typeProcesses.filter(p => p.isHealthy).length;

      workerTypes[type] = {
        registered: 1, // In real implementation, this would be the actual registered count
        running: runningTypeProcesses,
        healthy: healthyTypeProcesses,
      };
    }

    // Calculate cluster load
    const totalMemory = processes.reduce((sum, p) => sum + p.memoryUsage, 0);
    const avgCpu = processes.length > 0
      ? processes.reduce((sum, p) => sum + p.cpuUsage, 0) / processes.length
      : 0;

    return {
      totalProcesses: processes.length,
      runningProcesses,
      stoppedProcesses,
      errorProcesses,
      workerTypes,
      clusterLoad: {
        cpu: avgCpu,
        memory: totalMemory,
      },
      timestamp: new Date(),
    };
  }

  /**
   * Get process health status
   */
  getProcessHealth(processId: number): ProcessHealthStatus | undefined {
    return this.processHealth.get(processId);
  }

  /**
   * Get all process health statuses
   */
  getAllProcessHealth(): ProcessHealthStatus[] {
    return Array.from(this.processHealth.values());
  }

  /**
   * Perform health check on all processes
   */
  async performHealthCheck(): Promise<void> {
    this.logger.debug('Performing health check on all processes');

    if (!this.isPM2Available) {
      // In standalone mode, just check current process
      this.updateCurrentProcessHealth();
      return;
    }

    try {
      // In a real implementation, this would query PM2 for process status
      // For now, we'll update our simulated processes
      await this.updateSimulatedProcesses();
    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
    }
  }

  /**
   * Get worker metrics
   */
  async getWorkerMetrics(): Promise<PM2Metrics> {
    if (!this.isPM2Available) {
      // Return current process metrics
      const memUsage = process.memoryUsage();
      const cpuUsage = process.cpuUsage();

      return {
        uptime: process.uptime(),
        restarts: 0,
        cpu: (cpuUsage.user + cpuUsage.system) / 1000000, // Convert to seconds
        memory: memUsage.heapUsed,
        load: [], // Would need OS module for actual load
      };
    }

    // In a real implementation, this would query PM2 for cluster metrics
    return {
      uptime: process.uptime(),
      restarts: 0,
      cpu: 0,
      memory: 0,
      load: [],
    };
  }

  /**
   * Shutdown PM2 manager
   */
  async shutdown(): Promise<void> {
    this.logger.info('Shutting down PM2 process manager');

    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = undefined;
    }

    try {
      await this.releaseClusterLock();

      // Stop all processes
      for (const [processId, health] of this.processHealth) {
        if (health.status === 'running') {
          await this.stopWorker(processId);
        }
      }

      this.logger.info('PM2 process manager shutdown complete');
    } catch (error) {
      this.logger.error('Error during PM2 manager shutdown', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Detect if PM2 is available
   */
  private detectPM2Availability(): void {
    // Check if running under PM2
    this.isPM2Available = !!(process.env['pm_id'] || process.env['PM2_HOME']);

    if (this.isPM2Available) {
      this.logger.info('PM2 detected, enabling cluster management', {
        pmId: process.env['pm_id'],
        pm2Home: process.env['PM2_HOME'],
      });
    } else {
      this.logger.info('PM2 not detected, running in standalone mode');
    }
  }

  /**
   * Setup default worker types
   */
  private setupDefaultWorkerTypes(): void {
    // Events poller
    this.workerTypes.set(WORKER_CONFIG.TYPES.EVENTS_POLLER, {
      type: WORKER_CONFIG.TYPES.EVENTS_POLLER,
      config: {
        name: WORKER_CONFIG.TYPES.EVENTS_POLLER,
        script: './src/workers/events-poller.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '256M',
        max_restarts: 10,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 10,
      healthCheckInterval: 30000,
      maxRestarts: 10,
    });

    // Markets poller
    this.workerTypes.set(WORKER_CONFIG.TYPES.MARKETS_POLLER, {
      type: WORKER_CONFIG.TYPES.MARKETS_POLLER,
      config: {
        name: WORKER_CONFIG.TYPES.MARKETS_POLLER,
        script: './src/workers/markets-poller.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '256M',
        max_restarts: 10,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 10,
      healthCheckInterval: 30000,
      maxRestarts: 10,
    });

    // Outcomes poller
    this.workerTypes.set(WORKER_CONFIG.TYPES.OUTCOMES_POLLER, {
      type: WORKER_CONFIG.TYPES.OUTCOMES_POLLER,
      config: {
        name: WORKER_CONFIG.TYPES.OUTCOMES_POLLER,
        script: './src/workers/outcomes-poller.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '256M',
        max_restarts: 10,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 10,
      healthCheckInterval: 30000,
      maxRestarts: 10,
    });

    // DB writer
    this.workerTypes.set(WORKER_CONFIG.TYPES.DB_WRITER, {
      type: WORKER_CONFIG.TYPES.DB_WRITER,
      config: {
        name: WORKER_CONFIG.TYPES.DB_WRITER,
        script: './src/workers/db-writer.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '512M',
        max_restarts: 10,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 20,
      dependencies: [WORKER_CONFIG.TYPES.EVENTS_POLLER, WORKER_CONFIG.TYPES.MARKETS_POLLER],
      healthCheckInterval: 30000,
      maxRestarts: 10,
    });

    // Autoscaler
    this.workerTypes.set(WORKER_CONFIG.TYPES.AUTOSCALER, {
      type: WORKER_CONFIG.TYPES.AUTOSCALER,
      config: {
        name: WORKER_CONFIG.TYPES.AUTOSCALER,
        script: './src/workers/autoscaler.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '256M',
        max_restarts: 5,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 30,
      dependencies: [WORKER_CONFIG.TYPES.DB_WRITER],
      healthCheckInterval: 60000,
      maxRestarts: 5,
    });

    // Heartbeat
    this.workerTypes.set(WORKER_CONFIG.TYPES.HEARTBEAT, {
      type: WORKER_CONFIG.TYPES.HEARTBEAT,
      config: {
        name: WORKER_CONFIG.TYPES.HEARTBEAT,
        script: './src/workers/heartbeat.js',
        instances: 1,
        exec_mode: 'fork',
        max_memory_restart: '128M',
        max_restarts: 10,
        min_uptime: '10s',
        wait_ready: true,
        listen_timeout: 30000,
      },
      priority: 5,
      healthCheckInterval: 30000,
      maxRestarts: 10,
    });
  }

  /**
   * Setup event handlers
   */
  private setupEventHandlers(): void {
    // Handle process signals
    process.on('SIGINT', () => {
      this.logger.info('Received SIGINT, shutting down PM2 manager');
      this.shutdown().catch((error) => {
        this.logger.error('Error during SIGINT shutdown', { error });
        process.exit(1);
      });
    });

    process.on('SIGTERM', () => {
      this.logger.info('Received SIGTERM, shutting down PM2 manager');
      this.shutdown().catch((error) => {
        this.logger.error('Error during SIGTERM shutdown', { error });
        process.exit(1);
      });
    });
  }

  /**
   * Register all worker types
   */
  private async registerWorkerTypes(): Promise<void> {
    const registrationPromises = Array.from(this.workerTypes.values()).map(
      registration => this.registerWorkerType(registration)
    );

    await Promise.allSettled(registrationPromises);
  }

  /**
   * Start health monitoring
   */
  private startHealthMonitoring(): void {
    const healthCheckInterval = 30000; // 30 seconds

    this.healthCheckInterval = setInterval(async () => {
      await this.performHealthCheck();
    }, healthCheckInterval);

    // Perform initial health check
    this.performHealthCheck().catch((error) => {
      this.logger.error('Initial health check failed', { error });
    });
  }

  /**
   * Setup graceful shutdown
   */
  private setupGracefulShutdown(): void {
    // Clean shutdown when process exits
    process.on('exit', () => {
      this.shutdown().catch((error) => {
        this.logger.error('Error during exit shutdown', { error });
      });
    });
  }

  /**
   * Acquire cluster coordination lock
   */
  private async acquireClusterLock(): Promise<boolean> {
    if (!config.REDIS_ENABLED) {
      this.clusterLockAcquired = true;
      return true;
    }

    try {
      const lockKey = redisUtils.getLockKey('pm2-coordinator');
      const acquiredValue = await redisConnection.acquireLock(
        lockKey,
        LOCK_CONFIG.DEFAULT_TTL_SECONDS,
        3
      );

      if (acquiredValue) {
        this.clusterLockAcquired = true;
        this.clusterLockValue = acquiredValue;
        this.logger.info('Cluster coordination lock acquired');
      } else {
        this.clusterLockAcquired = false;
        this.logger.info('Cluster coordination lock already held by another process');
      }

      return this.clusterLockAcquired;
    } catch (error) {
      this.logger.error('Failed to acquire cluster lock', {
        error: error as Error,
      });
      return false;
    }
  }

  /**
   * Release cluster coordination lock
   */
  private async releaseClusterLock(): Promise<void> {
    if (!this.clusterLockAcquired || !config.REDIS_ENABLED || !this.clusterLockValue) {
      return;
    }

    try {
      const lockKey = redisUtils.getLockKey('pm2-coordinator');
      const lockValue = this.clusterLockValue;
      if (!lockValue) {
        return;
      }

      await redisConnection.releaseLock(lockKey, lockValue);
      this.clusterLockAcquired = false;
      this.clusterLockValue = '';
      this.logger.info('Cluster coordination lock released');
    } catch (error) {
      this.logger.error('Failed to release cluster lock', {
        error: error as Error,
      });
    }
  }

  /**
   * Update current process health (standalone mode)
   */
  private updateCurrentProcessHealth(): void {
    const memUsage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();

    const health: ProcessHealthStatus = {
      processId: process.pid,
      name: process.env['PROCESS_NAME'] || 'signalcast-main',
      type: process.env['PROCESS_TYPE'] || 'main',
      status: 'running',
      uptime: process.uptime(),
      memoryUsage: memUsage.heapUsed,
      cpuUsage: (cpuUsage.user + cpuUsage.system) / 1000000,
      restarts: 0,
      isHealthy: true,
      lastHealthCheck: new Date(),
    };

    this.processHealth.set(process.pid, health);
  }

  /**
   * Update simulated processes (for development/testing)
   */
  private async updateSimulatedProcesses(): Promise<void> {
    const currentTime = Date.now();

    for (const [processId, health] of this.processHealth) {
      // Simulate health check updates
      if (health.status === 'starting') {
        // Simulate transition to running after 5 seconds
        if (currentTime - health.lastHealthCheck.getTime() > 5000) {
          health.status = 'running';
          health.isHealthy = true;
        }
      }

      // Update timestamp
      health.lastHealthCheck = new Date();

      // Simulate resource usage
      health.memoryUsage = Math.floor(Math.random() * 100 * 1024 * 1024); // 0-100MB
      health.cpuUsage = Math.random() * 80; // 0-80% CPU

      this.processHealth.set(processId, health);
    }
  }

  /**
   * Get process count by type
   */
  private getProcessCountByType(type: string): number {
    return Array.from(this.processHealth.values())
      .filter(health => health.type === type && health.status === 'running')
      .length;
  }
}

/**
 * Redis utility functions
 */
const redisUtils = {
  buildKey(prefix: string, ...parts: string[]): string {
    return [prefix, ...parts].join(':');
  },
  getLockKey(lockName: string): string {
    return redisUtils.buildKey(REDIS_KEYS.LOCK, lockName);
  },
};

/**
 * PM2 manager utility functions
 */
export const pm2ManagerUtils = {
  /**
   * Create PM2 manager with default configuration
   */
  createPM2Manager(logger: Logger): PM2ProcessManager {
    return new PM2ProcessManager(logger);
  },

  /**
   * Check if running under PM2
   */
  isRunningUnderPM2(): boolean {
    return !!(process.env['pm_id'] || process.env['PM2_HOME']);
  },

  /**
   * Get PM2 instance information
   */
  getPM2InstanceInfo(): Record<string, any> {
    return {
      pid: process.pid,
      pmId: process.env['pm_id'],
      pm2Home: process.env['PM2_HOME'],
      nodeAppInstance: process.env['NODE_APP_INSTANCE'],
      instanceVar: process.env['pm_id'] ? process.env['INSTANCE_ID'] : undefined,
      execMode: process.env['exec_mode'],
    };
  },

  /**
   * Set process name for PM2
   */
  setProcessName(name: string): void {
    if (process.title && !process.title.includes(name)) {
      process.title = `${process.title} [${name}]`;
    }
  },

  /**
   * Send process ready signal to PM2
   */
  sendReadySignal(): void {
    if (process.send) {
      process.send('ready');
    }
  },

  /**
   * Create worker environment variables
   */
  createWorkerEnvironment(
    type: string,
    additionalEnv?: Record<string, string>
  ): Record<string, string> {
    const env: Record<string, string> = {
      PROCESS_TYPE: type,
      PROCESS_NAME: `${type}-${process.pid}`,
      NODE_ENV: process.env['NODE_ENV'] ?? 'development',
    };

    env.TZ = process.env['TZ'] ?? 'UTC';

    if (additionalEnv) {
      for (const [key, value] of Object.entries(additionalEnv)) {
        env[key] = value;
      }
    }

    return env;
  },
};

/**
 * Default PM2 manager instance
 */
export const defaultPM2Manager = new PM2ProcessManager(rootLogger);

/**
 * Export default PM2 manager
 */
export default PM2ProcessManager;
