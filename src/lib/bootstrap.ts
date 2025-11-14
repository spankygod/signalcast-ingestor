/**
 * Service initialization coordination for SignalCast ingestion system
 * Provides database and Redis initialization, health check coordination, and graceful startup sequence
 */

import { EventEmitter } from 'events';
import { Logger, LogCategory } from './logger';
import { dbConnection } from './db';
import { redisConnection } from './redis';
import { RetryManager } from './retry';
import {
  initializeConfiguration,
  shutdownConfiguration,
  config
} from '../config';

/**
 * Service initialization states
 */
export enum ServiceState {
  UNINITIALIZED = 'uninitialized',
  INITIALIZING = 'initializing',
  INITIALIZED = 'initialized',
  HEALTHY = 'healthy',
  UNHEALTHY = 'unhealthy',
  STOPPING = 'stopping',
  STOPPED = 'stopped',
  ERROR = 'error',
}

/**
 * Service types
 */
export enum ServiceType {
  DATABASE = 'database',
  REDIS = 'redis',
  CONFIGURATION = 'configuration',
  LOGGER = 'logger',
  WORKER_MANAGER = 'worker_manager',
  HEALTH_CHECKER = 'health_checker',
}

/**
 * Service interface
 */
export interface Service {
  name: ServiceType;
  priority: number;
  dependencies: ServiceType[];
  initialize(): Promise<void>;
  shutdown(): Promise<void>;
  healthCheck(): Promise<boolean>;
  isInitialized: boolean;
  isHealthy: boolean;
}

/**
 * Bootstrap configuration
 */
export interface BootstrapConfig {
  services?: Service[];
  timeoutMs: number;
  retryAttempts: number;
  retryDelayMs: number;
  healthCheckIntervalMs: number;
  gracefulShutdownTimeoutMs: number;
  enableHealthMonitoring: boolean;
}

/**
 * Service status interface
 */
export interface ServiceStatus {
  name: ServiceType;
  state: ServiceState;
  priority: number;
  initialized: boolean;
  healthy: boolean;
  lastHealthCheck?: Date;
  initializationTime?: number;
  error?: Error;
  dependencies?: ServiceType[];
}

/**
 * Bootstrap status
 */
export interface BootstrapStatus {
  state: ServiceState;
  services: ServiceStatus[];
  initializedServices: number;
  healthyServices: number;
  totalServices: number;
  startTime: Date;
  initializationTime?: number;
  errors: Array<{ service: ServiceType; error: Error; timestamp: Date }>;
}

/**
 * Service initialization coordinator
 */
export class BootstrapCoordinator extends EventEmitter {
  private logger: Logger;
  private config: BootstrapConfig;
  private services: Map<ServiceType, Service> = new Map();
  private serviceStates: Map<ServiceType, ServiceState> = new Map();
  private serviceHealth: Map<ServiceType, boolean> = new Map();
  private retryManager: RetryManager;
  private healthCheckInterval: NodeJS.Timeout | undefined;
  private startTime: Date = new Date();
  private isShuttingDown: boolean = false;
  private errors: Array<{ service: ServiceType; error: Error; timestamp: Date }> = [];

  constructor(logger: Logger, config: Partial<BootstrapConfig> = {}) {
    super();
    this.logger = logger.child({ category: LogCategory.SYSTEM });
    this.retryManager = new RetryManager(this.logger);

    this.config = {
      services: [],
      timeoutMs: 30000,
      retryAttempts: 3,
      retryDelayMs: 1000,
      healthCheckIntervalMs: 30000,
      gracefulShutdownTimeoutMs: 10000,
      enableHealthMonitoring: true,
      ...config,
    };

    this.setupDefaultServices();
    if (this.config.services?.length) {
      this.config.services.forEach(service => this.addService(service));
    }
    this.setupEventHandlers();
  }

  /**
   * Initialize all services
   */
  async initialize(): Promise<BootstrapStatus> {
    this.startTime = new Date();
    this.logger.info('Starting service initialization');

    try {
      // Sort services by priority and dependencies
      const sortedServices = this.sortServicesByDependencies();

      // Initialize services in order
      for (const service of sortedServices) {
        await this.initializeService(service);
      }

      // Start health monitoring if enabled
      if (this.config.enableHealthMonitoring) {
        this.startHealthMonitoring();
      }

      const status = this.getStatus();
      this.logger.info('Service initialization completed', {
        services: status.initializedServices,
        total: status.totalServices,
        duration: status.initializationTime,
        errors: status.errors.length,
      });

      this.emit('initialized', status);
      return status;
    } catch (error) {
      this.logger.error('Service initialization failed', {
        error: error as Error,
        duration: Date.now() - this.startTime.getTime(),
      });

      this.emit('error', error);
      throw error;
    }
  }

  /**
   * Gracefully shutdown all services
   */
  async shutdown(): Promise<void> {
    if (this.isShuttingDown) {
      this.logger.warn('Shutdown already in progress');
      return;
    }

    this.isShuttingDown = true;
    this.logger.info('Starting graceful service shutdown');

    // Stop health monitoring
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = undefined;
    }

    // Sort services by reverse priority (shutdown in reverse order)
    const sortedServices = Array.from(this.services.values())
      .sort((a, b) => b.priority - a.priority);

    // Shutdown services with timeout
    const shutdownPromises = sortedServices.map(async (service) => {
      try {
        await Promise.race([
          service.shutdown(),
          this.timeout(this.config.gracefulShutdownTimeoutMs, `Service ${service.name} shutdown timeout`),
        ]);

        this.serviceStates.set(service.name, ServiceState.STOPPED);
        this.logger.debug(`Service ${service.name} stopped successfully`);
      } catch (error) {
        this.logger.error(`Failed to stop service ${service.name}`, {
          error: error as Error,
        });
        this.errors.push({
          service: service.name,
          error: error as Error,
          timestamp: new Date(),
        });
      }
    });

    await Promise.allSettled(shutdownPromises);

    // Shutdown configuration resources
    try {
      await shutdownConfiguration();
    } catch (error) {
      this.logger.error('Failed to shutdown configuration', {
        error: error as Error,
      });
    }

    const status = this.getStatus();
    this.logger.info('Service shutdown completed', {
      services: status.totalServices - status.healthyServices,
      total: status.totalServices,
      errors: status.errors.length,
    });

    this.emit('shutdown', status);
  }

  /**
   * Get current bootstrap status
   */
  getStatus(): BootstrapStatus {
    const services: ServiceStatus[] = Array.from(this.services.values()).map(service => ({
      name: service.name,
      state: this.serviceStates.get(service.name) || ServiceState.UNINITIALIZED,
      priority: service.priority,
      initialized: service.isInitialized,
      healthy: service.isHealthy,
      dependencies: service.dependencies,
    }));

    const initializedServices = services.filter(s => s.initialized).length;
    const healthyServices = services.filter(s => s.healthy).length;
    const totalServices = services.length;
    const currentTime = Date.now();
    const initializationTime = this.getServiceState(ServiceType.CONFIGURATION) === ServiceState.INITIALIZED
      ? currentTime - this.startTime.getTime()
      : undefined;

    // Determine overall state
    let state: ServiceState;
    if (initializedServices === 0) {
      state = ServiceState.UNINITIALIZED;
    } else if (initializedServices < totalServices) {
      state = ServiceState.INITIALIZING;
    } else if (healthyServices === totalServices) {
      state = ServiceState.HEALTHY;
    } else if (healthyServices > 0) {
      state = ServiceState.UNHEALTHY;
    } else if (this.isShuttingDown) {
      state = ServiceState.STOPPING;
    } else {
      state = ServiceState.ERROR;
    }

    const status: BootstrapStatus = {
      state,
      services,
      initializedServices,
      healthyServices,
      totalServices,
      startTime: this.startTime,
      errors: [...this.errors],
    };

    if (initializationTime !== undefined) {
      status.initializationTime = initializationTime;
    }

    return status;
  }

  /**
   * Get service by type
   */
  getService(type: ServiceType): Service | undefined {
    return this.services.get(type);
  }

  /**
   * Get service state
   */
  getServiceState(type: ServiceType): ServiceState {
    return this.serviceStates.get(type) || ServiceState.UNINITIALIZED;
  }

  /**
   * Check if service is healthy
   */
  isServiceHealthy(type: ServiceType): boolean {
    return this.serviceHealth.get(type) || false;
  }

  /**
   * Add custom service
   */
  addService(service: Service): void {
    this.services.set(service.name, service);
    this.serviceStates.set(service.name, ServiceState.UNINITIALIZED);
    this.serviceHealth.set(service.name, false);
  }

  /**
   * Remove service
   */
  removeService(type: ServiceType): void {
    this.services.delete(type);
    this.serviceStates.delete(type);
    this.serviceHealth.delete(type);
  }

  /**
   * Manually trigger health check for all services
   */
  async triggerHealthCheck(): Promise<void> {
    const healthCheckPromises = Array.from(this.services.values()).map(async (service) => {
      try {
        const isHealthy = await service.healthCheck();
        this.serviceHealth.set(service.name, isHealthy);

        if (!isHealthy && this.serviceStates.get(service.name) === ServiceState.HEALTHY) {
          this.serviceStates.set(service.name, ServiceState.UNHEALTHY);
          this.emit('serviceUnhealthy', service.name);
        } else if (isHealthy && this.serviceStates.get(service.name) === ServiceState.UNHEALTHY) {
          this.serviceStates.set(service.name, ServiceState.HEALTHY);
          this.emit('serviceHealthy', service.name);
        }
      } catch (error) {
        this.serviceHealth.set(service.name, false);
        this.serviceStates.set(service.name, ServiceState.ERROR);
        this.logger.error(`Health check failed for service ${service.name}`, {
          error: error as Error,
        });
      }
    });

    await Promise.allSettled(healthCheckPromises);
  }

  /**
   * Initialize single service
   */
  private async initializeService(service: Service): Promise<void> {
    const serviceStartTime = Date.now();

    // Check dependencies
    for (const dependency of service.dependencies) {
      const depService = this.services.get(dependency);
      if (!depService || !depService.isInitialized) {
        throw new Error(`Service ${service.name} depends on ${dependency} which is not initialized`);
      }
    }

    this.logger.debug(`Initializing service ${service.name}`, {
      priority: service.priority,
      dependencies: service.dependencies,
    });

    this.serviceStates.set(service.name, ServiceState.INITIALIZING);
    this.emit('serviceInitializing', service.name);

    try {
      await Promise.race([
        this.retryManager.execute(
          async () => {
            await service.initialize();
          },
          `Initialize ${service.name}`,
          {
            maxAttempts: this.config.retryAttempts,
            baseDelay: this.config.retryDelayMs,
          }
        ),
        this.timeout(this.config.timeoutMs, `Service ${service.name} initialization timeout`),
      ]);

      this.serviceStates.set(service.name, ServiceState.INITIALIZED);
      const initializationTime = Date.now() - serviceStartTime;

      this.logger.info(`Service ${service.name} initialized successfully`, {
        initializationTime,
        priority: service.priority,
      });

      this.emit('serviceInitialized', service.name, initializationTime);

      // Perform initial health check
      try {
        const isHealthy = await service.healthCheck();
        this.serviceHealth.set(service.name, isHealthy);
        this.serviceStates.set(
          service.name,
          isHealthy ? ServiceState.HEALTHY : ServiceState.UNHEALTHY
        );
      } catch (error) {
        this.serviceHealth.set(service.name, false);
        this.serviceStates.set(service.name, ServiceState.ERROR);
        throw error;
      }
    } catch (error) {
      this.serviceStates.set(service.name, ServiceState.ERROR);
      this.errors.push({
        service: service.name,
        error: error as Error,
        timestamp: new Date(),
      });

      this.logger.error(`Failed to initialize service ${service.name}`, {
        error: error as Error,
        initializationTime: Date.now() - serviceStartTime,
      });

      this.emit('serviceError', service.name, error as Error);
      throw error;
    }
  }

  /**
   * Sort services by dependencies (topological sort)
   */
  private sortServicesByDependencies(): Service[] {
    const services = Array.from(this.services.values());
    const sorted: Service[] = [];
    const visited = new Set<ServiceType>();
    const visiting = new Set<ServiceType>();

    const visit = (service: Service): void => {
      if (visiting.has(service.name)) {
        throw new Error(`Circular dependency detected involving ${service.name}`);
      }

      if (visited.has(service.name)) {
        return;
      }

      visiting.add(service.name);

      for (const dependency of service.dependencies) {
        const depService = this.services.get(dependency);
        if (!depService) {
          throw new Error(`Service ${service.name} depends on missing service ${dependency}`);
        }
        visit(depService);
      }

      visiting.delete(service.name);
      visited.add(service.name);
      sorted.push(service);
    };

    // Sort by priority first, then resolve dependencies
    services.sort((a, b) => a.priority - b.priority);

    for (const service of services) {
      visit(service);
    }

    return sorted;
  }

  /**
   * Setup default services
   */
  private setupDefaultServices(): void {
    // Configuration service
    this.addService(new ConfigurationService());

    // Database service
    this.addService(new DatabaseService());

    // Redis service
    this.addService(new RedisService());

    // Logger service (always initialized first)
    this.addService(new LoggerService());
  }

  /**
   * Setup event handlers
   */
  private setupEventHandlers(): void {
    // Process signals for graceful shutdown
    process.on('SIGTERM', () => {
      this.logger.info('Received SIGTERM, initiating graceful shutdown');
      this.shutdown().catch((error) => {
        this.logger.error('Error during SIGTERM shutdown', { error });
        process.exit(1);
      });
    });

    process.on('SIGINT', () => {
      this.logger.info('Received SIGINT, initiating graceful shutdown');
      this.shutdown().catch((error) => {
        this.logger.error('Error during SIGINT shutdown', { error });
        process.exit(1);
      });
    });

    process.on('uncaughtException', (error) => {
      this.logger.error('Uncaught exception', { error });
      this.shutdown().then(() => {
        process.exit(1);
      });
    });

    process.on('unhandledRejection', (reason, promise) => {
      this.logger.error('Unhandled rejection', { reason, promise });
    });
  }

  /**
   * Start health monitoring
   */
  private startHealthMonitoring(): void {
    this.healthCheckInterval = setInterval(async () => {
      await this.triggerHealthCheck();
    }, this.config.healthCheckIntervalMs);

    // Perform initial health check
    this.triggerHealthCheck().catch((error) => {
      this.logger.error('Initial health check failed', { error });
    });
  }

  /**
   * Timeout utility
   */
  private timeout(ms: number, message: string): Promise<never> {
    return new Promise((_, reject) => {
      setTimeout(() => reject(new Error(message)), ms);
    });
  }
}

/**
 * Configuration service
 */
class ConfigurationService implements Service {
  name = ServiceType.CONFIGURATION;
  priority = 1;
  dependencies: ServiceType[] = [];
  isInitialized = false;
  isHealthy = false;

  async initialize(): Promise<void> {
    await initializeConfiguration();
    this.isInitialized = true;
    this.isHealthy = true;
  }

  async shutdown(): Promise<void> {
    // Configuration shutdown is handled by bootstrap coordinator
    this.isInitialized = false;
    this.isHealthy = false;
  }

  async healthCheck(): Promise<boolean> {
    return this.isInitialized;
  }
}

/**
 * Database service
 */
class DatabaseService implements Service {
  name = ServiceType.DATABASE;
  priority = 10;
  dependencies = [ServiceType.CONFIGURATION];
  isInitialized = false;
  isHealthy = false;

  async initialize(): Promise<void> {
    // Test database connection
    await dbConnection.healthCheck();
    this.isInitialized = true;
    this.isHealthy = true;
  }

  async shutdown(): Promise<void> {
    await dbConnection.close();
    this.isInitialized = false;
    this.isHealthy = false;
  }

  async healthCheck(): Promise<boolean> {
    try {
      const isHealthy = await dbConnection.healthCheck();
      this.isHealthy = isHealthy;
      return isHealthy;
    } catch (error) {
      this.isHealthy = false;
      return false;
    }
  }
}

/**
 * Redis service
 */
class RedisService implements Service {
  name = ServiceType.REDIS;
  priority = 10;
  dependencies = [ServiceType.CONFIGURATION];
  isInitialized = false;
  isHealthy = false;

  async initialize(): Promise<void> {
    if (!config.REDIS_ENABLED) {
      this.isInitialized = true;
      this.isHealthy = true;
      return;
    }

    // Test Redis connection
    await redisConnection.healthCheck();
    this.isInitialized = true;
    this.isHealthy = true;
  }

  async shutdown(): Promise<void> {
    if (config.REDIS_ENABLED) {
      await redisConnection.close();
    }
    this.isInitialized = false;
    this.isHealthy = false;
  }

  async healthCheck(): Promise<boolean> {
    if (!config.REDIS_ENABLED) {
      this.isHealthy = true;
      return true;
    }

    try {
      const isHealthy = await redisConnection.healthCheck();
      this.isHealthy = isHealthy;
      return isHealthy;
    } catch (error) {
      this.isHealthy = false;
      return false;
    }
  }
}

/**
 * Logger service
 */
class LoggerService implements Service {
  name = ServiceType.LOGGER;
  priority = 0;
  dependencies: ServiceType[] = [];
  isInitialized = false;
  isHealthy = false;

  async initialize(): Promise<void> {
    // Logger is already initialized by this point
    this.isInitialized = true;
    this.isHealthy = true;
  }

  async shutdown(): Promise<void> {
    // Logger cleanup
    this.isInitialized = false;
    this.isHealthy = false;
  }

  async healthCheck(): Promise<boolean> {
    return this.isInitialized;
  }
}

/**
 * Bootstrap utility functions
 */
export const bootstrapUtils = {
  /**
   * Create bootstrap coordinator with default configuration
   */
  createBootstrapCoordinator(
    logger: Logger,
    config?: Partial<BootstrapConfig>
  ): BootstrapCoordinator {
    return new BootstrapCoordinator(logger, config);
  },

  /**
   * Quick bootstrap with default services
   */
  async quickBootstrap(logger: Logger): Promise<BootstrapStatus> {
    const coordinator = new BootstrapCoordinator(logger);
    return await coordinator.initialize();
  },

  /**
   * Bootstrap with custom services
   */
  async bootstrapWithServices(
    logger: Logger,
    customServices: Service[],
    config?: Partial<BootstrapConfig>
  ): Promise<BootstrapStatus> {
    const coordinator = new BootstrapCoordinator(logger, config);

    // Add custom services
    customServices.forEach(service => {
      coordinator.addService(service);
    });

    return await coordinator.initialize();
  },

  /**
   * Create service instance
   */
  createService(
    name: ServiceType,
    initializeFn: () => Promise<void>,
    shutdownFn: () => Promise<void>,
    healthCheckFn: () => Promise<boolean>,
    priority: number = 50,
    dependencies: ServiceType[] = []
  ): Service {
    return {
      name,
      priority,
      dependencies,
      isInitialized: false,
      isHealthy: false,
      async initialize() {
        await initializeFn();
        this.isInitialized = true;
      },
      async shutdown() {
        await shutdownFn();
        this.isInitialized = false;
        this.isHealthy = false;
      },
      async healthCheck() {
        const isHealthy = await healthCheckFn();
        this.isHealthy = isHealthy;
        return isHealthy;
      },
    };
  },
};

/**
 * Export default bootstrap coordinator
 */
export default BootstrapCoordinator;
