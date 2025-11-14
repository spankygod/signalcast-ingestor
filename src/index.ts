/**
 * SignalCast Ingestion System - Main Entry Point
 * Complete worker orchestration with PM2 integration, bootstrap coordination,
 * and graceful shutdown handling
 */

import { EventEmitter } from 'events';
import { program } from 'commander';
import {
  Logger,
  LoggerFactory,
  LogCategory
} from './lib/logger';
import {
  BootstrapCoordinator,
  bootstrapUtils
} from './lib/bootstrap';
import {
  PM2ProcessManager,
  pm2ManagerUtils,
  type WorkerRegistration
} from './lib/pm2-manager';
import {
  WorkerFactory,
  createAndStartWorkerSystem,
  getWorkerSystemInfo
} from './workers';
import {
  initializeConfiguration,
  shutdownConfiguration,
  config,
  features,
  isDevelopment
} from './config';

/**
 * Application modes
 */
enum AppMode {
  ORCHESTRATOR = 'orchestrator',
  WORKER = 'worker',
  STANDALONE = 'standalone',
  DEVELOPMENT = 'development'
}

/**
 * Worker configuration interface
 */
interface WorkerConfig {
  type: string;
  instances?: number;
  execMode?: 'cluster' | 'fork';
  memory?: string;
  priority?: number;
  dependencies?: string[];
  env?: Record<string, string>;
}

/**
 * Application state
 */
enum AppState {
  INITIALIZING = 'initializing',
  STARTING = 'starting',
  RUNNING = 'running',
  STOPPING = 'stopping',
  STOPPED = 'stopped',
  ERROR = 'error'
}

interface CliOptions {
  mode?: string;
  worker?: string;
  development?: boolean;
  standalone?: boolean;
  status?: boolean;
  healthCheck?: boolean;
}

/**
 * Main SignalCast application class
 * Orchestrates all workers with proper lifecycle management
 */
class SignalCastApp extends EventEmitter {
  private logger: Logger;
  private appMode: AppMode;
  private appState: AppState = AppState.INITIALIZING;
  private bootstrapCoordinator?: BootstrapCoordinator;
  private pm2Manager?: PM2ProcessManager;
  private workerFactory?: WorkerFactory;
  private workerSystem?: any;
  private healthCheckInterval: NodeJS.Timeout | undefined;
  private startTime: Date = new Date();
  private workerConfigs: Map<string, WorkerConfig> = new Map();

  constructor(mode: AppMode = AppMode.ORCHESTRATOR) {
    super();
    this.appMode = mode;
    this.logger = LoggerFactory.getLogger('signalcast-main', {
      category: LogCategory.SYSTEM,
    });

    this.setupWorkerConfigs();
    this.setupEventHandlers();
  }

  /**
   * Initialize the application
   */
  async initialize(): Promise<void> {
    this.logger.info('Initializing SignalCast Ingestion System', {
      mode: this.appMode,
      nodeEnv: config.NODE_ENV,
      pid: process.pid,
    });

    try {
      // Initialize configuration
      await initializeConfiguration();

      // Initialize bootstrap coordinator
      this.bootstrapCoordinator = bootstrapUtils.createBootstrapCoordinator(
        this.logger,
        {
          timeoutMs: 60000,
          retryAttempts: 5,
          retryDelayMs: 2000,
          healthCheckIntervalMs: 30000,
          enableHealthMonitoring: true,
        }
      );

      // Initialize PM2 manager (if not running as worker)
      if (this.appMode !== AppMode.WORKER) {
        this.pm2Manager = pm2ManagerUtils.createPM2Manager(this.logger);
        await this.pm2Manager.initialize();
      }

      // Initialize worker factory
      this.workerFactory = new WorkerFactory({
        enableLogging: true,
        logLevel: config.LOG_LEVEL || 'info',
        enableMetrics: features.metrics,
        enableHealthChecks: true,
      });

      await this.workerFactory.initialize();

      // Set process name for PM2
      pm2ManagerUtils.setProcessName('signalcast-main');

      this.appState = AppState.STARTING;
      this.logger.info('SignalCast application initialized successfully');

    } catch (error) {
      this.appState = AppState.ERROR;
      this.logger.error('Failed to initialize SignalCast application', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Start the application
   */
  async start(): Promise<void> {
    if (this.appState !== AppState.STARTING) {
      throw new Error(`Cannot start in state: ${this.appState}`);
    }

    this.logger.info('Starting SignalCast Ingestion System');

    try {
      // Bootstrap core services
      const bootstrapStatus = await this.bootstrapCoordinator!.initialize();
      this.logger.info('Core services bootstrapped', bootstrapStatus);

      // Start based on application mode
      switch (this.appMode) {
        case AppMode.ORCHESTRATOR:
          await this.startOrchestratorMode();
          break;
        case AppMode.STANDALONE:
          await this.startStandaloneMode();
          break;
        case AppMode.WORKER:
          await this.startWorkerMode();
          break;
        case AppMode.DEVELOPMENT:
          await this.startDevelopmentMode();
          break;
        default:
          throw new Error(`Unknown application mode: ${this.appMode}`);
      }

      // Start health monitoring
      this.startHealthMonitoring();

      // Send ready signal to PM2
      pm2ManagerUtils.sendReadySignal();

      this.appState = AppState.RUNNING;
      this.logger.info('SignalCast Ingestion System started successfully', {
        mode: this.appMode,
        uptime: Date.now() - this.startTime.getTime(),
      });

      this.emit('started', {
        mode: this.appMode,
        state: this.appState,
        startTime: this.startTime,
      });

    } catch (error) {
      this.appState = AppState.ERROR;
      this.logger.error('Failed to start SignalCast application', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Start orchestrator mode (PM2 coordinator)
   */
  private async startOrchestratorMode(): Promise<void> {
    this.logger.info('Starting in orchestrator mode - managing PM2 workers');

    // Register all worker types with PM2
    for (const [workerType, config] of this.workerConfigs) {
      const registration = {
        type: workerType,
        config: {
          name: workerType,
          script: `./src/workers/${config.type}.ts`,
          instances: config.instances ?? 1,
          exec_mode: config.execMode ?? 'fork',
          max_memory_restart: config.memory ?? '256M',
          max_restarts: 10,
          min_uptime: '10s',
          wait_ready: true,
          listen_timeout: 30000,
          env: pm2ManagerUtils.createWorkerEnvironment(workerType, config.env),
        },
        priority: config.priority ?? 50,
        healthCheckInterval: 30000,
        maxRestarts: 10,
      } satisfies Omit<WorkerRegistration, 'dependencies'>;

      const finalRegistration: WorkerRegistration = config.dependencies?.length
        ? { ...registration, dependencies: [...config.dependencies] }
        : registration;

      await this.pm2Manager!.registerWorkerType(finalRegistration);
    }

    // Start essential workers in dependency order
    const workerStartupOrder = [
      'heartbeat',
      'wss-market-channel',
      'wss-user-channel',
      'wss-user-controller',
      'events-poller',
      'markets-poller',
      'outcomes-poller',
      'db-writer',
      'autoscaler',
    ];

    for (const workerType of workerStartupOrder) {
      if (this.workerConfigs.has(workerType)) {
        await this.pm2Manager!.startWorker(workerType);
        this.logger.info(`Started worker: ${workerType}`);
      }
    }
  }

  /**
   * Start standalone mode (single process)
   */
  private async startStandaloneMode(): Promise<void> {
    this.logger.info('Starting in standalone mode - running all workers in single process');

    // Create and start complete worker system
    const { factory, workers } = await createAndStartWorkerSystem({
      enableLogging: true,
      logLevel: config.LOG_LEVEL || 'info',
      enableMetrics: features.metrics,
      enableHealthChecks: true,
      workerConfigs: this.getWorkerConfigsForFactory(),
    });

    this.workerSystem = { factory, workers };
    this.logger.info('Standalone worker system started');
  }

  /**
   * Start worker mode (individual worker process)
   */
  private async startWorkerMode(): Promise<void> {
    const workerType = process.env['PROCESS_TYPE'];
    if (!workerType) {
      throw new Error('PROCESS_TYPE environment variable required for worker mode');
    }

    this.logger.info(`Starting worker mode: ${workerType}`);

    // Load and run the specific worker
    const workerModule = await import(`./workers/${workerType}`);
    const runWorker = workerModule[`run${workerType.split('-').map(s =>
      s.charAt(0).toUpperCase() + s.slice(1)
    ).join('')}`];

    if (typeof runWorker === 'function') {
      await runWorker();
    } else {
      throw new Error(`Worker runner function not found for ${workerType}`);
    }
  }

  /**
   * Start development mode
   */
  private async startDevelopmentMode(): Promise<void> {
    this.logger.info('Starting in development mode with hot reload');

    // Start in standalone mode for development
    await this.startStandaloneMode();

    // Enable development-specific features
    if (isDevelopment) {
      this.logger.info('Development features enabled');
      // Add development-specific monitoring, debugging, etc.
    }
  }

  /**
   * Start health monitoring
   */
  private startHealthMonitoring(): void {
    this.healthCheckInterval = setInterval(async () => {
      try {
        await this.performHealthCheck();
      } catch (error) {
        this.logger.error('Health check failed', { error: error as Error });
      }
    }, 30000); // 30 seconds
    this.healthCheckInterval.unref?.();

    // Perform initial health check
    this.performHealthCheck().catch((error) => {
      this.logger.error('Initial health check failed', { error });
    });
  }

  /**
   * Perform health check
   */
  public async performHealthCheck(): Promise<void> {
    const services: Record<string, unknown> = {};
    const health = {
      appState: this.appState,
      uptime: Date.now() - this.startTime.getTime(),
      mode: this.appMode,
      memory: process.memoryUsage(),
      services,
    };

    // Check bootstrap services
    if (this.bootstrapCoordinator) {
      const bootstrapStatus = this.bootstrapCoordinator.getStatus();
      services['bootstrap'] = {
        healthy: bootstrapStatus.healthyServices,
        total: bootstrapStatus.totalServices,
      };
    }

    // Check PM2 workers (if in orchestrator mode)
    if (this.pm2Manager && this.appMode === AppMode.ORCHESTRATOR) {
      const clusterStatus = await this.pm2Manager.getClusterStatus();
      services['pm2'] = clusterStatus;
    }

    // Check worker system (if in standalone mode)
    if (this.workerSystem) {
      services['workers'] = {
        factory: this.workerSystem.factory ? 'initialized' : 'not_initialized',
        workers: this.workerSystem.workers ? 'running' : 'not_started',
      };
    }

    this.logger.debug('Health check completed', health);
    this.emit('healthCheck', health);
  }

  /**
   * Graceful shutdown
   */
  async shutdown(): Promise<void> {
    if (this.appState === AppState.STOPPING) {
      this.logger.warn('Shutdown already in progress');
      return;
    }

    this.appState = AppState.STOPPING;
    this.logger.info('Starting graceful shutdown of SignalCast Ingestion System');

    // Clear intervals
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = undefined;
    }

    try {
      // Shutdown based on mode
      switch (this.appMode) {
        case AppMode.ORCHESTRATOR:
          await this.shutdownOrchestratorMode();
          break;
        case AppMode.STANDALONE:
          await this.shutdownStandaloneMode();
          break;
        case AppMode.WORKER:
          // Workers are managed by PM2, just clean up local resources
          break;
      }

      // Shutdown bootstrap coordinator
      if (this.bootstrapCoordinator) {
        await this.bootstrapCoordinator.shutdown();
      }

      // Shutdown PM2 manager
      if (this.pm2Manager) {
        await this.pm2Manager.shutdown();
      }

      // Cleanup worker factory
      if (this.workerFactory) {
        await this.workerFactory.cleanup();
      }

      // Shutdown configuration
      await shutdownConfiguration();

      this.appState = AppState.STOPPED;
      this.logger.info('SignalCast Ingestion System shutdown complete');

      this.emit('shutdown', {
        mode: this.appMode,
        state: this.appState,
        duration: Date.now() - this.startTime.getTime(),
      });

    } catch (error) {
      this.appState = AppState.ERROR;
      this.logger.error('Error during graceful shutdown', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Shutdown orchestrator mode
   */
  private async shutdownOrchestratorMode(): Promise<void> {
    if (!this.pm2Manager) return;

    const clusterStatus = await this.pm2Manager.getClusterStatus();
    this.logger.info('Shutting down PM2 workers', clusterStatus);

    // Stop all workers in reverse priority order
    const workerShutdownOrder = [
      'autoscaler',
      'db-writer',
      'outcomes-poller',
      'markets-poller',
      'events-poller',
      'wss-user-controller',
      'wss-user-channel',
      'wss-market-channel',
      'heartbeat',
    ];

    for (const workerType of workerShutdownOrder) {
      if (!this.pm2Manager) continue;

      const processes = this.pm2Manager.getAllProcessHealth()
        .filter(p => p.type === workerType);

      for (const process of processes) {
        await this.pm2Manager.stopWorker(process.processId);
      }
    }
  }

  /**
   * Shutdown standalone mode
   */
  private async shutdownStandaloneMode(): Promise<void> {
    if (!this.workerSystem) return;

    this.logger.info('Shutting down standalone worker system');

    const { factory, workers } = this.workerSystem;

    // Stop worker system
    await factory.stopProductionWorkerSet(workers);

    // Cleanup factory
    await factory.cleanup();

    this.workerSystem = undefined;
  }

  /**
   * Setup worker configurations
   */
  private setupWorkerConfigs(): void {
    // CPU-intensive workers (cluster mode)
    this.workerConfigs.set('events-poller', {
      type: 'events-poller',
      instances: isDevelopment ? 1 : 3,
      execMode: 'cluster',
      memory: '256M',
      priority: 10,
      env: {
        POLLING_INTERVAL: '5000',
        BATCH_SIZE: '100',
      },
    });

    this.workerConfigs.set('markets-poller', {
      type: 'markets-poller',
      instances: isDevelopment ? 1 : 3,
      execMode: 'cluster',
      memory: '256M',
      priority: 10,
      env: {
        POLLING_INTERVAL: '2000',
        BATCH_SIZE: '100',
      },
    });

    this.workerConfigs.set('outcomes-poller', {
      type: 'outcomes-poller',
      instances: isDevelopment ? 1 : 3,
      execMode: 'cluster',
      memory: '256M',
      priority: 10,
      env: {
        POLLING_INTERVAL: '2000',
        BATCH_SIZE: '200',
      },
    });

    // I/O-intensive workers (fork mode)
    this.workerConfigs.set('db-writer', {
      type: 'db-writer',
      instances: isDevelopment ? 1 : 2,
      execMode: 'fork',
      memory: '512M',
      priority: 20,
      dependencies: ['events-poller', 'markets-poller', 'outcomes-poller'],
      env: {
        BATCH_SIZE: '100',
        QUEUE_DRAIN_INTERVAL: '5000',
      },
    });

    this.workerConfigs.set('wss-market-channel', {
      type: 'wss-market-channel',
      instances: isDevelopment ? 1 : 2,
      execMode: 'fork',
      memory: '256M',
      priority: 15,
      env: {
        WEBSOCKET_PORT: '8080',
      },
    });

    this.workerConfigs.set('wss-user-channel', {
      type: 'wss-user-channel',
      instances: isDevelopment ? 1 : 2,
      execMode: 'fork',
      memory: '256M',
      priority: 15,
      env: {
        WEBSOCKET_PORT: '8081',
      },
    });

    // Control workers (single instances)
    this.workerConfigs.set('autoscaler', {
      type: 'autoscaler',
      instances: 1,
      execMode: 'fork',
      memory: '256M',
      priority: 30,
      dependencies: ['db-writer'],
      env: {
        CHECK_INTERVAL: '10000',
        MIN_WORKERS: '1',
        MAX_WORKERS: '3',
        SCALE_UP_THRESHOLD: '5000',
        SCALE_DOWN_THRESHOLD: '500',
      },
    });

    this.workerConfigs.set('heartbeat', {
      type: 'heartbeat',
      instances: 1,
      execMode: 'fork',
      memory: '128M',
      priority: 5,
      env: {
        CHECK_INTERVAL: '30000',
      },
    });

    this.workerConfigs.set('wss-user-controller', {
      type: 'wss-user-controller',
      instances: 1,
      execMode: 'fork',
      memory: '256M',
      priority: 25,
      dependencies: ['wss-market-channel', 'wss-user-channel'],
      env: {
        CONTROLLER_PORT: '8082',
      },
    });
  }

  /**
   * Get worker configs for factory
   */
  private getWorkerConfigsForFactory(): Record<string, any> {
    const configs: Record<string, any> = {};
    for (const [type, config] of this.workerConfigs) {
      configs[type.replace('-', '')] = config.env;
    }
    return configs;
  }

  /**
   * Setup event handlers
   */
  private setupEventHandlers(): void {
    // Process signals
    process.on('SIGTERM', async () => {
      this.logger.info('Received SIGTERM, initiating graceful shutdown');
      try {
        await this.shutdown();
        process.exit(0);
      } catch (error) {
        this.logger.error('Error during SIGTERM shutdown', { error: this.normalizeError(error) });
        process.exit(1);
      }
    });

    process.on('SIGINT', async () => {
      this.logger.info('Received SIGINT, initiating graceful shutdown');
      try {
        await this.shutdown();
        process.exit(0);
      } catch (error) {
        this.logger.error('Error during SIGINT shutdown', { error: this.normalizeError(error) });
        process.exit(1);
      }
    });

    process.on('uncaughtException', (error) => {
      this.logger.error('Uncaught exception', { error: this.normalizeError(error) });
      this.shutdown().then(() => {
        process.exit(1);
      });
    });

    process.on('unhandledRejection', (reason, promise) => {
      this.logger.error('Unhandled rejection', { reason, promise });
    });
  }

  private normalizeError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error));
  }

  /**
   * Get application status
   */
  getStatus(): any {
    return {
      state: this.appState,
      mode: this.appMode,
      uptime: Date.now() - this.startTime.getTime(),
      startTime: this.startTime,
      pid: process.pid,
      memory: process.memoryUsage(),
      workers: Array.from(this.workerConfigs.keys()),
      systemInfo: getWorkerSystemInfo(),
    };
  }
}

/**
 * CLI setup and application entry point
 */
async function main(): Promise<void> {
  // Setup CLI
  program
    .name('signalcast')
    .description('SignalCast Data Ingestion System')
    .version('1.0.0');

  program
    .option('--mode <mode>', 'Application mode', 'orchestrator')
    .option('--worker <type>', 'Run as specific worker type')
    .option('--development', 'Run in development mode')
    .option('--standalone', 'Run in standalone mode')
    .option('--status', 'Show application status')
    .option('--health-check', 'Perform health check')
    .parse();

  const options = program.opts<CliOptions>();

  // Determine application mode
  let appMode: AppMode;
  if (options.worker) {
    appMode = AppMode.WORKER;
    process.env['PROCESS_TYPE'] = options.worker;
  } else if (options.standalone) {
    appMode = AppMode.STANDALONE;
  } else if (options.development) {
    appMode = AppMode.DEVELOPMENT;
  } else {
    appMode = AppMode.ORCHESTRATOR;
  }

  // Create and initialize application
  const app = new SignalCastApp(appMode);

  try {
    await app.initialize();

    if (options.status) {
      console.log(JSON.stringify(app.getStatus(), null, 2));
      process.exit(0);
    }

    if (options.healthCheck) {
      await app.performHealthCheck();
      console.log('Health check completed');
      process.exit(0);
    }

    await app.start();

    // Keep process alive in non-worker modes
    if (appMode !== AppMode.WORKER) {
      console.log(`SignalCast running in ${appMode} mode (PID: ${process.pid})`);

      // Setup periodic status logging
      setInterval(() => {
        const status = app.getStatus();
        console.log(`Status: ${status.state} | Uptime: ${Math.floor(status.uptime / 1000)}s | Memory: ${Math.floor(status.memory.heapUsed / 1024 / 1024)}MB`);
      }, 60000); // Every minute
    }

  } catch (error) {
    console.error('Failed to start SignalCast application:', error);
    process.exit(1);
  }
}

// Handle uncaught errors
function formatError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', formatError(error));
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled rejection at:', promise, 'reason:', formatError(reason));
  process.exit(1);
});

// Start the application
if (require.main === module) {
  main().catch((error) => {
    console.error('Application startup failed:', error);
    process.exit(1);
  });
}

export { SignalCastApp, AppMode, AppState };
