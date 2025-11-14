/**
 * SignalCast Workers Index
 * Central export point for all worker classes and factory functions
 * Provides unified access to the complete worker ecosystem
 */

// Core Workers
import {
  EventsPollerWorker,
  createEventsPoller,
  runEventsPoller,
} from './events-poller';
import {
  MarketsPollerWorker,
  createMarketsPoller,
  runMarketsPoller,
} from './markets-poller';
import {
  DatabaseWriterWorker,
  createDatabaseWriter,
  runDatabaseWriter,
} from './db-writer';
import {
  AutoscalerWorker,
  createAutoscaler,
  runAutoscaler,
} from './autoscaler';
import {
  HeartbeatWorker,
  createHeartbeat,
  runHeartbeat,
} from './heartbeat';

// WebSocket Workers
import {
  MarketChannelWebSocketHandler,
  createMarketChannelHandler,
  runMarketChannelHandler,
} from './wss-market-channel';
import {
  UserChannelWebSocketHandler,
  createUserChannelHandler,
  runUserChannelHandler,
} from './wss-user-channel';
import {
  WebSocketUserController,
  createWebSocketUserController,
  runWebSocketUserController,
} from './wss-user-controller';

// Re-export worker APIs
export {
  EventsPollerWorker,
  createEventsPoller,
  runEventsPoller,
  MarketsPollerWorker,
  createMarketsPoller,
  runMarketsPoller,
  DatabaseWriterWorker,
  createDatabaseWriter,
  runDatabaseWriter,
  AutoscalerWorker,
  createAutoscaler,
  runAutoscaler,
  HeartbeatWorker,
  createHeartbeat,
  runHeartbeat,
  MarketChannelWebSocketHandler,
  createMarketChannelHandler,
  runMarketChannelHandler,
  UserChannelWebSocketHandler,
  createUserChannelHandler,
  runUserChannelHandler,
  WebSocketUserController,
  createWebSocketUserController,
  runWebSocketUserController,
};

// Re-export for convenience
export {
  EventsPollerWorker as EventsPoller,
  MarketsPollerWorker as MarketsPoller,
  DatabaseWriterWorker as DatabaseWriter,
  AutoscalerWorker as Autoscaler,
  HeartbeatWorker as Heartbeat,
  MarketChannelWebSocketHandler as MarketChannel,
  UserChannelWebSocketHandler as UserChannel,
  WebSocketUserController as WebSocketController,
} from './';

// Import dependencies for factory functions
import { Logger, LoggerFactory } from '../lib/logger';
import { PolymarketAuthService } from '../services/polymarket-auth';
import { PolymarketApiClient } from '../services/polymarket-api';
import { PM2ProcessManager, defaultPM2Manager } from '../lib/pm2-manager';
import { dbConnection } from '../lib/db';
import { redisConnection } from '../lib/redis';
import { EventsQueue } from '../queues/events.queue';
import { MarketsQueue } from '../queues/markets.queue';
import { OutcomesQueue } from '../queues/outcomes.queue';
import { TicksQueue } from '../queues/ticks.queue';
import { DeadletterQueue } from '../queues/dead-letter.queue';
import { config } from '../config';

/**
 * Worker factory configuration
 */
export interface WorkerFactoryConfig {
  enableLogging: boolean;
  logLevel: string;
  enableMetrics: boolean;
  enableHealthChecks: boolean;
  customLogger?: Logger;
}

/**
 * Worker factory for creating and managing workers
 */
export class WorkerFactory {
  private logger: Logger;
  private config: WorkerFactoryConfig;
  private authService: PolymarketAuthService;
  private apiClient: PolymarketApiClient;
  private pm2Manager: PM2ProcessManager;
  private queues: {
    events: EventsQueue;
    markets: MarketsQueue;
    outcomes: OutcomesQueue;
    ticks: TicksQueue;
    deadletter: DeadletterQueue;
  };

  constructor(config?: Partial<WorkerFactoryConfig>) {
    this.config = {
      enableLogging: true,
      logLevel: 'info',
      enableMetrics: true,
      enableHealthChecks: true,
      ...config,
    };

    // Initialize logger
    this.logger = this.config.customLogger || LoggerFactory.getLogger('worker-factory', {
      category: 'SYSTEM',
    });

    // Initialize core services
    this.authService = new PolymarketAuthService();
    this.apiClient = new PolymarketApiClient(this.authService);
    this.pm2Manager = defaultPM2Manager;

    // Initialize queues
    this.queues = {
      events: new EventsQueue(),
      markets: new MarketsQueue(),
      outcomes: new OutcomesQueue(),
      ticks: new TicksQueue(),
      deadletter: new DeadletterQueue(),
    };

    this.logger.info('Worker factory initialized', {
      config: this.config,
    });
  }

  /**
   * Initialize all services
   */
  async initialize(): Promise<void> {
    try {
      this.logger.info('Initializing worker factory services');

      // Initialize authentication service
      await this.authService.initialize();

      // Initialize API client
      await this.apiClient.initialize();

      // Initialize PM2 manager
      await this.pm2Manager.initialize();

      // Initialize database and Redis connections
      await dbConnection.healthCheck();
      await redisConnection.healthCheck();

      // Initialize queues
      await Promise.all([
        this.queues.events.initialize(),
        this.queues.markets.initialize(),
        this.queues.outcomes.initialize(),
        this.queues.ticks.initialize(),
        this.queues.deadletter.initialize(),
      ]);

      this.logger.info('Worker factory services initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize worker factory services', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Create events poller worker
   */
  async createEventsPoller(config?: any): Promise<EventsPollerWorker> {
    return await import('./events-poller').then(({ createEventsPoller }) =>
      createEventsPoller(this.apiClient, this.queues.events, config)
    );
  }

  /**
   * Create markets poller worker
   */
  async createMarketsPoller(config?: any): Promise<MarketsPollerWorker> {
    return await import('./markets-poller').then(({ createMarketsPoller }) =>
      createMarketsPoller(this.apiClient, this.queues.markets, config)
    );
  }

  /**
   * Create database writer worker
   */
  async createDatabaseWriter(config?: any): Promise<DatabaseWriterWorker> {
    return await import('./db-writer').then(({ createDatabaseWriter }) =>
      createDatabaseWriter(
        this.queues.events,
        this.queues.markets,
        this.queues.outcomes,
        this.queues.ticks,
        config
      )
    );
  }

  /**
   * Create autoscaler worker
   */
  async createAutoscaler(config?: any): Promise<AutoscalerWorker> {
    return await import('./autoscaler').then(({ createAutoscaler }) =>
      createAutoscaler(
        this.pm2Manager,
        this.queues.events,
        this.queues.markets,
        this.queues.outcomes,
        this.queues.ticks,
        config
      )
    );
  }

  /**
   * Create heartbeat worker
   */
  async createHeartbeat(config?: any): Promise<HeartbeatWorker> {
    return await import('./heartbeat').then(({ createHeartbeat }) =>
      createHeartbeat(
        dbConnection,
        redisConnection,
        this.pm2Manager,
        this.queues.events,
        this.queues.markets,
        this.queues.outcomes,
        this.queues.ticks,
        config
      )
    );
  }

  /**
   * Create market channel WebSocket handler
   */
  async createMarketChannelHandler(config?: any): Promise<MarketChannelWebSocketHandler> {
    return await import('./wss-market-channel').then(({ createMarketChannelHandler }) =>
      createMarketChannelHandler(this.queues.ticks, undefined, config)
    );
  }

  /**
   * Create user channel WebSocket handler
   */
  async createUserChannelHandler(config?: any): Promise<UserChannelWebSocketHandler> {
    return await import('./wss-user-channel').then(({ createUserChannelHandler }) =>
      createUserChannelHandler(this.authService, dbConnection, config)
    );
  }

  /**
   * Create WebSocket user controller
   */
  async createWebSocketUserController(config?: any): Promise<WebSocketUserController> {
    const userChannelHandler = await this.createUserChannelHandler();
    const marketChannelHandler = await this.createMarketChannelHandler();

    return await import('./wss-user-controller').then(({ createWebSocketUserController }) =>
      createWebSocketUserController(
        userChannelHandler,
        marketChannelHandler,
        this.authService,
        dbConnection,
        config
      )
    );
  }

  /**
   * Create complete worker set for production deployment
   */
  async createProductionWorkerSet(config?: {
    eventsPoller?: any;
    marketsPoller?: any;
    databaseWriter?: any;
    autoscaler?: any;
    heartbeat?: any;
    marketChannel?: any;
    userChannel?: any;
    webSocketController?: any;
  }): Promise<{
    eventsPoller: EventsPollerWorker;
    marketsPoller: MarketsPollerWorker;
    databaseWriter: DatabaseWriterWorker;
    autoscaler: AutoscalerWorker;
    heartbeat: HeartbeatWorker;
    marketChannel: MarketChannelWebSocketHandler;
    userChannel: UserChannelWebSocketHandler;
    webSocketController: WebSocketUserController;
  }> {
    this.logger.info('Creating production worker set');

    const [
      eventsPoller,
      marketsPoller,
      databaseWriter,
      autoscaler,
      heartbeat,
      marketChannel,
      userChannel,
      webSocketController,
    ] = await Promise.all([
      this.createEventsPoller(config?.eventsPoller),
      this.createMarketsPoller(config?.marketsPoller),
      this.createDatabaseWriter(config?.databaseWriter),
      this.createAutoscaler(config?.autoscaler),
      this.createHeartbeat(config?.heartbeat),
      this.createMarketChannelHandler(config?.marketChannel),
      this.createUserChannelHandler(config?.userChannel),
      this.createWebSocketUserController(config?.webSocketController),
    ]);

    this.logger.info('Production worker set created successfully');

    return {
      eventsPoller,
      marketsPoller,
      databaseWriter,
      autoscaler,
      heartbeat,
      marketChannel,
      userChannel,
      webSocketController,
    };
  }

  /**
   * Start all workers in dependency order
   */
  async startProductionWorkerSet(workerSet: ReturnType<typeof this.createProductionWorkerSet>): Promise<void> {
    this.logger.info('Starting production worker set');

    const {
      eventsPoller,
      marketsPoller,
      databaseWriter,
      autoscaler,
      heartbeat,
      marketChannel,
      userChannel,
      webSocketController,
    } = await workerSet;

    try {
      // Start in dependency order
      await heartbeat.start();
      await marketChannel.start();
      await userChannel.start();
      await webSocketController.start();

      // Start data producers
      await eventsPoller.start();
      await marketsPoller.start();

      // Start data consumer
      await databaseWriter.start();

      // Start autoscaler last
      await autoscaler.start();

      this.logger.info('Production worker set started successfully');

    } catch (error) {
      this.logger.error('Failed to start production worker set', {
        error: error as Error,
      });

      // Cleanup on failure
      await this.stopProductionWorkerSet({
        eventsPoller,
        marketsPoller,
        databaseWriter,
        autoscaler,
        heartbeat,
        marketChannel,
        userChannel,
        webSocketController,
      });

      throw error;
    }
  }

  /**
   * Stop all workers in reverse dependency order
   */
  async stopProductionWorkerSet(workerSet: any): Promise<void> {
    this.logger.info('Stopping production worker set');

    const {
      eventsPoller,
      marketsPoller,
      databaseWriter,
      autoscaler,
      heartbeat,
      marketChannel,
      userChannel,
      webSocketController,
    } = workerSet;

    try {
      // Stop in reverse dependency order
      await autoscaler.stop();
      await databaseWriter.stop();
      await marketsPoller.stop();
      await eventsPoller.stop();
      await webSocketController.stop();
      await userChannel.stop();
      await marketChannel.stop();
      await heartbeat.stop();

      this.logger.info('Production worker set stopped successfully');

    } catch (error) {
      this.logger.error('Error stopping production worker set', {
        error: error as Error,
      });
    }
  }

  /**
   * Get factory configuration
   */
  getConfig(): WorkerFactoryConfig {
    return { ...this.config };
  }

  /**
   * Get service instances
   */
  getServices() {
    return {
      logger: this.logger,
      authService: this.authService,
      apiClient: this.apiClient,
      pm2Manager: this.pm2Manager,
      queues: this.queues,
      database: dbConnection,
      redis: redisConnection,
    };
  }

  /**
   * Cleanup all resources
   */
  async cleanup(): Promise<void> {
    try {
      this.logger.info('Cleaning up worker factory resources');

      // Close database connection
      await dbConnection.close();

      // Close Redis connection
      await redisConnection.close();

      this.logger.info('Worker factory resources cleaned up');

    } catch (error) {
      this.logger.error('Error during cleanup', {
        error: error as Error,
      });
    }
  }
}

/**
 * Default worker factory instance
 */
export const defaultWorkerFactory = new WorkerFactory();

/**
 * Initialize default worker factory
 */
export async function initializeWorkerFactory(config?: Partial<WorkerFactoryConfig>): Promise<WorkerFactory> {
  const factory = new WorkerFactory(config);
  await factory.initialize();
  return factory;
}

/**
 * Create and start complete worker system
 */
export async function createAndStartWorkerSystem(
  config?: Partial<WorkerFactoryConfig> & {
    workerConfigs?: {
      eventsPoller?: any;
      marketsPoller?: any;
      databaseWriter?: any;
      autoscaler?: any;
      heartbeat?: any;
      marketChannel?: any;
      userChannel?: any;
      webSocketController?: any;
    };
  }
): Promise<{
  factory: WorkerFactory;
  workers: any;
}> {
  const factory = await initializeWorkerFactory(config);
  const workers = await factory.createProductionWorkerSet(config?.workerConfigs);
  await factory.startProductionWorkerSet(Promise.resolve(workers));

  return { factory, workers };
}

/**
 * Worker system information
 */
export interface WorkerSystemInfo {
  version: string;
  description: string;
  architecture: string;
  components: Array<{
    name: string;
    type: string;
    description: string;
    dependencies: string[];
  }>;
  configuration: {
    polling: Record<string, number>;
    batchSizes: Record<string, number>;
    scaling: Record<string, number>;
    timeouts: Record<string, number>;
  };
}

/**
 * Get worker system information
 */
export function getWorkerSystemInfo(): WorkerSystemInfo {
  return {
    version: '1.0.0',
    description: 'SignalCast Data Ingestion System - Complete Worker Ecosystem',
    architecture: 'microservices-with-queue-based-processing',
    components: [
      {
        name: 'EventsPoller',
        type: 'poller',
        description: 'Polls Polymarket API for event data every 5 seconds',
        dependencies: ['PolymarketApiClient', 'EventsQueue'],
      },
      {
        name: 'MarketsPoller',
        type: 'poller',
        description: 'Polls Polymarket API for market data every 2 seconds',
        dependencies: ['PolymarketApiClient', 'MarketsQueue'],
      },
      {
        name: 'OutcomesPoller',
        type: 'poller',
        description: 'Polls Polymarket API for outcome data every 2 seconds',
        dependencies: ['PolymarketApiClient', 'OutcomesQueue'],
      },
      {
        name: 'DatabaseWriter',
        type: 'processor',
        description: 'Processes queue messages with bulk UPSERT operations',
        dependencies: ['EventsQueue', 'MarketsQueue', 'OutcomesQueue', 'TicksQueue', 'Database'],
      },
      {
        name: 'Autoscaler',
        type: 'controller',
        description: 'Dynamically scales workers based on queue depth',
        dependencies: ['PM2Manager', 'All Queues'],
      },
      {
        name: 'Heartbeat',
        type: 'monitor',
        description: 'Monitors system health every 30 seconds',
        dependencies: ['Database', 'Redis', 'PM2Manager', 'All Queues'],
      },
      {
        name: 'MarketChannel',
        type: 'websocket',
        description: 'Handles real-time market data from Polymarket WebSocket',
        dependencies: ['TicksQueue', 'WebSocket'],
      },
      {
        name: 'UserChannel',
        type: 'websocket',
        description: 'Handles user-specific WebSocket connections',
        dependencies: ['AuthService', 'Database', 'WebSocket'],
      },
      {
        name: 'WebSocketController',
        type: 'controller',
        description: 'Manages WebSocket connection lifecycle and routing',
        dependencies: ['MarketChannel', 'UserChannel', 'AuthService'],
      },
    ],
    configuration: {
      polling: {
        events: 5000,
        markets: 2000,
        outcomes: 2000,
        heartbeat: 30000,
      },
      batchSizes: {
        events: 100,
        markets: 100,
        outcomes: 200,
        ticks: 100,
        database: 100,
      },
      scaling: {
        minWorkers: 1,
        maxWorkers: 3,
        scaleUpThreshold: 5000,
        scaleDownThreshold: 500,
      },
      timeouts: {
        apiRequest: 30000,
        databaseQuery: 10000,
        websocketHandshake: 10000,
        workerShutdown: 5000,
      },
    },
  };
}

// Export system info
export type { WorkerSystemInfo };

/**
 * Worker utilities
 */
export const WorkerUtils = {
  /**
   * Validate worker configuration
   */
  validateConfig(config: any): boolean {
    // Basic validation
    return typeof config === 'object' && config !== null;
  },

  /**
   * Get worker health status summary
   */
  getHealthSummary(workers: any[]): {
    total: number;
    healthy: number;
    unhealthy: number;
    unknown: number;
  } {
    const summary = { total: workers.length, healthy: 0, unhealthy: 0, unknown: 0 };

    workers.forEach(worker => {
      if (typeof worker.isHealthy === 'function') {
        if (worker.isHealthy()) {
          summary.healthy++;
        } else {
          summary.unhealthy++;
        }
      } else {
        summary.unknown++;
      }
    });

    return summary;
  },

  /**
   * Format worker statistics for logging
   */
  formatStats(stats: any): string {
    return JSON.stringify(stats, null, 2);
  },
};

export default {
  // Workers
  EventsPollerWorker,
  MarketsPollerWorker,
  DatabaseWriterWorker,
  AutoscalerWorker,
  HeartbeatWorker,
  MarketChannelWebSocketHandler,
  UserChannelWebSocketHandler,
  WebSocketUserController,

  // Factory
  WorkerFactory,
  defaultWorkerFactory,
  initializeWorkerFactory,
  createAndStartWorkerSystem,

  // Utilities
  WorkerUtils,
  getWorkerSystemInfo,
};
