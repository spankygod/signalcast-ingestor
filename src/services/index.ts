/**
 * Services Module Exports
 * Provides unified access to all Polymarket services and initialization
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { PolymarketAuthService, getAuthService, initializeAuth } from './polymarket-auth';
import { PolymarketApiClient, createApiClient } from './polymarket-api';
import { PolymarketWebSocketHandler, PolymarketWebSocketManager, createWebSocketHandler } from './wss-handlers';
import { WS_CONFIG, WS_CHANNELS, WS_MESSAGE_TYPES, WS_FILTERS, ERROR_HANDLING, DATA_MAPPING } from '../config/polymarket';

/**
 * Service manager interface
 */
export interface ServiceManager {
  authService: PolymarketAuthService;
  apiClient: PolymarketApiClient;
  wsManager: PolymarketWebSocketManager;
  wsHandler: PolymarketWebSocketHandler;
  initialized: boolean;
  workerId: string;
}

/**
 * Service initialization options
 */
export interface ServiceInitOptions {
  workerId?: string;
  enableApiClient?: boolean;
  enableWebSocket?: boolean;
  apiPolling?: boolean;
  wsSubscriptions?: string[];
  authRequired?: boolean;
}

/**
 * Service configuration
 */
export interface ServiceConfig {
  auth: {
    autoRefresh: boolean;
    refreshThreshold: number;
  };
  api: {
    polling: {
      events: boolean;
      markets: boolean;
      outcomes: boolean;
      intervals: {
        events: number;
        markets: number;
        outcomes: number;
      };
    };
    rateLimiting: boolean;
    batchSize: number;
  };
  websocket: {
    autoReconnect: boolean;
    maxReconnectAttempts: number;
    subscriptions: {
      priceUpdates: boolean;
      trades: boolean;
      marketData: boolean;
      ticker: boolean;
    };
  };
}

/**
 * Default service configuration
 */
export const DEFAULT_SERVICE_CONFIG: ServiceConfig = {
  auth: {
    autoRefresh: true,
    refreshThreshold: 300000, // 5 minutes
  },
  api: {
    polling: {
      events: true,
      markets: true,
      outcomes: true,
      intervals: {
        events: 5000,   // 5 seconds
        markets: 2000,  // 2 seconds
        outcomes: 2000, // 2 seconds
      },
    },
    rateLimiting: true,
    batchSize: 100,
  },
  websocket: {
    autoReconnect: true,
    maxReconnectAttempts: 10,
    subscriptions: {
      priceUpdates: true,
      trades: true,
      marketData: true,
      ticker: false,
    },
  },
};

/**
 * Service manager class
 */
export class PolymarketServiceManager {
  private logger: Logger;
  private config: ServiceConfig;
  private authService: PolymarketAuthService;
  private apiClient: PolymarketApiClient | null = null;
  private wsManager: PolymarketWebSocketManager;
  private wsHandler: PolymarketWebSocketHandler | null = null;
  private initialized: boolean = false;
  private workerId: string;
  private initOptions: ServiceInitOptions;

  constructor(workerId: string = process.pid.toString(), config: Partial<ServiceConfig> = {}) {
    this.workerId = workerId;
    this.config = { ...DEFAULT_SERVICE_CONFIG, ...config };
    this.initOptions = {
      workerId,
      enableApiClient: true,
      enableWebSocket: true,
      apiPolling: true,
      wsSubscriptions: ['price_updates', 'market_data'],
      authRequired: true,
    };

    this.logger = LoggerFactory.getLogger(`service-manager-${this.workerId}`, {
      category: LogCategory.SYSTEM,
      workerId: this.workerId,
    });

    this.authService = getAuthService();
    this.wsManager = new PolymarketWebSocketManager();

    this.logger.info('Service manager created', {
      workerId: this.workerId,
      config: this.config,
    });
  }

  /**
   * Initialize all services
   */
  async initialize(options: Partial<ServiceInitOptions> = {}): Promise<void> {
    if (this.initialized) {
      this.logger.warn('Services already initialized');
      return;
    }

    this.initOptions = { ...this.initOptions, ...options };

    this.logger.info('Initializing services', this.initOptions);

    try {
      // Initialize authentication first
      if (this.initOptions.authRequired) {
        await this.initializeAuthentication();
      }

      // Initialize API client
      if (this.initOptions.enableApiClient) {
        await this.initializeApiClient();
      }

      // Initialize WebSocket handler
      if (this.initOptions.enableWebSocket) {
        await this.initializeWebSocket();
      }

      this.initialized = true;
      this.logger.info('All services initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize services', {
        error: error as Error,
      });
      await this.cleanup();
      throw error;
    }
  }

  /**
   * Initialize authentication service
   */
  private async initializeAuthentication(): Promise<void> {
    this.logger.info('Initializing authentication service');

    await this.authService.initialize();

    const authStatus = this.authService.getAuthStatus();
    this.logger.info('Authentication service initialized', authStatus);
  }

  /**
   * Initialize API client
   */
  private async initializeApiClient(): Promise<void> {
    this.logger.info('Initializing API client');

    this.apiClient = new PolymarketApiClient(this.authService);
    await this.apiClient.initialize();

    // Start polling if enabled
    if (this.initOptions.apiPolling) {
      this.apiClient.startPolling();
      this.logger.info('API polling started');
    }

    this.logger.info('API client initialized');
  }

  /**
   * Initialize WebSocket handler
   */
  private async initializeWebSocket(): Promise<void> {
    this.logger.info('Initializing WebSocket handler');

    this.wsHandler = this.wsManager.createHandler(this.workerId);
    await this.wsHandler.initialize();

    // Setup default subscriptions
    await this.setupDefaultSubscriptions();

    this.logger.info('WebSocket handler initialized');
  }

  /**
   * Setup default WebSocket subscriptions
   */
  private async setupDefaultSubscriptions(): Promise<void> {
    if (!this.wsHandler || !this.initOptions.wsSubscriptions) {
      return;
    }

    const subscriptions = this.initOptions.wsSubscriptions;

    if (subscriptions.includes('price_updates')) {
      this.wsHandler.subscribe('price_updates');
      this.logger.info('Subscribed to price updates');
    }

    if (subscriptions.includes('market_data')) {
      this.wsHandler.subscribe('market_data');
      this.logger.info('Subscribed to market data');
    }

    if (subscriptions.includes('trades')) {
      this.wsHandler.subscribe('trades');
      this.logger.info('Subscribed to trades');
    }

    if (subscriptions.includes('ticker')) {
      this.wsHandler.subscribe('ticker');
      this.logger.info('Subscribed to ticker');
    }
  }

  /**
   * Get service status
   */
  getServiceStatus(): {
    initialized: boolean;
    auth: any;
    api: any;
    websocket: any;
  } {
    return {
      initialized: this.initialized,
      auth: this.authService.getAuthStatus(),
      api: this.apiClient ? this.apiClient.getStats() : { enabled: false },
      websocket: {
        handler: this.wsHandler ? this.wsHandler.getStats() : { enabled: false },
        manager: this.wsManager.getAggregatedStats(),
      },
    };
  }

  /**
   * Health check for all services
   */
  async healthCheck(): Promise<{
    healthy: boolean;
    services: {
      auth: boolean;
      api: boolean;
      websocket: boolean;
    };
    details: any;
  }> {
    const services = {
      auth: false,
      api: false,
      websocket: false,
    };

    const details: any = {};

    // Check authentication
    try {
      services.auth = this.authService.isAuthenticated();
      details.auth = this.authService.getAuthStatus();
    } catch (error) {
      details.auth = { error: (error as Error).message };
    }

    // Check API client
    try {
      if (this.apiClient) {
        await this.apiClient.healthCheck();
        services.api = true;
        details.api = this.apiClient.getStats();
      } else {
        details.api = { enabled: false };
      }
    } catch (error) {
      details.api = { error: (error as Error).message };
    }

    // Check WebSocket
    try {
      if (this.wsHandler) {
        services.websocket = this.wsHandler.isHealthy();
        details.websocket = this.wsHandler.getStats();
      } else {
        details.websocket = { enabled: false };
      }
    } catch (error) {
      details.websocket = { error: (error as Error).message };
    }

    const healthy = Object.values(services).every(Boolean);

    return {
      healthy,
      services,
      details,
    };
  }

  /**
   * Restart services
   */
  async restart(): Promise<void> {
    this.logger.info('Restarting services');

    await this.cleanup();
    await this.initialize(this.initOptions);

    this.logger.info('Services restarted successfully');
  }

  /**
   * Get authentication service
   */
  getAuthService(): PolymarketAuthService {
    return this.authService;
  }

  /**
   * Get API client
   */
  getApiClient(): PolymarketApiClient | null {
    return this.apiClient;
  }

  /**
   * Get WebSocket handler
   */
  getWebSocketHandler(): PolymarketWebSocketHandler | null {
    return this.wsHandler;
  }

  /**
   * Get WebSocket manager
   */
  getWebSocketManager(): PolymarketWebSocketManager {
    return this.wsManager;
  }

  /**
   * Update service configuration
   */
  updateConfig(newConfig: Partial<ServiceConfig>): void {
    this.config = { ...this.config, ...newConfig };
    this.logger.info('Service configuration updated', { config: this.config });
  }

  /**
   * Get current configuration
   */
  getConfig(): ServiceConfig {
    return { ...this.config };
  }

  /**
   * Cleanup all services
   */
  async cleanup(): Promise<void> {
    this.logger.info('Cleaning up services');

    this.initialized = false;

    try {
      // Cleanup API client
      if (this.apiClient) {
        this.apiClient.cleanup();
        this.apiClient = null;
      }

      // Cleanup WebSocket handler
      if (this.wsHandler) {
        this.wsManager.removeHandler(this.workerId);
        this.wsHandler = null;
      }

      // Cleanup WebSocket manager
      this.wsManager.cleanup();

      // Cleanup authentication service
      this.authService.cleanup();

      this.logger.info('All services cleaned up');

    } catch (error) {
      this.logger.error('Error during service cleanup', {
        error: error as Error,
      });
    }
  }
}

/**
 * Global service manager instance
 */
let globalServiceManager: PolymarketServiceManager | null = null;

/**
 * Get or create global service manager
 */
export function getServiceManager(workerId?: string, config?: Partial<ServiceConfig>): PolymarketServiceManager {
  if (!globalServiceManager) {
    globalServiceManager = new PolymarketServiceManager(workerId, config);
  }
  return globalServiceManager;
}

/**
 * Initialize services with default configuration
 */
export async function initializeServices(
  workerId?: string,
  options?: Partial<ServiceInitOptions>,
  config?: Partial<ServiceConfig>
): Promise<PolymarketServiceManager> {
  const manager = getServiceManager(workerId, config);
  await manager.initialize(options);
  return manager;
}

/**
 * Quick initialization for common use cases
 */
export async function quickInitialize(workerId?: string): Promise<{
  auth: PolymarketAuthService;
  api: PolymarketApiClient;
  websocket: PolymarketWebSocketHandler;
  manager: PolymarketServiceManager;
}> {
  const manager = await initializeServices(workerId, {
    enableApiClient: true,
    enableWebSocket: true,
    apiPolling: true,
    wsSubscriptions: ['price_updates', 'market_data', 'trades'],
    authRequired: true,
  });

  const auth = manager.getAuthService();
  const api = manager.getApiClient()!;
  const websocket = manager.getWebSocketHandler()!;

  return {
    auth,
    api,
    websocket,
    manager,
  };
}

/**
 * Initialize only authentication service
 */
export async function initializeAuthOnly(workerId?: string): Promise<PolymarketAuthService> {
  const manager = new PolymarketServiceManager(workerId);
  await manager.initialize({
    enableApiClient: false,
    enableWebSocket: false,
    authRequired: true,
  });
  return manager.getAuthService();
}

/**
 * Initialize only API client
 */
export async function initializeApiOnly(workerId?: string): Promise<PolymarketApiClient> {
  const manager = new PolymarketServiceManager(workerId);
  await manager.initialize({
    enableApiClient: true,
    enableWebSocket: false,
    apiPolling: true,
    authRequired: true,
  });
  return manager.getApiClient()!;
}

/**
 * Initialize only WebSocket handler
 */
export async function initializeWebSocketOnly(workerId?: string): Promise<PolymarketWebSocketHandler> {
  const manager = new PolymarketServiceManager(workerId);
  await manager.initialize({
    enableApiClient: false,
    enableWebSocket: true,
    wsSubscriptions: ['price_updates', 'market_data', 'trades'],
    authRequired: false,
  });
  return manager.getWebSocketHandler()!;
}

/**
 * Service utilities
 */
export const serviceUtils = {
  /**
   * Create service ID
   */
  createServiceId(type: string, workerId?: string): string {
    return `${type}_${workerId || process.pid}_${Date.now()}`;
  },

  /**
   * Validate service configuration
   */
  validateConfig(config: Partial<ServiceConfig>): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (config.auth) {
      if (typeof config.auth.autoRefresh !== 'boolean') {
        errors.push('auth.autoRefresh must be a boolean');
      }
      if (typeof config.auth.refreshThreshold !== 'number' || config.auth.refreshThreshold < 0) {
        errors.push('auth.refreshThreshold must be a positive number');
      }
    }

    if (config.api) {
      if (config.api.polling) {
        if (typeof config.api.polling.events !== 'boolean') {
          errors.push('api.polling.events must be a boolean');
        }
        if (config.api.polling.intervals) {
          const { intervals } = config.api.polling;
          if (intervals.events && intervals.events < 1000) {
            errors.push('api.polling.intervals.events must be at least 1000ms');
          }
          if (intervals.markets && intervals.markets < 1000) {
            errors.push('api.polling.intervals.markets must be at least 1000ms');
          }
          if (intervals.outcomes && intervals.outcomes < 1000) {
            errors.push('api.polling.intervals.outcomes must be at least 1000ms');
          }
        }
      }
    }

    if (config.websocket) {
      if (typeof config.websocket.autoReconnect !== 'boolean') {
        errors.push('websocket.autoReconnect must be a boolean');
      }
      if (typeof config.websocket.maxReconnectAttempts !== 'number' || config.websocket.maxReconnectAttempts < 0) {
        errors.push('websocket.maxReconnectAttempts must be a positive number');
      }
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  },

  /**
   * Get default configuration
   */
  getDefaultConfig(): ServiceConfig {
    return { ...DEFAULT_SERVICE_CONFIG };
  },
};

// Export default service manager
export default PolymarketServiceManager;
