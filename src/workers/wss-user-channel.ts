/**
 * User Channel WebSocket Handler
 * Handles user-specific WebSocket connections for real-time data delivery
 * Manages authentication, subscriptions, and personalized data streaming
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { PolymarketAuthService } from '../services/polymarket-auth';
import { database } from '../lib/db';

/**
 * User channel configuration
 */
interface UserChannelConfig {
  maxConnections: number;
  connectionTimeoutMs: number;
  authTimeoutMs: number;
  maxSubscriptionsPerConnection: number;
  heartbeatIntervalMs: number;
  heartbeatTimeoutMs: number;
  maxMessageSize: number;
  enableCompression: boolean;
  rateLimitRequestsPerMinute: number;
  rateLimitWindowSizeMs: number;
  enablePersistedSubscriptions: boolean;
  healthCheckIntervalMs: number;
}

/**
 * User connection information
 */
interface UserConnection {
  id: string;
  userId?: string;
  socketId: string;
  socket: WebSocket; // This would be a WebSocket instance
  authenticated: boolean;
  authPending: boolean;
  connectedAt: number;
  lastActivityAt: number;
  subscriptions: Set<string>;
  rateLimitRequests: number[];
  metadata: {
    userAgent?: string;
    ipAddress?: string;
    origin?: string;
  };
  preferences: UserPreferences;
}

/**
 * User preferences
 */
interface UserPreferences {
  categories: string[];
  eventIds: string[];
  marketIds: string[];
  minLiquidity: number;
  enablePriceAlerts: boolean;
  enableTradeNotifications: boolean;
  language: string;
  timezone: string;
}

/**
 * WebSocket message types
 */
interface WebSocketMessage {
  type: string;
  id?: string;
  timestamp: number;
  data?: any;
  error?: string;
}

/**
 * User channel statistics
 */
interface UserChannelStats {
  totalConnections: number;
  activeConnections: number;
  authenticatedConnections: number;
  totalSubscriptions: number;
  messagesSent: number;
  messagesReceived: number;
  authenticationFailures: number;
  rateLimitViolations: number;
  errorsCount: number;
  averageConnectionDuration: number;
  uptime: number;
  lastConnectionAt?: string;
  topUserIds: Array<{
    userId: string;
    connections: number;
    subscriptions: number;
  }>;
  subscriptionsByType: Record<string, number>;
}

/**
 * User channel WebSocket handler class
 */
export class UserChannelWebSocketHandler {
  private logger: Logger;
  private config: UserChannelConfig;
  private authService: PolymarketAuthService;
  private isRunning: boolean = false;
  private startTime: number;
  private stats: UserChannelStats;
  private connections: Map<string, UserConnection> = new Map();
  private userConnections: Map<string, Set<string>> = new Map(); // userId -> connectionIds
  private healthCheckTimer: NodeJS.Timeout | null = null;

  constructor(
    authService: PolymarketAuthService,
    config?: Partial<UserChannelConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('wss-user-channel', process.pid.toString());
    this.authService = authService;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      maxConnections: 10000,
      connectionTimeoutMs: 300000, // 5 minutes
      authTimeoutMs: 30000, // 30 seconds
      maxSubscriptionsPerConnection: 50,
      heartbeatIntervalMs: 30000, // 30 seconds
      heartbeatTimeoutMs: 90000, // 90 seconds
      maxMessageSize: 64 * 1024, // 64KB
      enableCompression: true,
      rateLimitRequestsPerMinute: 60,
      rateLimitWindowSizeMs: 60000, // 1 minute
      enablePersistedSubscriptions: true,
      healthCheckIntervalMs: 30000,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      authenticatedConnections: 0,
      totalSubscriptions: 0,
      messagesSent: 0,
      messagesReceived: 0,
      authenticationFailures: 0,
      rateLimitViolations: 0,
      errorsCount: 0,
      averageConnectionDuration: 0,
      uptime: 0,
      topUserIds: [],
      subscriptionsByType: {},
    };

    this.setupGracefulShutdown();
    this.logger.info('User channel WebSocket handler initialized', {
      config: this.config,
    });
  }

  /**
   * Start the user channel handler
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('User channel WebSocket handler is already running');
      return;
    }

    this.logger.info('Starting user channel WebSocket handler');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Start health monitoring
      this.startHealthMonitoring();

      // In a real implementation, this would set up a WebSocket server
      // For now, we'll just log that it's started
      this.logger.info('User channel WebSocket server would be started here');

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('User channel WebSocket handler started successfully');

    } catch (error) {
      this.logger.error('Failed to start user channel WebSocket handler', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the user channel handler
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('User channel WebSocket handler is not running');
      return;
    }

    this.logger.info('Stopping user channel WebSocket handler');
    this.isRunning = false;

    try {
      // Stop health monitoring
      if (this.healthCheckTimer) {
        clearInterval(this.healthCheckTimer);
        this.healthCheckTimer = null;
      }

      // Close all connections
      for (const connection of this.connections.values()) {
        this.closeConnection(connection.id, 'Server shutdown');
      }

      // Clear all connections
      this.connections.clear();
      this.userConnections.clear();

      this.logger.info('User channel WebSocket handler stopped', {
        finalStats: this.getStats(),
      });

    } catch (error) {
      this.logger.error('Error during shutdown', {
        error: error as Error,
      });
    }
  }

  /**
   * Handle new WebSocket connection
   */
  async handleConnection(socket: any, metadata: any): Promise<void> {
    const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    if (this.connections.size >= this.config.maxConnections) {
      this.logger.warn('Connection limit reached', {
        connectionId,
        currentConnections: this.connections.size,
        maxConnections: this.config.maxConnections,
      });

      // Reject connection
      if (socket.close) {
        socket.close(1013, 'Server overloaded');
      }
      return;
    }

    const connection: UserConnection = {
      id: connectionId,
      socketId: metadata.socketId || connectionId,
      socket,
      authenticated: false,
      authPending: true,
      connectedAt: Date.now(),
      lastActivityAt: Date.now(),
      subscriptions: new Set(),
      rateLimitRequests: [],
      metadata: {
        userAgent: metadata.userAgent,
        ipAddress: metadata.ipAddress,
        origin: metadata.origin,
      },
      preferences: {
        categories: [],
        eventIds: [],
        marketIds: [],
        minLiquidity: 0,
        enablePriceAlerts: true,
        enableTradeNotifications: true,
        language: 'en',
        timezone: 'UTC',
      },
    };

    this.connections.set(connectionId, connection);

    // Set up connection timeout
    setTimeout(() => {
      const conn = this.connections.get(connectionId);
      if (conn && !conn.authenticated) {
        this.logger.info('Authentication timeout', { connectionId });
        this.closeConnection(connectionId, 'Authentication timeout');
      }
    }, this.config.authTimeoutMs);

    // Set up message handlers
    this.setupConnectionHandlers(connection);

    this.stats.totalConnections++;
    this.stats.activeConnections++;
    this.stats.lastConnectionAt = new Date().toISOString();

    this.logger.info('New user connection established', {
      connectionId,
      metadata: connection.metadata,
    });

    // Send authentication challenge
    this.sendMessage(connectionId, {
      type: 'auth_challenge',
      timestamp: Date.now(),
      data: {
        message: 'Please authenticate to access personalized data',
        timeout: this.config.authTimeoutMs,
      },
    });
  }

  /**
   * Set up connection message handlers
   */
  private setupConnectionHandlers(connection: UserConnection): void {
    // In a real implementation, this would set up WebSocket event handlers
    // For now, we'll just log what would happen
    this.logger.debug('Connection handlers would be set up here', {
      connectionId: connection.id,
    });
  }

  /**
   * Handle WebSocket message
   */
  async handleMessage(connectionId: string, message: WebSocketMessage): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    connection.lastActivityAt = Date.now();
    this.stats.messagesReceived++;

    try {
      // Check rate limit
      if (this.isRateLimited(connection)) {
        this.stats.rateLimitViolations++;
        this.sendMessage(connectionId, {
          type: 'error',
          timestamp: Date.now(),
          error: 'Rate limit exceeded',
        });
        return;
      }

      // Handle different message types
      switch (message.type) {
        case 'auth':
          await this.handleAuthentication(connectionId, message.data);
          break;

        case 'subscribe':
          await this.handleSubscription(connectionId, message.data);
          break;

        case 'unsubscribe':
          await this.handleUnsubscription(connectionId, message.data);
          break;

        case 'preferences':
          await this.handlePreferencesUpdate(connectionId, message.data);
          break;

        case 'ping':
          this.handlePing(connectionId);
          break;

        default:
          this.logger.warn('Unknown message type', {
            connectionId,
            messageType: message.type,
          });
          this.sendMessage(connectionId, {
            type: 'error',
            timestamp: Date.now(),
            error: `Unknown message type: ${message.type}`,
          });
      }

    } catch (error) {
      this.stats.errorsCount++;
      this.logger.error('Error handling message', {
        connectionId,
        message,
        error: error as Error,
      });

      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Internal server error',
      });
    }
  }

  /**
   * Handle user authentication
   */
  private async handleAuthentication(connectionId: string, authData: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      const { token, apiKey, signature } = authData;

      if (!token && !apiKey && !signature) {
        throw new Error('Authentication credentials required');
      }

      // Verify authentication
      let authResult;
      if (token) {
        authResult = await this.authService.verifyToken(token);
      } else if (apiKey) {
        authResult = await this.authService.verifyApiKey(apiKey);
      } else if (signature) {
        authResult = await this.authService.verifySignature(signature);
      } else {
        throw new Error('Invalid authentication method');
      }

      if (!authResult.valid) {
        throw new Error(authResult.error || 'Authentication failed');
      }

      // Update connection
      connection.authenticated = true;
      connection.authPending = false;
      connection.userId = authResult.userId;

      // Add to user connections map
      if (!this.userConnections.has(authResult.userId)) {
        this.userConnections.set(authResult.userId, new Set());
      }
      this.userConnections.get(authResult.userId)!.add(connectionId);

      // Load user preferences
      await this.loadUserPreferences(connection);

      this.stats.authenticatedConnections++;

      this.logger.info('User authenticated successfully', {
        connectionId,
        userId: authResult.userId,
      });

      // Send authentication success
      this.sendMessage(connectionId, {
        type: 'auth_success',
        timestamp: Date.now(),
        data: {
          userId: authResult.userId,
          preferences: connection.preferences,
        },
      });

      // Subscribe to user's saved subscriptions if enabled
      if (this.config.enablePersistedSubscriptions) {
        await this.subscribeToUserSavedData(connectionId);
      }

    } catch (error) {
      this.stats.authenticationFailures++;
      this.logger.warn('Authentication failed', {
        connectionId,
        error: error as Error,
      });

      this.sendMessage(connectionId, {
        type: 'auth_failure',
        timestamp: Date.now(),
        error: (error as Error).message,
      });

      // Close connection after authentication failure
      setTimeout(() => {
        this.closeConnection(connectionId, 'Authentication failed');
      }, 5000);
    }
  }

  /**
   * Handle subscription request
   */
  private async handleSubscription(connectionId: string, subscriptionData: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.authenticated) {
      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Authentication required',
      });
      return;
    }

    if (connection.subscriptions.size >= this.config.maxSubscriptionsPerConnection) {
      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Maximum subscriptions exceeded',
      });
      return;
    }

    const { type, filters } = subscriptionData;

    // Create subscription ID
    const subscriptionId = `sub_${connectionId}_${type}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    try {
      // Validate subscription
      await this.validateSubscription(type, filters);

      // Add to connection subscriptions
      connection.subscriptions.add(subscriptionId);

      // In a real implementation, this would set up actual data streaming
      // For now, we'll just acknowledge the subscription

      this.stats.totalSubscriptions++;
      this.stats.subscriptionsByType[type] = (this.stats.subscriptionsByType[type] || 0) + 1;

      this.logger.info('User subscription created', {
        connectionId,
        userId: connection.userId,
        subscriptionId,
        type,
        filters,
      });

      // Send subscription success
      this.sendMessage(connectionId, {
        type: 'subscription_success',
        timestamp: Date.now(),
        data: {
          subscriptionId,
          type,
          filters,
        },
      });

    } catch (error) {
      this.logger.error('Failed to create subscription', {
        connectionId,
        type,
        filters,
        error: error as Error,
      });

      this.sendMessage(connectionId, {
        type: 'subscription_failure',
        timestamp: Date.now(),
        error: (error as Error).message,
      });
    }
  }

  /**
   * Handle unsubscription request
   */
  private async handleUnsubscription(connectionId: string, unsubscriptionData: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    const { subscriptionId } = unsubscriptionData;

    if (connection.subscriptions.has(subscriptionId)) {
      connection.subscriptions.delete(subscriptionId);
      this.stats.totalSubscriptions--;

      this.logger.info('User subscription removed', {
        connectionId,
        subscriptionId,
      });

      this.sendMessage(connectionId, {
        type: 'unsubscription_success',
        timestamp: Date.now(),
        data: {
          subscriptionId,
        },
      });
    } else {
      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Subscription not found',
      });
    }
  }

  /**
   * Handle preferences update
   */
  private async handlePreferencesUpdate(connectionId: string, preferencesData: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.authenticated) {
      return;
    }

    try {
      // Update preferences
      connection.preferences = {
        ...connection.preferences,
        ...preferencesData,
      };

      // Save preferences to database
      await this.saveUserPreferences(connection);

      this.logger.info('User preferences updated', {
        connectionId,
        userId: connection.userId,
        preferences: connection.preferences,
      });

      this.sendMessage(connectionId, {
        type: 'preferences_updated',
        timestamp: Date.now(),
        data: {
          preferences: connection.preferences,
        },
      });

    } catch (error) {
      this.logger.error('Failed to update user preferences', {
        connectionId,
        preferencesData,
        error: error as Error,
      });

      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Failed to update preferences',
      });
    }
  }

  /**
   * Handle ping message
   */
  private handlePing(connectionId: string): void {
    this.sendMessage(connectionId, {
      type: 'pong',
      timestamp: Date.now(),
    });
  }

  /**
   * Send message to connection
   */
  private sendMessage(connectionId: string, message: WebSocketMessage): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      // In a real implementation, this would send the message via WebSocket
      // For now, we'll just log it
      this.logger.debug('Message sent to user', {
        connectionId,
        messageType: message.type,
      });

      this.stats.messagesSent++;

    } catch (error) {
      this.logger.error('Failed to send message', {
        connectionId,
        message,
        error: error as Error,
      });
      this.stats.errorsCount++;
    }
  }

  /**
   * Close connection
   */
  private closeConnection(connectionId: string, reason?: string): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      // Remove from user connections map
      if (connection.userId) {
        const userConns = this.userConnections.get(connection.userId);
        if (userConns) {
          userConns.delete(connectionId);
          if (userConns.size === 0) {
            this.userConnections.delete(connection.userId);
          }
        }
      }

      // Close WebSocket
      if (connection.socket && connection.socket.close) {
        connection.socket.close(1000, reason || 'Connection closed');
      }

      // Update statistics
      const connectionDuration = Date.now() - connection.connectedAt;
      this.stats.activeConnections--;
      if (connection.authenticated) {
        this.stats.authenticatedConnections--;
      }

      // Remove connection
      this.connections.delete(connectionId);

      this.logger.info('User connection closed', {
        connectionId,
        userId: connection.userId,
        reason,
        duration: connectionDuration,
      });

    } catch (error) {
      this.logger.error('Error closing connection', {
        connectionId,
        error: error as Error,
      });
    }
  }

  /**
   * Check if connection is rate limited
   */
  private isRateLimited(connection: UserConnection): boolean {
    const now = Date.now();
    const windowStart = now - this.config.rateLimitWindowSizeMs;

    // Remove old requests from the window
    connection.rateLimitRequests = connection.rateLimitRequests.filter(
      timestamp => timestamp > windowStart
    );

    // Check if limit exceeded
    return connection.rateLimitRequests.length >= this.config.rateLimitRequestsPerMinute;
  }

  /**
   * Validate subscription
   */
  private async validateSubscription(type: string, filters: any): Promise<void> {
    const validTypes = ['market_data', 'price_alerts', 'trade_notifications', 'portfolio_updates'];
    if (!validTypes.includes(type)) {
      throw new Error(`Invalid subscription type: ${type}`);
    }

    // Additional validation based on subscription type
    switch (type) {
      case 'market_data':
        if (filters && filters.marketIds && Array.isArray(filters.marketIds)) {
          if (filters.marketIds.length > 100) {
            throw new Error('Too many market IDs in subscription');
          }
        }
        break;

      case 'price_alerts':
        if (filters && filters.threshold) {
          if (typeof filters.threshold !== 'number' || filters.threshold < 0 || filters.threshold > 1) {
            throw new Error('Invalid price threshold');
          }
        }
        break;
    }
  }

  /**
   * Load user preferences from database
   */
  private async loadUserPreferences(connection: UserConnection): Promise<void> {
    if (!connection.userId) {
      return;
    }

    try {
      // In a real implementation, this would load from database
      // For now, use default preferences
      connection.preferences = {
        categories: ['sports', 'politics'],
        eventIds: [],
        marketIds: [],
        minLiquidity: 1000,
        enablePriceAlerts: true,
        enableTradeNotifications: true,
        language: 'en',
        timezone: 'UTC',
      };

    } catch (error) {
      this.logger.error('Failed to load user preferences', {
        userId: connection.userId,
        error: error as Error,
      });
    }
  }

  /**
   * Save user preferences to database
   */
  private async saveUserPreferences(connection: UserConnection): Promise<void> {
    if (!connection.userId) {
      return;
    }

    try {
      // In a real implementation, this would save to database
      this.logger.debug('User preferences saved', {
        userId: connection.userId,
        preferences: connection.preferences,
      });

    } catch (error) {
      this.logger.error('Failed to save user preferences', {
        userId: connection.userId,
        preferences: connection.preferences,
        error: error as Error,
      });
    }
  }

  /**
   * Subscribe to user's saved data
   */
  private async subscribeToUserSavedData(connectionId: string): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.authenticated) {
      return;
    }

    try {
      // In a real implementation, this would:
      // 1. Load user's saved subscriptions from database
      // 2. Create subscriptions for each saved item
      // 3. Start streaming personalized data

      this.logger.debug('User subscribed to saved data', {
        connectionId,
        userId: connection.userId,
      });

    } catch (error) {
      this.logger.error('Failed to subscribe to user saved data', {
        connectionId,
        error: error as Error,
      });
    }
  }

  /**
   * Start health monitoring
   */
  private startHealthMonitoring(): void {
    this.healthCheckTimer = setInterval(async () => {
      if (this.isRunning) {
        await this.performHealthCheck();
      }
    }, this.config.healthCheckIntervalMs);
  }

  /**
   * Perform health check
   */
  private async performHealthCheck(): Promise<void> {
    try {
      const now = Date.now();
      const connectionTimeout = this.config.connectionTimeoutMs;

      // Check for idle connections
      const connectionsToClose: string[] = [];

      for (const [connectionId, connection] of this.connections) {
        if (now - connection.lastActivityAt > connectionTimeout) {
          connectionsToClose.push(connectionId);
        }
      }

      // Close idle connections
      for (const connectionId of connectionsToClose) {
        this.closeConnection(connectionId, 'Connection timeout');
      }

      // Update top users
      this.updateTopUsers();

      this.logger.debug('Health check completed', {
        totalConnections: this.connections.size,
        authenticatedConnections: this.stats.authenticatedConnections,
        idleConnectionsClosed: connectionsToClose.length,
      });

    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
      this.stats.errorsCount++;
    }
  }

  /**
   * Update top users statistics
   */
  private updateTopUsers(): void {
    const userStats = new Map<string, { connections: number; subscriptions: number }>();

    for (const connection of this.connections.values()) {
      if (connection.authenticated && connection.userId) {
        const stats = userStats.get(connection.userId) || { connections: 0, subscriptions: 0 };
        stats.connections++;
        stats.subscriptions += connection.subscriptions.size;
        userStats.set(connection.userId, stats);
      }
    }

    this.stats.topUserIds = Array.from(userStats.entries())
      .sort((a, b) => b[1].connections - a[1].connections)
      .slice(0, 10)
      .map(([userId, stats]) => ({
        userId,
        ...stats,
      }));
  }

  /**
   * Get current statistics
   */
  getStats(): UserChannelStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
      activeConnections: this.connections.size,
    };
  }

  /**
   * Get connection information
   */
  getConnection(connectionId: string): UserConnection | undefined {
    return this.connections.get(connectionId);
  }

  /**
   * Get user connections
   */
  getUserConnections(userId: string): UserConnection[] {
    const connectionIds = this.userConnections.get(userId);
    if (!connectionIds) {
      return [];
    }

    return Array.from(connectionIds)
      .map(id => this.connections.get(id))
      .filter((conn): conn is UserConnection => conn !== undefined);
  }

  /**
   * Check if handler is healthy
   */
  isHealthy(): boolean {
    const connectionLoad = this.connections.size / this.config.maxConnections;
    const acceptableLoad = connectionLoad < 0.9;

    return this.isRunning && acceptableLoad;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down user channel handler`);
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
}

/**
 * Create and initialize user channel WebSocket handler
 */
export async function createUserChannelHandler(
  authService: PolymarketAuthService,
  db: typeof database,
  config?: Partial<UserChannelConfig>
): Promise<UserChannelWebSocketHandler> {
  const handler = new UserChannelWebSocketHandler(authService, db, config);

  // Send ready signal if running under PM2
  if (process.send) {
    process.send('ready');
  }

  return handler;
}

/**
 * Standalone worker entry point
 */
export async function runUserChannelHandler(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('user-channel-main', {
      category: LogCategory.WEBSOCKET,
    });

    logger.info('Initializing user channel WebSocket handler standalone');

    // Initialize dependencies
    const authService = new PolymarketAuthService();
    await authService.initialize();

    // Create and start handler
    const handler = await createUserChannelHandler(authService, database);
    await handler.start();

    logger.info('User channel WebSocket handler running standalone');

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
    console.error('Failed to start user channel handler:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runUserChannelHandler().catch((error) => {
    console.error('User channel handler failed:', error);
    process.exit(1);
  });
}

export default UserChannelWebSocketHandler;