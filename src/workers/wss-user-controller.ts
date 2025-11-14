/**
 * WebSocket User Controller
 * Manages WebSocket connection lifecycle and routing
 * Coordinates between market channel and user channel handlers
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { UserChannelWebSocketHandler, createUserChannelHandler } from './wss-user-channel';
import { MarketChannelWebSocketHandler, createMarketChannelHandler } from './wss-market-channel';
import { PolymarketAuthService } from '../services/polymarket-auth';
import { database } from '../lib/db';
import { TicksQueue } from '../queues';

/**
 * WebSocket controller configuration
 */
interface WebSocketControllerConfig {
  serverPort: number;
  maxConnections: number;
  enableCompression: boolean;
  enablePerMessageDeflate: boolean;
  handshakeTimeoutMs: number;
  pingIntervalMs: number;
  pongTimeoutMs: number;
  enableSSL: boolean;
  sslCertPath?: string;
  sslKeyPath?: string;
  corsOrigins: string[];
  rateLimitEnabled: boolean;
  rateLimitRequestsPerMinute: number;
  enableMetrics: boolean;
  metricsPort: number;
  healthCheckIntervalMs: number;
}

/**
 * Connection routing information
 */
interface ConnectionRoute {
  connectionId: string;
  userId?: string;
  type: 'market' | 'user' | 'system';
  handler: 'market-channel' | 'user-channel';
  metadata: {
    userAgent?: string;
    ipAddress?: string;
    origin?: string;
    path?: string;
  };
  connectedAt: number;
  lastActivityAt: number;
  messagesReceived: number;
  messagesSent: number;
  bytesReceived: number;
  bytesSent: number;
}

/**
 * WebSocket statistics
 */
interface WebSocketStats {
  totalConnections: number;
  activeConnections: number;
  authenticatedConnections: number;
  connectionsByType: Record<string, number>;
  connectionsByHandler: Record<string, number>;
  totalMessagesReceived: number;
  totalMessagesSent: number;
  totalBytesReceived: number;
  totalBytesSent: number;
  errorsCount: number;
  authenticationFailures: number;
  rejectedConnections: number;
  uptime: number;
  lastConnectionAt?: string;
  averageConnectionDuration: number;
  topOrigins: Array<{
    origin: string;
    connections: number;
  }>;
  messageTypes: Record<string, number>;
}

/**
 * WebSocket user controller class
 */
export class WebSocketUserController {
  private logger: Logger;
  private config: WebSocketControllerConfig;
  private userChannelHandler: UserChannelWebSocketHandler;
  private marketChannelHandler: MarketChannelWebSocketHandler;
  private isRunning: boolean = false;
  private startTime: number;
  private stats: WebSocketStats;
  private connections: Map<string, ConnectionRoute> = new Map();
  private healthCheckTimer: NodeJS.Timeout | null = null;

  constructor(
    userChannelHandler: UserChannelWebSocketHandler,
    marketChannelHandler: MarketChannelWebSocketHandler,
    config?: Partial<WebSocketControllerConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('wss-user-controller', process.pid.toString());
    this.userChannelHandler = userChannelHandler;
    this.marketChannelHandler = marketChannelHandler;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      serverPort: 8080,
      maxConnections: 10000,
      enableCompression: true,
      enablePerMessageDeflate: true,
      handshakeTimeoutMs: 10000,
      pingIntervalMs: 30000,
      pongTimeoutMs: 5000,
      enableSSL: false,
      sslCertPath: undefined,
      sslKeyPath: undefined,
      corsOrigins: ['https://signalcast.io', 'https://www.signalcast.io', 'http://localhost:3000'],
      rateLimitEnabled: true,
      rateLimitRequestsPerMinute: 60,
      enableMetrics: true,
      metricsPort: 9090,
      healthCheckIntervalMs: 30000,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalConnections: 0,
      activeConnections: 0,
      authenticatedConnections: 0,
      connectionsByType: {},
      connectionsByHandler: {},
      totalMessagesReceived: 0,
      totalMessagesSent: 0,
      totalBytesReceived: 0,
      totalBytesSent: 0,
      errorsCount: 0,
      authenticationFailures: 0,
      rejectedConnections: 0,
      uptime: 0,
      averageConnectionDuration: 0,
      topOrigins: [],
      messageTypes: {},
    };

    this.setupGracefulShutdown();
    this.logger.info('WebSocket user controller initialized', {
      config: this.config,
    });
  }

  /**
   * Start the WebSocket controller
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('WebSocket user controller is already running');
      return;
    }

    this.logger.info('Starting WebSocket user controller');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Start handlers
      await this.userChannelHandler.start();
      await this.marketChannelHandler.start();

      // Start health monitoring
      this.startHealthMonitoring();

      // In a real implementation, this would start the WebSocket server
      // For now, we'll just log what would happen
      await this.startWebSocketServer();

      // Send ready signal if running under PM2
      if (process.send) {
        process.send('ready');
      }

      this.logger.info('WebSocket user controller started successfully', {
        port: this.config.serverPort,
        sslEnabled: this.config.enableSSL,
      });

    } catch (error) {
      this.logger.error('Failed to start WebSocket user controller', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the WebSocket controller
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('WebSocket user controller is not running');
      return;
    }

    this.logger.info('Stopping WebSocket user controller');
    this.isRunning = false;

    try {
      // Stop health monitoring
      if (this.healthCheckTimer) {
        clearInterval(this.healthCheckTimer);
        this.healthCheckTimer = null;
      }

      // Stop handlers
      await this.userChannelHandler.stop();
      await this.marketChannelHandler.stop();

      // Close all connections
      for (const connectionId of this.connections.keys()) {
        await this.closeConnection(connectionId, 'Server shutdown');
      }

      this.connections.clear();

      this.logger.info('WebSocket user controller stopped', {
        finalStats: this.getStats(),
      });

    } catch (error) {
      this.logger.error('Error during shutdown', {
        error: error as Error,
      });
    }
  }

  /**
   * Start WebSocket server
   */
  private async startWebSocketServer(): Promise<void> {
    this.logger.info('WebSocket server would be started here', {
      port: this.config.serverPort,
      sslEnabled: this.config.enableSSL,
    });

    // In a real implementation, this would:
    // 1. Create WebSocket server using ws or socket.io
    // 2. Set up SSL if enabled
    // 3. Configure CORS and compression
    // 4. Set up connection handling
    // 5. Start listening on the configured port

    // For now, we'll simulate the server startup
    this.logger.debug('WebSocket server configuration', {
      port: this.config.serverPort,
      maxConnections: this.config.maxConnections,
      corsOrigins: this.config.corsOrigins,
      compression: this.config.enableCompression,
      perMessageDeflate: this.config.enablePerMessageDeflate,
    });
  }

  /**
   * Handle new WebSocket connection
   */
  async handleConnection(socket: any, metadata: any): Promise<void> {
    const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    // Check connection limits
    if (this.connections.size >= this.config.maxConnections) {
      this.stats.rejectedConnections++;
      this.logger.warn('Connection limit reached', {
        connectionId,
        currentConnections: this.connections.size,
        maxConnections: this.config.maxConnections,
      });

      if (socket.close) {
        socket.close(1013, 'Server overloaded');
      }
      return;
    }

    // Validate origin
    if (!this.isOriginAllowed(metadata.origin)) {
      this.stats.rejectedConnections++;
      this.logger.warn('Origin not allowed', {
        connectionId,
        origin: metadata.origin,
      });

      if (socket.close) {
        socket.close(1008, 'Origin not allowed');
      }
      return;
    }

    // Determine connection type and route
    const connectionType = this.determineConnectionType(metadata.path);
    const handler = this.selectHandler(connectionType);

    // Create connection route
    const connectionRoute: ConnectionRoute = {
      connectionId,
      type: connectionType,
      handler,
      metadata: {
        userAgent: metadata.userAgent,
        ipAddress: metadata.ipAddress,
        origin: metadata.origin,
        path: metadata.path,
      },
      connectedAt: Date.now(),
      lastActivityAt: Date.now(),
      messagesReceived: 0,
      messagesSent: 0,
      bytesReceived: 0,
      bytesSent: 0,
    };

    this.connections.set(connectionId, connectionRoute);

    // Update statistics
    this.stats.totalConnections++;
    this.stats.activeConnections++;
    this.stats.connectionsByType[connectionType] = (this.stats.connectionsByType[connectionType] || 0) + 1;
    this.stats.connectionsByHandler[handler] = (this.stats.connectionsByHandler[handler] || 0) + 1;
    this.stats.lastConnectionAt = new Date().toISOString();

    // Update origin statistics
    this.updateOriginStats(metadata.origin);

    this.logger.info('New WebSocket connection routed', {
      connectionId,
      type: connectionType,
      handler,
      metadata: connectionRoute.metadata,
    });

    // Route to appropriate handler
    await this.routeConnection(connectionId, socket, metadata, handler);

    // Set up connection monitoring
    this.setupConnectionMonitoring(connectionId, socket);
  }

  /**
   * Determine connection type from request path
   */
  private determineConnectionType(path?: string): ConnectionRoute['type'] {
    if (!path) {
      return 'user'; // Default to user
    }

    if (path.includes('/market') || path.includes('/stream')) {
      return 'market';
    } else if (path.includes('/user')) {
      return 'user';
    } else if (path.includes('/system')) {
      return 'system';
    }

    return 'user'; // Default
  }

  /**
   * Select appropriate handler for connection
   */
  private selectHandler(connectionType: ConnectionRoute['type']): ConnectionRoute['handler'] {
    switch (connectionType) {
      case 'market':
        return 'market-channel';
      case 'user':
      case 'system':
      default:
        return 'user-channel';
    }
  }

  /**
   * Route connection to appropriate handler
   */
  private async routeConnection(
    connectionId: string,
    socket: any,
    metadata: any,
    handler: ConnectionRoute['handler']
  ): Promise<void> {
    try {
      switch (handler) {
        case 'market-channel':
          await this.userChannelHandler.handleConnection(socket, {
            ...metadata,
            connectionId,
            connectionType: 'market',
          });
          break;

        case 'user-channel':
          await this.userChannelHandler.handleConnection(socket, {
            ...metadata,
            connectionId,
            connectionType: 'user',
          });
          break;

        default:
          throw new Error(`Unknown handler: ${handler}`);
      }

      this.logger.debug('Connection routed successfully', {
        connectionId,
        handler,
      });

    } catch (error) {
      this.logger.error('Failed to route connection', {
        connectionId,
        handler,
        error: error as Error,
      });

      this.connections.delete(connectionId);
      if (socket.close) {
        socket.close(1011, 'Routing failed');
      }
    }
  }

  /**
   * Set up connection monitoring
   */
  private setupConnectionMonitoring(connectionId: string, socket: any): void {
    // Set up message handlers
    if (socket.on) {
      socket.on('message', async (data: any) => {
        await this.handleMessage(connectionId, data);
      });

      socket.on('close', async (code: number, reason: string) => {
        await this.handleDisconnection(connectionId, code, reason);
      });

      socket.on('error', (error: Error) => {
        this.handleConnectionError(connectionId, error);
      });

      socket.on('ping', () => {
        if (socket.pong) {
          socket.pong();
        }
      });

      socket.on('pong', () => {
        // Update last activity
        const connection = this.connections.get(connectionId);
        if (connection) {
          connection.lastActivityAt = Date.now();
        }
      });
    }

    // Start ping/pong
    this.startPingPong(connectionId, socket);
  }

  /**
   * Start ping/pong for connection
   */
  private startPingPong(connectionId: string, socket: any): void {
    const pingInterval = setInterval(() => {
      const connection = this.connections.get(connectionId);
      if (!connection) {
        clearInterval(pingInterval);
        return;
      }

      if (socket.ping) {
        socket.ping();

        // Set timeout for pong
        setTimeout(() => {
          const conn = this.connections.get(connectionId);
          if (conn && Date.now() - conn.lastActivityAt > this.config.pongTimeoutMs) {
            this.logger.warn('Connection timeout - no pong received', {
              connectionId,
            });
            this.closeConnection(connectionId, 'Ping timeout');
          }
        }, this.config.pongTimeoutMs);
      }
    }, this.config.pingIntervalMs);
  }

  /**
   * Handle WebSocket message
   */
  private async handleMessage(connectionId: string, data: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      // Update statistics
      connection.lastActivityAt = Date.now();
      connection.messagesReceived++;
      connection.bytesReceived += data.length || 0;

      this.stats.totalMessagesReceived++;
      this.stats.totalBytesReceived += data.length || 0;

      // Parse message
      let message;
      try {
        message = JSON.parse(data.toString());
      } catch (error) {
        throw new Error('Invalid message format');
      }

      // Update message type statistics
      this.stats.messageTypes[message.type] = (this.stats.messageTypes[message.type] || 0) + 1;

      // Route message to appropriate handler
      await this.routeMessage(connectionId, message);

    } catch (error) {
      this.stats.errorsCount++;
      this.logger.error('Error handling message', {
        connectionId,
        error: error as Error,
      });

      // Send error response
      this.sendMessage(connectionId, {
        type: 'error',
        timestamp: Date.now(),
        error: 'Invalid message format',
      });
    }
  }

  /**
   * Route message to appropriate handler
   */
  private async routeMessage(connectionId: string, message: any): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      switch (connection.handler) {
        case 'market-channel':
          // In a real implementation, this would route to market channel handler
          this.logger.debug('Message routed to market channel', {
            connectionId,
            messageType: message.type,
          });
          break;

        case 'user-channel':
          await this.userChannelHandler.handleMessage(connectionId, message);
          break;

        default:
          throw new Error(`Unknown handler: ${connection.handler}`);
      }

    } catch (error) {
      this.logger.error('Failed to route message', {
        connectionId,
        message,
        error: error as Error,
      });
    }
  }

  /**
   * Handle WebSocket disconnection
   */
  private async handleDisconnection(connectionId: string, code: number, reason: string): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    const connectionDuration = Date.now() - connection.connectedAt;

    this.logger.info('WebSocket connection closed', {
      connectionId,
      code,
      reason,
      duration: connectionDuration,
      messagesReceived: connection.messagesReceived,
      messagesSent: connection.messagesSent,
    });

    // Update statistics
    this.stats.activeConnections--;
    if (connection.userId) {
      this.stats.authenticatedConnections--;
    }

    // Update average connection duration
    const totalDuration = this.stats.averageConnectionDuration * (this.stats.totalConnections - 1) + connectionDuration;
    this.stats.averageConnectionDuration = totalDuration / this.stats.totalConnections;

    // Remove connection
    this.connections.delete(connectionId);

    // Notify handlers
    await this.notifyHandlersOfDisconnection(connectionId);
  }

  /**
   * Handle connection error
   */
  private async handleConnectionError(connectionId: string, error: Error): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    this.logger.error('WebSocket connection error', {
      connectionId,
      error: error.message,
      stack: error.stack,
    });

    this.stats.errorsCount++;

    // Close connection on error
    await this.closeConnection(connectionId, 'Connection error');
  }

  /**
   * Close connection
   */
  private async closeConnection(connectionId: string, reason?: string): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      // Remove from connections
      this.connections.delete(connectionId);

      // Update statistics
      this.stats.activeConnections--;
      if (connection.userId) {
        this.stats.authenticatedConnections--;
      }

      this.logger.info('Connection closed by controller', {
        connectionId,
        reason,
      });

    } catch (error) {
      this.logger.error('Error closing connection', {
        connectionId,
        error: error as Error,
      });
    }
  }

  /**
   * Send message to connection
   */
  private sendMessage(connectionId: string, message: any): void {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      const messageStr = JSON.stringify(message);
      const messageBytes = Buffer.byteLength(messageStr);

      connection.messagesSent++;
      connection.bytesSent += messageBytes;
      connection.lastActivityAt = Date.now();

      this.stats.totalMessagesSent++;
      this.stats.totalBytesSent += messageBytes;

      // In a real implementation, this would send via WebSocket
      this.logger.debug('Message sent to connection', {
        connectionId,
        messageType: message.type,
        bytes: messageBytes,
      });

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
   * Notify handlers of disconnection
   */
  private async notifyHandlersOfDisconnection(connectionId: string): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      return;
    }

    try {
      // In a real implementation, this would notify handlers
      this.logger.debug('Handlers notified of disconnection', {
        connectionId,
        handler: connection.handler,
      });

    } catch (error) {
      this.logger.error('Failed to notify handlers of disconnection', {
        connectionId,
        error: error as Error,
      });
    }
  }

  /**
   * Check if origin is allowed
   */
  private isOriginAllowed(origin?: string): boolean {
    if (!origin || this.config.corsOrigins.includes('*')) {
      return true;
    }

    return this.config.corsOrigins.includes(origin);
  }

  /**
   * Update origin statistics
   */
  private updateOriginStats(origin?: string): void {
    if (!origin) {
      return;
    }

    const existing = this.stats.topOrigins.find(item => item.origin === origin);
    if (existing) {
      existing.connections++;
    } else {
      this.stats.topOrigins.push({ origin, connections: 1 });
    }

    // Keep only top 10 origins
    this.stats.topOrigins = this.stats.topOrigins
      .sort((a, b) => b.connections - a.connections)
      .slice(0, 10);
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
      // Check handler health
      const userChannelHealthy = this.userChannelHandler.isHealthy();
      const marketChannelHealthy = this.marketChannelHandler.isHealthy();

      // Clean up stale connections
      await this.cleanupStaleConnections();

      this.logger.debug('Health check completed', {
        totalConnections: this.connections.size,
        authenticatedConnections: this.stats.authenticatedConnections,
        userChannelHealthy,
        marketChannelHealthy,
      });

    } catch (error) {
      this.logger.error('Health check failed', {
        error: error as Error,
      });
      this.stats.errorsCount++;
    }
  }

  /**
   * Clean up stale connections
   */
  private async cleanupStaleConnections(): Promise<void> {
    const now = Date.now();
    const staleThreshold = 5 * 60 * 1000; // 5 minutes

    for (const [connectionId, connection] of this.connections) {
      if (now - connection.lastActivityAt > staleThreshold) {
        this.logger.info('Cleaning up stale connection', {
          connectionId,
          lastActivity: connection.lastActivityAt,
          staleDuration: now - connection.lastActivityAt,
        });

        await this.closeConnection(connectionId, 'Connection stale');
      }
    }
  }

  /**
   * Get current statistics
   */
  getStats(): WebSocketStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
      activeConnections: this.connections.size,
    };
  }

  /**
   * Get connection information
   */
  getConnection(connectionId: string): ConnectionRoute | undefined {
    return this.connections.get(connectionId);
  }

  /**
   * Get all connections
   */
  getAllConnections(): ConnectionRoute[] {
    return Array.from(this.connections.values());
  }

  /**
   * Get connections by type
   */
  getConnectionsByType(type: ConnectionRoute['type']): ConnectionRoute[] {
    return Array.from(this.connections.values()).filter(conn => conn.type === type);
  }

  /**
   * Get connections by handler
   */
  getConnectionsByHandler(handler: ConnectionRoute['handler']): ConnectionRoute[] {
    return Array.from(this.connections.values()).filter(conn => conn.handler === handler);
  }

  /**
   * Check if controller is healthy
   */
  isHealthy(): boolean {
    const connectionLoad = this.connections.size / this.config.maxConnections;
    const acceptableLoad = connectionLoad < 0.9;
    const handlersHealthy = this.userChannelHandler.isHealthy() && this.marketChannelHandler.isHealthy();

    return this.isRunning && acceptableLoad && handlersHealthy;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down WebSocket controller`);
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
   * Update connection authentication status
   */
  updateConnectionAuth(connectionId: string, userId: string): void {
    const connection = this.connections.get(connectionId);
    if (connection) {
      connection.userId = userId;
      this.stats.authenticatedConnections++;
    }
  }
}

/**
 * Create and initialize WebSocket user controller
 */
export async function createWebSocketUserController(
  userChannelHandler: UserChannelWebSocketHandler,
  marketChannelHandler: MarketChannelWebSocketHandler,
  authService: PolymarketAuthService,
  db: typeof database,
  config?: Partial<WebSocketControllerConfig>
): Promise<WebSocketUserController> {
  const controller = new WebSocketUserController(
    userChannelHandler,
    marketChannelHandler,
    authService,
    db,
    config
  );

  // Send ready signal if running under PM2
  if (process.send) {
    process.send('ready');
  }

  return controller;
}

/**
 * Standalone worker entry point
 */
export async function runWebSocketUserController(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('wss-controller-main', {
      category: LogCategory.WEBSOCKET,
    });

    logger.info('Initializing WebSocket user controller standalone');

    // Initialize dependencies
    const authService = new PolymarketAuthService();
    await authService.initialize();

    const userChannelHandler = await createUserChannelHandler(authService, database);
    const marketChannelHandler = await createMarketChannelHandler(new TicksQueue());

    // Create and start controller
    const controller = await createWebSocketUserController(
      userChannelHandler,
      marketChannelHandler,
      authService,
      database
    );
    await controller.start();

    logger.info('WebSocket user controller running standalone');

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
    console.error('Failed to start WebSocket user controller:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runWebSocketUserController().catch((error) => {
    console.error('WebSocket user controller failed:', error);
    process.exit(1);
  });
}

export default WebSocketUserController;