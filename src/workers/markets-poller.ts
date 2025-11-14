/**
 * Markets Poller Worker
 * Polls Polymarket API for market data every 2 seconds and queues normalized markets
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { retryUtils } from '../lib/retry';
import { PolymarketAuthService } from '../services/polymarket-auth';
import { PolymarketApiClient, type PolymarketMarket } from '../services/polymarket-api';
import { MarketsQueue } from '../queues';
import { normalizeMarket } from '../utils/normalizeMarket';
import { queueMessageGuards, type MarketQueueMessage } from '../queues/types';
import { config, features } from '../config';

/**
 * Markets poller configuration
 */
interface MarketsPollerConfig {
  pollingIntervalMs: number;
  batchSize: number;
  maxRetries: number;
  retryDelayMs: number;
  timeoutMs: number;
  maxConcurrentRequests: number;
  filterByRelevance: boolean;
  minRelevanceScore: number;
  includeInactive: boolean;
}

/**
 * Markets poller statistics
 */
interface MarketsPollerStats {
  totalPolls: number;
  successfulPolls: number;
  failedPolls: number;
  marketsFetched: number;
  marketsQueued: number;
  errorsCount: number;
  lastPollAt?: string;
  lastSuccessAt?: string;
  averagePollTime: number;
  uptime: number;
  categoriesProcessed: Record<string, number>;
  liquidityDistribution: {
    low: number; // < $1,000
    medium: number; // $1,000 - $10,000
    high: number; // > $10,000
  };
}

/**
 * Markets poller worker class
 */
export class MarketsPollerWorker {
  private logger: Logger;
  private config: MarketsPollerConfig;
  private apiClient: PolymarketApiClient;
  private marketsQueue: MarketsQueue;
  private pollingInterval: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private startTime: number;
  private stats: MarketsPollerStats;
  private lastCursor?: string;
  private activeEventIds: Set<string> = new Set();

  constructor(
    apiClient: PolymarketApiClient,
    marketsQueue: MarketsQueue,
    config?: Partial<MarketsPollerConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('markets-poller', process.pid.toString());
    this.apiClient = apiClient;
    this.marketsQueue = marketsQueue;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      pollingIntervalMs: 2000, // 2 seconds
      batchSize: 100,
      maxRetries: 3,
      retryDelayMs: 1000,
      timeoutMs: 30000,
      maxConcurrentRequests: 2,
      filterByRelevance: true,
      minRelevanceScore: 0.5,
      includeInactive: false,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalPolls: 0,
      successfulPolls: 0,
      failedPolls: 0,
      marketsFetched: 0,
      marketsQueued: 0,
      errorsCount: 0,
      averagePollTime: 0,
      uptime: 0,
      categoriesProcessed: {},
      liquidityDistribution: { low: 0, medium: 0, high: 0 },
    };

    this.setupGracefulShutdown();
    this.logger.info('Markets poller worker initialized', {
      config: this.config,
    });
  }

  /**
   * Start the markets poller
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Markets poller is already running');
      return;
    }

    this.logger.info('Starting markets poller worker');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Load active events for filtering
      await this.loadActiveEvents();

      // Perform initial poll
      await this.pollMarkets();

      // Setup periodic polling
      this.pollingInterval = setInterval(async () => {
        if (this.isRunning) {
          await this.performPollWithRetry();
        }
      }, this.config.pollingIntervalMs);

      // Refresh active events periodically
      setInterval(async () => {
        if (this.isRunning) {
          await this.loadActiveEvents();
        }
      }, 60000); // Every minute

      this.logger.info('Markets poller worker started successfully', {
        intervalMs: this.config.pollingIntervalMs,
        activeEventsCount: this.activeEventIds.size,
      });
    } catch (error) {
      this.logger.error('Failed to start markets poller worker', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the markets poller
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Markets poller is not running');
      return;
    }

    this.logger.info('Stopping markets poller worker');
    this.isRunning = false;

    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
    }

    // Wait for any ongoing poll to complete
    await new Promise(resolve => setTimeout(resolve, 1000));

    this.logger.info('Markets poller worker stopped', {
      finalStats: this.getStats(),
    });
  }

  /**
   * Perform poll with retry logic
   */
  private async performPollWithRetry(): Promise<void> {
    return this.logger.timed('markets-poll', LogCategory.WORKER, async () => {
      await retryUtils.withExponentialBackoff(
        async () => {
          await this.pollMarkets();
        },
        this.config.maxRetries,
        this.config.retryDelayMs,
        10000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Load active events for filtering
   */
  private async loadActiveEvents(): Promise<void> {
    try {
      const response = await this.apiClient.fetchEvents({
        active: true,
        limit: 1000, // Get all active events
      });

      if (response.success) {
        this.activeEventIds = new Set(response.data.map(event => event.id));
        this.logger.debug('Active events loaded', {
          count: this.activeEventIds.size,
        });
      }
    } catch (error) {
      this.logger.warn('Failed to load active events', {
        error: error as Error,
      });
    }
  }

  /**
   * Poll markets from Polymarket API
   */
  private async pollMarkets(): Promise<void> {
    const pollStartTime = Date.now();
    this.stats.totalPolls++;

    try {
      this.logger.debug('Polling markets from Polymarket API', {
        cursor: this.lastCursor,
        batchSize: this.config.batchSize,
        activeEventsCount: this.activeEventIds.size,
      });

      // Fetch markets with pagination
      const response = await this.apiClient.fetchMarkets({
        active: !this.config.includeInactive,
        limit: this.config.batchSize,
        cursor: this.lastCursor,
      });

      if (!response.success) {
        throw new Error(`API request failed: ${response.error}`);
      }

      let markets = response.data;
      this.stats.marketsFetched += markets.length;

      // Filter by active events if we have them
      if (this.activeEventIds.size > 0) {
        markets = markets.filter(market => this.activeEventIds.has(market.eventId));
      }

      // Filter by relevance score if enabled
      if (this.config.filterByRelevance) {
        markets = markets.filter(market =>
          (market.relevanceScore || 0) >= this.config.minRelevanceScore
        );
      }

      // Process and queue markets
      if (markets.length > 0) {
        await this.processMarkets(markets);

        // Update cursor for next iteration
        if (response.pagination?.cursor) {
          this.lastCursor = response.pagination.cursor;
        } else if (response.pagination?.hasNext === false) {
          // Reset cursor if we've reached the end
          this.lastCursor = undefined;
        }
      }

      // Update statistics
      const pollDuration = Date.now() - pollStartTime;
      this.updateStats(pollDuration, true);

      this.logger.debug('Markets poll completed successfully', {
        marketsCount: markets.length,
        originalCount: response.data.length,
        duration: pollDuration,
        hasMore: response.pagination?.hasNext,
      });

      this.stats.lastSuccessAt = new Date().toISOString();
      this.stats.successfulPolls++;

    } catch (error) {
      const pollDuration = Date.now() - pollStartTime;
      this.updateStats(pollDuration, false);

      this.logger.error('Markets poll failed', {
        error: error as Error,
        duration: pollDuration,
        attempt: this.stats.totalPolls,
      });

      this.stats.errorsCount++;
      this.stats.failedPolls++;
      throw error;
    }
  }

  /**
   * Process fetched markets and queue them
   */
  private async processMarkets(markets: PolymarketMarket[]): Promise<void> {
    if (markets.length === 0) {
      return;
    }

    this.logger.debug('Processing markets', {
      marketsCount: markets.length,
    });

    // Normalize markets
    const normalizedMarkets = markets.map(market => {
      const result = normalizeMarket(market, {
        strictValidation: false,
        generateRelevanceScore: true,
        calculateDerivedFields: true,
      });

      return {
        ...result.market,
        polymarketId: market.id, // Ensure polymarket ID is preserved
      };
    });

    // Create queue messages
    const queueMessages: MarketQueueMessage[] = normalizedMarkets.map(market => ({
      id: `market_${market.polymarketId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date().toISOString(),
      polymarketId: market.polymarketId,
      eventId: market.eventId,
      question: market.question,
      description: market.description,
      liquidity: market.liquidity,
      currentPrice: market.currentPrice || market.price,
      bestBid: market.bestBid,
      bestAsk: market.bestAsk,
      spread: market.bestBid && market.bestAsk ? market.bestAsk - market.bestBid : undefined,
      volume24h: market.volume24h,
      active: market.active,
      closed: market.closed,
      resolved: !!market.resolvedAt,
      outcomePrices: market.outcomePrices || {},
      startDate: market.startDate,
      endDate: market.endDate,
      createdAt: market.createdAt,
      updatedAt: market.updatedAt,
      source: 'polymarket-api-poller',
      version: '1.0.0',
    }));

    // Validate messages
    const validMessages = queueMessages.filter(message => {
      return queueMessageGuards.isMarketMessage(message);
    });

    if (validMessages.length !== queueMessages.length) {
      this.logger.warn('Some markets failed validation', {
        total: queueMessages.length,
        valid: validMessages.length,
      });
    }

    // Add to queue
    if (validMessages.length > 0) {
      await this.marketsQueue.addMessages(validMessages);
      this.stats.marketsQueued += validMessages.length;

      // Update category statistics
      this.updateCategoryStats(validMessages);

      // Update liquidity distribution
      this.updateLiquidityStats(validMessages);

      this.logger.info('Markets queued successfully', {
        marketsCount: validMessages.length,
        avgLiquidity: validMessages.reduce((sum, m) => sum + m.liquidity, 0) / validMessages.length,
        activeCount: validMessages.filter(m => m.active).length,
      });
    }
  }

  /**
   * Update category statistics
   */
  private updateCategoryStats(messages: MarketQueueMessage[]): void {
    // This would typically extract category from market data
    // For now, we'll use a placeholder
    const categories = ['sports', 'politics', 'business', 'technology', 'entertainment', 'other'];

    messages.forEach(message => {
      const category = categories[Math.floor(Math.random() * categories.length)];
      this.stats.categoriesProcessed[category] = (this.stats.categoriesProcessed[category] || 0) + 1;
    });
  }

  /**
   * Update liquidity distribution statistics
   */
  private updateLiquidityStats(messages: MarketQueueMessage[]): void {
    messages.forEach(message => {
      if (message.liquidity < 1000) {
        this.stats.liquidityDistribution.low++;
      } else if (message.liquidity < 10000) {
        this.stats.liquidityDistribution.medium++;
      } else {
        this.stats.liquidityDistribution.high++;
      }
    });
  }

  /**
   * Update poller statistics
   */
  private updateStats(duration: number, success: boolean): void {
    const totalDuration = this.stats.averagePollTime * (this.stats.totalPolls - 1) + duration;
    this.stats.averagePollTime = totalDuration / this.stats.totalPolls;
    this.stats.uptime = Date.now() - this.startTime;
    this.stats.lastPollAt = new Date().toISOString();
  }

  /**
   * Get current poller statistics
   */
  getStats(): MarketsPollerStats {
    return {
      ...this.stats,
      uptime: Date.now() - this.startTime,
    };
  }

  /**
   * Check if poller is healthy
   */
  isHealthy(): boolean {
    const now = Date.now();
    const timeSinceLastSuccess = this.stats.lastSuccessAt
      ? now - new Date(this.stats.lastSuccessAt).getTime()
      : Infinity;

    // Consider healthy if last successful poll was within last 1 minute
    const recentSuccess = timeSinceLastSuccess < 60000;

    // Consider healthy if error rate is below 15%
    const errorRate = this.stats.totalPolls > 0
      ? this.stats.failedPolls / this.stats.totalPolls
      : 0;
    const acceptableErrorRate = errorRate < 0.15;

    return this.isRunning && recentSuccess && acceptableErrorRate;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down markets poller`);
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
   * Get poller configuration
   */
  getConfig(): MarketsPollerConfig {
    return { ...this.config };
  }

  /**
   * Reset cursor to start from beginning
   */
  resetCursor(): void {
    this.lastCursor = undefined;
    this.logger.info('Markets poller cursor reset');
  }

  /**
   * Update poller configuration
   */
  updateConfig(newConfig: Partial<MarketsPollerConfig>): void {
    this.config = { ...this.config, ...newConfig };
    this.logger.info('Markets poller configuration updated', {
      newConfig,
      fullConfig: this.config,
    });
  }

  /**
   * Force refresh of active events
   */
  async refreshActiveEvents(): Promise<void> {
    this.logger.info('Forcing refresh of active events');
    await this.loadActiveEvents();
  }
}

/**
 * Create and initialize markets poller worker
 */
export async function createMarketsPoller(
  apiClient: PolymarketApiClient,
  marketsQueue: MarketsQueue,
  config?: Partial<MarketsPollerConfig>
): Promise<MarketsPollerWorker> {
  const worker = new MarketsPollerWorker(apiClient, marketsQueue, config);

  // Send ready signal if running under PM2
  if (process.send) {
    process.send('ready');
  }

  return worker;
}

/**
 * Standalone worker entry point
 */
export async function runMarketsPoller(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('markets-poller-main', {
      category: LogCategory.WORKER,
    });

    logger.info('Initializing markets poller worker standalone');

    // Initialize dependencies
    const authService = new PolymarketAuthService();
    await authService.initialize();

    const apiClient = new PolymarketApiClient(authService);
    await apiClient.initialize();

    const marketsQueue = new MarketsQueue();

    // Create and start worker
    const worker = await createMarketsPoller(apiClient, marketsQueue);
    await worker.start();

    logger.info('Markets poller worker running standalone');

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
    console.error('Failed to start markets poller worker:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runMarketsPoller().catch((error) => {
    console.error('Markets poller worker failed:', error);
    process.exit(1);
  });
}

export default MarketsPollerWorker;