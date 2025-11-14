/**
 * Events Poller Worker
 * Polls Polymarket API for event data every 5 seconds and queues normalized events
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { retryUtils } from '../lib/retry';
import { PolymarketAuthService } from '../services/polymarket-auth';
import { PolymarketApiClient, type PolymarketEvent } from '../services/polymarket-api';
import { EventsQueue } from '../queues';
import { normalizeEvent } from '../utils/normalizeEvent';
import { isPolitical, type PoliticalEvent } from '../utils/politicsFilter';
import { queueMessageGuards, type EventQueueMessage } from '../queues/types';
import { config, features } from '../config';

/**
 * Events poller configuration
 */
interface EventsPollerConfig {
  pollingIntervalMs: number;
  batchSize: number;
  maxRetries: number;
  retryDelayMs: number;
  timeoutMs: number;
  enablePoliticsFilter: boolean;
  normalizeCategories: boolean;
  maxConcurrentRequests: number;
}

/**
 * Events poller statistics
 */
interface EventsPollerStats {
  totalPolls: number;
  successfulPolls: number;
  failedPolls: number;
  eventsFetched: number;
  eventsQueued: number;
  errorsCount: number;
  lastPollAt?: string;
  lastSuccessAt?: string;
  averagePollTime: number;
  uptime: number;
}

/**
 * Events poller worker class
 */
export class EventsPollerWorker {
  private logger: Logger;
  private config: EventsPollerConfig;
  private apiClient: PolymarketApiClient;
  private eventsQueue: EventsQueue;
  private pollingInterval: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private startTime: number;
  private stats: EventsPollerStats;
  private lastCursor?: string;

  constructor(
    apiClient: PolymarketApiClient,
    eventsQueue: EventsQueue,
    config?: Partial<EventsPollerConfig>
  ) {
    this.logger = LoggerFactory.getWorkerLogger('events-poller', process.pid.toString());
    this.apiClient = apiClient;
    this.eventsQueue = eventsQueue;
    this.startTime = Date.now();

    // Default configuration
    this.config = {
      pollingIntervalMs: 5000, // 5 seconds
      batchSize: 100,
      maxRetries: 3,
      retryDelayMs: 1000,
      timeoutMs: 30000,
      enablePoliticsFilter: true,
      normalizeCategories: true,
      maxConcurrentRequests: 1,
      ...config,
    };

    // Initialize statistics
    this.stats = {
      totalPolls: 0,
      successfulPolls: 0,
      failedPolls: 0,
      eventsFetched: 0,
      eventsQueued: 0,
      errorsCount: 0,
      averagePollTime: 0,
      uptime: 0,
    };

    this.setupGracefulShutdown();
    this.logger.info('Events poller worker initialized', {
      config: this.config,
    });
  }

  /**
   * Start the events poller
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      this.logger.warn('Events poller is already running');
      return;
    }

    this.logger.info('Starting events poller worker');
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      // Perform initial poll
      await this.pollEvents();

      // Setup periodic polling
      this.pollingInterval = setInterval(async () => {
        if (this.isRunning) {
          await this.performPollWithRetry();
        }
      }, this.config.pollingIntervalMs);

      this.logger.info('Events poller worker started successfully', {
        intervalMs: this.config.pollingIntervalMs,
      });
    } catch (error) {
      this.logger.error('Failed to start events poller worker', {
        error: error as Error,
      });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop the events poller
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn('Events poller is not running');
      return;
    }

    this.logger.info('Stopping events poller worker');
    this.isRunning = false;

    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
    }

    // Wait for any ongoing poll to complete
    await new Promise(resolve => setTimeout(resolve, 1000));

    this.logger.info('Events poller worker stopped', {
      finalStats: this.getStats(),
    });
  }

  /**
   * Perform poll with retry logic
   */
  private async performPollWithRetry(): Promise<void> {
    return this.logger.timed('events-poll', LogCategory.WORKER, async () => {
      await retryUtils.withExponentialBackoff(
        async () => {
          await this.pollEvents();
        },
        this.config.maxRetries,
        this.config.retryDelayMs,
        10000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Poll events from Polymarket API
   */
  private async pollEvents(): Promise<void> {
    const pollStartTime = Date.now();
    this.stats.totalPolls++;

    try {
      this.logger.debug('Polling events from Polymarket API', {
        cursor: this.lastCursor,
        batchSize: this.config.batchSize,
      });

      // Fetch events with pagination
      const response = await this.apiClient.fetchEvents({
        active: true, // Only fetch active events
        limit: this.config.batchSize,
        cursor: this.lastCursor,
      });

      if (!response.success) {
        throw new Error(`API request failed: ${response.error}`);
      }

      const events = response.data;
      this.stats.eventsFetched += events.length;

      // Process and queue events
      if (events.length > 0) {
        await this.processEvents(events);

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

      this.logger.debug('Events poll completed successfully', {
        eventsCount: events.length,
        duration: pollDuration,
        hasMore: response.pagination?.hasNext,
      });

      this.stats.lastSuccessAt = new Date().toISOString();
      this.stats.successfulPolls++;

    } catch (error) {
      const pollDuration = Date.now() - pollStartTime;
      this.updateStats(pollDuration, false);

      this.logger.error('Events poll failed', {
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
   * Process fetched events and queue them
   */
  private async processEvents(events: PolymarketEvent[]): Promise<void> {
    if (events.length === 0) {
      return;
    }

    this.logger.debug('Processing events', {
      eventsCount: events.length,
    });

    // Apply political filtering BEFORE normalization and queue insertion
    const politicalEvents = events.filter(event => {
      const politicalEvent: PoliticalEvent = {
        slug: event.slug || '',
        title: event.title || '',
        description: event.description || ''
      };

      const isPol = isPolitical(politicalEvent);

      if (!isPol) {
        this.logger.debug(`Skipping non-political event: ${event.slug}`);
      }

      return isPol;
    });

    this.logger.debug('Political filtering completed', {
      totalEvents: events.length,
      politicalEvents: politicalEvents.length,
      filteredEvents: events.length - politicalEvents.length,
    });

    if (politicalEvents.length === 0) {
      return; // No political events to process
    }

    // Normalize only political events
    const normalizedEvents = politicalEvents.map(event => {
      const result = normalizeEvent(event, {
        strictValidation: false,
        enablePoliticsFilter: false, // Already filtered
        normalizeCategories: this.config.normalizeCategories,
        generateRelevanceScore: true,
      });

      return {
        ...result.event,
        polymarketId: event.id, // Ensure polymarket ID is preserved
      };
    });

    // Create queue messages from political events only
    const queueMessages: EventQueueMessage[] = normalizedEvents.map(event => ({
      id: `event_${event.polymarketId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date().toISOString(),
      polymarketId: event.polymarketId,
      slug: event.slug,
      title: event.title,
      description: event.description,
      category: event.category,
      subcategory: event.subcategory,
      liquidity: event.liquidity,
      volume: event.volume24h || event.volumeTotal,
      active: event.active,
      closed: event.closed,
      resolutionDate: event.endDate,
      tags: [event.category, event.subcategory].filter(Boolean),
      createdAt: event.createdAt,
      updatedAt: event.updatedAt,
      source: 'polymarket-api-poller',
      version: '1.0.0',
    }));

    // Validate messages
    const validMessages = queueMessages.filter(message => {
      return queueMessageGuards.isEventMessage(message);
    });

    if (validMessages.length !== queueMessages.length) {
      this.logger.warn('Some political events failed validation', {
        total: queueMessages.length,
        valid: validMessages.length,
      });
    }

    // Add only political events to queue
    if (validMessages.length > 0) {
      await this.eventsQueue.addMessages(validMessages);
      this.stats.eventsQueued += validMessages.length;

      this.logger.info('Political events queued successfully', {
        eventsCount: validMessages.length,
        categories: [...new Set(validMessages.map(m => m.category))],
        totalProcessed: events.length,
        filteredOut: events.length - politicalEvents.length,
      });
    }
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
  getStats(): EventsPollerStats {
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

    // Consider healthy if last successful poll was within last 2 minutes
    const recentSuccess = timeSinceLastSuccess < 120000;

    // Consider healthy if error rate is below 20%
    const errorRate = this.stats.totalPolls > 0
      ? this.stats.failedPolls / this.stats.totalPolls
      : 0;
    const acceptableErrorRate = errorRate < 0.2;

    return this.isRunning && recentSuccess && acceptableErrorRate;
  }

  /**
   * Setup graceful shutdown handlers
   */
  private setupGracefulShutdown(): void {
    const shutdown = async (signal: string) => {
      this.logger.info(`Received ${signal}, shutting down events poller`);
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
  getConfig(): EventsPollerConfig {
    return { ...this.config };
  }

  /**
   * Reset cursor to start from beginning
   */
  resetCursor(): void {
    this.lastCursor = undefined;
    this.logger.info('Events poller cursor reset');
  }

  /**
   * Update poller configuration
   */
  updateConfig(newConfig: Partial<EventsPollerConfig>): void {
    this.config = { ...this.config, ...newConfig };
    this.logger.info('Events poller configuration updated', {
      newConfig,
      fullConfig: this.config,
    });
  }
}

/**
 * Create and initialize events poller worker
 */
export async function createEventsPoller(
  apiClient: PolymarketApiClient,
  eventsQueue: EventsQueue,
  config?: Partial<EventsPollerConfig>
): Promise<EventsPollerWorker> {
  const worker = new EventsPollerWorker(apiClient, eventsQueue, config);

  // Send ready signal if running under PM2
  if (process.send) {
    process.send('ready');
  }

  return worker;
}

/**
 * Standalone worker entry point
 */
export async function runEventsPoller(): Promise<void> {
  try {
    const logger = LoggerFactory.getLogger('events-poller-main', {
      category: LogCategory.WORKER,
    });

    logger.info('Initializing events poller worker standalone');

    // Initialize dependencies
    const authService = new PolymarketAuthService();
    await authService.initialize();

    const apiClient = new PolymarketApiClient(authService);
    await apiClient.initialize();

    const eventsQueue = new EventsQueue();

    // Create and start worker
    const worker = await createEventsPoller(apiClient, eventsQueue);
    await worker.start();

    logger.info('Events poller worker running standalone');

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
    console.error('Failed to start events poller worker:', error);
    process.exit(1);
  }
}

// Run standalone if this file is executed directly
if (require.main === module) {
  runEventsPoller().catch((error) => {
    console.error('Events poller worker failed:', error);
    process.exit(1);
  });
}

export default EventsPollerWorker;