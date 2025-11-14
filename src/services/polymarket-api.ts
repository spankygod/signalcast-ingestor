/**
 * Polymarket API Client Service
 * Handles REST API calls for fetching events, markets, and outcomes data
 */

import { Logger, LoggerFactory, LogCategory } from '../lib/logger';
import { retryUtils } from '../lib/retry';
import { PolymarketAuthService } from './polymarket-auth';
import { EventsQueue, MarketsQueue, OutcomesQueue } from '../queues';
import {
  POLYMARKET_CONFIG,
  API_ENDPOINTS,
  REQUEST_CONFIG,
  PAGINATION,
  ERROR_HANDLING,
} from '../config/polymarket';
import type {
  EventQueueMessage,
  MarketQueueMessage,
  OutcomeQueueMessage,
} from '../queues/types';

/**
 * API response interfaces
 */
export interface ApiResponse<T = any> {
  data: T;
  success: boolean;
  error?: string;
  statusCode: number;
  headers: Record<string, string>;
  pagination?: {
    page?: number;
    limit?: number;
    total?: number;
    hasNext?: boolean;
    hasPrev?: boolean;
    cursor?: string;
  };
}

/**
 * Polymarket event data structure
 */
export interface PolymarketEvent {
  id: string;
  slug: string;
  title: string;
  description?: string;
  category: string;
  subcategory?: string;
  liquidity: number;
  volume24h?: number;
  volumeTotal?: number;
  active: boolean;
  closed: boolean;
  archived?: boolean;
  restricted?: boolean;
  startDate?: string;
  endDate?: string;
  relevanceScore?: number;
  updatedAt: string;
}

/**
 * Polymarket market data structure
 */
export interface PolymarketMarket {
  id: string;
  eventId: string;
  question: string;
  slug: string;
  description?: string;
  liquidity: number;
  volume24h?: number;
  volumeTotal?: number;
  price: number;
  lastTradePrice?: number;
  bestBid?: number;
  bestAsk?: number;
  active: boolean;
  closed: boolean;
  archived?: boolean;
  restricted?: boolean;
  approved?: boolean;
  status?: string;
  resolvedAt?: string;
  relevanceScore?: number;
  startDate?: string;
  endDate?: string;
  outcomes?: PolymarketOutcome[];
  updatedAt: string;
}

/**
 * Polymarket outcome data structure
 */
export interface PolymarketOutcome {
  id: string;
  marketId: string;
  title: string;
  description?: string;
  price: number;
  probability: number;
  volume?: number;
  status: string;
  displayOrder?: number;
  updatedAt: string;
}

/**
 * API request options
 */
export interface ApiRequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
  params?: Record<string, any>;
  body?: any;
  headers?: Record<string, string>;
  timeout?: number;
  retries?: number;
  pagination?: {
    page?: number;
    limit?: number;
    cursor?: string;
  };
}

/**
 * Polling configuration
 */
export interface PollingConfig {
  enabled: boolean;
  intervalMs: number;
  batchSize?: number;
  filters?: Record<string, any>;
  lastPollTime?: number;
}

/**
 * Polymarket API client class
 */
export class PolymarketApiClient {
  private logger: Logger;
  private authService: PolymarketAuthService;
  private eventsQueue: EventsQueue;
  private marketsQueue: MarketsQueue;
  private outcomesQueue: OutcomesQueue;
  private baseUrl: string;
  private pollingConfigs: Map<string, PollingConfig>;
  private pollingIntervals: Map<string, NodeJS.Timeout>;

  constructor(authService: PolymarketAuthService) {
    this.logger = LoggerFactory.getLogger('polymarket-api', {
      category: LogCategory.API,
    });
    this.authService = authService;
    this.eventsQueue = new EventsQueue();
    this.marketsQueue = new MarketsQueue();
    this.outcomesQueue = new OutcomesQueue();
    this.baseUrl = POLYMARKET_CONFIG.API_BASE_URL;
    this.pollingConfigs = new Map();
    this.pollingIntervals = new Map();

    this.logger.info('Polymarket API client initialized', {
      baseUrl: this.baseUrl,
    });
  }

  /**
   * Initialize the API client
   */
  async initialize(): Promise<void> {
    this.logger.info('Initializing API client');

    try {
      // Test API connection
      await this.healthCheck();

      // Setup default polling configurations
      this.setupDefaultPolling();

      this.logger.info('API client initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize API client', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Make HTTP request to Polymarket API
   */
  async request<T = any>(
    endpoint: string,
    options: ApiRequestOptions = {}
  ): Promise<ApiResponse<T>> {
    return this.logger.timed(
      `api-request-${endpoint}`,
      LogCategory.API,
      this.logger.getLevel(),
      async () => {
      const {
        method = 'GET',
        params = {},
        body,
        headers = {},
        timeout = REQUEST_CONFIG.TIMEOUT_MS,
        retries = REQUEST_CONFIG.MAX_RETRIES,
        pagination,
      } = options;

      // Build URL
      let url = `${this.baseUrl}${endpoint}`;

      // Add query parameters
      const searchParams = new URLSearchParams();

      // Add pagination parameters
      if (pagination) {
        if (pagination.page !== undefined) {
          searchParams.append(PAGINATION.OFFSET.PAGE_PARAM, pagination.page.toString());
        }
        if (pagination.limit !== undefined) {
          searchParams.append(PAGINATION.OFFSET.LIMIT_PARAM, pagination.limit.toString());
        }
        if (pagination.cursor) {
          searchParams.append(PAGINATION.CURSOR.PARAM_NAME, pagination.cursor);
        }
      }

      // Add other parameters
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
          searchParams.append(key, value.toString());
        }
      });

      if (searchParams.toString()) {
        url += `?${searchParams.toString()}`;
      }

      // Prepare headers
      const requestHeaders: Record<string, string> = {
        ...this.authService.getAuthHeaders(),
        ...headers,
      };

      this.logger.debug(`Making ${method} request to ${url}`, {
        method,
        url,
        hasAuth: Boolean(requestHeaders['Authorization']),
        hasBody: !!body,
      });

      return await retryUtils.withExponentialBackoff(
        async () => {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), timeout);

          try {
            const fetchOptions: RequestInit = {
              method,
              headers: requestHeaders,
              signal: controller.signal,
            };

            if (body !== undefined && body !== null) {
              fetchOptions.body = typeof body === 'string' ? body : JSON.stringify(body);
            }

            const response = await fetch(url, fetchOptions);

            clearTimeout(timeoutId);

            // Parse response headers
            const responseHeaders: Record<string, string> = {};
            response.headers.forEach((value, key) => {
              responseHeaders[key] = value;
            });

            // Handle different response statuses
            if (!response.ok) {
              const errorText = await response.text();
              const error = new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);

              // Check if this is an authentication error
              if (this.authService.handleAuthError({ message: errorText, code: response.status.toString() })) {
                throw error;
              }

              // Determine if error is retryable
              const retryableStatuses: ReadonlyArray<number> = ERROR_HANDLING.RETRYABLE_STATUS_CODES;
              const isRetryable = retryableStatuses.includes(response.status);
              if (!isRetryable) {
                throw error;
              }

              throw error;
            }

            // Parse response body
            const data = await response.json();

            // Extract pagination info if available
            let paginationInfo;
            if (responseHeaders['x-pagination'] || responseHeaders['link']) {
              paginationInfo = this.parsePaginationHeaders(responseHeaders);
            }

            this.logger.debug(`${method} request to ${url} successful`, {
              statusCode: response.status,
              dataSize: Array.isArray(data) ? data.length : typeof data,
            });

            return {
              data,
              success: true,
              statusCode: response.status,
              headers: responseHeaders,
              pagination: paginationInfo,
            };
          } catch (error) {
            clearTimeout(timeoutId);

            const normalizedError = error instanceof Error ? error : new Error(String(error));

            if (normalizedError.name === 'AbortError') {
              throw new Error(`Request timeout after ${timeout}ms`);
            }

            throw normalizedError;
          }
        },
        retries,
        REQUEST_CONFIG.RETRY_DELAY_MS,
        30000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Fetch events from Polymarket API
   */
  async fetchEvents(options: {
    active?: boolean;
    category?: string;
    limit?: number;
    cursor?: string;
  } = {}): Promise<ApiResponse<PolymarketEvent[]>> {
    const params: Record<string, any> = {};

    if (options.active !== undefined) {
      params.active = options.active;
    }
    if (options.category) {
      params.category = options.category;
    }

    const response = await this.request<PolymarketEvent[]>(API_ENDPOINTS.EVENTS, {
      params,
      pagination: this.buildPagination(options.limit, options.cursor),
    });

    if (response.success) {
      // Normalize and queue events
      await this.processEvents(response.data);
    }

    return response;
  }

  private buildPagination(limit?: number, cursor?: string): { limit: number; cursor?: string } {
    const pagination: { limit: number; cursor?: string } = {
      limit: limit ?? PAGINATION.DEFAULT_PAGE_SIZE,
    };

    if (cursor !== undefined) {
      pagination.cursor = cursor;
    }

    return pagination;
  }

  /**
   * Fetch markets from Polymarket API
   */
  async fetchMarkets(options: {
    eventId?: string;
    active?: boolean;
    limit?: number;
    cursor?: string;
  } = {}): Promise<ApiResponse<PolymarketMarket[]>> {
    const params: Record<string, any> = {};

    if (options.eventId) {
      params.event_id = options.eventId;
    }
    if (options.active !== undefined) {
      params.active = options.active;
    }

    const response = await this.request<PolymarketMarket[]>(API_ENDPOINTS.MARKETS, {
      params,
      pagination: this.buildPagination(options.limit, options.cursor),
    });

    if (response.success) {
      // Normalize and queue markets
      await this.processMarkets(response.data);
    }

    return response;
  }

  /**
   * Fetch outcomes from Polymarket API
   */
  async fetchOutcomes(options: {
    marketId?: string;
    limit?: number;
    cursor?: string;
  } = {}): Promise<ApiResponse<PolymarketOutcome[]>> {
    let endpoint: string = API_ENDPOINTS.OUTCOMES;

    if (options.marketId) {
      endpoint = API_ENDPOINTS.OUTCOMES_BY_MARKET(options.marketId);
    }

    const response = await this.request<PolymarketOutcome[]>(endpoint, {
      pagination: this.buildPagination(options.limit, options.cursor),
    });

    if (response.success) {
      // Normalize and queue outcomes
      await this.processOutcomes(response.data);
    }

    return response;
  }

  /**
   * Fetch specific event by ID
   */
  async fetchEventById(eventId: string): Promise<ApiResponse<PolymarketEvent>> {
    const response = await this.request<PolymarketEvent>(API_ENDPOINTS.EVENT_BY_ID(eventId));

    if (response.success) {
      await this.processEvents([response.data]);
    }

    return response;
  }

  /**
   * Fetch specific market by ID
   */
  async fetchMarketById(marketId: string): Promise<ApiResponse<PolymarketMarket>> {
    const response = await this.request<PolymarketMarket>(API_ENDPOINTS.MARKET_BY_ID(marketId));

    if (response.success) {
      await this.processMarkets([response.data]);
    }

    return response;
  }

  /**
   * Health check for API
   */
  async healthCheck(): Promise<ApiResponse<{ status: string; timestamp: string }>> {
    return this.request(API_ENDPOINTS.HEALTH);
  }

  /**
   * Start polling for data updates
   */
  startPolling(): void {
    this.logger.info('Starting data polling');

    // Events polling (5 seconds)
    this.startPollingFor('events', 5000, async () => {
      await this.fetchEvents({ active: true });
    });

    // Markets polling (2 seconds)
    this.startPollingFor('markets', 2000, async () => {
      await this.fetchMarkets({ active: true });
    });

    // Outcomes polling (2 seconds)
    this.startPollingFor('outcomes', 2000, async () => {
      await this.fetchOutcomes();
    });
  }

  /**
   * Stop polling for data updates
   */
  stopPolling(): void {
    this.logger.info('Stopping data polling');

    this.pollingIntervals.forEach((interval, type) => {
      clearInterval(interval);
      this.logger.debug(`Stopped polling for ${type}`);
    });

    this.pollingIntervals.clear();
  }

  /**
   * Process events data and add to queue
   */
  private async processEvents(events: PolymarketEvent[]): Promise<void> {
    const messages: EventQueueMessage[] = events.map(event => this.normalizeEvent(event));

    try {
      await this.eventsQueue.addMessages(messages);
      this.logger.debug(`Processed ${events.length} events`, {
        eventIds: events.map(e => e.id),
      });
    } catch (error) {
      this.logger.error('Failed to queue events', {
        error: error as Error,
        eventCount: events.length,
      });
    }
  }

  /**
   * Process markets data and add to queue
   */
  private async processMarkets(markets: PolymarketMarket[]): Promise<void> {
    const messages: MarketQueueMessage[] = markets.map(market => this.normalizeMarket(market));

    try {
      await this.marketsQueue.addMessages(messages);
      this.logger.debug(`Processed ${markets.length} markets`, {
        marketIds: markets.map(m => m.id),
      });
    } catch (error) {
      this.logger.error('Failed to queue markets', {
        error: error as Error,
        marketCount: markets.length,
      });
    }
  }

  /**
   * Process outcomes data and add to queue
   */
  private async processOutcomes(outcomes: PolymarketOutcome[]): Promise<void> {
    const messages: OutcomeQueueMessage[] = outcomes.map(outcome => this.normalizeOutcome(outcome));

    try {
      await this.outcomesQueue.addMessages(messages);
      this.logger.debug(`Processed ${outcomes.length} outcomes`, {
        outcomeIds: outcomes.map(o => o.id),
      });
    } catch (error) {
      this.logger.error('Failed to queue outcomes', {
        error: error as Error,
        outcomeCount: outcomes.length,
      });
    }
  }

  /**
   * Normalize Polymarket event data to our queue format
   */
  private normalizeEvent(event: PolymarketEvent): EventQueueMessage {
    return {
      id: `event_${event.id}_${Date.now()}`,
      timestamp: new Date().toISOString(),
      polymarketId: event.id,
      slug: event.slug,
      title: event.title,
      description: event.description,
      category: event.category,
      subcategory: event.subcategory,
      liquidity: Number(event.liquidity) || 0,
      volume: Number(event.volume24h) || Number(event.volumeTotal) || 0,
      active: Boolean(event.active),
      closed: Boolean(event.closed),
      resolutionDate: event.endDate,
      tags: [event.category, event.subcategory].filter(Boolean),
      createdAt: new Date().toISOString(),
      updatedAt: event.updatedAt || new Date().toISOString(),
      source: 'polymarket-api',
      version: '1.0.0',
    };
  }

  /**
   * Normalize Polymarket market data to our queue format
   */
  private normalizeMarket(market: PolymarketMarket): MarketQueueMessage {
    const spread = market.bestBid && market.bestAsk ? market.bestAsk - market.bestBid : undefined;

    return {
      id: `market_${market.id}_${Date.now()}`,
      timestamp: new Date().toISOString(),
      polymarketId: market.id,
      eventId: market.eventId,
      question: market.question,
      description: market.description,
      liquidity: Number(market.liquidity) || 0,
      currentPrice: Number(market.price) || 0,
      bestBid: market.bestBid ? Number(market.bestBid) : undefined,
      bestAsk: market.bestAsk ? Number(market.bestAsk) : undefined,
      spread,
      volume24h: Number(market.volume24h) || 0,
      active: Boolean(market.active),
      closed: Boolean(market.closed),
      resolved: Boolean(market.resolvedAt),
      outcomePrices: market.outcomes?.reduce((acc, outcome) => {
        acc[outcome.id] = Number(outcome.price);
        return acc;
      }, {} as Record<string, number>),
      startDate: market.startDate,
      endDate: market.endDate,
      createdAt: new Date().toISOString(),
      updatedAt: market.updatedAt || new Date().toISOString(),
      source: 'polymarket-api',
      version: '1.0.0',
    };
  }

  /**
   * Normalize Polymarket outcome data to our queue format
   */
  private normalizeOutcome(outcome: PolymarketOutcome): OutcomeQueueMessage {
    const probability = Number(outcome.probability) || 0;
    const price = Number(outcome.price) || 0;

    return {
      id: `outcome_${outcome.id}_${Date.now()}`,
      timestamp: new Date().toISOString(),
      polymarketId: outcome.id,
      marketId: outcome.marketId,
      eventId: '', // Would need to be fetched separately if needed
      title: outcome.title,
      description: outcome.description,
      price,
      probability,
      impliedProbability: price,
      bestBid: undefined,
      bestAsk: undefined,
      liquidity: 0,
      volume: Number(outcome.volume) || 0,
      status: outcome.status as any,
      odds: {
        decimal: probability > 0 ? 1 / probability : 0,
        fractional: probability > 0 ? `${Math.round(1 / probability - 1)}/1` : '0/1',
        american: probability > 0.5 ? Math.round((probability / (1 - probability)) * 100) : Math.round(-100 / (probability / (1 - probability))),
      },
      createdAt: new Date().toISOString(),
      updatedAt: outcome.updatedAt || new Date().toISOString(),
      source: 'polymarket-api',
      version: '1.0.0',
    };
  }

  /**
   * Parse pagination information from response headers
   */
  private parsePaginationHeaders(headers: Record<string, string>): any {
    // This would depend on the specific pagination headers Polymarket uses
    // Implementation would vary based on their API
    return {
      hasNext: headers['x-has-next'] === 'true',
      hasPrev: headers['x-has-prev'] === 'true',
      total: headers['x-total'] ? parseInt(headers['x-total']) : undefined,
    };
  }

  /**
   * Setup default polling configurations
   */
  private setupDefaultPolling(): void {
    this.pollingConfigs.set('events', {
      enabled: true,
      intervalMs: 5000,
      batchSize: 50,
    });

    this.pollingConfigs.set('markets', {
      enabled: true,
      intervalMs: 2000,
      batchSize: 100,
    });

    this.pollingConfigs.set('outcomes', {
      enabled: true,
      intervalMs: 2000,
      batchSize: 200,
    });
  }

  /**
   * Start polling for specific data type
   */
  private startPollingFor(type: string, intervalMs: number, pollFn: () => Promise<void>): void {
    if (this.pollingIntervals.has(type)) {
      clearInterval(this.pollingIntervals.get(type)!);
    }

    const interval = setInterval(async () => {
      try {
        await pollFn();
      } catch (error) {
        this.logger.error(`Polling error for ${type}`, {
          error: error as Error,
          type,
        });
      }
    }, intervalMs);

    this.pollingIntervals.set(type, interval);
    this.logger.debug(`Started polling for ${type}`, {
      intervalMs,
    });
  }

  /**
   * Get API client statistics
   */
  getStats(): {
    pollingActive: boolean;
    pollingTypes: string[];
    authStatus: any;
  } {
    return {
      pollingActive: this.pollingIntervals.size > 0,
      pollingTypes: Array.from(this.pollingIntervals.keys()),
      authStatus: this.authService.getAuthStatus(),
    };
  }

  /**
   * Cleanup resources
   */
  cleanup(): void {
    this.stopPolling();
    this.logger.info('API client cleaned up');
  }
}

/**
 * Create and initialize API client
 */
export async function createApiClient(authService: PolymarketAuthService): Promise<PolymarketApiClient> {
  const client = new PolymarketApiClient(authService);
  await client.initialize();
  return client;
}

/**
 * Export types and utilities
 */
export type {
  ApiResponse,
  PolymarketEvent,
  PolymarketMarket,
  PolymarketOutcome,
  ApiRequestOptions,
  PollingConfig,
};

export default PolymarketApiClient;
