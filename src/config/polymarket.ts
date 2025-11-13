import axios, { AxiosInstance } from 'axios';
import logger, { formatError } from '../lib/logger';
import { sleep } from '../lib/retry';

export interface PolymarketTag {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  color: string | null;
  isActive: boolean;
  createdAt: string;
}

export interface PolymarketOutcome {
  id: string;
  title: string;
  description: string | null;
  price: number;
  probability: number;
  volume: number;
  createdAt: string;
  status: string;
  displayOrder: number;
}

export interface PolymarketMarket {
  id: string;
  question: string;
  description: string | null;
  slug: string;
  eventId?: string;
  startDate: string | null;
  endDate: string | null;
  liquidity: number;
  volume: number;
  createdAt: string;
  isActive: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  approved: boolean;
  currentPrice: number | null;
  lastTradePrice: number | null;
  bestBid: number | null;
  bestAsk: number | null;
  spread: number | null;
  status: string;
  resolvedAt: string | null;
  outcomes: PolymarketOutcome[];
  category?: string;
  tags?: PolymarketTag[];
  marketMakerAddress?: string | null;
  imageUrl?: string;
  event?: PolymarketEvent;
}

export interface PolymarketEvent {
  id: string;
  ticker: string | null;
  slug: string;
  title: string;
  description: string | null;
  startDate: string | null;
  endDate: string | null;
  liquidity: number;
  volume: number;
  createdAt: string;
  isActive: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  markets: PolymarketMarket[];
  category?: string;
  subcategory?: string;
  tags?: PolymarketTag[];
}

export interface ListEventsParams extends Record<string, unknown> {
  limit?: number;
  offset?: number;
  closed?: boolean;
  order?: string;
  ascending?: boolean;
  id?: string[];
}

export interface ListMarketsParams extends Record<string, unknown> {
  limit?: number;
  offset?: number;
  closed?: boolean;
  order?: string;
  ascending?: boolean;
  id?: string[];
}

export interface ApiError {
  message: string;
  status: number;
  code?: string;
  details?: any;
}

export interface PolymarketConfig {
  restBaseUrl: string;
  wsBaseUrl: string;
  rateLimitMs: number;
}

export const polymarketConfig: PolymarketConfig = {
  restBaseUrl: process.env.POLYMARKET_REST_URL || 'https://gamma-api.polymarket.com',
  wsBaseUrl: process.env.POLYMARKET_WS_URL || 'wss://ws.polymarket.com',
  rateLimitMs: Number(process.env.POLYMARKET_RATE_LIMIT_MS || 1000)
};

class PolymarketApiClient {
  private http: AxiosInstance;
  private activeRequests = new Map<string, boolean>();
  private requestCounter = new Map<string, number>();

  constructor(private config = polymarketConfig) {
    this.http = axios.create({
      baseURL: config.restBaseUrl,
      timeout: Number(process.env.POLYMARKET_TIMEOUT_MS || 10_000)
    });
  }

  private makeKey(endpoint: string, params: Record<string, unknown>): string {
    const serialized = Object.keys(params)
      .sort()
      .map(key => `${key}:${JSON.stringify(params[key])}`)
      .join('|');
    return `${endpoint}?${serialized}`;
  }

  private async request<T>(endpoint: string, params: Record<string, unknown> = {}): Promise<T[]> {
    const key = this.makeKey(endpoint, params);
    const count = (this.requestCounter.get(key) || 0) + 1;
    this.requestCounter.set(key, count);

    if (this.activeRequests.has(key)) {
      logger.debug('api deduplicated request', { key });
      return [];
    }

    this.activeRequests.set(key, true);

    try {
      logger.debug('api requesting endpoint', { endpoint, attempt: count, params });
      const response = await this.http.get(endpoint, {
        params,
        headers: this.buildHeaders()
      });

      if (Array.isArray(response.data)) {
        return response.data;
      }

      if (Array.isArray(response.data?.data)) {
        return response.data.data;
      }

      return [];
    } catch (error: any) {
      const apiError: ApiError = {
        message: error?.message || 'Unknown error',
        status: error?.response?.status || 500,
        code: error?.code,
        details: error?.response?.data
      };
      logger.error(`api request failed for ${endpoint}`, { error: formatError(apiError) });
      throw apiError;
    } finally {
      this.activeRequests.delete(key);
    }
  }

  async listEvents(params: ListEventsParams = {}): Promise<PolymarketEvent[]> {
    const events = await this.request<PolymarketEvent>('/events', params);
    await sleep(this.config.rateLimitMs);
    return events;
  }

  async listMarkets(params: ListMarketsParams = {}): Promise<PolymarketMarket[]> {
    const markets = await this.request<PolymarketMarket>('/markets', params);
    await sleep(this.config.rateLimitMs);
    return markets;
  }

  async getEvent(id: string): Promise<PolymarketEvent | null> {
    const [event] = await this.request<PolymarketEvent>('/events', { id: [id] });
    return event || null;
  }

  async getMarket(id: string): Promise<PolymarketMarket | null> {
    const [market] = await this.request<PolymarketMarket>('/markets', { id: [id] });
    return market || null;
  }
  private buildHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'User-Agent': 'SignalCast-Ingestor/1.0'
    };

    if (process.env.POLYMARKET_API_KEY) {
      headers.Authorization = `Bearer ${process.env.POLYMARKET_API_KEY}`;
    }

    return headers;
  }
}

export const polymarketClient = new PolymarketApiClient();
