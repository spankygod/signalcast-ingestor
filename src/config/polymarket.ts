import { config } from './settings';

/**
 * Polymarket API configuration
 * Based on the confirmed architecture and existing environment variables
 */
export const POLYMARKET_CONFIG = {
  // Base URLs
  API_BASE_URL: config.POLYMARKET_API_BASE_URL,
  WS_BASE_URL: config.POLYMARKET_WS_URL,

  // Authentication
  API_KEY: config.POLYMARKET_API_KEY,
  SECRET: config.POLYMARKET_SECRET,
  PASSPHRASE: config.POLYMARKET_PASSPHRASE,

  // WebSocket configuration
  WS_PATH: config.POLYMARKET_WS_PATH,

  // Rate limiting
  RATE_LIMIT_MS: config.POLYMARKET_RATE_LIMIT_MS,
  BATCH_SIZE: config.POLYMARKET_BATCH_SIZE,
} as const;

/**
 * Polymarket API endpoints
 */
export const API_ENDPOINTS = {
  // Events endpoints
  EVENTS: '/events',
  EVENT_BY_ID: (id: string) => `/events/${id}`,
  ACTIVE_EVENTS: '/events?active=true',

  // Markets endpoints
  MARKETS: '/markets',
  MARKET_BY_ID: (id: string) => `/markets/${id}`,
  ACTIVE_MARKETS: '/markets?active=true',
  MARKETS_BY_EVENT: (eventId: string) => `/events/${eventId}/markets`,

  // Outcomes endpoints
  OUTCOMES: '/outcomes',
  OUTCOME_BY_ID: (id: string) => `/outcomes/${id}`,
  OUTCOMES_BY_MARKET: (marketId: string) => `/markets/${marketId}/outcomes`,

  // Price data endpoints
  PRICES: '/prices',
  MARKET_PRICE: (marketId: string) => `/markets/${marketId}/price`,
  PRICE_HISTORY: (marketId: string) => `/markets/${marketId}/price-history`,

  // Token endpoints (if using CLOB API)
  TOKEN: '/token',
  REFRESH_TOKEN: '/refresh',

  // Health/status endpoints
  HEALTH: '/health',
  STATUS: '/status',
} as const;

/**
 * WebSocket message types and subscriptions
 */
export const WS_CONFIG = {
  // Connection settings
  URL: `${POLYMARKET_CONFIG.WS_BASE_URL}${POLYMARKET_CONFIG.WS_PATH}`,
  RECONNECT_INTERVAL_MS: 5000,
  MAX_RECONNECT_ATTEMPTS: 10,
  PING_INTERVAL_MS: 30000,
  PONG_TIMEOUT_MS: 10000,

  // Subscription channels
  CHANNELS: {
    MARKET_DATA: 'market_data',
    PRICE_UPDATES: 'price_updates',
    ORDER_BOOK: 'order_book',
    TRADES: 'trades',
    EVENTS: 'events',
    TICKER: 'ticker',
  },

  // Message types
  MESSAGE_TYPES: {
    // Client to server
    SUBSCRIBE: 'subscribe',
    UNSUBSCRIBE: 'unsubscribe',
    PING: 'ping',
    AUTH: 'auth',

    // Server to client
    SUBSCRIPTION_ACK: 'subscription_ack',
    UNSUBSCRIPTION_ACK: 'unsubscription_ack',
    DATA: 'data',
    ERROR: 'error',
    PONG: 'pong',
    AUTH_SUCCESS: 'auth_success',
    AUTH_FAILURE: 'auth_failure',
  },

  // Subscription filters
  FILTERS: {
    ALL_MARKETS: '*',
    ACTIVE_MARKETS: 'active',
    SPECIFIC_MARKET: (marketId: string) => `market:${marketId}`,
    SPECIFIC_EVENT: (eventId: string) => `event:${eventId}`,
    PRICE_RANGE: (min: number, max: number) => `price:${min}-${max}`,
  },
} as const;

/**
 * WebSocket channels - extracted for easier import
 */
export const WS_CHANNELS = WS_CONFIG.CHANNELS;

/**
 * WebSocket message types - extracted for easier import
 */
export const WS_MESSAGE_TYPES = WS_CONFIG.MESSAGE_TYPES;

/**
 * WebSocket filters - extracted for easier import
 */
export const WS_FILTERS = WS_CONFIG.FILTERS;

/**
 * Data transformation configuration
 */
export const DATA_MAPPING = {
  // Field mappings from Polymarket to our schema
  EVENTS: {
    ID: 'id',
    SLUG: 'slug',
    TITLE: 'title',
    DESCRIPTION: 'description',
    CATEGORY: 'category',
    SUBCATEGORY: 'subcategory',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume24h',
    VOLUME_TOTAL: 'volumeTotal',
    ACTIVE: 'active',
    CLOSED: 'closed',
    ARCHIVED: 'archived',
    RESTRICTED: 'restricted',
    START_DATE: 'startDate',
    END_DATE: 'endDate',
    RELEVANCE_SCORE: 'relevanceScore',
    UPDATED_AT: 'updatedAt',
  },

  MARKETS: {
    ID: 'id',
    EVENT_ID: 'eventId',
    QUESTION: 'question',
    SLUG: 'slug',
    DESCRIPTION: 'description',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume24h',
    VOLUME_TOTAL: 'volumeTotal',
    CURRENT_PRICE: 'price',
    LAST_TRADE_PRICE: 'lastTradePrice',
    BEST_BID: 'bestBid',
    BEST_ASK: 'bestAsk',
    ACTIVE: 'active',
    CLOSED: 'closed',
    ARCHIVED: 'archived',
    RESTRICTED: 'restricted',
    APPROVED: 'approved',
    STATUS: 'status',
    RESOLVED_AT: 'resolvedAt',
    RELEVANCE_SCORE: 'relevanceScore',
    UPDATED_AT: 'updatedAt',
  },

  OUTCOMES: {
    ID: 'id',
    MARKET_ID: 'marketId',
    TITLE: 'title',
    DESCRIPTION: 'description',
    PRICE: 'price',
    PROBABILITY: 'probability',
    VOLUME: 'volume',
    STATUS: 'status',
    DISPLAY_ORDER: 'displayOrder',
    UPDATED_AT: 'updatedAt',
  },

  TICKS: {
    MARKET_ID: 'marketId',
    PRICE: 'price',
    BEST_BID: 'bestBid',
    BEST_ASK: 'bestAsk',
    LAST_TRADE_PRICE: 'lastTradePrice',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume24h',
    TIMESTAMP: 'timestamp',
  },
} as const;

/**
 * Data validation and sanitization
 */
export const VALIDATION = {
  // Required fields for each data type
  REQUIRED_FIELDS: {
    EVENTS: ['id', 'title'],
    MARKETS: ['id', 'question', 'eventId'],
    OUTCOMES: ['id', 'title', 'marketId'],
    TICKS: ['marketId', 'timestamp'],
  },

  // Field types and constraints
  CONSTRAINTS: {
    ID_MIN_LENGTH: 1,
    TITLE_MAX_LENGTH: 500,
    DESCRIPTION_MAX_LENGTH: 2000,
    CATEGORY_MAX_LENGTH: 100,
    PRICE_MIN: 0,
    PRICE_MAX: 1,
    PROBABILITY_MIN: 0,
    PROBABILITY_MAX: 1,
    LIQUIDITY_MIN: 0,
    VOLUME_MIN: 0,
    DISPLAY_ORDER_MIN: 0,
  },

  // Fields that should be converted to numbers
  NUMERIC_FIELDS: [
    'liquidity',
    'volume24h',
    'volumeTotal',
    'price',
    'lastTradePrice',
    'bestBid',
    'bestAsk',
    'probability',
    'volume',
    'displayOrder',
    'relevanceScore',
  ],

  // Fields that should be converted to booleans
  BOOLEAN_FIELDS: [
    'active',
    'closed',
    'archived',
    'restricted',
    'approved',
  ],

  // Fields that should be converted to dates
  DATE_FIELDS: [
    'startDate',
    'endDate',
    'resolvedAt',
    'updatedAt',
    'timestamp',
  ],
} as const;

/**
 * Request configuration and timeouts
 */
export const REQUEST_CONFIG = {
  // Default request settings
  TIMEOUT_MS: 30000,
  MAX_RETRIES: 3,
  RETRY_DELAY_MS: 1000,
  BACKOFF_MULTIPLIER: 2,

  // Request headers
  HEADERS: {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'User-Agent': 'signalcast-ingestor/1.0.0',
    'X-API-Key': POLYMARKET_CONFIG.API_KEY,
  },

  // Rate limiting
  RATE_LIMIT: {
    REQUESTS_PER_SECOND: Math.floor(1000 / POLYMARKET_CONFIG.RATE_LIMIT_MS),
    BURST_SIZE: 10,
    WINDOW_SIZE_MS: 1000,
  },
} as const;

/**
 * Pagination configuration for API calls
 */
export const PAGINATION = {
  // Default page sizes
  DEFAULT_PAGE_SIZE: 100,
  MAX_PAGE_SIZE: 1000,

  // Pagination strategies
  STRATEGIES: {
    OFFSET: 'offset',
    CURSOR: 'cursor',
    PAGE: 'page',
  },

  // Cursor-based pagination settings
  CURSOR: {
    PARAM_NAME: 'cursor',
    DEFAULT_LIMIT: 100,
    MAX_LIMIT: 1000,
  },

  // Offset-based pagination settings
  OFFSET: {
    PAGE_PARAM: 'page',
    LIMIT_PARAM: 'limit',
    DEFAULT_PAGE: 1,
    DEFAULT_LIMIT: 100,
    MAX_LIMIT: 1000,
  },
} as const;

/**
 * Data filtering and selection
 */
export const FILTERING = {
  // Event filters
  EVENTS: {
    ACTIVE_ONLY: 'active=true',
    CLOSED_ONLY: 'closed=true',
    ARCHIVED_ONLY: 'archived=true',
    BY_CATEGORY: (category: string) => `category=${category}`,
    BY_RELEVANCE: (minRelevance: number) => `min_relevance=${minRelevance}`,
    BY_VOLUME: (minVolume: number) => `min_volume=${minVolume}`,
  },

  // Market filters
  MARKETS: {
    ACTIVE_ONLY: 'active=true',
    BY_EVENT: (eventId: string) => `event_id=${eventId}`,
    BY_PRICE_RANGE: (min: number, max: number) => `min_price=${min}&max_price=${max}`,
    BY_LIQUIDITY: (minLiquidity: number) => `min_liquidity=${minLiquidity}`,
  },

  // Outcome filters
  OUTCOMES: {
    BY_MARKET: (marketId: string) => `market_id=${marketId}`,
    BY_STATUS: (status: string) => `status=${status}`,
  },

  // Sorting options
  SORTING: {
    BY_RELEVANCE: 'sort=relevance',
    BY_VOLUME: 'sort=volume',
    BY_LIQUIDITY: 'sort=liquidity',
    BY_UPDATED_AT: 'sort=updated_at',
    BY_START_DATE: 'sort=start_date',
    ORDER_ASC: 'order=asc',
    ORDER_DESC: 'order=desc',
  },
} as const;

/**
 * Error handling configuration
 */
export const ERROR_HANDLING = {
  // Retryable HTTP status codes
  RETRYABLE_STATUS_CODES: [408, 429, 500, 502, 503, 504],

  // Non-retryable status codes
  NON_RETRYABLE_STATUS_CODES: [400, 401, 403, 404, 422],

  // Error categories
  CATEGORIES: {
    NETWORK: 'network',
    AUTHENTICATION: 'authentication',
    RATE_LIMIT: 'rate_limit',
    VALIDATION: 'validation',
    SERVER: 'server',
    UNKNOWN: 'unknown',
  },

  // Maximum retry attempts per error category
  MAX_RETRIES: {
    NETWORK: 5,
    AUTHENTICATION: 2,
    RATE_LIMIT: 3,
    VALIDATION: 1, // Don't retry validation errors
    SERVER: 3,
    UNKNOWN: 2,
  },
} as const;

/**
 * Authentication configuration
 */
export const AUTH_CONFIG = {
  // JWT token configuration (if applicable)
  JWT: {
    ALGORITHM: 'HS256',
    EXPIRES_IN_SECONDS: 3600, // 1 hour
    REFRESH_THRESHOLD_SECONDS: 300, // 5 minutes
  },

  // API key authentication
  API_KEY: {
    HEADER_NAME: 'X-API-Key',
    QUERY_PARAM: 'api_key',
  },

  // Signature authentication (if using HMAC)
  SIGNATURE: {
    ALGORITHM: 'sha256',
    TIMESTAMP_HEADER: 'X-Timestamp',
    SIGNATURE_HEADER: 'X-Signature',
    NONCE_HEADER: 'X-Nonce',
  },
} as const;

/**
 * Caching configuration for API responses
 */
export const CACHING = {
  // Cache TTLs for different data types (in seconds)
  TTL_SECONDS: {
    EVENTS: 300,      // 5 minutes
    MARKETS: 60,      // 1 minute
    OUTCOMES: 60,     // 1 minute
    PRICES: 5,        // 5 seconds
    HISTORICAL: 3600, // 1 hour
  },

  // Cache keys pattern
  KEY_PATTERNS: {
    EVENT: (id: string) => `event:${id}`,
    MARKET: (id: string) => `market:${id}`,
    OUTCOME: (id: string) => `outcome:${id}`,
    PRICE: (id: string) => `price:${id}`,
    LIST: (type: string, filters: string) => `list:${type}:${filters}`,
  },

  // Cache invalidation strategies
  INVALIDATION: {
    ON_UPDATE: true,
    ON_NEW_DATA: true,
    TTL_EXPIRY: true,
    MANUAL: false,
  },
} as const;

/**
 * Export Polymarket configuration
 */
export default {
  config: POLYMARKET_CONFIG,
  endpoints: API_ENDPOINTS,
  websocket: WS_CONFIG,
  dataMapping: DATA_MAPPING,
  validation: VALIDATION,
  request: REQUEST_CONFIG,
  pagination: PAGINATION,
  filtering: FILTERING,
  errorHandling: ERROR_HANDLING,
  auth: AUTH_CONFIG,
  caching: CACHING,
};