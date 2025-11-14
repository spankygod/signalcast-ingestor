/**
 * Shared utility constants for SignalCast data normalization and processing
 * Pure constants used across all utility functions
 */

/**
 * Data type conversion constants
 */
export const TYPE_CONVERSION = {
  // Numeric precision
  PRICE_PRECISION: 6,
  PROBABILITY_PRECISION: 4,
  LIQUIDITY_PRECISION: 2,
  VOLUME_PRECISION: 2,

  // Rounding modes
  ROUNDING_MODE: 'half-away-from-zero' as const,

  // Default values for missing data
  DEFAULTS: {
    PRICE: 0,
    PROBABILITY: 0,
    LIQUIDITY: 0,
    VOLUME: 0,
    RELEVANCE_SCORE: 0,
    DISPLAY_ORDER: 0,
  },

  // Maximum values
  LIMITS: {
    MAX_TITLE_LENGTH: 500,
    MAX_DESCRIPTION_LENGTH: 2000,
    MAX_SLUG_LENGTH: 200,
    MAX_CATEGORY_LENGTH: 100,
    MAX_ID_LENGTH: 100,
    MAX_TAGS_COUNT: 10,
  },
} as const;

/**
 * Time-related constants
 */
export const TIME_CONSTANTS = {
  // Milliseconds in units
  MILLISECONDS_PER_SECOND: 1000,
  MILLISECONDS_PER_MINUTE: 60 * 1000,
  MILLISECONDS_PER_HOUR: 60 * 60 * 1000,
  MILLISECONDS_PER_DAY: 24 * 60 * 60 * 1000,
  MILLISECONDS_PER_WEEK: 7 * 24 * 60 * 60 * 1000,

  // Common time periods
  RETENTION_PERIODS: {
    TICK_DATA: 60 * 60 * 1000, // 1 hour
    PRICE_HISTORY: 24 * 60 * 60 * 1000, // 1 day
    AUDIT_LOG: 7 * 24 * 60 * 60 * 1000, // 7 days
    METRICS: 30 * 24 * 60 * 60 * 1000, // 30 days
  },

  // Cache durations
  CACHE_TTL: {
    EVENT_DATA: 5 * 60 * 1000, // 5 minutes
    MARKET_DATA: 2 * 60 * 1000, // 2 minutes
    OUTCOME_DATA: 2 * 60 * 1000, // 2 minutes
    USER_SESSION: 30 * 60 * 1000, // 30 minutes
  },
} as const;

/**
 * Content filtering and validation constants
 */
export const CONTENT_CONSTANTS = {
  // Relevance score ranges
  RELEVANCE_SCORES: {
    MIN: 0,
    MAX: 100,
    HIGH_QUALITY_MIN: 80,
    MEDIUM_QUALITY_MIN: 60,
    LOW_QUALITY_MIN: 40,
    SPAM_THRESHOLD: 20,
  },

  // Politics filtering
  POLITICS_KEYWORDS: [
    'election', 'political', 'politics', 'president', 'congress', 'senate',
    'government', 'policy', 'vote', 'campaign', 'democrat', 'republican',
    'trump', 'biden', 'election2024', 'midterms', 'primary', 'debate',
    'impeachment', 'supreme court', 'federal', 'administration', 'gop',
    'democratic party', 'conservative', 'liberal', 'libertarian',
  ],

  // Spam detection patterns
  SPAM_PATTERNS: [
    /\b(free|click|win|prize|congratulations|limited time|act now)\b/gi,
    /\b(!!!|\?\?\?|\$+|urgent|exclusive)\b/gi,
    /([a-z])\1{3,}/gi, // Repeated characters
    /\b.{1,2}\s.{1,2}\s.{1,2}/gi, // Short repeated words
  ],

  // Content quality indicators
  QUALITY_INDICATORS: {
    MIN_TITLE_LENGTH: 10,
    MIN_DESCRIPTION_LENGTH: 20,
    MAX_CAPS_RATIO: 0.5,
    MAX_SPECIAL_CHARS_RATIO: 0.3,
    MIN_WORDS_IN_DESCRIPTION: 5,
  },

  // Category mappings for normalization
  CATEGORY_NORMALIZATION: {
    'crypto': 'cryptocurrency',
    'cryptocurrencies': 'cryptocurrency',
    'bitcoin': 'cryptocurrency',
    'ethereum': 'cryptocurrency',
    'sports-betting': 'sports',
    'entertainment-news': 'entertainment',
    'tech': 'technology',
    'world-news': 'world-events',
    'business-news': 'business',
    'health-news': 'health',
  } as const,
} as const;

/**
 * Market data normalization constants
 */
export const MARKET_CONSTANTS = {
  // Price ranges
  PRICE_RANGES: {
    MIN: 0,
    MAX: 1,
    VALID_TOLERANCE: 1e-6, // Small tolerance for floating point errors
  },

  // Probability ranges
  PROBABILITY_RANGES: {
    MIN: 0,
    MAX: 1,
    VALID_TOLERANCE: 1e-6,
  },

  // Liquidity thresholds
  LIQUIDITY_THRESHOLDS: {
    VERY_LOW: 100,
    LOW: 1000,
    MEDIUM: 10000,
    HIGH: 100000,
    VERY_HIGH: 1000000,
  },

  // Volume thresholds
  VOLUME_THRESHOLDS: {
    VERY_LOW: 1000,
    LOW: 10000,
    MEDIUM: 100000,
    HIGH: 1000000,
    VERY_HIGH: 10000000,
  },

  // Spread thresholds (for market making)
  SPREAD_THRESHOLDS: {
    TIGHT: 0.01,  // 1%
    NORMAL: 0.05, // 5%
    WIDE: 0.10,   // 10%
    VERY_WIDE: 0.20, // 20%
  },

  // Market status mappings
  STATUS_MAPPINGS: {
    'open': 'active',
    'closed': 'closed',
    'resolved': 'resolved',
    'cancelled': 'cancelled',
    'suspended': 'suspended',
    'expired': 'closed',
  } as const,
} as const;

/**
 * Outcome data constants
 */
export const OUTCOME_CONSTANTS = {
  // Outcome status mappings
  STATUS_MAPPINGS: {
    'active': 'active',
    'winning': 'resolved',
    'losing': 'resolved',
    'won': 'resolved',
    'lost': 'resolved',
    'cancelled': 'cancelled',
    'suspended': 'suspended',
    'pending': 'active',
  } as const,

  // Resolution mappings
  RESOLUTION_MAPPINGS: {
    'true': 'yes',
    'yes': 'yes',
    '1': 'yes',
    'false': 'no',
    'no': 'no',
    '0': 'no',
    'invalid': 'invalid',
    'void': 'invalid',
    'cancelled': 'invalid',
  } as const,

  // Odds calculation constants
  ODDS_CALCULATION: {
    MIN_PROBABILITY: 0.01, // Minimum probability for odds calculation
    MAX_ODDS: 10000, // Maximum odds to prevent division errors
    AMERICAN_ODDS_ROUNDING: 10, // Round American odds to nearest 10
  },
} as const;

/**
 * Tick data constants
 */
export const TICK_CONSTANTS = {
  // Tick types
  TYPES: {
    PRICE_UPDATE: 'price_update',
    TRADE: 'trade',
    LIQUIDITY_CHANGE: 'liquidity_change',
    MARKET_STATE: 'market_state',
    VOLUME_UPDATE: 'volume_update',
  } as const,

  // Market states
  STATES: {
    OPEN: 'open',
    CLOSED: 'closed',
    SUSPENDED: 'suspended',
    RESOLVED: 'resolved',
  } as const,

  // Validation thresholds
  VALIDATION: {
    MAX_PRICE_JUMP: 0.50, // 50% price jump threshold
    MAX_VOLUME_SPIKE: 1000, // 1000x volume spike threshold
    MAX_TICK_FREQUENCY_MS: 10, // Minimum time between ticks
    MIN_PRICE: 0.001, // Minimum valid price
    MAX_PRICE: 0.999, // Maximum valid price
  },

  // Aggregation windows
  AGGREGATION_WINDOWS: {
    ONE_SECOND: 1000,
    FIVE_SECONDS: 5000,
    THIRTY_SECONDS: 30000,
    ONE_MINUTE: 60000,
    FIVE_MINUTES: 300000,
  },
} as const;

/**
 * Error handling constants
 */
export const ERROR_CONSTANTS = {
  // Error severity levels
  SEVERITY: {
    LOW: 'low',
    MEDIUM: 'medium',
    HIGH: 'high',
    CRITICAL: 'critical',
  } as const,

  // Error codes for validation
  VALIDATION_CODES: {
    INVALID_PRICE: 'INVALID_PRICE',
    INVALID_PROBABILITY: 'INVALID_PROBABILITY',
    INVALID_TIMESTAMP: 'INVALID_TIMESTAMP',
    MISSING_REQUIRED_FIELD: 'MISSING_REQUIRED_FIELD',
    INVALID_ID: 'INVALID_ID',
    INVALID_CATEGORY: 'INVALID_CATEGORY',
    CONTENT_TOO_LONG: 'CONTENT_TOO_LONG',
    INVALID_STATUS: 'INVALID_STATUS',
    SPAM_DETECTED: 'SPAM_DETECTED',
    POLITICS_FILTERED: 'POLITICS_FILTERED',
  } as const,

  // Recovery actions
  RECOVERY_ACTIONS: {
    RETRY: 'retry',
    SKIP: 'skip',
    QUEUE_FOR_REVIEW: 'queue_for_review',
    LOG_AND_CONTINUE: 'log_and_continue',
    FALLBACK_TO_DEFAULT: 'fallback_to_default',
  } as const,
} as const;

/**
 * Performance constants
 */
export const PERFORMANCE_CONSTANTS = {
  // Batch processing
  BATCH_SIZES: {
    SMALL: 10,
    MEDIUM: 100,
    LARGE: 1000,
    EXTRA_LARGE: 10000,
  },

  // Memory thresholds (bytes)
  MEMORY_THRESHOLDS: {
    WARNING: 100 * 1024 * 1024, // 100MB
    CRITICAL: 500 * 1024 * 1024, // 500MB
    MAX: 1024 * 1024 * 1024, // 1GB
  },

  // Processing timeouts
  TIMEOUTS: {
    VALIDATION: 100, // 100ms
    NORMALIZATION: 200, // 200ms
    FILTERING: 50, // 50ms
    TRANSFORMATION: 300, // 300ms
    BATCH_PROCESSING: 5000, // 5 seconds
  },

  // Concurrency limits
  CONCURRENCY_LIMITS: {
    MAX_CONCURRENT_TRANSFORMATIONS: 10,
    MAX_CONCURRENT_VALIDATIONS: 20,
    MAX_QUEUE_SIZE: 10000,
  },
} as const;

/**
 * Export all utility constants
 */
export default {
  typeConversion: TYPE_CONVERSION,
  time: TIME_CONSTANTS,
  content: CONTENT_CONSTANTS,
  market: MARKET_CONSTANTS,
  outcome: OUTCOME_CONSTANTS,
  tick: TICK_CONSTANTS,
  error: ERROR_CONSTANTS,
  performance: PERFORMANCE_CONSTANTS,
} as const;