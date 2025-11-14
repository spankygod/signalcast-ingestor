/**
 * SignalCast Utility Functions
 * Comprehensive utility library for data normalization, filtering, and transformations
 *
 * This module exports all utility functions organized by category:
 * - Constants: Shared constants and configuration values
 * - Time Helpers: Date/time parsing, formatting, and validation
 * - Data Normalization: Transform Polymarket data to match database schema
 * - Content Filtering: Politics detection, spam filtering, and quality assessment
 */

// Constants
export {
  TYPE_CONVERSION,
  TIME_CONSTANTS,
  CONTENT_CONSTANTS,
  MARKET_CONSTANTS,
  OUTCOME_CONSTANTS,
  TICK_CONSTANTS,
  ERROR_CONSTANTS,
  PERFORMANCE_CONSTANTS,
} from './constants';
export { default as constants } from './constants';

// Time Helpers
export {
  normalizeDate,
  getTimezoneInfo,
  convertToTimezone,
  formatDate,
  timeDifference,
  isDateInRange,
  getDateRange,
  isRecent,
  getDayBoundaries,
  getWeekBoundaries,
  getMonthBoundaries,
  roundTimeToInterval,
  isValidTimestamp,
  type TimezoneInfo,
  type DateRange,
  type ParsedDate,
} from './timeHelpers';

// Event Normalization
export {
  normalizeEvent,
  normalizeEvents,
  validateEvent,
  generateSlug as generateEventSlug,
  type RawPolymarketEvent,
  type NormalizedEvent,
  type EventValidationResult,
  type NormalizationOptions,
} from './normalizeEvent';

// Market Normalization
export {
  normalizeMarket,
  normalizeMarkets,
  validateMarket,
  calculateMarketStats,
  generateSlug as generateMarketSlug,
  type RawPolymarketMarket,
  type NormalizedMarket,
  type RawPolymarketOutcome,
  type MarketValidationResult,
  type MarketStats,
  type MarketNormalizationOptions,
} from './normalizeMarket';

// Tick Normalization
export {
  normalizeTick,
  normalizeTicks,
  validateTick,
  aggregateTicks,
  calculateTickStats,
  detectAnomalousTicks,
  type RawTick,
  type NormalizedTick,
  type TickValidationResult,
  type TickAggregation,
  type TickStats,
  type TickNormalizationOptions,
} from './normalizeTick';

// Content Filtering
export {
  analyzeContent,
  filterContent,
  analyzePoliticsContent,
  analyzeSpamContent,
  analyzeContentQuality,
  extractContentMetadata,
  cleanContent,
  type ContentFilterConfig,
  type ContentAnalysisResult,
  type ContentMetadata,
  type FilterResult,
} from './politicsFilter';

// Re-export commonly used combinations
export * from './constants';
export * from './timeHelpers';
export * from './normalizeEvent';
export * from './normalizeMarket';
export * from './normalizeTick';
export * from './politicsFilter';

/**
 * Utility bundle for common operations
 * Groups related utilities for convenience
 */
export const Utils = {
  // Data transformation utilities
  normalize: {
    event: normalizeEvent,
    events: normalizeEvents,
    market: normalizeMarket,
    markets: normalizeMarkets,
    tick: normalizeTick,
    ticks: normalizeTicks,
    validateEvent,
    validateMarket,
    validateTick,
  },

  // Content filtering utilities
  filter: {
    analyzeContent,
    filterContent,
    analyzePolitics: analyzePoliticsContent,
    analyzeSpam: analyzeSpamContent,
    analyzeQuality: analyzeContentQuality,
    extractMetadata: extractContentMetadata,
    clean: cleanContent,
  },

  // Time utilities
  time: {
    normalizeDate,
    getTimezoneInfo,
    convertToTimezone,
    formatDate,
    timeDifference,
    isDateInRange,
    getDateRange,
    isRecent,
    getBoundaries: {
      day: getDayBoundaries,
      week: getWeekBoundaries,
      month: getMonthBoundaries,
    },
    roundToInterval: roundTimeToInterval,
    isValidTimestamp,
  },

  // Statistics utilities
  stats: {
    calculateMarketStats,
    calculateTickStats,
    aggregateTicks,
    detectAnomalousTicks,
  },

  // Constants
  constants: {
    typeConversion: TYPE_CONVERSION,
    time: TIME_CONSTANTS,
    content: CONTENT_CONSTANTS,
    market: MARKET_CONSTANTS,
    outcome: OUTCOME_CONSTANTS,
    tick: TICK_CONSTANTS,
    error: ERROR_CONSTANTS,
    performance: PERFORMANCE_CONSTANTS,
  },
} as const;

/**
 * Default configuration for utility functions
 */
export const DefaultConfig = {
  // Event normalization defaults
  eventNormalization: {
    strictValidation: false,
    enablePoliticsFilter: true,
    normalizeCategories: true,
    generateRelevanceScore: false,
    maxTitleLength: TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH,
    maxDescriptionLength: TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH,
  } as const,

  // Market normalization defaults
  marketNormalization: {
    strictValidation: false,
    normalizeStatus: true,
    calculateStatistics: true,
    generateRelevanceScore: false,
    maxQuestionLength: TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH,
    maxDescriptionLength: TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH,
    validatePriceRange: true,
  } as const,

  // Tick normalization defaults
  tickNormalization: {
    strictValidation: false,
    validatePriceRange: true,
    validateTimestamps: true,
    roundTimestamps: true,
    calculateDerivatives: true,
    aggregationWindowMs: TICK_CONSTANTS.AGGREGATION_WINDOWS.ONE_MINUTE,
    maxPriceJump: TICK_CONSTANTS.VALIDATION.MAX_PRICE_JUMP,
    maxVolumeSpike: TICK_CONSTANTS.VALIDATION.MAX_VOLUME_SPIKE,
  } as const,

  // Content filtering defaults
  contentFiltering: {
    enablePoliticsFilter: true,
    enableSpamFilter: true,
    enableQualityFilter: true,
    politicsThreshold: 0.5,
    spamThreshold: 0.7,
    qualityThreshold: 0.3,
  } as const,
} as const;

/**
 * Utility function versions
 */
export const VERSION = {
  library: '1.0.0',
  normalizeEvent: '1.0.0',
  normalizeMarket: '1.0.0',
  normalizeTick: '1.0.0',
  politicsFilter: '1.0.0',
  timeHelpers: '1.0.0',
  constants: '1.0.0',
} as const;

export default Utils;