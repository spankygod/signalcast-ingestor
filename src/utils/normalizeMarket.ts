/**
 * Market data normalization utilities for SignalCast
 * Pure functions to transform Polymarket market data to match database schema
 */

import { normalizeDate, isValidTimestamp } from './timeHelpers';
import { TYPE_CONVERSION, MARKET_CONSTANTS, ERROR_CONSTANTS } from './constants';

/**
 * Raw Polymarket market data (from API)
 */
export interface RawPolymarketMarket {
  id: string;
  eventId?: string;
  question: string;
  slug?: string;
  description?: string;
  liquidity?: number | string;
  volume24h?: number | string;
  volumeTotal?: number | string;
  price?: number | string;
  lastTradePrice?: number | string;
  bestBid?: number | string;
  bestAsk?: number | string;
  active?: boolean;
  closed?: boolean;
  archived?: boolean;
  restricted?: boolean;
  approved?: boolean;
  status?: string;
  resolvedAt?: string;
  relevanceScore?: number | string;
  startDate?: string;
  endDate?: string;
  outcomes?: RawPolymarketOutcome[];
  createdAt?: string;
  updatedAt?: string;
}

/**
 * Map the flexible Gamma/Polymarket market payload into our raw interface shape.
 * Handles stringified outcome arrays, optimized image structures, and nested event links.
 */
function mapPolymarketMarketApiToRaw(market: any): RawPolymarketMarket {
  if (!market || typeof market !== 'object') {
    return market as RawPolymarketMarket;
  }

  const primaryEvent = Array.isArray(market.events) && market.events.length > 0
    ? market.events[0]
    : undefined;

  const parsedOutcomeLabels = parseJsonArray(market.outcomes);
  const parsedOutcomePrices = parseJsonArray(market.outcomePrices);

  const outcomes: RawPolymarketOutcome[] | undefined = parsedOutcomeLabels
    ? parsedOutcomeLabels.map((label, index) => {
        const priceValue = parsedOutcomePrices && parsedOutcomePrices[index];
        return {
          id: `${market.id || 'market'}-${index}`,
          title: typeof label === 'string' ? label : label?.title ?? `Outcome ${index + 1}`,
          description: typeof label === 'object' ? label?.description : undefined,
          price: priceValue,
          probability: priceValue,
          status: market.status,
          updatedAt: market.updatedAt,
        };
      })
    : Array.isArray(market.outcomes)
      ? market.outcomes
      : undefined;

  const fallbackQuestion =
    market.question ??
    market.title ??
    market.slug ??
    market.ticker ??
    primaryEvent?.title ??
    market.id ??
    'Untitled market';

  const fallbackDescription =
    market.description ??
    primaryEvent?.description ??
    fallbackQuestion;

  const fallbackSlug =
    market.slug ??
    market.ticker ??
    primaryEvent?.slug ??
    fallbackQuestion;

  const liquidity =
    market.liquidity ??
    market.liquidityNum ??
    market.liquidityAmm ??
    market.liquidityClob;

  const volume24hr =
    market.volume24hr ??
    market.volume24hrAmm ??
    market.volume24hrClob;

  const volume =
    market.volume ??
    market.volumeNum ??
    market.volumeAmm ??
    market.volumeClob;

  const bestGuessPrice =
    market.price ??
    market.currentPrice ??
    market.lastTradePrice ??
    (outcomes && outcomes[0]?.price) ??
    (parsedOutcomePrices && parsedOutcomePrices[0]);

  return {
    id: market.id ?? market.marketId ?? market.conditionId ?? fallbackSlug,
    eventId: market.eventId ?? primaryEvent?.id ?? market.conditionId ?? market.slug,
    question: fallbackQuestion,
    slug: fallbackSlug,
    description: fallbackDescription,
    liquidity,
    volume24h,
    volumeTotal: volume,
    price: bestGuessPrice,
    lastTradePrice: market.lastTradePrice,
    bestBid: market.bestBid,
    bestAsk: market.bestAsk,
    active: market.active,
    closed: market.closed,
    archived: market.archived,
    restricted: market.restricted ?? primaryEvent?.restricted,
    approved: market.approved ?? true,
    status: market.status ?? (market.closed ? 'closed' : 'active'),
    resolvedAt: market.resolvedAt ?? market.closedTime ?? market.endDate,
    relevanceScore: market.relevanceScore ?? market.score ?? market.competitive,
    startDate: market.startDate ?? market.startDateIso ?? market.gameStartTime ?? primaryEvent?.startDate,
    endDate: market.endDate ?? market.endDateIso ?? market.umaEndDate ?? market.umaEndDateIso ?? primaryEvent?.endDate,
    outcomes,
    createdAt: market.createdAt ?? primaryEvent?.createdAt,
    updatedAt: market.updatedAt ?? primaryEvent?.updatedAt,
  };
}

/**
 * Raw outcome data (nested in market data)
 */
export interface RawPolymarketOutcome {
  id: string;
  title: string;
  description?: string;
  price?: number | string;
  probability?: number | string;
  volume?: number | string;
  status?: string;
  displayOrder?: number | string;
  updatedAt?: string;
}

/**
 * Normalized market data for database insertion
 */
export interface NormalizedMarket {
  polymarketId: string;
  eventId: string;
  question: string;
  slug: string;
  description?: string;
  liquidity: number;
  volume24h: number;
  volumeTotal: number;
  currentPrice: number;
  lastTradePrice?: number;
  bestBid?: number;
  bestAsk?: number;
  active: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  approved: boolean;
  status: string;
  resolvedAt?: string;
  relevanceScore: number;
  startDate?: string;
  endDate?: string;
  lastIngestedAt: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Market validation result
 */
export interface MarketValidationResult {
  isValid: boolean;
  errors: Array<{
    code: string;
    message: string;
    field?: string;
  }>;
  warnings: Array<{
    code: string;
    message: string;
    field?: string;
  }>;
}

/**
 * Market statistics
 */
export interface MarketStats {
  spread?: number;
  spreadPercentage?: number;
  priceHistoryPoints: number;
  liquidityScore: 'very-low' | 'low' | 'medium' | 'high' | 'very-high';
  volumeScore: 'very-low' | 'low' | 'medium' | 'high' | 'very-high';
  isActive: boolean;
  isResolved: boolean;
  daysToResolution?: number;
}

/**
 * Normalization options
 */
export interface MarketNormalizationOptions {
  strictValidation?: boolean;
  normalizeStatus?: boolean;
  calculateStatistics?: boolean;
  generateRelevanceScore?: boolean;
  maxQuestionLength?: number;
  maxDescriptionLength?: number;
  validatePriceRange?: boolean;
}

/**
 * Normalize raw market data
 * Pure function - no side effects
 */
export function normalizeMarket(
  rawInput: RawPolymarketMarket | any,
  options: MarketNormalizationOptions = {}
): { market: NormalizedMarket; validation: MarketValidationResult; stats?: MarketStats } {
  const rawData = mapPolymarketMarketApiToRaw(rawInput);
  const {
    strictValidation = false,
    normalizeStatus = true,
    calculateStatistics = true,
    generateRelevanceScore = false,
    maxQuestionLength = TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH,
    maxDescriptionLength = TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH,
    validatePriceRange = true,
  } = options;

  // Initialize validation result
  const validation: MarketValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Validate and normalize ID
  const polymarketId = normalizeId(rawData.id);
  if (!polymarketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market ID is required',
      field: 'id',
    });
    validation.isValid = false;
  }

  // Validate and normalize event ID
  const eventId = normalizeId(rawData.eventId);
  if (!eventId && strictValidation) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event ID is required',
      field: 'eventId',
    });
    validation.isValid = false;
  }

  // Normalize and validate question
  const question = normalizeText(rawData.question, maxQuestionLength);
  if (!question) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market question is required',
      field: 'question',
    });
    validation.isValid = false;
  }

  // Normalize description
  const description = normalizeText(rawData.description, maxDescriptionLength);

  // Normalize numeric values
  const liquidity = normalizeNumber(rawData.liquidity, TYPE_CONVERSION.DEFAULTS.LIQUIDITY);
  const volume24h = normalizeNumber(rawData.volume24h, TYPE_CONVERSION.DEFAULTS.VOLUME);
  const volumeTotal = normalizeNumber(rawData.volumeTotal, TYPE_CONVERSION.DEFAULTS.VOLUME);

  // Normalize price values
  let currentPrice = normalizeNumber(rawData.price, TYPE_CONVERSION.DEFAULTS.PRICE);
  const lastTradePrice = normalizeNumber(rawData.lastTradePrice);
  const bestBid = normalizeNumber(rawData.bestBid);
  const bestAsk = normalizeNumber(rawData.bestAsk);

  // Validate price ranges
  if (validatePriceRange) {
    if (currentPrice < MARKET_CONSTANTS.PRICE_RANGES.MIN - MARKET_CONSTANTS.PRICE_RANGES.VALID_TOLERANCE ||
        currentPrice > MARKET_CONSTANTS.PRICE_RANGES.MAX + MARKET_CONSTANTS.PRICE_RANGES.VALID_TOLERANCE) {
      validation.warnings.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
        message: `Current price ${currentPrice} is outside valid range [0, 1]`,
        field: 'price',
      });
      // Clamp price to valid range
      currentPrice = Math.max(MARKET_CONSTANTS.PRICE_RANGES.MIN,
                              Math.min(MARKET_CONSTANTS.PRICE_RANGES.MAX, currentPrice));
    }

    if (bestBid !== undefined) {
      if (bestBid < MARKET_CONSTANTS.PRICE_RANGES.MIN || bestBid > MARKET_CONSTANTS.PRICE_RANGES.MAX) {
        validation.warnings.push({
          code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
          message: `Best bid ${bestBid} is outside valid range [0, 1]`,
          field: 'bestBid',
        });
      }
    }

    if (bestAsk !== undefined) {
      if (bestAsk < MARKET_CONSTANTS.PRICE_RANGES.MIN || bestAsk > MARKET_CONSTANTS.PRICE_RANGES.MAX) {
        validation.warnings.push({
          code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
          message: `Best ask ${bestAsk} is outside valid range [0, 1]`,
          field: 'bestAsk',
        });
      }
    }

    // Validate bid-ask spread
    if (bestBid !== undefined && bestAsk !== undefined && bestBid > bestAsk) {
      validation.warnings.push({
        code: 'INVALID_SPREAD',
        message: `Best bid ${bestBid} is greater than best ask ${bestAsk}`,
      });
    }
  }

  // Normalize boolean values
  const active = normalizeBoolean(rawData.active, true);
  const closed = normalizeBoolean(rawData.closed, false);
  const archived = normalizeBoolean(rawData.archived, false);
  const restricted = normalizeBoolean(rawData.restricted, false);
  const approved = normalizeBoolean(rawData.approved, true);

  // Normalize status
  let status = normalizeText(rawData.status);
  if (normalizeStatus && MARKET_CONSTANTS.STATUS_MAPPINGS[status as keyof typeof MARKET_CONSTANTS.STATUS_MAPPINGS]) {
    status = MARKET_CONSTANTS.STATUS_MAPPINGS[status as keyof typeof MARKET_CONSTANTS.STATUS_MAPPINGS];
  }

  // Normalize relevance score
  let relevanceScore = normalizeNumber(rawData.relevanceScore, TYPE_CONVERSION.DEFAULTS.RELEVANCE_SCORE);

  if (generateRelevanceScore || !relevanceScore) {
    relevanceScore = calculateMarketRelevanceScore({
      question,
      description,
      liquidity,
      volume24h,
      active,
      closed,
      approved,
    });
  }

  // Normalize dates
  const startDate = normalizeDateString(rawData.startDate);
  const endDate = normalizeDateString(rawData.endDate);
  const resolvedAt = normalizeDateString(rawData.resolvedAt);
  const createdAt = normalizeDateString(rawData.createdAt) || new Date().toISOString();
  const updatedAt = normalizeDateString(rawData.updatedAt) || new Date().toISOString();

  // Validate date consistency
  if (startDate && endDate && new Date(startDate) > new Date(endDate)) {
    validation.warnings.push({
      code: 'INVALID_DATE_RANGE',
      message: 'Start date is after end date',
    });
  }

  if (resolvedAt && !closed) {
    validation.warnings.push({
      code: 'INCONSISTENT_RESOLVE_STATUS',
      message: 'Market has resolved at date but is not marked as closed',
    });
  }

  // Create normalized market
  const normalizedMarket: NormalizedMarket = {
    polymarketId,
    eventId: eventId || '',
    question,
    slug: normalizeSlug(rawData.slug || generateSlug(question)),
    description,
    liquidity,
    volume24h,
    volumeTotal,
    currentPrice,
    lastTradePrice,
    bestBid,
    bestAsk,
    active,
    closed,
    archived,
    restricted,
    approved,
    status: status || 'active',
    resolvedAt,
    relevanceScore,
    startDate,
    endDate,
    lastIngestedAt: new Date().toISOString(),
    createdAt,
    updatedAt,
  };

  const result: { market: NormalizedMarket; validation: MarketValidationResult; stats?: MarketStats } = {
    market: normalizedMarket,
    validation,
  };

  if (calculateStatistics) {
    result.stats = calculateMarketStats(normalizedMarket);
  }

  return result;
}

/**
 * Normalize multiple markets
 * Pure function - no side effects
 */
export function normalizeMarkets(
  markets: Array<RawPolymarketMarket | any>,
  options: MarketNormalizationOptions = {}
): {
  markets: NormalizedMarket[];
  validations: MarketValidationResult[];
  summary: {
    total: number;
    valid: number;
    invalid: number;
    warnings: number;
  };
  stats: MarketStats[];
} {
  const results = markets.map(market => normalizeMarket(market, options));
  const validations = results.map(r => r.validation);
  const stats = results.map(r => r.stats).filter(Boolean) as MarketStats[];

  const summary = {
    total: markets.length,
    valid: results.filter(r => r.validation.isValid).length,
    invalid: results.filter(r => !r.validation.isValid).length,
    warnings: validations.reduce((sum, v) => sum + v.warnings.length, 0),
  };

  return {
    markets: results.map(r => r.market),
    validations,
    summary,
    stats,
  };
}

/**
 * Validate market data against business rules
 * Pure function - no side effects
 */
export function validateMarket(market: NormalizedMarket): MarketValidationResult {
  const validation: MarketValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Required fields
  if (!market.polymarketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market ID is required',
      field: 'polymarketId',
    });
    validation.isValid = false;
  }

  if (!market.question) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market question is required',
      field: 'question',
    });
    validation.isValid = false;
  }

  // Field length validation
  if (market.question.length > TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.CONTENT_TOO_LONG,
      message: `Question exceeds maximum length of ${TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH}`,
      field: 'question',
    });
    validation.isValid = false;
  }

  if (market.description && market.description.length > TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.CONTENT_TOO_LONG,
      message: `Description exceeds maximum length of ${TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH}`,
      field: 'description',
    });
    validation.isValid = false;
  }

  // Price validation
  if (market.currentPrice < MARKET_CONSTANTS.PRICE_RANGES.MIN ||
      market.currentPrice > MARKET_CONSTANTS.PRICE_RANGES.MAX) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
      message: `Current price must be between ${MARKET_CONSTANTS.PRICE_RANGES.MIN} and ${MARKET_CONSTANTS.PRICE_RANGES.MAX}`,
      field: 'currentPrice',
    });
    validation.isValid = false;
  }

  if (market.bestBid !== undefined &&
      (market.bestBid < MARKET_CONSTANTS.PRICE_RANGES.MIN || market.bestBid > MARKET_CONSTANTS.PRICE_RANGES.MAX)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
      message: `Best bid must be between ${MARKET_CONSTANTS.PRICE_RANGES.MIN} and ${MARKET_CONSTANTS.PRICE_RANGES.MAX}`,
      field: 'bestBid',
    });
    validation.isValid = false;
  }

  if (market.bestAsk !== undefined &&
      (market.bestAsk < MARKET_CONSTANTS.PRICE_RANGES.MIN || market.bestAsk > MARKET_CONSTANTS.PRICE_RANGES.MAX)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
      message: `Best ask must be between ${MARKET_CONSTANTS.PRICE_RANGES.MIN} and ${MARKET_CONSTANTS.PRICE_RANGES.MAX}`,
      field: 'bestAsk',
    });
    validation.isValid = false;
  }

  // Business logic validation
  if (market.closed && market.active) {
    validation.warnings.push({
      code: 'CONFLICTING_STATUS',
      message: 'Market is both active and closed',
    });
  }

  if (market.archived && market.active) {
    validation.warnings.push({
      code: 'CONFLICTING_STATUS',
      message: 'Market is both active and archived',
    });
  }

  // Spread validation
  if (market.bestBid !== undefined && market.bestAsk !== undefined) {
    if (market.bestBid > market.bestAsk) {
      validation.errors.push({
        code: 'INVALID_SPREAD',
        message: 'Best bid cannot be greater than best ask',
        field: 'spread',
      });
      validation.isValid = false;
    }
  }

  // Status validation
  const validStatuses = Object.values(MARKET_CONSTANTS.STATUS_MAPPINGS);
  if (!validStatuses.includes(market.status as any)) {
    validation.warnings.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_STATUS,
      message: `Unknown market status: ${market.status}`,
      field: 'status',
    });
  }

  return validation;
}

/**
 * Calculate market statistics
 * Pure function - no side effects
 */
export function calculateMarketStats(market: NormalizedMarket): MarketStats {
  const stats: MarketStats = {
    isActive: market.active && !market.closed,
    isResolved: !!market.resolvedAt,
    priceHistoryPoints: 0, // Would be populated from historical data
    liquidityScore: getLiquidityScore(market.liquidity),
    volumeScore: getVolumeScore(market.volume24h),
  };

  // Calculate spread
  if (market.bestBid !== undefined && market.bestAsk !== undefined) {
    stats.spread = market.bestAsk - market.bestBid;
    stats.spreadPercentage = stats.spread / market.currentPrice;
  }

  // Calculate days to resolution
  if (market.endDate && !market.resolvedAt) {
    const now = new Date();
    const endDate = new Date(market.endDate);
    stats.daysToResolution = Math.ceil((endDate.getTime() - now.getTime()) / (24 * 60 * 60 * 1000));
  }

  return stats;
}

/**
 * Generate market slug from question
 * Pure function - no side effects
 */
export function generateSlug(question: string): string {
  return question
    .toLowerCase()
    .replace(/[^\w\s-]/g, '') // Remove special characters
    .replace(/\s+/g, '-') // Replace spaces with hyphens
    .replace(/-+/g, '-') // Replace multiple hyphens with single
    .replace(/^-|-$/g, '') // Remove leading/trailing hyphens
    .substring(0, TYPE_CONVERSION.LIMITS.MAX_SLUG_LENGTH);
}

// Helper functions

/**
 * Normalize ID field
 */
function normalizeId(id: string | undefined): string | undefined {
  if (!id || typeof id !== 'string') return undefined;
  const trimmed = id.trim();
  return trimmed.length > 0 && trimmed.length <= TYPE_CONVERSION.LIMITS.MAX_ID_LENGTH ? trimmed : undefined;
}

/**
 * Normalize text field
 */
function normalizeText(text: string | undefined, maxLength?: number): string | undefined {
  if (!text || typeof text !== 'string') return undefined;

  let trimmed = text.trim();
  if (maxLength) {
    trimmed = trimmed.substring(0, maxLength);
  }

  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Normalize slug
 */
function normalizeSlug(slug: string | undefined): string {
  if (!slug) return '';

  return slug
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, TYPE_CONVERSION.LIMITS.MAX_SLUG_LENGTH);
}

/**
 * Normalize number field
 */
function normalizeNumber(value: number | string | undefined, defaultValue: number = TYPE_CONVERSION.DEFAULTS.PRICE): number {
  if (value === undefined || value === null) return defaultValue;

  if (typeof value === 'number') {
    return isNaN(value) ? defaultValue : value;
  }

  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isNaN(parsed) ? defaultValue : parsed;
  }

  return defaultValue;
}

/**
 * Normalize boolean field
 */
function normalizeBoolean(value: boolean | string | undefined, defaultValue: boolean): boolean {
  if (value === undefined || value === null) return defaultValue;

  if (typeof value === 'boolean') return value;

  if (typeof value === 'string') {
    const lower = value.toLowerCase().trim();
    return lower === 'true' || lower === '1' || lower === 'yes';
  }

  return defaultValue;
}

/**
 * Normalize date string
 */
function normalizeDateString(dateString: string | undefined): string | undefined {
  if (!dateString) return undefined;

  const parsed = normalizeDate(dateString);
  return parsed.isValid ? parsed.normalizedISO : undefined;
}

/**
 * Get liquidity score based on amount
 */
function getLiquidityScore(liquidity: number): 'very-low' | 'low' | 'medium' | 'high' | 'very-high' {
  if (liquidity >= MARKET_CONSTANTS.LIQUIDITY_THRESHOLDS.VERY_HIGH) return 'very-high';
  if (liquidity >= MARKET_CONSTANTS.LIQUIDITY_THRESHOLDS.HIGH) return 'high';
  if (liquidity >= MARKET_CONSTANTS.LIQUIDITY_THRESHOLDS.MEDIUM) return 'medium';
  if (liquidity >= MARKET_CONSTANTS.LIQUIDITY_THRESHOLDS.LOW) return 'low';
  return 'very-low';
}

/**
 * Get volume score based on amount
 */
function getVolumeScore(volume: number): 'very-low' | 'low' | 'medium' | 'high' | 'very-high' {
  if (volume >= MARKET_CONSTANTS.VOLUME_THRESHOLDS.VERY_HIGH) return 'very-high';
  if (volume >= MARKET_CONSTANTS.VOLUME_THRESHOLDS.HIGH) return 'high';
  if (volume >= MARKET_CONSTANTS.VOLUME_THRESHOLDS.MEDIUM) return 'medium';
  if (volume >= MARKET_CONSTANTS.VOLUME_THRESHOLDS.LOW) return 'low';
  return 'very-low';
}

/**
 * Calculate market relevance score
 */
function calculateMarketRelevanceScore(params: {
  question?: string;
  description?: string;
  liquidity: number;
  volume24h: number;
  active: boolean;
  closed: boolean;
  approved: boolean;
}): number {
  let score = TYPE_CONVERSION.DEFAULTS.RELEVANCE_SCORE;

  // Base score for having all required fields
  if (params.question) {
    score += 20;
  }

  if (params.description) {
    score += 10;
  }

  // Score based on approval status
  if (params.approved) {
    score += 15;
  }

  // Score based on activity
  if (params.active && !params.closed) {
    score += 25;
  } else if (params.active) {
    score += 10;
  }

  // Score based on liquidity
  if (params.liquidity > 100000) {
    score += 20;
  } else if (params.liquidity > 10000) {
    score += 15;
  } else if (params.liquidity > 1000) {
    score += 10;
  } else if (params.liquidity > 100) {
    score += 5;
  }

  // Score based on volume
  if (params.volume24h > 100000) {
    score += 15;
  } else if (params.volume24h > 10000) {
    score += 10;
  } else if (params.volume24h > 1000) {
    score += 5;
  }

  return Math.min(Math.max(score, TYPE_CONVERSION.DEFAULTS.RELEVANCE_SCORE), 100);
}

function parseJsonArray(value: unknown): any[] | undefined {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : undefined;
    } catch {
      return undefined;
    }
  }

  return undefined;
}
