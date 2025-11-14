/**
 * Tick data normalization utilities for SignalCast
 * Pure functions to transform real-time price tick data to match database schema
 */

import { normalizeDate, isValidTimestamp, roundTimeToInterval } from './timeHelpers';
import { TYPE_CONVERSION, TICK_CONSTANTS, ERROR_CONSTANTS } from './constants';

/**
 * Raw tick data (from WebSocket or API)
 */
export interface RawTick {
  marketId?: string;
  price?: number | string;
  bestBid?: number | string;
  bestAsk?: number | string;
  liquidity?: number | string;
  volume24h?: number | string;
  priceChange?: number | string;
  priceChangePercent?: number | string;
  timestamp?: string | number;
  sequenceId?: number | string;
  tradeId?: string;
  tradeSize?: number | string;
  side?: 'buy' | 'sell' | string;
  tickType?: string;
  marketState?: string;
}

/**
 * Normalized tick data for database insertion
 */
export interface NormalizedTick {
  marketId: string;
  price: number;
  bestBid?: number;
  bestAsk?: number;
  lastTradePrice?: number;
  liquidity: number;
  volume24h?: number;
  priceChange?: number;
  priceChangePercent?: number;
  timestamp: string;
  sequenceId?: number;
  tradeId?: string;
  tradeSize?: number;
  side?: 'buy' | 'sell';
  tickType: 'price_update' | 'trade' | 'liquidity_change' | 'market_state';
  marketState: 'open' | 'closed' | 'suspended' | 'resolved';
  highPrecisionTimestamp: string;
  processedAt: string;
}

/**
 * Tick validation result
 */
export interface TickValidationResult {
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
 * Tick aggregation window result
 */
export interface TickAggregation {
  windowStart: string;
  windowEnd: string;
  tickCount: number;
  openPrice: number;
  closePrice: number;
  highPrice: number;
  lowPrice: number;
  volume: number;
  trades: number;
  averagePrice: number;
  priceChange: number;
  priceChangePercent: number;
}

/**
 * Tick statistics
 */
export interface TickStats {
  priceStats: {
    current: number;
    open: number;
    high: number;
    low: number;
    change: number;
    changePercent: number;
    volatility: number;
  };
  volumeStats: {
    current: number;
    total: number;
    trades: number;
    averageTradeSize: number;
  };
  spreadStats?: {
    current: number;
    average: number;
    width: 'tight' | 'normal' | 'wide' | 'very-wide';
  };
  marketStats: {
    state: string;
    lastUpdate: string;
    updateFrequency: number;
  };
}

/**
 * Normalization options
 */
export interface TickNormalizationOptions {
  strictValidation?: boolean;
  validatePriceRange?: boolean;
  validateTimestamps?: boolean;
  roundTimestamps?: boolean;
  calculateDerivatives?: boolean;
  aggregationWindowMs?: number;
  maxPriceJump?: number;
  maxVolumeSpike?: number;
}

/**
 * Normalize raw tick data
 * Pure function - no side effects
 */
export function normalizeTick(
  rawData: RawTick,
  options: TickNormalizationOptions = {}
): { tick: NormalizedTick; validation: TickValidationResult } {
  const {
    strictValidation = false,
    validatePriceRange = true,
    validateTimestamps = true,
    roundTimestamps = true,
    calculateDerivatives = true,
    aggregationWindowMs = TICK_CONSTANTS.AGGREGATION_WINDOWS.ONE_MINUTE,
    maxPriceJump = TICK_CONSTANTS.VALIDATION.MAX_PRICE_JUMP,
    maxVolumeSpike = TICK_CONSTANTS.VALIDATION.MAX_VOLUME_SPIKE,
  } = options;

  // Initialize validation result
  const validation: TickValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Validate and normalize market ID
  const marketId = normalizeId(rawData.marketId);
  if (!marketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market ID is required',
      field: 'marketId',
    });
    validation.isValid = false;
  }

  // Normalize and validate price
  let price = normalizeNumber(rawData.price);
  if (price === undefined) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Price is required',
      field: 'price',
    });
    validation.isValid = false;
    price = TYPE_CONVERSION.DEFAULTS.PRICE;
  }

  // Validate price range
  if (validatePriceRange && price !== undefined) {
    if (price < TICK_CONSTANTS.VALIDATION.MIN_PRICE || price > TICK_CONSTANTS.VALIDATION.MAX_PRICE) {
      validation.errors.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
        message: `Price ${price} is outside valid range [${TICK_CONSTANTS.VALIDATION.MIN_PRICE}, ${TICK_CONSTANTS.VALIDATION.MAX_PRICE}]`,
        field: 'price',
      });
      validation.isValid = false;
    }
  }

  // Normalize other numeric values
  const bestBid = normalizeNumber(rawData.bestBid);
  const bestAsk = normalizeNumber(rawData.bestAsk);
  const liquidity = normalizeNumber(rawData.liquidity, TYPE_CONVERSION.DEFAULTS.LIQUIDITY);
  const volume24h = normalizeNumber(rawData.volume24h);
  const priceChange = normalizeNumber(rawData.priceChange);
  const priceChangePercent = normalizeNumber(rawData.priceChangePercent);
  const tradeSize = normalizeNumber(rawData.tradeSize);

  // Validate bid-ask spread
  if (bestBid !== undefined && bestAsk !== undefined) {
    if (bestBid > bestAsk) {
      validation.warnings.push({
        code: 'INVALID_SPREAD',
        message: `Best bid ${bestBid} is greater than best ask ${bestAsk}`,
      });
    }

    // Validate price is within bid-ask range
    if (price < bestBid || price > bestAsk) {
      validation.warnings.push({
        code: 'PRICE_OUTSIDE_SPREAD',
        message: `Price ${price} is outside bid-ask spread [${bestBid}, ${bestAsk}]`,
      });
    }
  }

  // Normalize and validate timestamp
  let timestamp = normalizeTimestamp(rawData.timestamp);
  if (!timestamp) {
    timestamp = new Date().toISOString();
    validation.warnings.push({
      code: 'MISSING_TIMESTAMP',
      message: 'Using current timestamp as default',
      field: 'timestamp',
    });
  }

  // Validate timestamp
  if (validateTimestamps) {
    const timestampValidation = isValidTimestamp(new Date(timestamp).getTime());
    if (!timestampValidation.isValid) {
      validation.errors.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_TIMESTAMP,
        message: timestampValidation.reason || 'Invalid timestamp',
        field: 'timestamp',
      });
      validation.isValid = false;
    }

    if (timestampValidation.isFuture) {
      validation.warnings.push({
        code: 'FUTURE_TIMESTAMP',
        message: 'Timestamp is in the future',
        field: 'timestamp',
      });
    }
  }

  // Round timestamp if requested
  if (roundTimestamps && aggregationWindowMs) {
    const roundedDate = roundTimeToInterval(new Date(timestamp), aggregationWindowMs);
    timestamp = roundedDate.toISOString();
  }

  // Normalize sequence ID
  const sequenceId = normalizeNumber(rawData.sequenceId);

  // Normalize trade ID
  const tradeId = normalizeText(rawData.tradeId);

  // Normalize side
  let side: 'buy' | 'sell' | undefined;
  if (rawData.side) {
    const normalizedSide = rawData.side.toLowerCase().trim();
    if (normalizedSide === 'buy' || normalizedSide === 'sell') {
      side = normalizedSide;
    } else {
      validation.warnings.push({
        code: 'INVALID_SIDE',
        message: `Invalid side: ${rawData.side}`,
        field: 'side',
      });
    }
  }

  // Normalize tick type
  let tickType: NormalizedTick['tickType'];
  if (rawData.tickType && Object.values(TICK_CONSTANTS.TYPES).includes(rawData.tickType as any)) {
    tickType = rawData.tickType as NormalizedTick['tickType'];
  } else {
    // Determine tick type based on available data
    if (tradeId) {
      tickType = TICK_CONSTANTS.TYPES.TRADE;
    } else if (bestBid !== undefined || bestAsk !== undefined) {
      tickType = TICK_CONSTANTS.TYPES.LIQUIDITY_CHANGE;
    } else {
      tickType = TICK_CONSTANTS.TYPES.PRICE_UPDATE;
    }

    if (rawData.tickType) {
      validation.warnings.push({
        code: 'INVALID_TICK_TYPE',
        message: `Invalid tick type: ${rawData.tickType}, using: ${tickType}`,
        field: 'tickType',
      });
    }
  }

  // Normalize market state
  let marketState: NormalizedTick['marketState'];
  if (rawData.marketState && Object.values(TICK_CONSTANTS.STATES).includes(rawData.marketState as any)) {
    marketState = rawData.marketState as NormalizedTick['marketState'];
  } else {
    marketState = TICK_CONSTANTS.STATES.OPEN;
    if (rawData.marketState) {
      validation.warnings.push({
        code: 'INVALID_MARKET_STATE',
        message: `Invalid market state: ${rawData.marketState}, using: ${marketState}`,
        field: 'marketState',
      });
    }
  }

  // Calculate derivatives if requested
  if (calculateDerivatives && priceChange === undefined && priceChangePercent === undefined) {
    // These would typically be calculated from historical data
    // For now, we'll leave them undefined
    validation.warnings.push({
      code: 'MISSING_DERIVATIVES',
      message: 'Could not calculate price change without historical data',
    });
  }

  // Create high precision timestamp
  const highPrecisionTimestamp = new Date().toISOString();

  // Create normalized tick
  const normalizedTick: NormalizedTick = {
    marketId: marketId || '',
    price,
    bestBid,
    bestAsk,
    liquidity,
    volume24h,
    priceChange,
    priceChangePercent,
    timestamp,
    sequenceId,
    tradeId,
    tradeSize,
    side,
    tickType,
    marketState,
    highPrecisionTimestamp,
    processedAt: new Date().toISOString(),
  };

  return {
    tick: normalizedTick,
    validation,
  };
}

/**
 * Normalize multiple ticks
 * Pure function - no side effects
 */
export function normalizeTicks(
  ticks: RawTick[],
  options: TickNormalizationOptions = {}
): {
  ticks: NormalizedTick[];
  validations: TickValidationResult[];
  summary: {
    total: number;
    valid: number;
    invalid: number;
    warnings: number;
  };
} {
  const results = ticks.map(tick => normalizeTick(tick, options));
  const validations = results.map(r => r.validation);

  const summary = {
    total: ticks.length,
    valid: results.filter(r => r.validation.isValid).length,
    invalid: results.filter(r => !r.validation.isValid).length,
    warnings: validations.reduce((sum, v) => sum + v.warnings.length, 0),
  };

  return {
    ticks: results.map(r => r.tick),
    validations,
    summary,
  };
}

/**
 * Validate tick data against business rules
 * Pure function - no side effects
 */
export function validateTick(tick: NormalizedTick): TickValidationResult {
  const validation: TickValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Required fields
  if (!tick.marketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Market ID is required',
      field: 'marketId',
    });
    validation.isValid = false;
  }

  if (tick.price === undefined || tick.price === null) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Price is required',
      field: 'price',
    });
    validation.isValid = false;
  }

  // Price validation
  if (tick.price !== undefined && (tick.price < 0 || tick.price > 1)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
      message: 'Price must be between 0 and 1',
      field: 'price',
    });
    validation.isValid = false;
  }

  // Bid-ask validation
  if (tick.bestBid !== undefined && tick.bestAsk !== undefined) {
    if (tick.bestBid > tick.bestAsk) {
      validation.errors.push({
        code: 'INVALID_SPREAD',
        message: 'Best bid cannot be greater than best ask',
        field: 'spread',
      });
      validation.isValid = false;
    }

    if (tick.bestBid < 0 || tick.bestBid > 1) {
      validation.errors.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
        message: 'Best bid must be between 0 and 1',
        field: 'bestBid',
      });
      validation.isValid = false;
    }

    if (tick.bestAsk < 0 || tick.bestAsk > 1) {
      validation.errors.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_PRICE,
        message: 'Best ask must be between 0 and 1',
        field: 'bestAsk',
      });
      validation.isValid = false;
    }
  }

  // Tick type validation
  const validTickTypes = Object.values(TICK_CONSTANTS.TYPES);
  if (!validTickTypes.includes(tick.tickType)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_STATUS,
      message: `Invalid tick type: ${tick.tickType}`,
      field: 'tickType',
    });
    validation.isValid = false;
  }

  // Market state validation
  const validMarketStates = Object.values(TICK_CONSTANTS.STATES);
  if (!validMarketStates.includes(tick.marketState)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_STATUS,
      message: `Invalid market state: ${tick.marketState}`,
      field: 'marketState',
    });
    validation.isValid = false;
  }

  // Business logic validation
  if (tick.tickType === TICK_CONSTANTS.TYPES.TRADE && !tick.tradeId) {
    validation.warnings.push({
      code: 'MISSING_TRADE_ID',
      message: 'Trade tick should have a trade ID',
      field: 'tradeId',
    });
  }

  if (tick.tickType === TICK_CONSTANTS.TYPES.TRADE && !tick.side) {
    validation.warnings.push({
      code: 'MISSING_TRADE_SIDE',
      message: 'Trade tick should have a side (buy/sell)',
      field: 'side',
    });
  }

  return validation;
}

/**
 * Aggregate ticks by time window
 * Pure function - no side effects
 */
export function aggregateTicks(
  ticks: NormalizedTick[],
  windowMs: number = TICK_CONSTANTS.AGGREGATION_WINDOWS.ONE_MINUTE
): TickAggregation[] {
  if (ticks.length === 0) return [];

  // Group ticks by time windows
  const windows = new Map<string, NormalizedTick[]>();

  ticks.forEach(tick => {
    const tickDate = new Date(tick.timestamp);
    const windowStart = roundTimeToInterval(tickDate, windowMs);
    const windowKey = windowStart.toISOString();

    if (!windows.has(windowKey)) {
      windows.set(windowKey, []);
    }
    windows.get(windowKey)!.push(tick);
  });

  // Aggregate each window
  const aggregations: TickAggregation[] = [];

  windows.forEach((windowTicks, windowStart) => {
    if (windowTicks.length === 0) return;

    const prices = windowTicks.map(t => t.price).filter(p => p !== undefined);
    const trades = windowTicks.filter(t => t.tickType === TICK_CONSTANTS.TYPES.TRADE);
    const volumes = trades.map(t => t.tradeSize || 0);

    const openPrice = prices[0] || 0;
    const closePrice = prices[prices.length - 1] || 0;
    const highPrice = Math.max(...prices);
    const lowPrice = Math.min(...prices);
    const averagePrice = prices.reduce((sum, p) => sum + p, 0) / prices.length;

    const priceChange = closePrice - openPrice;
    const priceChangePercent = openPrice > 0 ? (priceChange / openPrice) * 100 : 0;

    const windowEnd = new Date(new Date(windowStart).getTime() + windowMs).toISOString();

    aggregations.push({
      windowStart,
      windowEnd,
      tickCount: windowTicks.length,
      openPrice,
      closePrice,
      highPrice,
      lowPrice,
      volume: volumes.reduce((sum, v) => sum + v, 0),
      trades: trades.length,
      averagePrice,
      priceChange,
      priceChangePercent,
    });
  });

  return aggregations.sort((a, b) => a.windowStart.localeCompare(b.windowStart));
}

/**
 * Calculate tick statistics
 * Pure function - no side effects
 */
export function calculateTickStats(ticks: NormalizedTick[]): TickStats | null {
  if (ticks.length === 0) return null;

  const prices = ticks.map(t => t.price).filter(p => p !== undefined);
  const trades = ticks.filter(t => t.tickType === TICK_CONSTANTS.TYPES.TRADE);
  const tradeSizes = trades.map(t => t.tradeSize || 0);
  const spreads = ticks
    .filter(t => t.bestBid !== undefined && t.bestAsk !== undefined)
    .map(t => (t.bestAsk! - t.bestBid!));

  const currentPrice = prices[prices.length - 1] || 0;
  const openPrice = prices[0] || 0;
  const highPrice = Math.max(...prices);
  const lowPrice = Math.min(...prices);
  const priceChange = currentPrice - openPrice;
  const priceChangePercent = openPrice > 0 ? (priceChange / openPrice) * 100 : 0;

  // Calculate volatility (standard deviation of price changes)
  const priceChanges = [];
  for (let i = 1; i < prices.length; i++) {
    priceChanges.push(prices[i] - prices[i - 1]);
  }
  const volatility = calculateStandardDeviation(priceChanges);

  const currentVolume = trades.length > 0 ? tradeSizes[tradeSizes.length - 1] || 0 : 0;
  const totalVolume = tradeSizes.reduce((sum, v) => sum + v, 0);
  const averageTradeSize = trades.length > 0 ? totalVolume / trades.length : 0;

  let spreadStats: TickStats['spreadStats'];
  if (spreads.length > 0) {
    const currentSpread = spreads[spreads.length - 1] || 0;
    const averageSpread = spreads.reduce((sum, s) => sum + s, 0) / spreads.length;

    let width: 'tight' | 'normal' | 'wide' | 'very-wide';
    if (currentSpread <= 0.01) width = 'tight';
    else if (currentSpread <= 0.05) width = 'normal';
    else if (currentSpread <= 0.10) width = 'wide';
    else width = 'very-wide';

    spreadStats = {
      current: currentSpread,
      average: averageSpread,
      width,
    };
  }

  // Calculate update frequency
  const timestamps = ticks.map(t => new Date(t.timestamp).getTime()).sort((a, b) => a - b);
  let updateFrequency = 0;
  if (timestamps.length > 1) {
    const intervals = [];
    for (let i = 1; i < timestamps.length; i++) {
      intervals.push(timestamps[i] - timestamps[i - 1]);
    }
    updateFrequency = intervals.reduce((sum, interval) => sum + interval, 0) / intervals.length;
  }

  const marketState = ticks.length > 0 ? ticks[ticks.length - 1].marketState : TICK_CONSTANTS.STATES.OPEN;
  const lastUpdate = ticks.length > 0 ? ticks[ticks.length - 1].timestamp : new Date().toISOString();

  return {
    priceStats: {
      current: currentPrice,
      open: openPrice,
      high: highPrice,
      low: lowPrice,
      change: priceChange,
      changePercent: priceChangePercent,
      volatility,
    },
    volumeStats: {
      current: currentVolume,
      total: totalVolume,
      trades: trades.length,
      averageTradeSize,
    },
    spreadStats,
    marketStats: {
      state: marketState,
      lastUpdate,
      updateFrequency,
    },
  };
}

/**
 * Detect anomalous ticks
 * Pure function - no side effects
 */
export function detectAnomalousTicks(
  ticks: NormalizedTick[],
  maxPriceJump: number = TICK_CONSTANTS.VALIDATION.MAX_PRICE_JUMP,
  maxVolumeSpike: number = TICK_CONSTANTS.VALIDATION.MAX_VOLUME_SPIKE
): {
  anomalousIndices: number[];
  anomalies: Array<{
    index: number;
    type: 'price_jump' | 'volume_spike' | 'invalid_spread' | 'future_timestamp';
    severity: 'low' | 'medium' | 'high';
    description: string;
  }>;
} {
  const anomalousIndices: number[] = [];
  const anomalies: Array<{
    index: number;
    type: 'price_jump' | 'volume_spike' | 'invalid_spread' | 'future_timestamp';
    severity: 'low' | 'medium' | 'high';
    description: string;
  }> = [];

  for (let i = 0; i < ticks.length; i++) {
    const tick = ticks[i];
    if (!tick) {
      continue;
    }
    let isAnomalous = false;

    // Check for price jumps
    if (i > 0) {
      const prevTick = ticks[i - 1];
      if (!prevTick) {
        continue;
      }
      const priceChange = Math.abs(tick.price - prevTick.price);
      const priceChangePercent = (priceChange / prevTick.price) * 100;

      if (priceChangePercent > maxPriceJump * 100) {
        anomalousIndices.push(i);
        anomalies.push({
          index: i,
          type: 'price_jump',
          severity: priceChangePercent > maxPriceJump * 200 ? 'high' : 'medium',
          description: `Price jump of ${priceChangePercent.toFixed(2)}% detected`,
        });
        isAnomalous = true;
      }
    }

    // Check for volume spikes
    if (tick.tickType === TICK_CONSTANTS.TYPES.TRADE && tick.tradeSize) {
      const tradeTicks = ticks.filter(
        (t): t is NormalizedTick =>
          typeof t !== 'undefined' &&
          t.tickType === TICK_CONSTANTS.TYPES.TRADE &&
          !!t.tradeSize
      );
      const avgTradeSize = tradeTicks.length
        ? tradeTicks.reduce((sum, t) => sum + (t.tradeSize || 0), 0) / tradeTicks.length
        : 0;

      if (tick.tradeSize > avgTradeSize * maxVolumeSpike) {
        anomalousIndices.push(i);
        anomalies.push({
          index: i,
          type: 'volume_spike',
          severity: 'medium',
          description: `Trade size ${tick.tradeSize} is ${maxVolumeSpike}x average`,
        });
        isAnomalous = true;
      }
    }

    // Check for invalid spreads
    if (tick.bestBid !== undefined && tick.bestAsk !== undefined) {
      if (tick.bestBid >= tick.bestAsk) {
        anomalousIndices.push(i);
        anomalies.push({
          index: i,
          type: 'invalid_spread',
          severity: 'high',
          description: `Invalid spread: bid ${tick.bestBid} >= ask ${tick.bestAsk}`,
        });
        isAnomalous = true;
      }
    }

    // Check for future timestamps
    const tickTime = new Date(tick.timestamp).getTime();
    const now = Date.now();
    if (tickTime > now + 60000) { // Allow 1 minute clock skew
      anomalousIndices.push(i);
      anomalies.push({
        index: i,
        type: 'future_timestamp',
        severity: 'high',
        description: `Timestamp ${tick.timestamp} is in the future`,
      });
      isAnomalous = true;
    }
  }

  return {
    anomalousIndices: [...new Set(anomalousIndices)], // Remove duplicates
    anomalies,
  };
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
function normalizeText(text: string | undefined): string | undefined {
  if (!text || typeof text !== 'string') return undefined;
  const trimmed = text.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Normalize number field
 */
function normalizeNumber(value: number | string | undefined): number | undefined {
  if (value === undefined || value === null) return undefined;

  if (typeof value === 'number') {
    return isNaN(value) ? undefined : value;
  }

  if (typeof value === 'string') {
    const parsed = parseFloat(value);
    return isNaN(parsed) ? undefined : parsed;
  }

  return undefined;
}

/**
 * Normalize timestamp
 */
function normalizeTimestamp(timestamp: string | number | undefined): string | undefined {
  if (timestamp === undefined || timestamp === null) return undefined;

  const parsed = normalizeDate(timestamp);
  return parsed.isValid ? parsed.normalizedISO : undefined;
}

/**
 * Calculate standard deviation
 */
function calculateStandardDeviation(values: number[]): number {
  if (values.length === 0) return 0;

  const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
  const squaredDiffs = values.map(val => Math.pow(val - mean, 2));
  const avgSquaredDiff = squaredDiffs.reduce((sum, val) => sum + val, 0) / squaredDiffs.length;

  return Math.sqrt(avgSquaredDiff);
}
