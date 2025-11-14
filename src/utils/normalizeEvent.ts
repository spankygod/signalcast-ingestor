/**
 * Event data normalization utilities for SignalCast
 * Pure functions to transform Polymarket event data to match database schema
 */

import { normalizeDate, isValidTimestamp } from './timeHelpers';
import { TYPE_CONVERSION, CONTENT_CONSTANTS, ERROR_CONSTANTS } from './constants';

/**
 * Raw Polymarket event from API (minimal subset)
 * Updated to match real API field names
 */
export interface RawPolymarketEvent {
  id: string;
  slug?: string;
  title: string;
  description?: string;
  category: string;
  subcategory?: string;
  liquidity?: number | string;
  volume24hr?: number | string;     // Real API uses volume24hr
  volume?: number | string;         // Real API uses volume
  active?: boolean;
  closed?: boolean;
  archived?: boolean;
  restricted?: boolean;
  startDate?: string;
  endDate?: string;
  relevanceScore?: number | string;
  image?: string;                   // Real API uses image/icon
  icon?: string;
  tags?: Array<{ label: string }>;  // Real API: array of objects
  createdAt?: string;
  updatedAt?: string;
  // Optional fallbacks
  volume24hrAmm?: number | string;
  volume24hrClob?: number | string;
  liquidityAmm?: number | string;
  liquidityClob?: number | string;
  eventStartTime?: string;
  endDateIso?: string;
  score?: number | string;
  creationDate?: string;
  published_at?: string;
}

/**
 * Normalized event data for database insertion
 */
export interface NormalizedEvent {
  polymarketId: string;
  slug: string;
  title: string;
  description?: string;
  category: string;
  subcategory?: string;
  liquidity: number;
  volume24h: number;
  volumeTotal: number;
  active: boolean;
  closed: boolean;
  archived: boolean;
  restricted: boolean;
  relevanceScore: number;
  startDate?: string;
  endDate?: string;
  lastIngestedAt: string;
  createdAt: string;
  updatedAt: string;
}

/**
 * Event validation result
 */
export interface EventValidationResult {
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
 * Normalization options
 */
export interface NormalizationOptions {
  strictValidation?: boolean;
  enablePoliticsFilter?: boolean;
  normalizeCategories?: boolean;
  generateRelevanceScore?: boolean;
  maxTitleLength?: number;
  maxDescriptionLength?: number;
}

/**
 * Map real Polymarket API payload to RawPolymarketEvent interface
 * Handles field name differences and fallbacks
 */
export function mapPolymarketApiToRaw(event: any): RawPolymarketEvent {
  const primaryMarket = Array.isArray(event.markets) && event.markets.length > 0
    ? event.markets[0]
    : undefined;

  const firstSeries = Array.isArray(event.series) && event.series.length > 0
    ? event.series[0]
    : undefined;

  const firstCollection = Array.isArray(event.collections) && event.collections.length > 0
    ? event.collections[0]
    : undefined;

  const firstCategory = Array.isArray(event.categories) && event.categories.length > 0
    ? event.categories[0]
    : undefined;

  const normalizedTags = Array.isArray(event.tags)
    ? event.tags.map((tag: any) => {
        if (typeof tag === 'string') {
          return { label: tag };
        }
        return tag;
      })
    : undefined;

  const fallbackTitle =
    event.title ??
    primaryMarket?.question ??
    event.slug ??
    event.ticker ??
    event.id;

  const fallbackDescription =
    event.description ??
    event.subtitle ??
    primaryMarket?.description ??
    primaryMarket?.question;

  const fallbackCategory =
    event.category ??
    primaryMarket?.category ??
    firstCategory?.label ??
    firstCollection?.collectionType ??
    firstSeries?.title;

  const fallbackSubcategory =
    event.subcategory ??
    firstCollection?.title ??
    firstCategory?.parentCategory ??
    firstSeries?.seriesType;

  const slugSource =
    event.slug ??
    event.ticker ??
    primaryMarket?.slug ??
    fallbackTitle;

  return {
    id: event.id ?? event.event_id ?? event.conditionId ?? primaryMarket?.id,
    slug: slugSource,
    title: fallbackTitle,
    description: fallbackDescription,
    category: fallbackCategory,
    subcategory: fallbackSubcategory,
    liquidity:
      event.liquidity ??
      event.liquidityAmm ??
      event.liquidityClob ??
      primaryMarket?.liquidity ??
      primaryMarket?.liquidityNum ??
      primaryMarket?.liquidityAmm ??
      primaryMarket?.liquidityClob,
    volume24hr:
      event.volume24hr ??
      event.volume24hrAmm ??
      event.volume24hrClob ??
      primaryMarket?.volume24hr ??
      primaryMarket?.volume24hrAmm ??
      primaryMarket?.volume24hrClob,
    volume:
      event.volume ??
      event.volumeNum ??
      primaryMarket?.volume ??
      primaryMarket?.volumeNum,
    active: event.active ?? primaryMarket?.active,
    closed: event.closed ?? primaryMarket?.closed,
    archived: event.archived ?? primaryMarket?.archived,
    restricted: event.restricted ?? primaryMarket?.restricted ?? firstSeries?.restricted,
    startDate:
      event.startDate ??
      event.startTime ??
      event.eventStartTime ??
      primaryMarket?.startDate ??
      primaryMarket?.startDateIso ??
      firstSeries?.startDate,
    endDate:
      event.endDate ??
      event.endDateIso ??
      primaryMarket?.endDate ??
      primaryMarket?.endDateIso ??
      event.closedTime ??
      primaryMarket?.closedTime,
    relevanceScore: event.relevanceScore ?? event.score ?? event.competitive,
    image:
      event.image ??
      event.featuredImage ??
      primaryMarket?.image ??
      firstSeries?.image ??
      firstCollection?.image ??
      event.featuredImageOptimized?.imageUrlOptimized ??
      event.imageOptimized?.imageUrlOptimized,
    icon:
      event.icon ??
      primaryMarket?.icon ??
      firstSeries?.icon ??
      firstCollection?.icon ??
      event.iconOptimized?.imageUrlOptimized,
    tags: normalizedTags,
    createdAt:
      event.createdAt ??
      event.creationDate ??
      event.published_at ??
      primaryMarket?.createdAt ??
      firstSeries?.createdAt,
    updatedAt:
      event.updatedAt ??
      event.published_at ??
      primaryMarket?.updatedAt ??
      firstSeries?.updatedAt,
  };
}

/**
 * Normalize raw event data
 * Pure function - no side effects
 */
export function normalizeEvent(
  rawData: RawPolymarketEvent,
  options: NormalizationOptions = {}
): { event: NormalizedEvent; validation: EventValidationResult } {
  const {
    strictValidation = false,
    enablePoliticsFilter = true,
    normalizeCategories = true,
    generateRelevanceScore = false,
    maxTitleLength = TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH,
    maxDescriptionLength = TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH,
  } = options;

  // Initialize validation result
  const validation: EventValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Validate and normalize ID
  const polymarketId = normalizeId(rawData.id);
  if (!polymarketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event ID is required',
      field: 'id',
    });
    validation.isValid = false;
  }

  // Normalize and validate title
  let title = normalizeText(rawData.title, maxTitleLength);
  if (!title) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event title is required',
      field: 'title',
    });
    validation.isValid = false;

    title =
      normalizeText(rawData.slug, maxTitleLength) ||
      rawData.slug ||
      rawData.id ||
      'Untitled Event';
  } else if (title.length < CONTENT_CONSTANTS.QUALITY_INDICATORS.MIN_TITLE_LENGTH) {
    validation.warnings.push({
      code: 'SHORT_TITLE',
      message: 'Event title is very short',
      field: 'title',
    });
  }

  // Normalize and validate description
  const description = normalizeText(rawData.description, maxDescriptionLength);
  if (description && description.length < CONTENT_CONSTANTS.QUALITY_INDICATORS.MIN_DESCRIPTION_LENGTH) {
    validation.warnings.push({
      code: 'SHORT_DESCRIPTION',
      message: 'Event description is very short',
      field: 'description',
    });
  }

  // Normalize category
  let category = normalizeCategory(rawData.category, normalizeCategories);
  if (!category) {
    category = 'other';
    validation.warnings.push({
      code: 'MISSING_CATEGORY',
      message: 'Using default category: other',
      field: 'category',
    });
  }

  // Apply politics filter
  if (enablePoliticsFilter && isPoliticsContent(title, description, category)) {
    validation.warnings.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.POLITICS_FILTERED,
      message: 'Event contains politics-related content',
    });
  }

  // Check for spam content
  if (isSpamContent(title, description)) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.SPAM_DETECTED,
      message: 'Event appears to contain spam content',
    });
    validation.isValid = false;
  }

  // Normalize subcategory
  const subcategory = normalizeText(rawData.subcategory);

  // Normalize numeric values
  const liquidity = normalizeNumber(rawData.liquidity, TYPE_CONVERSION.DEFAULTS.LIQUIDITY);
  const volume24h = normalizeNumber(rawData.volume24hr, TYPE_CONVERSION.DEFAULTS.VOLUME);
  const volumeTotal = normalizeNumber(rawData.volume, TYPE_CONVERSION.DEFAULTS.VOLUME);

  // Normalize boolean values
  const active = normalizeBoolean(rawData.active, true);
  const closed = normalizeBoolean(rawData.closed, false);
  const archived = normalizeBoolean(rawData.archived, false);
  const restricted = normalizeBoolean(rawData.restricted, false);

  // Normalize relevance score
  let relevanceScore = normalizeNumber(
    rawData.relevanceScore,
    TYPE_CONVERSION.DEFAULTS.RELEVANCE_SCORE
  );

  if (generateRelevanceScore || !relevanceScore) {
    relevanceScore = calculateRelevanceScore({
      title,
      description,
      category,
      liquidity,
      volume24h,
      active,
      closed,
    });
  }

  // Normalize dates
  const startDate = normalizeDateString(rawData.startDate);
  const endDate = normalizeDateString(rawData.endDate);
  const createdAt = normalizeDateString(rawData.createdAt) || new Date().toISOString();
  const updatedAt = normalizeDateString(rawData.updatedAt) || new Date().toISOString();

  // Validate date consistency
  if (startDate && endDate && new Date(startDate) > new Date(endDate)) {
    validation.warnings.push({
      code: 'INVALID_DATE_RANGE',
      message: 'Start date is after end date',
    });
  }

  // Create normalized event
  const normalizedEvent: NormalizedEvent = {
    polymarketId,
    slug: (() => {
      const slugSource = rawData.slug && rawData.slug.trim().length > 0
        ? rawData.slug
        : title || polymarketId || 'event';
      let slug = normalizeSlug(slugSource);
      if (!slug && polymarketId) {
        slug = normalizeSlug(polymarketId);
      }
      if (!slug) {
        slug = generateSlug(slugSource);
      }
      return slug || `event-${Date.now()}`;
    })(),
    title: title,
    description,
    category,
    subcategory,
    liquidity,
    volume24h,
    volumeTotal,
    active,
    closed,
    archived,
    restricted,
    relevanceScore,
    startDate,
    endDate,
    lastIngestedAt: new Date().toISOString(),
    createdAt,
    updatedAt,
  };

  return {
    event: normalizedEvent,
    validation,
  };
}

/**
 * Normalize multiple events
 * Pure function - no side effects
 */
export function normalizeEvents(
  events: any[], // Accept raw API objects
  options: NormalizationOptions = {}
): {
  events: NormalizedEvent[];
  validations: EventValidationResult[];
  summary: {
    total: number;
    valid: number;
    invalid: number;
    warnings: number;
  };
} {
  const mapped = events.map(mapPolymarketApiToRaw);
  const results = mapped.map(event => normalizeEvent(event, options));
  const validations = results.map(r => r.validation);

  const summary = {
    total: events.length,
    valid: results.filter(r => r.validation.isValid).length,
    invalid: results.filter(r => !r.validation.isValid).length,
    warnings: validations.reduce((sum, v) => sum + v.warnings.length, 0),
  };

  return {
    events: results.map(r => r.event),
    validations,
    summary,
  };
}

/**
 * Validate event data against business rules
 * Pure function - no side effects
 */
export function validateEvent(event: NormalizedEvent): EventValidationResult {
  const validation: EventValidationResult = {
    isValid: true,
    errors: [],
    warnings: [],
  };

  // Required fields
  if (!event.polymarketId) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event ID is required',
      field: 'polymarketId',
    });
    validation.isValid = false;
  }

  if (!event.title) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event title is required',
      field: 'title',
    });
    validation.isValid = false;
  }

  if (!event.category) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.MISSING_REQUIRED_FIELD,
      message: 'Event category is required',
      field: 'category',
    });
    validation.isValid = false;
  }

  // Field length validation
  if (event.title.length > TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.CONTENT_TOO_LONG,
      message: `Title exceeds maximum length of ${TYPE_CONVERSION.LIMITS.MAX_TITLE_LENGTH}`,
      field: 'title',
    });
    validation.isValid = false;
  }

  if (event.description && event.description.length > TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.CONTENT_TOO_LONG,
      message: `Description exceeds maximum length of ${TYPE_CONVERSION.LIMITS.MAX_DESCRIPTION_LENGTH}`,
      field: 'description',
    });
    validation.isValid = false;
  }

  // Numeric range validation
  if (event.relevanceScore < CONTENT_CONSTANTS.RELEVANCE_SCORES.MIN ||
      event.relevanceScore > CONTENT_CONSTANTS.RELEVANCE_SCORES.MAX) {
    validation.errors.push({
      code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_ID,
      message: `Relevance score must be between ${CONTENT_CONSTANTS.RELEVANCE_SCORES.MIN} and ${CONTENT_CONSTANTS.RELEVANCE_SCORES.MAX}`,
      field: 'relevanceScore',
    });
    validation.isValid = false;
  }

  // Business logic validation
  if (event.closed && event.active) {
    validation.warnings.push({
      code: 'CONFLICTING_STATUS',
      message: 'Event is both active and closed',
    });
  }

  if (event.archived && event.active) {
    validation.warnings.push({
      code: 'CONFLICTING_STATUS',
      message: 'Event is both active and archived',
    });
  }

  // Date validation
  if (event.startDate && event.endDate) {
    const startDate = new Date(event.startDate);
    const endDate = new Date(event.endDate);

    if (startDate > endDate) {
      validation.errors.push({
        code: ERROR_CONSTANTS.VALIDATION_CODES.INVALID_TIMESTAMP,
        message: 'Start date cannot be after end date',
        field: 'dates',
      });
      validation.isValid = false;
    }
  }

  return validation;
}

/**
 * Generate slug from text
 * Pure function - no side effects
 */
export function generateSlug(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\w\s-]/g, '') // Remove special characters
    .replace(/\s+/g, '-') // Replace spaces with hyphens
    .replace(/-+/g, '-') // Replace multiple hyphens with single
    .replace(/^-|-$/g, '') // Remove leading/trailing hyphens
    .substring(0, TYPE_CONVERSION.LIMITS.MAX_SLUG_LENGTH);
}

// Helper functions

function normalizeId(id: string | undefined): string | undefined {
  if (!id || typeof id !== 'string') return undefined;
  const trimmed = id.trim();
  return trimmed.length > 0 && trimmed.length <= TYPE_CONVERSION.LIMITS.MAX_ID_LENGTH ? trimmed : undefined;
}

function normalizeText(text: string | undefined, maxLength?: number): string | undefined {
  if (!text || typeof text !== 'string') return undefined;

  let trimmed = text.trim();
  if (maxLength) {
    trimmed = trimmed.substring(0, maxLength);
  }

  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeCategory(category: string | undefined, normalize: boolean): string | undefined {
  if (!category || typeof category !== 'string') return undefined;

  let normalized = category.trim().toLowerCase();

  if (normalize && CONTENT_CONSTANTS.CATEGORY_NORMALIZATION[normalized as keyof typeof CONTENT_CONSTANTS.CATEGORY_NORMALIZATION]) {
    normalized = CONTENT_CONSTANTS.CATEGORY_NORMALIZATION[normalized as keyof typeof CONTENT_CONSTANTS.CATEGORY_NORMALIZATION];
  }

  return normalized;
}

function normalizeSlug(slug: string | undefined): string {
  if (!slug) return '';

  return slug
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .substring(0, TYPE_CONVERSION.LIMITS.MAX_SLUG_LENGTH);
}

function normalizeNumber(value: number | string | undefined, defaultValue: number): number {
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

function normalizeBoolean(value: boolean | string | undefined, defaultValue: boolean): boolean {
  if (value === undefined || value === null) return defaultValue;

  if (typeof value === 'boolean') return value;

  if (typeof value === 'string') {
    const lower = value.toLowerCase().trim();
    return lower === 'true' || lower === '1' || lower === 'yes';
  }

  return defaultValue;
}

function normalizeDateString(dateString: string | undefined): string | undefined {
  if (!dateString) return undefined;

  const parsed = normalizeDate(dateString);
  return parsed.isValid ? parsed.normalizedISO : undefined;
}

function isPoliticsContent(title?: string, description?: string, category?: string): boolean {
  const content = [title, description, category].filter(Boolean).join(' ').toLowerCase();

  return CONTENT_CONSTANTS.POLITICS_KEYWORDS.some(keyword =>
    content.includes(keyword.toLowerCase())
  );
}

function isSpamContent(title?: string, description?: string): boolean {
  const content = [title, description].filter(Boolean).join(' ');

  return CONTENT_CONSTANTS.SPAM_PATTERNS.some(pattern => pattern.test(content));
}

function calculateRelevanceScore(params: {
  title?: string;
  description?: string;
  category?: string;
  liquidity: number;
  volume24h: number;
  active: boolean;
  closed: boolean;
}): number {
  let score = CONTENT_CONSTANTS.RELEVANCE_SCORES.MIN;

  if (params.title && params.category) {
    score += 20;
  }

  if (params.description) {
    score += 10;
  }

  if (params.active && !params.closed) {
    score += 25;
  } else if (params.active) {
    score += 10;
  }

  if (params.liquidity > 100000) {
    score += 20;
  } else if (params.liquidity > 10000) {
    score += 15;
  } else if (params.liquidity > 1000) {
    score += 10;
  } else if (params.liquidity > 100) {
    score += 5;
  }

  if (params.volume24h > 100000) {
    score += 15;
  } else if (params.volume24h > 10000) {
    score += 10;
  } else if (params.volume24h > 1000) {
    score += 5;
  }

  const popularCategories = ['sports', 'politics', 'cryptocurrency', 'technology'];
  if (params.category && popularCategories.includes(params.category.toLowerCase())) {
    score += 10;
  }

  return Math.min(Math.max(score, CONTENT_CONSTANTS.RELEVANCE_SCORES.MIN), CONTENT_CONSTANTS.RELEVANCE_SCORES.MAX);
}
