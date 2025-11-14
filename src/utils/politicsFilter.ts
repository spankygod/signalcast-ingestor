/**
 * High-Performance Political Event Filter for SignalCast
 *
 * Purpose: Filter ONLY political events before queue insertion
 * Performance: Optimized for millions of events per day
 * Methodology: Deterministic keyword matching with false-positive exclusion
 */

// PRIMARY POLITICAL KEYWORDS (HIGH CONFIDENCE) - Immediate match if found
const PRIMARY_POLITICAL_KEYWORDS = [
  'trump', 'biden', 'democrat', 'republican', 'gop',
  'senate', 'congress', 'election', 'vote', 'primary',
  'debate', 'supreme court', 'governor', 'mayor',
  'politics', 'policy', 'minister', 'parliament',
  'house', 'representative', 'white house', 'president'
] as const;

// SECONDARY POLITICAL KEYWORDS (MEDIUM CONFIDENCE) - Require political context
const SECONDARY_POLITICAL_KEYWORDS = [
  'poll', 'approval', 'turnout', 'campaign', 'ballot'
] as const;

// Political context words for secondary keywords
const POLITICAL_CONTEXT_WORDS = [
  'state', 'country', 'candidate', 'party', 'president', 'government'
] as const;

// FALSE-POSITIVE EXCLUSIONS (IMMEDIATE REJECTION)
const SPORTS_EXCLUSIONS = [
  'nba', 'nfl', 'mlb', 'nhl', 'soccer', 'football',
  'basketball', 'ufc', 'fight', 'boxing', 'match',
  'tournament', 'championship', 'super bowl', 'playoffs'
] as const;

const SPORTS_TEAMS_PLAYERS = [
  'lakers', 'warriors', 'celtics', 'bucks', 'nuggets',
  'lebron', 'curry', 'giannis', 'durant', 'wembanyama'
] as const;

const SPORTS_TERMS = [
  'odds', 'spread', 'line', 'bookmaker'
] as const;

const CRYPTO_GOVERNANCE_EXCLUSIONS = [
  'dao', 'governance token', 'governance vote',
  'staking', 'crypto', 'blockchain', 'token'
] as const;

const FINANCIAL_EXCLUSIONS = [
  'stock', 'market cap', 'price', 'earnings'
] as const;

// Combined exclusion arrays for performance
const ALL_EXCLUSIONS = [
  ...SPORTS_EXCLUSIONS,
  ...SPORTS_TEAMS_PLAYERS,
  ...SPORTS_TERMS,
  ...CRYPTO_GOVERNANCE_EXCLUSIONS,
  ...FINANCIAL_EXCLUSIONS
] as const;

// Primary keyword set for O(1) lookup
const PRIMARY_KEYWORD_SET = new Set<string>(PRIMARY_POLITICAL_KEYWORDS);
const SECONDARY_KEYWORD_SET = new Set<string>(SECONDARY_POLITICAL_KEYWORDS);
const CONTEXT_WORD_SET = new Set<string>(POLITICAL_CONTEXT_WORDS);
const EXCLUSION_SET = new Set<string>(ALL_EXCLUSIONS);

/**
 * Event interface for political filtering
 */
export interface PoliticalEvent {
  slug: string;
  title: string;
  description?: string;
}

/**
 * Normalize text for keyword matching
 * Optimized: lowercase, strip punctuation, collapse spaces
 */
function normalizeText(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ')  // Replace punctuation with space
    .replace(/\s+/g, ' ')       // Collapse multiple spaces
    .trim();
}

/**
 * Create text blob from event fields
 */
function createTextBlob(event: PoliticalEvent): string {
  return normalizeText(`${event.slug} ${event.title} ${event.description || ''}`);
}

/**
 * Check if text contains any keyword from a set
 * Optimized: single pass with space boundaries
 */
function containsKeyword(text: string, keywordSet: ReadonlySet<string>): boolean {
  const words = text.split(' ');
  for (const word of words) {
    if (keywordSet.has(word)) {
      return true;
    }
  }
  return false;
}

/**
 * Check if text contains any exclusion keywords
 * Immediate rejection if any exclusion found
 */
function hasExclusions(text: string): boolean {
  // Check sports exclusions first (most common false positives)
  if (containsKeyword(text, EXCLUSION_SET)) {
    return true;
  }

  // Check multi-word exclusions that require substring matching
  const lowerText = text.toLowerCase();

  // Multi-word crypto exclusions
  if (lowerText.includes('governance token') ||
      lowerText.includes('governance vote') ||
      lowerText.includes('dao')) {
    return true;
  }

  // Multi-word financial exclusions
  if (lowerText.includes('market cap')) {
    return true;
  }

  return false;
}

/**
 * Check for primary political keywords (HIGH CONFIDENCE)
 * Returns true immediately if any primary keyword found
 */
function hasPrimaryKeywords(text: string): boolean {
  // Check single-word primary keywords
  if (containsKeyword(text, PRIMARY_KEYWORD_SET)) {
    return true;
  }

  // Check multi-word primary keywords
  const lowerText = text.toLowerCase();

  // Multi-word government/political terms
  if (lowerText.includes('supreme court') ||
      lowerText.includes('white house') ||
      lowerText.includes('united states')) {
    return true;
  }

  return false;
}

/**
 * Check for secondary political keywords with political context (MEDIUM CONFIDENCE)
 * Returns true only if secondary keyword AND political context found
 */
function hasSecondaryKeywordsWithContext(text: string): boolean {
  const lowerText = text.toLowerCase();

  // Check if any secondary keyword exists
  let hasSecondaryKeyword = false;
  for (const keyword of SECONDARY_POLITICAL_KEYWORDS) {
    if (lowerText.includes(keyword)) {
      hasSecondaryKeyword = true;
      break;
    }
  }

  if (!hasSecondaryKeyword) {
    return false;
  }

  // Check for political context words
  for (const contextWord of POLITICAL_CONTEXT_WORDS) {
    if (lowerText.includes(contextWord)) {
      return true;
    }
  }

  return false;
}

/**
 * Determine if an event is political
 *
 * Algorithm:
 * 1. Create normalized text blob from slug, title, description
 * 2. Check for exclusions (immediate false if found)
 * 3. Check for primary keywords (immediate true if found)
 * 4. Check for secondary keywords with context (true if both found)
 * 5. Default to false if no matches
 *
 * Performance: O(n) where n = number of words in text blob
 * Memory: Minimal, uses static sets for O(1) lookups
 */
export function isPolitical(event: PoliticalEvent): boolean {
  // Create normalized text blob for analysis
  const textBlob = createTextBlob(event);

  // Step 1: Check for exclusions (immediate rejection)
  if (hasExclusions(textBlob)) {
    return false;
  }

  // Step 2: Check for primary political keywords (immediate acceptance)
  if (hasPrimaryKeywords(textBlob)) {
    return true;
  }

  // Step 3: Check for secondary keywords with political context
  if (hasSecondaryKeywordsWithContext(textBlob)) {
    return true;
  }

  // Step 4: No political content detected
  return false;
}

/**
 * Batch process multiple events for political filtering
 * Optimized for processing large batches efficiently
 */
export function filterPoliticalEvents(events: PoliticalEvent[]): PoliticalEvent[] {
  return events.filter(event => isPolitical(event));
}

/**
 * Get statistics about political keyword matches in text
 * Useful for debugging and monitoring filter performance
 */
export function getPoliticalKeywordStats(text: string): {
  primaryMatches: string[];
  secondaryMatches: string[];
  contextMatches: string[];
  exclusionMatches: string[];
} {
  const normalized = normalizeText(text);
  const words = normalized.split(' ');

  const primaryMatches = words.filter(word => PRIMARY_KEYWORD_SET.has(word));
  const secondaryMatches = words.filter(word => SECONDARY_KEYWORD_SET.has(word));
  const contextMatches = words.filter(word => CONTEXT_WORD_SET.has(word));
  const exclusionMatches = words.filter(word => EXCLUSION_SET.has(word));

  return {
    primaryMatches: [...new Set(primaryMatches)],
    secondaryMatches: [...new Set(secondaryMatches)],
    contextMatches: [...new Set(contextMatches)],
    exclusionMatches: [...new Set(exclusionMatches)]
  };
}

// Legacy exports for backward compatibility - DO NOT USE IN NEW CODE
export interface ContentFilterConfig {
  enablePoliticsFilter?: boolean;
  enableSpamFilter?: boolean;
  enableQualityFilter?: boolean;
  politicsThreshold?: number;
  spamThreshold?: number;
  qualityThreshold?: number;
  customPoliticsKeywords?: string[];
  customSpamPatterns?: RegExp[];
  blockedCategories?: string[];
  allowedCategories?: string[];
}

export interface ContentAnalysisResult {
  isPolitics: boolean;
  isSpam: boolean;
  qualityScore: number;
  politicsScore: number;
  spamScore: number;
  contentFlags: Array<{
    type: 'politics' | 'spam' | 'quality' | 'category';
    severity: 'low' | 'medium' | 'high';
    reason: string;
    confidence: number;
  }>;
  recommendations: string[];
  shouldFilter: boolean;
}

export interface ContentMetadata {
  wordCount: number;
  characterCount: number;
  capsRatio: number;
  specialCharRatio: number;
  repeatedWords: number;
  hasEmojis: boolean;
  hasUrls: boolean;
  hasPhoneNumbers: boolean;
  hasEmails: boolean;
  language: string;
  sentiment?: {
    positive: number;
    negative: number;
    neutral: number;
  };
}

export interface FilterResult {
  allowed: boolean;
  originalContent: string;
  filteredContent?: string;
  analysis: ContentAnalysisResult;
  metadata: ContentMetadata;
  actions: Array<{
    type: 'filter' | 'modify' | 'flag' | 'queue_for_review';
    description: string;
  }>;
}

// Legacy functions for backward compatibility - DEPRECATED
export function analyzeContent(
  title: string,
  description?: string,
  _category?: string,
  tags?: string[],
  _config: ContentFilterConfig = {}
): ContentAnalysisResult {
  const event: PoliticalEvent = {
    slug: tags?.[0] || '',
    title,
    description: description ?? ''
  };

  return {
    isPolitics: isPolitical(event),
    isSpam: false,
    qualityScore: 0.5,
    politicsScore: isPolitical(event) ? 1.0 : 0.0,
    spamScore: 0.0,
    contentFlags: [],
    recommendations: [],
    shouldFilter: false,
  };
}

export function filterContent(
  title: string,
  description?: string,
  category?: string,
  tags?: string[],
  config: ContentFilterConfig = {}
): FilterResult {
  const analysis = analyzeContent(title, description, category, tags, config);
  const content = [title, description].filter(Boolean).join(' ');
  const wordCount = content.trim().split(/\s+/).length;

  return {
    allowed: !analysis.shouldFilter,
    originalContent: content,
    analysis,
    metadata: {
      wordCount,
      characterCount: content.length,
      capsRatio: (content.match(/[A-Z]/g) || []).length / content.length,
      specialCharRatio: (content.match(/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/g) || []).length / content.length,
      repeatedWords: 0,
      hasEmojis: false,
      hasUrls: false,
      hasPhoneNumbers: false,
      hasEmails: false,
      language: 'en',
    },
    actions: [],
  };
}

// Other legacy exports for compatibility
export function analyzePoliticsContent(
  content: string,
  _category?: string,
  _customKeywords: string[] = []
): {
  score: number;
  isPolitics: boolean;
  flags: Array<{
    type: 'politics';
    severity: 'low' | 'medium' | 'high';
    reason: string;
    confidence: number;
  }>;
} {
  const event: PoliticalEvent = {
    slug: '',
    title: content,
    description: ''
  };

  const isPol = isPolitical(event);

  return {
    score: isPol ? 1.0 : 0.0,
    isPolitics: isPol,
    flags: [],
  };
}
