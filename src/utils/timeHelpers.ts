/**
 * Time and date utility functions for SignalCast data processing
 * Pure functions for timezone handling, parsing, formatting, and validation
 */

import { TIME_CONSTANTS } from './constants';

/**
 * Timezone information
 */
export interface TimezoneInfo {
  name: string;
  offset: number; // Offset from UTC in minutes
  isDST: boolean;
  abbreviation: string;
}

/**
 * Date range interface
 */
export interface DateRange {
  start: Date;
  end: Date;
  duration: number; // Duration in milliseconds
}

/**
 * Parsed date information
 */
export interface ParsedDate {
  date: Date;
  isValid: boolean;
  timezone?: string | undefined;
  originalFormat?: string | undefined;
  normalizedISO: string;
  timestamp: number;
}

/**
 * Validate and normalize a date string or timestamp
 * Pure function - no side effects
 */
export function normalizeDate(input: string | number | Date): ParsedDate {
  let date: Date;
  let originalFormat: string | undefined;
  let timezone: string | undefined;

  try {
    if (typeof input === 'number') {
      // Handle Unix timestamp (seconds or milliseconds)
      if (input > 1e12) {
        // Milliseconds timestamp
        date = new Date(input);
        originalFormat = 'timestamp_ms';
      } else {
        // Seconds timestamp
        date = new Date(input * 1000);
        originalFormat = 'timestamp_s';
      }
    } else if (typeof input === 'string') {
      // Try to detect the format
      if (input.includes('T') && input.includes('Z')) {
        // ISO 8601 format
        date = new Date(input);
        originalFormat = 'iso8601';
      } else if (input.includes('-') && input.includes(':')) {
        // Likely RFC 2822 or similar
        date = new Date(input);
        originalFormat = 'rfc2822';
      } else if (/^\d{4}-\d{2}-\d{2}$/.test(input)) {
        // Date only
        date = new Date(input + 'T00:00:00Z');
        originalFormat = 'date_only';
        timezone = 'UTC';
      } else {
        // Try generic parsing
        date = new Date(input);
        originalFormat = 'generic';
      }
    } else if (input instanceof Date) {
      date = new Date(input.getTime());
      originalFormat = 'date_object';
    } else {
      throw new Error('Invalid input type');
    }

    // Validate the date
    const isValid = !isNaN(date.getTime()) && date.getFullYear() >= 1970 && date.getFullYear() <= 2100;

    return {
      date,
      isValid,
      timezone,
      originalFormat,
      normalizedISO: isValid ? date.toISOString() : new Date().toISOString(),
      timestamp: isValid ? date.getTime() : Date.now(),
    };
  } catch (error) {
    return {
      date: new Date(),
      isValid: false,
      originalFormat: 'error',
      normalizedISO: new Date().toISOString(),
      timestamp: Date.now(),
    };
  }
}

/**
 * Get timezone information for a given date
 * Pure function - no side effects
 */
export function getTimezoneInfo(date: Date = new Date()): TimezoneInfo {
  const utcDate = new Date(date.toUTCString());
  const localDate = new Date(date.toString());

  // Calculate offset in minutes
  const offset = (localDate.getTime() - utcDate.getTime()) / (60 * 1000);

  // Detect timezone (simplified approach)
  const timezoneString = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const abbreviation = getTimezoneAbbreviation(offset, date);

  return {
    name: timezoneString,
    offset: Math.round(offset),
    isDST: isDaylightSavingTime(date),
    abbreviation,
  };
}

/**
 * Convert date to different timezone
 * Pure function - no side effects
 */
export function convertToTimezone(date: Date, timezone: string): Date {
  try {
    // Use Intl.DateTimeFormat for timezone conversion
    const formatter = new Intl.DateTimeFormat('en-US', {
      timeZone: timezone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    });

    const parts = formatter.formatToParts(date);
    const year = parseInt(parts.find(p => p.type === 'year')?.value || '0');
    const month = parseInt(parts.find(p => p.type === 'month')?.value || '0') - 1;
    const day = parseInt(parts.find(p => p.type === 'day')?.value || '0');
    const hour = parseInt(parts.find(p => p.type === 'hour')?.value || '0');
    const minute = parseInt(parts.find(p => p.type === 'minute')?.value || '0');
    const second = parseInt(parts.find(p => p.type === 'second')?.value || '0');

    return new Date(year, month, day, hour, minute, second);
  } catch (error) {
    // Fallback to original date if timezone conversion fails
    return new Date(date.getTime());
  }
}

/**
 * Format date according to specified format
 * Pure function - no side effects
 */
export function formatDate(
  date: Date,
  format: 'iso' | 'short' | 'medium' | 'long' | 'timestamp' | 'custom',
  customFormat?: string
): string {
  switch (format) {
    case 'iso':
      return date.toISOString();

    case 'timestamp':
      return date.getTime().toString();

    case 'short':
      return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });

    case 'medium':
      return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });

    case 'long':
      return date.toLocaleDateString('en-US', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short',
      });

    case 'custom':
      if (!customFormat) {
        return date.toISOString();
      }
      return applyCustomFormat(date, customFormat);

    default:
      return date.toISOString();
  }
}

/**
 * Calculate time difference between two dates
 * Pure function - no side effects
 */
export function timeDifference(from: Date, to: Date = new Date()): {
  milliseconds: number;
  seconds: number;
  minutes: number;
  hours: number;
  days: number;
  weeks: number;
  months: number;
  years: number;
  humanReadable: string;
} {
  const diff = to.getTime() - from.getTime();
  const absoluteDiff = Math.abs(diff);
  const isPast = diff < 0;

  const milliseconds = absoluteDiff;
  const seconds = Math.floor(milliseconds / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  const weeks = Math.floor(days / 7);
  const months = Math.floor(days / 30.44); // Average month length
  const years = Math.floor(days / 365.25); // Account for leap years

  // Create human readable string
  let humanReadable: string;
  let value: number;
  let unit: string;

  if (years > 0) {
    value = years;
    unit = years === 1 ? 'year' : 'years';
  } else if (months > 0) {
    value = months;
    unit = months === 1 ? 'month' : 'months';
  } else if (weeks > 0) {
    value = weeks;
    unit = weeks === 1 ? 'week' : 'weeks';
  } else if (days > 0) {
    value = days;
    unit = days === 1 ? 'day' : 'days';
  } else if (hours > 0) {
    value = hours;
    unit = hours === 1 ? 'hour' : 'hours';
  } else if (minutes > 0) {
    value = minutes;
    unit = minutes === 1 ? 'minute' : 'minutes';
  } else if (seconds > 0) {
    value = seconds;
    unit = seconds === 1 ? 'second' : 'seconds';
  } else {
    value = milliseconds;
    unit = milliseconds === 1 ? 'millisecond' : 'milliseconds';
  }

  humanReadable = `${value} ${unit} ${isPast ? 'ago' : 'in the future'}`;

  return {
    milliseconds: diff,
    seconds,
    minutes,
    hours,
    days,
    weeks,
    months,
    years,
    humanReadable,
  };
}

/**
 * Check if a date is within a specific range
 * Pure function - no side effects
 */
export function isDateInRange(date: Date, range: DateRange): boolean {
  return date.getTime() >= range.start.getTime() && date.getTime() <= range.end.getTime();
}

/**
 * Get date range for a specific period
 * Pure function - no side effects
 */
export function getDateRange(
  from: Date,
  duration: number,
  unit: 'milliseconds' | 'seconds' | 'minutes' | 'hours' | 'days' | 'weeks' | 'months' | 'years'
): DateRange {
  const start = new Date(from.getTime());
  let end: Date;

  switch (unit) {
    case 'milliseconds':
      end = new Date(start.getTime() + duration);
      break;

    case 'seconds':
      end = new Date(start.getTime() + duration * TIME_CONSTANTS.MILLISECONDS_PER_SECOND);
      break;

    case 'minutes':
      end = new Date(start.getTime() + duration * TIME_CONSTANTS.MILLISECONDS_PER_MINUTE);
      break;

    case 'hours':
      end = new Date(start.getTime() + duration * TIME_CONSTANTS.MILLISECONDS_PER_HOUR);
      break;

    case 'days':
      end = new Date(start.getTime() + duration * TIME_CONSTANTS.MILLISECONDS_PER_DAY);
      break;

    case 'weeks':
      end = new Date(start.getTime() + duration * TIME_CONSTANTS.MILLISECONDS_PER_WEEK);
      break;

    case 'months':
      end = new Date(start);
      end.setMonth(end.getMonth() + duration);
      break;

    case 'years':
      end = new Date(start);
      end.setFullYear(end.getFullYear() + duration);
      break;

    default:
      end = new Date(start.getTime() + duration);
  }

  return {
    start,
    end,
    duration: end.getTime() - start.getTime(),
  };
}

/**
 * Check if date is recent (within specified time window)
 * Pure function - no side effects
 */
export function isRecent(date: Date, windowMs: number = TIME_CONSTANTS.CACHE_TTL.EVENT_DATA): boolean {
  const now = new Date();
  const diff = now.getTime() - date.getTime();
  return diff >= 0 && diff <= windowMs;
}

/**
 * Get start and end of day for a given date
 * Pure function - no side effects
 */
export function getDayBoundaries(date: Date = new Date(), timezone: string = 'UTC'): { start: Date; end: Date } {
  const tzDate = convertToTimezone(date, timezone);

  const start = new Date(tzDate);
  start.setHours(0, 0, 0, 0);

  const end = new Date(tzDate);
  end.setHours(23, 59, 59, 999);

  return { start, end };
}

/**
 * Get start and end of week for a given date
 * Pure function - no side effects
 */
export function getWeekBoundaries(date: Date = new Date(), timezone: string = 'UTC'): { start: Date; end: Date } {
  const tzDate = convertToTimezone(date, timezone);
  const day = tzDate.getDay(); // 0 = Sunday, 6 = Saturday

  const start = new Date(tzDate);
  start.setDate(tzDate.getDate() - day);
  start.setHours(0, 0, 0, 0);

  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  end.setHours(23, 59, 59, 999);

  return { start, end };
}

/**
 * Get start and end of month for a given date
 * Pure function - no side effects
 */
export function getMonthBoundaries(date: Date = new Date(), timezone: string = 'UTC'): { start: Date; end: Date } {
  const tzDate = convertToTimezone(date, timezone);

  const start = new Date(tzDate);
  start.setDate(1);
  start.setHours(0, 0, 0, 0);

  const end = new Date(start);
  end.setMonth(start.getMonth() + 1);
  end.setDate(0); // Last day of current month
  end.setHours(23, 59, 59, 999);

  return { start, end };
}

/**
 * Round time to nearest interval
 * Pure function - no side effects
 */
export function roundTimeToInterval(date: Date, intervalMs: number): Date {
  const timestamp = date.getTime();
  const roundedTimestamp = Math.round(timestamp / intervalMs) * intervalMs;
  return new Date(roundedTimestamp);
}

/**
 * Check if a timestamp is valid for processing
 * Pure function - no side effects
 */
export function isValidTimestamp(timestamp: number): {
  isValid: boolean;
  isFuture: boolean;
  isTooOld: boolean;
  reason?: string;
} {
  const now = Date.now();
  const oneYearAgo = now - TIME_CONSTANTS.MILLISECONDS_PER_DAY * 365;
  const oneYearFromNow = now + TIME_CONSTANTS.MILLISECONDS_PER_DAY * 365;

  if (isNaN(timestamp) || timestamp <= 0) {
    return {
      isValid: false,
      isFuture: false,
      isTooOld: false,
      reason: 'Invalid timestamp',
    };
  }

  if (timestamp > oneYearFromNow) {
    return {
      isValid: false,
      isFuture: true,
      isTooOld: false,
      reason: 'Timestamp too far in the future',
    };
  }

  if (timestamp < oneYearAgo) {
    return {
      isValid: false,
      isFuture: false,
      isTooOld: true,
      reason: 'Timestamp too far in the past',
    };
  }

  return {
    isValid: true,
    isFuture: timestamp > now,
    isTooOld: false,
  };
}

// Helper functions (not exported)

/**
 * Get timezone abbreviation from offset
 */
function getTimezoneAbbreviation(offset: number, _date: Date): string {
  // Simplified timezone abbreviation logic
  if (offset === 0) return 'UTC';
  if (offset === -300) return 'EST';
  if (offset === -240) return 'EDT';
  if (offset === -360) return 'CST';
  if (offset === -300) return 'CDT';
  if (offset === -420) return 'MST';
  if (offset === -360) return 'MDT';
  if (offset === -480) return 'PST';
  if (offset === -420) return 'PDT';

  const sign = offset >= 0 ? '+' : '-';
  const hours = Math.floor(Math.abs(offset) / 60);
  const minutes = Math.abs(offset) % 60;
  return `UTC${sign}${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
}

/**
 * Check if date is during daylight saving time
 */
function isDaylightSavingTime(date: Date): boolean {
  const january = new Date(date.getFullYear(), 0, 1);
  const july = new Date(date.getFullYear(), 6, 1);
  const timezoneOffset = date.getTimezoneOffset();

  return Math.max(january.getTimezoneOffset(), july.getTimezoneOffset()) !== timezoneOffset;
}

/**
 * Apply custom date format string
 */
function applyCustomFormat(date: Date, format: string): string {
  return format
    .replace('YYYY', date.getFullYear().toString())
    .replace('YY', (date.getFullYear() % 100).toString().padStart(2, '0'))
    .replace('MM', (date.getMonth() + 1).toString().padStart(2, '0'))
    .replace('DD', date.getDate().toString().padStart(2, '0'))
    .replace('HH', date.getHours().toString().padStart(2, '0'))
    .replace('mm', date.getMinutes().toString().padStart(2, '0'))
    .replace('ss', date.getSeconds().toString().padStart(2, '0'))
    .replace('SSS', date.getMilliseconds().toString().padStart(3, '0'));
}
