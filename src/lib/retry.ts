/**
 * Exponential backoff retry utility with jitter and circuit breaker patterns
 * Provides configurable retry strategies, error classification, and TypeScript generics
 */

import { RETRY_CONFIG } from '../config';
import { Logger, rootLogger } from './logger';

/**
 * Retry strategy types
 */
export enum RetryStrategyType {
  EXPONENTIAL = 'exponential',
  LINEAR = 'linear',
  FIXED = 'fixed',
  CUSTOM = 'custom',
}

/**
 * Error classification types
 */
export enum ErrorType {
  RETRYABLE = 'retryable',
  NON_RETRYABLE = 'non_retryable',
  RATE_LIMIT = 'rate_limit',
  TIMEOUT = 'timeout',
  NETWORK = 'network',
  AUTHENTICATION = 'authentication',
  AUTHORIZATION = 'authorization',
  SERVER_ERROR = 'server_error',
  CLIENT_ERROR = 'client_error',
}

/**
 * Circuit breaker states
 */
export enum CircuitBreakerState {
  CLOSED = 'closed',     // Normal operation
  OPEN = 'open',         // Circuit is open, calls fail immediately
  HALF_OPEN = 'half_open', // Testing if service has recovered
}

/**
 * Retry configuration interface
 */
export interface RetryConfig {
  maxAttempts: number;
  strategy: RetryStrategyType;
  baseDelay: number;
  maxDelay: number;
  multiplier?: number;
  jitter?: boolean;
  jitterAmount?: number;
  retryableErrors?: string[];
  nonRetryableErrors?: string[];
  onRetry?: (attempt: number, error: Error, delay: number) => void;
  onSuccess?: (attempt: number, result: any) => void;
  onFailure?: (error: Error) => void;
}

/**
 * Circuit breaker configuration
 */
export interface CircuitBreakerConfig {
  failureThreshold: number;
  resetTimeout: number;
  monitoringPeriod: number;
  expectedRecoveryTime?: number;
  onStateChange?: (state: CircuitBreakerState, reason: string) => void;
}

/**
 * Retry result interface
 */
export interface RetryResult<T> {
  success: boolean;
  result?: T;
  error?: Error;
  attempts: number;
  totalDuration: number;
  lastDelay?: number;
}

/**
 * Circuit breaker interface
 */
export interface CircuitBreaker {
  execute<T>(fn: () => Promise<T> | T): Promise<T>;
  getState(): CircuitBreakerState;
  getStats(): CircuitBreakerStats;
  reset(): void;
  isOpen(): boolean;
}

/**
 * Circuit breaker statistics
 */
export interface CircuitBreakerStats {
  state: CircuitBreakerState;
  failureCount: number;
  successCount: number;
  totalRequests: number;
  lastFailureTime?: Date;
  lastSuccessTime?: Date;
  nextRetryTime?: Date;
}

/**
 * Enhanced retry class
 */
export class RetryManager {
  private logger: Logger;
  private config: Partial<RetryConfig>;
  private circuitBreakers: Map<string, CircuitBreaker> = new Map();

  constructor(logger: Logger, config: Partial<RetryConfig> = {}) {
    this.logger = logger;
    this.config = {
      maxAttempts: RETRY_CONFIG.MAX_ATTEMPTS.API_REQUEST,
      strategy: RetryStrategyType.EXPONENTIAL,
      baseDelay: RETRY_CONFIG.BACKOFF.INITIAL_DELAY,
      maxDelay: RETRY_CONFIG.BACKOFF.MAX_DELAY,
      multiplier: RETRY_CONFIG.BACKOFF.MULTIPLIER,
      jitter: RETRY_CONFIG.BACKOFF.JITTER,
      ...config,
    };
  }

  /**
   * Execute function with retry logic
   */
  async execute<T>(
    fn: () => Promise<T> | T,
    name: string = 'operation',
    customConfig?: Partial<RetryConfig>
  ): Promise<RetryResult<T>> {
    const config = { ...this.config, ...customConfig };
    const startTime = Date.now();
    let lastError: Error | undefined;
    let lastDelay = 0;

    for (let attempt = 1; attempt <= config.maxAttempts!; attempt++) {
      try {
        const result = await this.attempt(fn, attempt, config.maxAttempts!);

        if (config.onSuccess) {
          config.onSuccess(attempt, result);
        }

        this.logger.debug(`${name} succeeded`, {
          name,
          attempt,
          duration: Date.now() - startTime,
        });

        return {
          success: true,
          result,
          attempts: attempt,
          totalDuration: Date.now() - startTime,
          lastDelay,
        };
      } catch (error) {
        lastError = error as Error;

        // Check if error is retryable
        const errorType = this.classifyError(lastError);
        if (errorType === ErrorType.NON_RETRYABLE) {
          this.logger.debug(`${name} failed with non-retryable error`, {
            name,
            attempt,
            error: lastError ?? new Error('Unknown error'),
            errorType,
          });
          break;
        }

        // If this is the last attempt, don't retry
        if (attempt === config.maxAttempts!) {
          this.logger.debug(`${name} failed after ${attempt} attempts`, {
            name,
            attempt,
            error: lastError ?? new Error('Unknown error'),
            errorType,
          });
          break;
        }

        // Calculate delay for next attempt
        const delay = this.calculateDelay(attempt, config);
        lastDelay = delay;

        this.logger.debug(`${name} failed, retrying in ${delay}ms`, {
          name,
          attempt,
          delay,
          error: lastError ?? new Error('Unknown error'),
          errorType,
        });

        if (config.onRetry) {
          config.onRetry(attempt, lastError, delay);
        }

        // Wait before next attempt
        await this.sleep(delay);
      }
    }

    if (config.onFailure) {
      config.onFailure(lastError!);
    }

    return {
      success: false,
      error: lastError || new Error('Operation failed'),
      attempts: config.maxAttempts!,
      totalDuration: Date.now() - startTime,
      lastDelay,
    };
  }

  /**
   * Execute function with circuit breaker
   */
  async executeWithCircuitBreaker<T>(
    fn: () => Promise<T> | T,
    name: string,
    retryConfig?: Partial<RetryConfig>,
    circuitBreakerConfig?: CircuitBreakerConfig
  ): Promise<RetryResult<T>> {
    const circuitBreaker = this.getCircuitBreaker(name, circuitBreakerConfig);

    if (circuitBreaker.isOpen()) {
      const error = new Error(`Circuit breaker is open for ${name}`);
      return {
        success: false,
        error,
        attempts: 0,
        totalDuration: 0,
      };
    }

    return this.execute(async () => circuitBreaker.execute(fn), name, retryConfig);
  }

  /**
   * Create or get circuit breaker
   */
  getCircuitBreaker(name: string, config?: CircuitBreakerConfig): CircuitBreaker {
    if (!this.circuitBreakers.has(name)) {
      this.circuitBreakers.set(name, new CircuitBreakerImpl(
        name,
        config || {
          failureThreshold: 5,
          resetTimeout: 60000,
          monitoringPeriod: 300000,
        },
        this.logger
      ));
    }

    return this.circuitBreakers.get(name)!;
  }

  /**
   * Execute a single attempt
   */
  private async attempt<T>(fn: () => Promise<T> | T, _attempt: number, _maxAttempts: number): Promise<T> {
    try {
      const result = await fn();
      return result;
    } catch (error) {
      throw error;
    }
  }

  /**
   * Calculate delay based on strategy
   */
  private calculateDelay(attempt: number, config: Partial<RetryConfig>): number {
    let delay: number;

    switch (config.strategy) {
      case RetryStrategyType.EXPONENTIAL:
        delay = config.baseDelay! * Math.pow(config.multiplier || 2, attempt - 1);
        break;
      case RetryStrategyType.LINEAR:
        delay = config.baseDelay! * attempt;
        break;
      case RetryStrategyType.FIXED:
        delay = config.baseDelay!;
        break;
      case RetryStrategyType.CUSTOM:
        // For custom strategy, call a custom function if provided
        delay = config.baseDelay!; // Default to base delay
        break;
      default:
        delay = config.baseDelay!;
    }

    // Apply maximum delay limit
    delay = Math.min(delay, config.maxDelay!);

    // Apply jitter if enabled
    if (config.jitter) {
      const jitterAmount = config.jitterAmount || delay * 0.1;
      const jitter = Math.random() * jitterAmount;
      delay = delay + (Math.random() > 0.5 ? jitter : -jitter);
      delay = Math.max(0, delay); // Ensure non-negative
    }

    return Math.floor(delay);
  }

  /**
   * Classify error for retry logic
   */
  private classifyError(error: Error): ErrorType {
    const errorMessage = error.message.toLowerCase();
    const errorCode = (error as any).code;

    // Check for non-retryable errors first
    if (this.isNonRetryableError(errorMessage, errorCode)) {
      return ErrorType.NON_RETRYABLE;
    }

    // Check for rate limiting
    if (this.isRateLimitError(errorMessage, errorCode)) {
      return ErrorType.RATE_LIMIT;
    }

    // Check for timeout
    if (this.isTimeoutError(errorMessage, errorCode)) {
      return ErrorType.TIMEOUT;
    }

    // Check for network errors
    if (this.isNetworkError(errorMessage, errorCode)) {
      return ErrorType.NETWORK;
    }

    // Check for authentication errors
    if (this.isAuthenticationError(errorMessage, errorCode)) {
      return ErrorType.AUTHENTICATION;
    }

    // Check for server errors (5xx)
    if (this.isServerError(errorMessage, errorCode)) {
      return ErrorType.SERVER_ERROR;
    }

    // Check for client errors (4xx)
    if (this.isClientError(errorMessage, errorCode)) {
      return ErrorType.CLIENT_ERROR;
    }

    // Default to retryable for unknown errors
    return ErrorType.RETRYABLE;
  }

  /**
   * Check if error is non-retryable
   */
  private isNonRetryableError(message: string, code?: string): boolean {
    const nonRetryablePatterns = [
      'authentication failed',
      'unauthorized',
      'forbidden',
      'not found',
      'invalid request',
      'bad request',
      'validation error',
    ];

    return nonRetryablePatterns.some(pattern => message.includes(pattern)) ||
           ['EACCES', 'EPERM', 'EINVAL'].includes(code || '');
  }

  /**
   * Check if error is rate limiting
   */
  private isRateLimitError(message: string, code?: string): boolean {
    return message.includes('rate limit') ||
           message.includes('too many requests') ||
           code === 'RATE_LIMIT_EXCEEDED' ||
           Boolean(code && code.includes('429'));
  }

  /**
   * Check if error is timeout
   */
  private isTimeoutError(message: string, code?: string): boolean {
    return message.includes('timeout') ||
           message.includes('timed out') ||
           code === 'ETIMEDOUT' ||
           code === 'TIMEOUT';
  }

  /**
   * Check if error is network-related
   */
  private isNetworkError(message: string, code?: string): boolean {
    return RETRY_CONFIG.RETRYABLE_ERRORS.some(retryableError =>
      message.includes(retryableError.toLowerCase())
    ) ||
           ['ECONNRESET', 'ECONNREFUSED', 'ENOTFOUND', 'ENETUNREACH'].includes(code || '');
  }

  /**
   * Check if error is authentication
   */
  private isAuthenticationError(message: string, code?: string): boolean {
    return message.includes('authentication') ||
           message.includes('unauthorized') ||
           code === 'EAUTH' ||
           code === 'AUTH_FAILED';
  }

  /**
   * Check if error is server error
   */
  private isServerError(message: string, code?: string): boolean {
    return message.includes('internal server error') ||
           message.includes('service unavailable') ||
           Boolean(code && code.startsWith('5'));
  }

  /**
   * Check if error is client error
   */
  private isClientError(message: string, code?: string): boolean {
    return message.includes('bad request') ||
           message.includes('invalid request') ||
           Boolean(code && code.startsWith('4'));
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Reset all circuit breakers
   */
  resetAllCircuitBreakers(): void {
    this.circuitBreakers.forEach(cb => cb.reset());
  }

  /**
   * Get all circuit breaker statistics
   */
  getAllCircuitBreakerStats(): Record<string, CircuitBreakerStats> {
    const stats: Record<string, CircuitBreakerStats> = {};
    this.circuitBreakers.forEach((cb, name) => {
      stats[name] = cb.getStats();
    });
    return stats;
  }
}

/**
 * Circuit breaker implementation
 */
class CircuitBreakerImpl implements CircuitBreaker {
  private name: string;
  private config: CircuitBreakerConfig;
  private logger: Logger;
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED;
  private failureCount: number = 0;
  private successCount: number = 0;
  private totalRequests: number = 0;
  private lastFailureTime: Date | undefined;
  private lastSuccessTime: Date | undefined;
  private nextRetryTime: Date | undefined;

  constructor(name: string, config: CircuitBreakerConfig, logger: Logger) {
    this.name = name;
    this.config = config;
    this.logger = logger;
  }

  /**
   * Execute function through circuit breaker
   */
  async execute<T>(fn: () => Promise<T> | T): Promise<T> {
    this.totalRequests++;

    // Check if circuit is open
    if (this.state === CircuitBreakerState.OPEN) {
      if (this.shouldAttemptReset()) {
        this.transitionToHalfOpen();
      } else {
        throw new Error(`Circuit breaker '${this.name}' is open`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  /**
   * Get current circuit breaker state
   */
  getState(): CircuitBreakerState {
    return this.state;
  }

  /**
   * Get circuit breaker statistics
   */
  getStats(): CircuitBreakerStats {
    return {
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      totalRequests: this.totalRequests,
      lastFailureTime: this.lastFailureTime || new Date(),
      lastSuccessTime: this.lastSuccessTime || new Date(),
      nextRetryTime: this.nextRetryTime || new Date(),
    };
  }

  /**
   * Reset circuit breaker to closed state
   */
  reset(): void {
    this.state = CircuitBreakerState.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = undefined;
    this.lastSuccessTime = undefined;
    this.nextRetryTime = undefined;

    this.logger.debug(`Circuit breaker '${this.name}' reset`, {
      name: this.name,
      state: this.state,
    });
  }

  /**
   * Check if circuit is open
   */
  isOpen(): boolean {
    return this.state === CircuitBreakerState.OPEN;
  }

  /**
   * Handle successful operation
   */
  private onSuccess(): void {
    this.successCount++;
    this.lastSuccessTime = new Date();

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.transitionToClosed();
    }

    this.logger.debug(`Circuit breaker '${this.name}' operation succeeded`, {
      name: this.name,
      state: this.state,
      successCount: this.successCount,
      failureCount: this.failureCount,
    });
  }

  /**
   * Handle failed operation
   */
  private onFailure(): void {
    this.failureCount++;
    this.lastFailureTime = new Date();

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.transitionToOpen();
    } else if (this.failureCount >= this.config.failureThreshold) {
      this.transitionToOpen();
    }

    this.logger.debug(`Circuit breaker '${this.name}' operation failed`, {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      threshold: this.config.failureThreshold,
    });
  }

  /**
   * Transition to open state
   */
  private transitionToOpen(): void {
    this.state = CircuitBreakerState.OPEN;
    this.nextRetryTime = new Date(Date.now() + this.config.resetTimeout);

    if (this.config.onStateChange) {
      this.config.onStateChange(this.state, `Failure threshold reached: ${this.failureCount}`);
    }

    this.logger.warn(`Circuit breaker '${this.name}' opened`, {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      threshold: this.config.failureThreshold,
      nextRetryTime: this.nextRetryTime,
    });
  }

  /**
   * Transition to closed state
   */
  private transitionToClosed(): void {
    this.state = CircuitBreakerState.CLOSED;
    this.failureCount = 0;
    this.nextRetryTime = undefined;

    if (this.config.onStateChange) {
      this.config.onStateChange(this.state, 'Service recovered');
    }

    this.logger.info(`Circuit breaker '${this.name}' closed`, {
      name: this.name,
      state: this.state,
      successCount: this.successCount,
    });
  }

  /**
   * Transition to half-open state
   */
  private transitionToHalfOpen(): void {
    this.state = CircuitBreakerState.HALF_OPEN;

    if (this.config.onStateChange) {
      this.config.onStateChange(this.state, 'Testing service recovery');
    }

    this.logger.info(`Circuit breaker '${this.name}' half-open`, {
      name: this.name,
      state: this.state,
    });
  }

  /**
   * Check if should attempt reset
   */
  private shouldAttemptReset(): boolean {
    if (!this.nextRetryTime) {
      return false;
    }

    return new Date() >= this.nextRetryTime;
  }
}

/**
 * Utility functions for retry operations
 */
export const retryUtils = {
  /**
   * Create retry manager with default configuration
   */
  createRetryManager(logger: Logger, config?: Partial<RetryConfig>): RetryManager {
    return new RetryManager(logger, config);
  },

  /**
   * Execute with exponential backoff (simple version)
   */
  async withExponentialBackoff<T>(
    fn: () => Promise<T>,
    maxAttempts: number = 3,
    baseDelay: number = 1000,
    maxDelay: number = 30000,
    logger?: Logger
  ): Promise<T> {
    const retryManager = new RetryManager(
      logger || console as any,
      {
        maxAttempts,
        baseDelay,
        maxDelay,
        strategy: RetryStrategyType.EXPONENTIAL,
        jitter: true,
      }
    );

    const result = await retryManager.execute(fn);
    if (!result.success) {
      throw result.error;
    }
    return result.result!;
  },

  /**
   * Execute with fixed delay
   */
  async withFixedDelay<T>(
    fn: () => Promise<T>,
    maxAttempts: number = 3,
    delay: number = 1000,
    logger?: Logger
  ): Promise<T> {
    const retryManager = new RetryManager(
      logger || console as any,
      {
        maxAttempts,
        baseDelay: delay,
        strategy: RetryStrategyType.FIXED,
        jitter: false,
      }
    );

    const result = await retryManager.execute(fn);
    if (!result.success) {
      throw result.error;
    }
    return result.result!;
  },

  /**
   * Create circuit breaker for service
   */
  createCircuitBreaker(
    name: string,
    failureThreshold: number = 5,
    resetTimeout: number = 60000,
    logger?: Logger
  ): CircuitBreaker {
    return new CircuitBreakerImpl(
      name,
      {
        failureThreshold,
        resetTimeout,
        monitoringPeriod: 300000,
      },
      logger || console as any
    );
  },

  /**
   * Sleep utility
   */
  sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  },
};

/**
 * Default retry manager instance
 */
export const defaultRetryManager = new RetryManager(rootLogger);

/**
 * Export default RetryManager
 */
export default RetryManager;
