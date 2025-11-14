/**
 * Structured logging system for SignalCast ingestion system
 * Provides different log levels, safe logging without secrets, and correlation IDs
 */

import pino, { type Logger as PinoLogger, type LoggerOptions } from 'pino';
import os from 'node:os';
import {
  config,
  isDevelopment,
  isProduction,
  isTest,
  features,
  LOGGING_CONSTANTS,
  APP_METADATA
} from '../config';

/**
 * Log levels based on configuration
 */
export enum LogLevel {
  ERROR = 0,
  WARN = 1,
  INFO = 2,
  DEBUG = 3,
  TRACE = 4,
}

/**
 * Log categories for different system components
 */
export enum LogCategory {
  SYSTEM = 'system',
  API = 'api',
  DATABASE = 'database',
  REDIS = 'redis',
  WEBSOCKET = 'websocket',
  WORKER = 'worker',
  AUTOSCALER = 'autoscaler',
  PERFORMANCE = 'performance',
  ERROR = 'error',
  SECURITY = 'security',
  AUDIT = 'audit',
}

/**
 * Log context interface
 */
export interface LogContext {
  correlationId?: string | null | undefined;
  workerId?: string | null | undefined;
  workerType?: string | null | undefined;
  requestId?: string | null | undefined;
  sessionId?: string | null | undefined;
  userId?: string | null | undefined;
  operation?: string | null | undefined;
  duration?: number | null | undefined;
  error?: Error | null | undefined;
  metadata?: Record<string, any> | null | undefined;
  category?: LogCategory | null | undefined;
  [key: string]: any;
}

/**
 * Structured log entry
 */
export interface LogEntry {
  level: LogLevel;
  category: LogCategory;
  message: string;
  timestamp: string;
  context?: LogContext;
  error?: {
    name: string;
    message: string;
    stack?: string;
    code?: string;
  };
  metrics?: Record<string, number>;
  tags?: string[];
}

/**
 * Logger configuration
 */
interface LoggerConfig {
  level: LogLevel;
  prettyPrint: boolean;
  colorize: boolean;
  timestamp: boolean;
  json: boolean;
  enabled: boolean;
  destination?: string;
  maxSize?: string;
  maxFiles?: number;
  environment?: string;
}

/**
 * Enhanced logger class
 */
export class Logger {
  private pinoLogger: PinoLogger;
  private correlationId: string | null = null;
  private defaultContext: LogContext = {};

  constructor(config: LoggerConfig, context: LogContext = {}) {
    this.defaultContext = context;

    // Build Pino configuration
    const pinoConfig: LoggerOptions = {
      level: this.getLevelName(config.level),
      timestamp: config.timestamp,
      formatters: {
        level: (label: string) => ({ level: label }),
        log: (object: Record<string, unknown>) => this.formatLog(object),
      },
      base: {
        pid: process.pid,
        hostname: os.hostname(),
        service: APP_METADATA.NAME,
        version: APP_METADATA.VERSION,
        environment: config.environment ?? 'development',
      },
    };

    // Configure pretty printing for development
    if (config.prettyPrint && !isProduction) {
      const prettyPrintConfig = {
        colorize: config.colorize,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname',
        messageFormat: '{category} [{workerId}] {msg}',
      };

      pinoConfig.transport = {
        target: 'pino-pretty',
        options: prettyPrintConfig,
      };
    }

    // Configure file output if destination is specified
    if (config.destination) {
      pinoConfig.transport = {
        target: 'pino/file',
        options: {
          destination: config.destination,
          mkdir: true,
        },
      };
    }

    this.pinoLogger = pino(pinoConfig);
  }

  /**
   * Log error message
   */
  error(message: string, context: LogContext = {}): void {
    this.log(LogLevel.ERROR, LogCategory.ERROR, message, context);
  }

  /**
   * Log warning message
   */
  warn(message: string, context: LogContext = {}): void {
    this.log(LogLevel.WARN, LogCategory.SYSTEM, message, context);
  }

  /**
   * Log info message
   */
  info(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.SYSTEM, message, context);
  }

  /**
   * Log debug message
   */
  debug(message: string, context: LogContext = {}): void {
    this.log(LogLevel.DEBUG, LogCategory.SYSTEM, message, context);
  }

  /**
   * Log trace message
   */
  trace(message: string, context: LogContext = {}): void {
    this.log(LogLevel.TRACE, LogCategory.SYSTEM, message, context);
  }

  /**
   * Log message with specific category
   */
  logCategory(category: LogCategory, level: LogLevel, message: string, context: LogContext = {}): void {
    this.log(level, category, message, context);
  }

  /**
   * Log API request/response
   */
  api(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.API, message, {
      ...context,
      category: LogCategory.API,
    });
  }

  /**
   * Log database operation
   */
  database(message: string, context: LogContext = {}): void {
    this.log(LogLevel.DEBUG, LogCategory.DATABASE, message, {
      ...context,
      category: LogCategory.DATABASE,
    });
  }

  /**
   * Log Redis operation
   */
  redis(message: string, context: LogContext = {}): void {
    this.log(LogLevel.DEBUG, LogCategory.REDIS, message, {
      ...context,
      category: LogCategory.REDIS,
    });
  }

  /**
   * Log WebSocket event
   */
  websocket(message: string, context: LogContext = {}): void {
    this.log(LogLevel.DEBUG, LogCategory.WEBSOCKET, message, {
      ...context,
      category: LogCategory.WEBSOCKET,
    });
  }

  /**
   * Log worker operation
   */
  worker(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.WORKER, message, {
      ...context,
      category: LogCategory.WORKER,
    });
  }

  /**
   * Log autoscaling event
   */
  autoscaler(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.AUTOSCALER, message, {
      ...context,
      category: LogCategory.AUTOSCALER,
    });
  }

  /**
   * Log performance metrics
   */
  performance(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.PERFORMANCE, message, {
      ...context,
      category: LogCategory.PERFORMANCE,
    });
  }

  /**
   * Log security event
   */
  security(message: string, context: LogContext = {}): void {
    this.log(LogLevel.WARN, LogCategory.SECURITY, message, {
      ...context,
      category: LogCategory.SECURITY,
    });
  }

  /**
   * Log audit event
   */
  audit(message: string, context: LogContext = {}): void {
    this.log(LogLevel.INFO, LogCategory.AUDIT, message, {
      ...context,
      category: LogCategory.AUDIT,
    });
  }

  /**
   * Log operation with timing
   */
  timed<T>(
    operation: string,
    category: LogCategory = LogCategory.SYSTEM,
    level: LogLevel = LogLevel.INFO,
    fn: () => T | Promise<T>,
    context: LogContext = {}
  ): T | Promise<T> {
    const startTime = Date.now();

    try {
      const result = fn();

      if (result instanceof Promise) {
        return result.then(
          (value) => {
            const duration = Date.now() - startTime;
            this.log(level, category, `${operation} completed`, {
              ...context,
              operation,
              duration,
              success: true,
            });
            return value;
          },
          (error) => {
            const duration = Date.now() - startTime;
            this.log(LogLevel.ERROR, category, `${operation} failed`, {
              ...context,
              operation,
              duration,
              success: false,
              error,
            });
            throw error;
          }
        );
      } else {
        const duration = Date.now() - startTime;
        this.log(level, category, `${operation} completed`, {
          ...context,
          operation,
          duration,
          success: true,
        });
        return result;
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.log(LogLevel.ERROR, category, `${operation} failed`, {
        ...context,
        operation,
        duration,
        success: false,
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Create child logger with additional context
   */
  child(context: LogContext): Logger {
    const childLogger = Object.create(this);
    childLogger.pinoLogger = this.pinoLogger.child(context);
    childLogger.defaultContext = { ...this.defaultContext, ...context };
    childLogger.correlationId = context.correlationId || this.correlationId;
    return childLogger;
  }

  /**
   * Set correlation ID for this logger instance
   */
  setCorrelationId(correlationId: string): void {
    this.correlationId = correlationId;
  }

  /**
   * Get current correlation ID
   */
  getCorrelationId(): string | null {
    return this.correlationId;
  }

  /**
   * Generate new correlation ID
   */
  generateCorrelationId(): string {
    return `corr_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Set default context for all log entries
   */
  setDefaultContext(context: LogContext): void {
    this.defaultContext = { ...this.defaultContext, ...context };
  }

  /**
   * Get current logger level
   */
  getLevel(): LogLevel {
    return this.pinoLogger.level as unknown as LogLevel;
  }

  /**
   * Set logger level
   */
  setLevel(level: LogLevel): void {
    this.pinoLogger.level = this.getLevelName(level);
  }

  /**
   * Check if level is enabled
   */
  isLevelEnabled(level: LogLevel): boolean {
    return this.getLevel() >= level;
  }

  /**
   * Internal log method
   */
  private log(level: LogLevel, category: LogCategory, message: string, context: LogContext = {}): void {
    if (!this.isLevelEnabled(level)) {
      return;
    }

    const logContext = {
      ...this.defaultContext,
      ...context,
      category,
      correlationId: context.correlationId || this.correlationId,
    };

    // Sanitize sensitive data
    const sanitizedContext = this.sanitizeContext(logContext);

    // Extract error if present
    let error: any = undefined;
    if (context.error) {
      error = {
        name: context.error.name,
        message: context.error.message,
        stack: isDevelopment ? context.error.stack : undefined,
        code: (context.error as any).code,
      };
    }

    const logData = {
      category,
      correlationId: logContext.correlationId,
      ...sanitizedContext,
      ...(error && { error }),
    };

    // Use appropriate Pino method based on level
    switch (level) {
      case LogLevel.ERROR:
        this.pinoLogger.error(logData, message);
        break;
      case LogLevel.WARN:
        this.pinoLogger.warn(logData, message);
        break;
      case LogLevel.INFO:
        this.pinoLogger.info(logData, message);
        break;
      case LogLevel.DEBUG:
        this.pinoLogger.debug(logData, message);
        break;
      case LogLevel.TRACE:
        this.pinoLogger.trace(logData, message);
        break;
    }
  }

  /**
   * Format log entry for output
   */
  private formatLog(object: any): any {
    // Remove empty fields
    const filtered: any = {};

    for (const [key, value] of Object.entries(object)) {
      if (value !== undefined && value !== null && value !== '') {
        filtered[key] = value;
      }
    }

    return filtered;
  }

  /**
   * Sanitize context to remove sensitive information
   */
  private sanitizeContext(context: LogContext): LogContext {
    const sanitized: LogContext = {};

    for (const [key, value] of Object.entries(context)) {
      if (value === undefined || value === null) {
        continue; // Skip undefined/null values
      }

      if (this.isSensitiveField(key)) {
        sanitized[key] = this.redactValue(value);
      } else if (typeof value === 'object' && value !== null) {
        sanitized[key] = this.sanitizeObject(value);
      } else {
        sanitized[key] = value;
      }
    }

    return sanitized;
  }

  /**
   * Check if a field contains sensitive information
   */
  private isSensitiveField(fieldName: string): boolean {
    const sensitivePatterns = LOGGING_CONSTANTS.SENSITIVE_FIELDS.map(pattern =>
      new RegExp(pattern, 'i')
    );

    return sensitivePatterns.some(pattern => pattern.test(fieldName));
  }

  /**
   * Redact sensitive values
   */
  private redactValue(value: any): string {
    if (typeof value === 'string') {
      return value.length > 4 ? `${value.substring(0, 2)}***${value.substring(value.length - 2)}` : '***';
    }
    return '***';
  }

  /**
   * Recursively sanitize objects
   */
  private sanitizeObject(obj: any): any {
    if (Array.isArray(obj)) {
      return obj.map(item => this.sanitizeObject(item));
    }

    if (typeof obj === 'object' && obj !== null) {
      const sanitized: any = {};
      for (const [key, value] of Object.entries(obj)) {
        if (this.isSensitiveField(key)) {
          sanitized[key] = this.redactValue(value);
        } else {
          sanitized[key] = this.sanitizeObject(value);
        }
      }
      return sanitized;
    }

    return obj;
  }

  /**
   * Get Pino level name from LogLevel enum
   */
  private getLevelName(level: LogLevel): string {
    switch (level) {
      case LogLevel.ERROR:
        return 'error';
      case LogLevel.WARN:
        return 'warn';
      case LogLevel.INFO:
        return 'info';
      case LogLevel.DEBUG:
        return 'debug';
      case LogLevel.TRACE:
        return 'trace';
      default:
        return 'info';
    }
  }
}

/**
 * Logger factory
 */
export class LoggerFactory {
  private static instances: Map<string, Logger> = new Map();
  private static defaultConfig: LoggerConfig;

  static initialize(overrides: Partial<LoggerConfig> = {}): void {
    this.defaultConfig = {
      level: isDevelopment ? LogLevel.DEBUG : LogLevel.INFO,
      prettyPrint: isDevelopment && !isTest,
      colorize: isDevelopment && !isTest,
      timestamp: true,
      json: isProduction || isTest,
      enabled: features.logging,
      environment: config.NODE_ENV,
      ...overrides,
    };
  }

  /**
   * Get or create logger instance
   */
  static getLogger(name: string, context: LogContext = {}): Logger {
    const key = `${name}-${JSON.stringify(context)}`;

    if (!this.instances.has(key)) {
      const logger = new Logger(this.defaultConfig, { name, ...context });
      this.instances.set(key, logger);
    }

    return this.instances.get(key)!;
  }

  /**
   * Create logger for specific worker type
   */
  static getWorkerLogger(workerType: string, workerId: string): Logger {
    return this.getLogger(`worker-${workerType}`, {
      workerType,
      workerId,
      category: LogCategory.WORKER,
    });
  }

  /**
   * Create logger for API operations
   */
  static getApiLogger(requestId: string): Logger {
    return this.getLogger('api', {
      requestId,
      category: LogCategory.API,
    });
  }

  /**
   * Create logger with correlation ID
   */
  static getCorrelationLogger(correlationId: string, name: string = 'correlated'): Logger {
    const logger = this.getLogger(name, {
      correlationId,
    });
    logger.setCorrelationId(correlationId);
    return logger;
  }

  /**
   * Get root logger
   */
  static getRootLogger(): Logger {
    return this.getLogger('root');
  }

  /**
   * Clear all logger instances
   */
  static clear(): void {
    this.instances.clear();
  }
}

/**
 * Initialize logger factory with default configuration
 */
LoggerFactory.initialize({
  level: features.debug ? LogLevel.DEBUG : LogLevel.INFO,
  prettyPrint: isDevelopment,
  colorize: isDevelopment,
});

/**
 * Default logger instances
 */
export const rootLogger = LoggerFactory.getRootLogger();
export const apiLogger = LoggerFactory.getApiLogger('default');
export const workerLogger = LoggerFactory.getWorkerLogger('default', process.pid.toString());
export const databaseLogger = LoggerFactory.getLogger('database', { category: LogCategory.DATABASE });
export const redisLogger = LoggerFactory.getLogger('redis', { category: LogCategory.REDIS });
export const websocketLogger = LoggerFactory.getLogger('websocket', { category: LogCategory.WEBSOCKET });
export const performanceLogger = LoggerFactory.getLogger('performance', { category: LogCategory.PERFORMANCE });
export const securityLogger = LoggerFactory.getLogger('security', { category: LogCategory.SECURITY });
export const auditLogger = LoggerFactory.getLogger('audit', { category: LogCategory.AUDIT });

/**
 * Logger utility functions
 */
export const loggerUtils = {
  /**
   * Create correlation ID
   */
  createCorrelationId(): string {
    return rootLogger.generateCorrelationId();
  },

  /**
   * Wrap function with logging
   */
  withLogging<T extends (...args: any[]) => any>(
    fn: T,
    name: string,
    logger: Logger = rootLogger,
    category: LogCategory = LogCategory.SYSTEM
  ): T {
    return ((...args: any[]) => {
      return logger.timed(name, category, LogLevel.INFO, () => fn(...args), {
        args: args.length > 0 ? `${args.length} arguments` : 'no arguments',
      });
    }) as T;
  },

  /**
   * Wrap async function with error logging
   */
  withErrorLogging<T extends (...args: any[]) => Promise<any>>(
    fn: T,
    name: string,
    logger: Logger = rootLogger,
    category: LogCategory = LogCategory.ERROR
  ): T {
    return (async (...args: any[]) => {
      try {
        return await fn(...args);
      } catch (error) {
        logger.error(`${name} failed`, {
          name,
          error: error as Error,
          category,
        });
        throw error;
      }
    }) as T;
  },

  /**
   * Log system startup
   */
  logStartup(): void {
    rootLogger.info('SignalCast ingestion system starting', {
      metadata: {
        version: APP_METADATA.VERSION,
        environment: config.NODE_ENV,
        nodeVersion: process.version,
        platform: process.platform,
        memory: process.memoryUsage(),
        pid: process.pid,
      },
      category: LogCategory.SYSTEM,
    });
  },

  /**
   * Log system shutdown
   */
  logShutdown(): void {
    rootLogger.info('SignalCast ingestion system shutting down', {
      metadata: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
      },
      category: LogCategory.SYSTEM,
    });
  },

  /**
   * Log performance metrics
   */
  logPerformanceMetrics(metrics: Record<string, number>): void {
    performanceLogger.info('Performance metrics', {
      metrics,
      category: LogCategory.PERFORMANCE,
    });
  },

  /**
   * Get sanitized config for logging
   */
  getSafeConfig(): Record<string, any> {
    const safeConfig: Record<string, any> = {
      NODE_ENV: config.NODE_ENV,
      REDIS_ENABLED: config.REDIS_ENABLED,
      DB_POOL_BUDGET: config.DB_POOL_BUDGET,
      DEBUG_QUERY_TIMEOUT: config.DEBUG_QUERY_TIMEOUT,
      HEARTBEAT_INTERVAL_MS: config.HEARTBEAT_INTERVAL_MS,
      // Add other non-sensitive config values
    };

    return safeConfig;
  },
};

/**
 * Export default logger
 */
export default rootLogger;
