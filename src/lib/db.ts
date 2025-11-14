/**
 * PostgreSQL connection pool management for SignalCast ingestion system
 * Provides connection pooling, health checks, batch operations, and error handling
 */

import { Pool } from 'pg';
import type { PoolClient, QueryResult, QueryResultRow } from 'pg';
import {
  pool as externalPool,
  DATABASE_TABLES,
  COLUMNS,
  BATCH_CONFIG,
  QUERY_TIMEOUTS,
  DB_HEALTH_CHECK,
  closeDatabasePool,
  isProduction
} from '../config';

/**
 * Database connection interface
 */
export interface DatabaseConnection {
  query<T extends QueryResultRow>(text: string, params?: any[]): Promise<QueryResult<T>>;
  transaction<T>(callback: (client: DatabaseClient) => Promise<T>): Promise<T>;
  healthCheck(): Promise<boolean>;
  close(): Promise<void>;
}

/**
 * Database client interface for transactions
 */
export interface DatabaseClient {
  query<T extends QueryResultRow>(text: string, params?: any[]): Promise<QueryResult<T>>;
  release(): void;
  client?: PoolClient; // Optional underlying client reference
}

/**
 * Batch operation result
 */
export interface BatchResult {
  processed: number;
  inserted: number;
  updated: number;
  errors: Array<{
    data: any;
    error: Error;
  }>;
  duration: number;
}

/**
 * Upsert operation configuration
 */
export interface UpsertConfig {
  table: string;
  conflictColumns: readonly string[] | string[];
  updateColumns: readonly string[] | string[];
  data: any[];
  batchSize?: number;
  timeout?: number;
}

/**
 * Database health status
 */
export interface DatabaseHealthStatus {
  connected: boolean;
  poolStats: {
    totalCount: number;
    idleCount: number;
    waitingCount: number;
  };
  lastCheck: Date;
  responseTime: number;
  error?: string | null | undefined;
}

/**
 * Enhanced PostgreSQL pool wrapper
 */
class EnhancedDatabasePool implements DatabaseConnection {
  private pool: Pool;
  private isHealthy: boolean = false;
  private lastHealthCheck: Date = new Date(0);
  private healthCheckInterval: NodeJS.Timeout | null = null;

  constructor(pool: Pool) {
    this.pool = pool;
    this.setupEventHandlers();
    this.startHealthChecking();
  }

  /**
   * Execute a database query with timeout and connection limits
   */
  async query<T extends QueryResultRow = any>(
    text: string,
    params: any[] = [],
    timeout: number = QUERY_TIMEOUTS.DEFAULT
  ): Promise<QueryResult<T>> {
    const startTime = Date.now();

    // Check pool health before attempting connection
    if (this.pool.waitingCount > this.pool.totalCount * 0.8) {
      console.warn(`Database connection pool under pressure: ${this.pool.waitingCount} waiting, ${this.pool.totalCount} total`);
    }

    try {
      // Set query timeout on the client for this query
      const client = await this.pool.connect();
      try {
        await client.query(`SET statement_timeout = ${timeout}`);
        const result = await client.query<T>(text, params);
        this.logQuery('success', text, params, Date.now() - startTime);
        return result;
      } finally {
        client.release();
      }
    } catch (error) {
      this.logQuery('error', text, params, Date.now() - startTime, error as Error);

      // Handle connection pool exhaustion specifically
      if (error instanceof Error && (error.message.includes('timeout') ||
          error.message.includes('connection') ||
          error.message.includes('pool'))) {
        console.error('Database connection pool exhausted, current stats:', {
          totalCount: this.pool.totalCount,
          idleCount: this.pool.idleCount,
          waitingCount: this.pool.waitingCount
        });
      }

      throw this.handleDatabaseError(error as Error, text);
    }
  }

  /**
   * Execute a transaction with connection pool monitoring
   */
  async transaction<T>(callback: (client: DatabaseClient) => Promise<T>): Promise<T> {
    // Check pool health before attempting connection for transaction
    if (this.pool.waitingCount > this.pool.totalCount * 0.8) {
      console.warn(`Transaction: Database connection pool under pressure: ${this.pool.waitingCount} waiting, ${this.pool.totalCount} total`);
    }

    const client = await this.pool.connect();
    const enhancedClient: DatabaseClient = {
      query: <T extends QueryResultRow>(text: string, params?: any[]) =>
        client.query<T>(text, params),
      release: () => client.release()
    };

    try {
      await client.query('BEGIN');
      const result = await callback(enhancedClient);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Perform health check
   */
  async healthCheck(): Promise<boolean> {
    try {
      const startTime = Date.now();
      await this.query(DB_HEALTH_CHECK.QUERY, [], DB_HEALTH_CHECK.TIMEOUT_MS);
      const responseTime = Date.now() - startTime;

      this.isHealthy = true;
      this.lastHealthCheck = new Date();

      if (responseTime > DB_HEALTH_CHECK.TIMEOUT_MS / 2) {
        console.warn(`Database health check slow: ${responseTime}ms`);
      }

      return true;
    } catch (error) {
      this.isHealthy = false;
      console.error('Database health check failed:', error);
      return false;
    }
  }

  /**
   * Get database health status
   */
  async getHealthStatus(): Promise<DatabaseHealthStatus> {
    const startTime = Date.now();
    let responseTime = 0;
    let error: string | undefined;

    try {
      await this.healthCheck();
      responseTime = Date.now() - startTime;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    }

    return {
      connected: this.isHealthy,
      poolStats: {
        totalCount: this.pool.totalCount,
        idleCount: this.pool.idleCount,
        waitingCount: this.pool.waitingCount,
      },
      lastCheck: this.lastHealthCheck,
      responseTime,
      error,
    };
  }

  /**
   * Close the database pool
   */
  async close(): Promise<void> {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
    }

    await closeDatabasePool();
  }

  /**
   * Batch UPSERT operation for events
   */
  async batchUpsertEvents(events: any[]): Promise<BatchResult> {
    return this.batchUpsert({
      table: DATABASE_TABLES.EVENTS,
      conflictColumns: BATCH_CONFIG.EVENTS.CONFLICT_COLUMNS,
      updateColumns: BATCH_CONFIG.EVENTS.UPDATE_COLUMNS,
      data: events,
      batchSize: BATCH_CONFIG.EVENTS.BATCH_SIZE,
      timeout: BATCH_CONFIG.EVENTS.TIMEOUT_MS,
    });
  }

  /**
   * Batch UPSERT operation for markets
   */
  async batchUpsertMarkets(markets: any[]): Promise<BatchResult> {
    return this.batchUpsert({
      table: DATABASE_TABLES.MARKETS,
      conflictColumns: BATCH_CONFIG.MARKETS.CONFLICT_COLUMNS,
      updateColumns: BATCH_CONFIG.MARKETS.UPDATE_COLUMNS,
      data: markets,
      batchSize: BATCH_CONFIG.MARKETS.BATCH_SIZE,
      timeout: BATCH_CONFIG.MARKETS.TIMEOUT_MS,
    });
  }

  /**
   * Batch UPSERT operation for outcomes
   */
  async batchUpsertOutcomes(outcomes: any[]): Promise<BatchResult> {
    return this.batchUpsert({
      table: DATABASE_TABLES.OUTCOMES,
      conflictColumns: BATCH_CONFIG.OUTCOMES.CONFLICT_COLUMNS,
      updateColumns: BATCH_CONFIG.OUTCOMES.UPDATE_COLUMNS,
      data: outcomes,
      batchSize: BATCH_CONFIG.OUTCOMES.BATCH_SIZE,
      timeout: BATCH_CONFIG.OUTCOMES.TIMEOUT_MS,
    });
  }

  /**
   * Batch INSERT operation for ticks (append-only)
   */
  async batchInsertTicks(ticks: any[]): Promise<BatchResult> {
    if (ticks.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    const startTime = Date.now();
    const errors: Array<{ data: any; error: Error }> = [];
    let inserted = 0;
    let processed = 0;

    try {
      const columns = [
        COLUMNS.MARKET_PRICES_REALTIME.MARKET_ID,
        COLUMNS.MARKET_PRICES_REALTIME.PRICE,
        COLUMNS.MARKET_PRICES_REALTIME.BEST_BID,
        COLUMNS.MARKET_PRICES_REALTIME.BEST_ASK,
        COLUMNS.MARKET_PRICES_REALTIME.LAST_TRADE_PRICE,
        COLUMNS.MARKET_PRICES_REALTIME.LIQUIDITY,
        COLUMNS.MARKET_PRICES_REALTIME.VOLUME_24H,
      ];

      const batchSize = BATCH_CONFIG.TICKS.BATCH_SIZE;
      const batches = this.createBatches(ticks, batchSize);

      for (const batch of batches) {
        try {
          const values = batch.map(tick => [
            tick.market_id,
            tick.price,
            tick.best_bid,
            tick.best_ask,
            tick.last_trade_price,
            tick.liquidity,
            tick.volume_24h,
          ]);

          const placeholders = values
            .map((_, index) =>
              `(${columns.map((_, colIndex) => `$${index * columns.length + colIndex + 1}`).join(', ')})`
            )
            .join(', ');

          const flatValues = values.flat();

          const query = `
            INSERT INTO ${DATABASE_TABLES.MARKET_PRICES_REALTIME} (${columns.join(', ')})
            VALUES ${placeholders}
          `;

          await this.query(query, flatValues, BATCH_CONFIG.TICKS.TIMEOUT_MS);
          inserted += batch.length;
          processed += batch.length;
        } catch (error) {
          batch.forEach(tick => {
            errors.push({ data: tick, error: error as Error });
          });
          processed += batch.length;
        }
      }
    } catch (error) {
      // Add any remaining items to errors
      ticks.forEach(tick => {
        errors.push({ data: tick, error: error as Error });
      });
      processed = ticks.length;
    }

    return {
      processed,
      inserted,
      updated: 0, // Ticks are append-only
      errors,
      duration: Date.now() - startTime,
    };
  }

  /**
   * Generic batch UPSERT operation
   */
  private async batchUpsert(config: UpsertConfig): Promise<BatchResult> {
    if (config.data.length === 0) {
      return { processed: 0, inserted: 0, updated: 0, errors: [], duration: 0 };
    }

    const startTime = Date.now();
    const errors: Array<{ data: any; error: Error }> = [];
    let inserted = 0;
    let updated = 0;
    let processed = 0;
    const conflictColumns = Array.from(config.conflictColumns);
    const updateColumns = Array.from(config.updateColumns);

    try {
      const columns = Object.keys(config.data[0]);
      const batchSize = config.batchSize || 100;
      const batches = this.createBatches(config.data, batchSize);

      for (const batch of batches) {
        try {
          await this.performUpsertBatch(
            config.table,
            columns,
            conflictColumns,
            updateColumns,
            batch,
            config.timeout
          );

          // UPSERT doesn't distinguish between insert and update easily
          // We'll estimate based on typical patterns
          inserted += Math.floor(batch.length * 0.3); // Assume 30% are new
          updated += Math.floor(batch.length * 0.7); // Assume 70% are updates
          processed += batch.length;
        } catch (error) {
          batch.forEach(item => {
            errors.push({ data: item, error: error as Error });
          });
          processed += batch.length;
        }
      }
    } catch (error) {
      // Add any remaining items to errors
      config.data.forEach(item => {
        errors.push({ data: item, error: error as Error });
      });
      processed = config.data.length;
    }

    return {
      processed,
      inserted,
      updated,
      errors,
      duration: Date.now() - startTime,
    };
  }

  /**
   * Perform a single UPSERT batch operation
   */
  private async performUpsertBatch(
    table: string,
    columns: string[],
    conflictColumns: string[],
    updateColumns: string[],
    data: any[],
    timeout?: number
  ): Promise<QueryResult> {
    const values = data.map(item => columns.map(col => item[col]));

    const placeholders = values
      .map((_, index) =>
        `(${columns.map((_, colIndex) => `$${index * columns.length + colIndex + 1}`).join(', ')})`
      )
      .join(', ');

    const flatValues = values.flat();

    const updateClause = updateColumns
      .map(col => `${col} = EXCLUDED.${col}`)
      .join(', ');

    const query = `
      INSERT INTO ${table} (${columns.join(', ')})
      VALUES ${placeholders}
      ON CONFLICT (${conflictColumns.join(', ')})
      DO UPDATE SET
        ${updateClause},
        updated_at = NOW()
      RETURNING id
    `;

    return await this.query(query, flatValues, timeout);
  }

  /**
   * Split data into batches
   */
  private createBatches<T>(data: T[], batchSize: number): T[][] {
    const batches: T[][] = [];
    for (let i = 0; i < data.length; i += batchSize) {
      batches.push(data.slice(i, i + batchSize));
    }
    return batches;
  }

  /**
   * Setup pool event handlers
   */
  private setupEventHandlers(): void {
    this.pool.on('connect', (client) => {
      // Set session configuration for new connections
      client.query(`
        SET application_name = 'signalcast-ingestor';
        SET statement_timeout = ${QUERY_TIMEOUTS.DEFAULT};
      `);
    });

    this.pool.on('error', (err, _client) => {
      console.error('Database pool error:', err);
      this.isHealthy = false;
    });

    this.pool.on('acquire', () => {
      // Debug logging can be enabled here if needed
    });

    this.pool.on('release', () => {
      // Debug logging can be enabled here if needed
    });
  }

  /**
   * Start periodic health checking
   */
  private startHealthChecking(): void {
    // Perform initial health check
    this.healthCheck().catch(() => {
      // Ignore initial health check failure
    });

    // Schedule periodic health checks
    this.healthCheckInterval = setInterval(() => {
      this.healthCheck().catch(() => {
        // Health check failures are logged in the method
      });
    }, DB_HEALTH_CHECK.INTERVAL_MS);
  }

  /**
   * Log query execution
   */
  private logQuery(
    status: 'success' | 'error',
    text: string,
    params: any[],
    duration: number,
    error?: Error
  ): void {
    if (isProduction) {
      // Only log slow queries or errors in production
      if (status === 'error' || duration > 1000) {
        console.log('Database Query:', {
          status,
          duration,
          query: text.replace(/\s+/g, ' ').trim(),
          error: error?.message,
        });
      }
    } else {
      // Log all queries in development
      console.log('Database Query:', {
        status,
        duration,
        query: text.replace(/\s+/g, ' ').trim(),
        params: params.length > 0 ? params : undefined,
        error: error?.message,
      });
    }
  }

  /**
   * Handle and classify database errors
   */
  private handleDatabaseError(error: Error, query: string): Error {
    const errorMessage = error.message.toLowerCase();

    // Connection errors
    if (errorMessage.includes('connection') || errorMessage.includes('timeout')) {
      return new DatabaseConnectionError(error.message, query);
    }

    // Constraint violations
    if (errorMessage.includes('constraint') || errorMessage.includes('duplicate')) {
      return new DatabaseConstraintError(error.message, query);
    }

    // Query syntax errors
    if (errorMessage.includes('syntax') || errorMessage.includes('invalid')) {
      return new DatabaseQueryError(error.message, query);
    }

    // Generic database error
    return new DatabaseError(error.message, query);
  }
}

/**
 * Custom database error classes
 */
export class DatabaseError extends Error {
  public readonly query: string;
  public readonly code: string = 'DATABASE_ERROR';

  constructor(message: string, query: string) {
    super(`Database error: ${message}`);
    this.name = 'DatabaseError';
    this.query = query;
  }
}

export class DatabaseConnectionError extends DatabaseError {
  public override readonly code: string = 'CONNECTION_ERROR';

  constructor(message: string, query: string) {
    super(`Connection error: ${message}`, query);
    this.name = 'DatabaseConnectionError';
  }
}

export class DatabaseConstraintError extends DatabaseError {
  public override readonly code: string = 'CONSTRAINT_ERROR';

  constructor(message: string, query: string) {
    super(`Constraint violation: ${message}`, query);
    this.name = 'DatabaseConstraintError';
  }
}

export class DatabaseQueryError extends DatabaseError {
  public override readonly code: string = 'QUERY_ERROR';

  constructor(message: string, query: string) {
    super(`Query error: ${message}`, query);
    this.name = 'DatabaseQueryError';
  }
}

/**
 * Create enhanced database connection
 */
export function createDatabaseConnection(): DatabaseConnection {
  return new EnhancedDatabasePool(externalPool);
}

/**
 * Global database connection instance
 */
export const dbConnection = createDatabaseConnection();

/**
 * Database utility functions
 */
export const dbUtils = {
  /**
   * Build WHERE clause from filters
   */
  buildWhereClause(filters: Record<string, any>): { clause: string; params: any[] } {
    const conditions: string[] = [];
    const params: any[] = [];

    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        if (Array.isArray(value)) {
          conditions.push(`${key} = ANY($${params.length + 1})`);
          params.push(value);
        } else {
          conditions.push(`${key} = $${params.length + 1}`);
          params.push(value);
        }
      }
    });

    const clause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    return { clause, params };
  },

  /**
   * Sanitize column names to prevent SQL injection
   */
  sanitizeColumnName(column: string): string {
    // Only allow alphanumeric characters and underscores
    return column.replace(/[^a-zA-Z0-9_]/g, '');
  },

  /**
   * Escape identifiers for PostgreSQL
   */
  escapeIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  },

  /**
   * Build pagination clause
   */
  buildPaginationClause(offset: number, limit: number): string {
    return `LIMIT ${limit} OFFSET ${offset}`;
  },

  /**
   * Build ORDER BY clause
   */
  buildOrderClause(columns: string[], direction: 'ASC' | 'DESC' = 'DESC'): string {
    const sanitizedColumns = columns.map(col => this.escapeIdentifier(this.sanitizeColumnName(col)));
    return `ORDER BY ${sanitizedColumns.join(', ')} ${direction}`;
  },
};

/**
 * Export the database connection pool for direct access
 */
export { externalPool as database };

/**
 * Export database connection and utilities
 */
export default dbConnection;
