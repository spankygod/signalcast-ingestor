import type { PoolConfig } from 'pg';
import { Pool } from 'pg';
import { config, buildDatabaseUrl, features } from './settings';

/**
 * Database table configuration based on existing schema
 */
export const DATABASE_TABLES = {
  EVENTS: 'public.events',
  MARKETS: 'public.markets',
  OUTCOMES: 'public.outcomes',
  MARKET_PRICES_REALTIME: 'public.market_prices_realtime',
} as const;

/**
 * Database column names for type safety
 */
export const COLUMNS = {
  EVENTS: {
    ID: 'id',
    POLYMARKET_ID: 'polymarket_id',
    SLUG: 'slug',
    TITLE: 'title',
    DESCRIPTION: 'description',
    CATEGORY: 'category',
    SUBCATEGORY: 'subcategory',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume_24h',
    VOLUME_TOTAL: 'volume_total',
    ACTIVE: 'active',
    CLOSED: 'closed',
    ARCHIVED: 'archived',
    RESTRICTED: 'restricted',
    RELEVANCE_SCORE: 'relevance_score',
    START_DATE: 'start_date',
    END_DATE: 'end_date',
    LAST_INGESTED_AT: 'last_ingested_at',
    UPDATED_AT: 'updated_at',
  },
  MARKETS: {
    ID: 'id',
    POLYMARKET_ID: 'polymarket_id',
    EVENT_ID: 'event_id',
    QUESTION: 'question',
    SLUG: 'slug',
    DESCRIPTION: 'description',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume_24h',
    VOLUME_TOTAL: 'volume_total',
    CURRENT_PRICE: 'current_price',
    LAST_TRADE_PRICE: 'last_trade_price',
    BEST_BID: 'best_bid',
    BEST_ASK: 'best_ask',
    ACTIVE: 'active',
    CLOSED: 'closed',
    ARCHIVED: 'archived',
    RESTRICTED: 'restricted',
    APPROVED: 'approved',
    STATUS: 'status',
    RESOLVED_AT: 'resolved_at',
    RELEVANCE_SCORE: 'relevance_score',
    LAST_INGESTED_AT: 'last_ingested_at',
    UPDATED_AT: 'updated_at',
  },
  OUTCOMES: {
    ID: 'id',
    POLYMARKET_ID: 'polymarket_id',
    MARKET_ID: 'market_id',
    TITLE: 'title',
    DESCRIPTION: 'description',
    PRICE: 'price',
    PROBABILITY: 'probability',
    VOLUME: 'volume',
    STATUS: 'status',
    DISPLAY_ORDER: 'display_order',
    UPDATED_AT: 'updated_at',
  },
  MARKET_PRICES_REALTIME: {
    ID: 'id',
    MARKET_ID: 'market_id',
    PRICE: 'price',
    BEST_BID: 'best_bid',
    BEST_ASK: 'best_ask',
    LAST_TRADE_PRICE: 'last_trade_price',
    LIQUIDITY: 'liquidity',
    VOLUME_24H: 'volume_24h',
    UPDATED_AT: 'updated_at',
  },
} as const;

/**
 * Database index configuration for query optimization
 */
export const INDEXES = {
  EVENTS: [
    'events_polymarket_id_key',
    'idx_events_slug',
    'idx_events_last_ingested',
    'idx_events_relevance',
    'idx_events_start_date',
    'idx_events_id',
    'idx_events_category',
    'idx_events_active',
    'idx_events_polymarket_update',
    'idx_events_category_updated',
    'idx_events_active_only',
  ],
  MARKETS: [
    'markets_polymarket_id_key',
    'idx_markets_event_id',
    'idx_markets_relevance',
    'idx_markets_active',
    'idx_markets_polymarket_update',
    'idx_markets_event_relevance',
    'idx_markets_active_only',
  ],
  OUTCOMES: [
    'outcomes_polymarket_id_key',
    'idx_outcomes_market_id',
    'idx_outcomes_polymarket_update',
    'idx_outcomes_market_display',
  ],
  MARKET_PRICES_REALTIME: [
    'idx_ticks_market_id',
    'idx_ticks_market_time',
  ],
} as const;

/**
 * PostgreSQL connection pool configuration
 */
export const poolConfig: PoolConfig = {
  connectionString: buildDatabaseUrl(),
  ssl: config.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: config.DB_POOL_BUDGET,
  min: 2, // Minimum number of connections to maintain
  idleTimeoutMillis: config.DB_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: config.DB_CONNECTION_TIMEOUT_MS,
  // Statement timeout for individual queries
  statement_timeout: config.DEBUG_QUERY_TIMEOUT,
  // Application name for monitoring
  application_name: 'signalcast-ingestor',
};

/**
 * Create PostgreSQL connection pool
 */
export const pool = new Pool(poolConfig);

/**
 * Database query timeout configuration
 */
export const QUERY_TIMEOUTS = {
  DEFAULT: 30000, // 30 seconds
  SHORT: 10000,   // 10 seconds for simple queries
  LONG: 120000,   // 2 minutes for complex operations
  BATCH: 60000,   // 1 minute for batch operations
  DEBUG: config.DEBUG_QUERY_TIMEOUT,
} as const;

/**
 * Batch processing configuration
 */
export const BATCH_CONFIG = {
  EVENTS: {
    BATCH_SIZE: config.EVENT_BATCH_SIZE,
    TIMEOUT_MS: QUERY_TIMEOUTS.BATCH,
    CONFLICT_COLUMNS: ['polymarket_id'],
    UPDATE_COLUMNS: [
      COLUMNS.EVENTS.TITLE,
      COLUMNS.EVENTS.DESCRIPTION,
      COLUMNS.EVENTS.LIQUIDITY,
      COLUMNS.EVENTS.VOLUME_24H,
      COLUMNS.EVENTS.VOLUME_TOTAL,
      COLUMNS.EVENTS.ACTIVE,
      COLUMNS.EVENTS.CLOSED,
      COLUMNS.EVENTS.ARCHIVED,
      COLUMNS.EVENTS.RESTRICTED,
      COLUMNS.EVENTS.RELEVANCE_SCORE,
      COLUMNS.EVENTS.START_DATE,
      COLUMNS.EVENTS.END_DATE,
      COLUMNS.EVENTS.LAST_INGESTED_AT,
    ],
  },
  MARKETS: {
    BATCH_SIZE: config.MARKET_BATCH_SIZE,
    TIMEOUT_MS: QUERY_TIMEOUTS.BATCH,
    CONFLICT_COLUMNS: ['polymarket_id'],
    UPDATE_COLUMNS: [
      COLUMNS.MARKETS.QUESTION,
      COLUMNS.MARKETS.DESCRIPTION,
      COLUMNS.MARKETS.LIQUIDITY,
      COLUMNS.MARKETS.VOLUME_24H,
      COLUMNS.MARKETS.VOLUME_TOTAL,
      COLUMNS.MARKETS.CURRENT_PRICE,
      COLUMNS.MARKETS.LAST_TRADE_PRICE,
      COLUMNS.MARKETS.BEST_BID,
      COLUMNS.MARKETS.BEST_ASK,
      COLUMNS.MARKETS.ACTIVE,
      COLUMNS.MARKETS.CLOSED,
      COLUMNS.MARKETS.ARCHIVED,
      COLUMNS.MARKETS.RESTRICTED,
      COLUMNS.MARKETS.APPROVED,
      COLUMNS.MARKETS.STATUS,
      COLUMNS.MARKETS.RESOLVED_AT,
      COLUMNS.MARKETS.RELEVANCE_SCORE,
      COLUMNS.MARKETS.LAST_INGESTED_AT,
    ],
  },
  OUTCOMES: {
    BATCH_SIZE: config.OUTCOME_BATCH_SIZE,
    TIMEOUT_MS: QUERY_TIMEOUTS.BATCH,
    CONFLICT_COLUMNS: ['polymarket_id'],
    UPDATE_COLUMNS: [
      COLUMNS.OUTCOMES.TITLE,
      COLUMNS.OUTCOMES.DESCRIPTION,
      COLUMNS.OUTCOMES.PRICE,
      COLUMNS.OUTCOMES.PROBABILITY,
      COLUMNS.OUTCOMES.VOLUME,
      COLUMNS.OUTCOMES.STATUS,
      COLUMNS.OUTCOMES.DISPLAY_ORDER,
    ],
  },
  TICKS: {
    BATCH_SIZE: config.TICK_BATCH_SIZE,
    TIMEOUT_MS: QUERY_TIMEOUTS.SHORT, // Ticks are time-sensitive
    NO_CONFLICT: true, // Ticks are append-only
  },
} as const;

/**
 * Query templates for common operations
 */
export const QUERIES = {
  UPSERT_EVENT: `
    INSERT INTO ${DATABASE_TABLES.EVENTS} (
      ${COLUMNS.EVENTS.POLYMARKET_ID},
      ${COLUMNS.EVENTS.SLUG},
      ${COLUMNS.EVENTS.TITLE},
      ${COLUMNS.EVENTS.DESCRIPTION},
      ${COLUMNS.EVENTS.CATEGORY},
      ${COLUMNS.EVENTS.SUBCATEGORY},
      ${COLUMNS.EVENTS.LIQUIDITY},
      ${COLUMNS.EVENTS.VOLUME_24H},
      ${COLUMNS.EVENTS.VOLUME_TOTAL},
      ${COLUMNS.EVENTS.ACTIVE},
      ${COLUMNS.EVENTS.CLOSED},
      ${COLUMNS.EVENTS.ARCHIVED},
      ${COLUMNS.EVENTS.RESTRICTED},
      ${COLUMNS.EVENTS.RELEVANCE_SCORE},
      ${COLUMNS.EVENTS.START_DATE},
      ${COLUMNS.EVENTS.END_DATE},
      ${COLUMNS.EVENTS.LAST_INGESTED_AT}
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    ON CONFLICT (${COLUMNS.EVENTS.POLYMARKET_ID})
    DO UPDATE SET
      ${COLUMNS.EVENTS.SLUG} = EXCLUDED.${COLUMNS.EVENTS.SLUG},
      ${COLUMNS.EVENTS.TITLE} = EXCLUDED.${COLUMNS.EVENTS.TITLE},
      ${COLUMNS.EVENTS.DESCRIPTION} = EXCLUDED.${COLUMNS.EVENTS.DESCRIPTION},
      ${COLUMNS.EVENTS.CATEGORY} = EXCLUDED.${COLUMNS.EVENTS.CATEGORY},
      ${COLUMNS.EVENTS.SUBCATEGORY} = EXCLUDED.${COLUMNS.EVENTS.SUBCATEGORY},
      ${COLUMNS.EVENTS.LIQUIDITY} = EXCLUDED.${COLUMNS.EVENTS.LIQUIDITY},
      ${COLUMNS.EVENTS.VOLUME_24H} = EXCLUDED.${COLUMNS.EVENTS.VOLUME_24H},
      ${COLUMNS.EVENTS.VOLUME_TOTAL} = EXCLUDED.${COLUMNS.EVENTS.VOLUME_TOTAL},
      ${COLUMNS.EVENTS.ACTIVE} = EXCLUDED.${COLUMNS.EVENTS.ACTIVE},
      ${COLUMNS.EVENTS.CLOSED} = EXCLUDED.${COLUMNS.EVENTS.CLOSED},
      ${COLUMNS.EVENTS.ARCHIVED} = EXCLUDED.${COLUMNS.EVENTS.ARCHIVED},
      ${COLUMNS.EVENTS.RESTRICTED} = EXCLUDED.${COLUMNS.EVENTS.RESTRICTED},
      ${COLUMNS.EVENTS.RELEVANCE_SCORE} = EXCLUDED.${COLUMNS.EVENTS.RELEVANCE_SCORE},
      ${COLUMNS.EVENTS.START_DATE} = EXCLUDED.${COLUMNS.EVENTS.START_DATE},
      ${COLUMNS.EVENTS.END_DATE} = EXCLUDED.${COLUMNS.EVENTS.END_DATE},
      ${COLUMNS.EVENTS.LAST_INGESTED_AT} = EXCLUDED.${COLUMNS.EVENTS.LAST_INGESTED_AT},
      ${COLUMNS.EVENTS.UPDATED_AT} = NOW()
    RETURNING ${COLUMNS.EVENTS.ID}
  `,

  SELECT_ACTIVE_EVENTS: `
    SELECT * FROM ${DATABASE_TABLES.EVENTS}
    WHERE ${COLUMNS.EVENTS.ACTIVE} = true
    AND ${COLUMNS.EVENTS.CLOSED} = false
    ORDER BY ${COLUMNS.EVENTS.UPDATED_AT} DESC
    LIMIT $1
  `,

  SELECT_EVENT_BY_POLYMARKET_ID: `
    SELECT * FROM ${DATABASE_TABLES.EVENTS}
    WHERE ${COLUMNS.EVENTS.POLYMARKET_ID} = $1
  `,
} as const;

/**
 * Database health check configuration
 */
export const HEALTH_CHECK = {
  QUERY: 'SELECT 1 as health_check',
  TIMEOUT_MS: 5000,
  INTERVAL_MS: 30000, // Check every 30 seconds
  MAX_FAILURES: 3, // Alert after 3 consecutive failures
} as const;

// Export with alias for compatibility with index.ts re-export
export { HEALTH_CHECK as DB_HEALTH_CHECK };

/**
 * Connection event handlers
 */
pool.on('connect', (client) => {
  if (features.debug) {
    console.log('New database connection established');
  }

  // Set session configuration
  client.query(`
    SET application_name = 'signalcast-ingestor';
    SET statement_timeout = ${config.DEBUG_QUERY_TIMEOUT};
  `);
});

pool.on('error', (err, client) => {
  console.error('Database connection pool error:', err);
  if (client) {
    console.error('Failed client connection');
  }
});

if (features.debug) {
  pool.on('acquire', () => {
    console.log('Database connection acquired from pool');
  });

  pool.on('release', () => {
    console.log('Database connection released to pool');
  });
}

/**
 * Graceful shutdown handler
 */
export async function closeDatabasePool(): Promise<void> {
  try {
    await pool.end();
    console.log('Database connection pool closed successfully');
  } catch (error) {
    console.error('Error closing database connection pool:', error);
    throw error;
  }
}

/**
 * Export database configuration
 */
export default {
  pool,
  poolConfig,
  tables: DATABASE_TABLES,
  columns: COLUMNS,
  indexes: INDEXES,
  batchConfig: BATCH_CONFIG,
  queries: QUERIES,
  timeouts: QUERY_TIMEOUTS,
  healthCheck: HEALTH_CHECK,
  close: closeDatabasePool,
};