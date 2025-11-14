import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import dotenv from 'dotenv';

dotenv.config();

import {
  pgTable,
  uuid,
  text,
  boolean,
  decimal,
  timestamp,
  integer,
  bigserial,
  index,
  uniqueIndex
} from 'drizzle-orm/pg-core';

export const events = pgTable(
  'events',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    polymarket_id: text('polymarket_id').notNull(),
    slug: text('slug'),
    title: text('title').notNull(),
    description: text('description'),
    category: text('category'),
    subcategory: text('subcategory'),
    liquidity: decimal('liquidity', { precision: 20, scale: 8 }).default('0'),
    volume_24h: decimal('volume_24h', { precision: 20, scale: 8 }).default('0'),
    volume_total: decimal('volume_total', { precision: 20, scale: 8 }).default('0'),
    active: boolean('active').default(false),
    closed: boolean('closed').default(false),
    archived: boolean('archived').default(false),
    restricted: boolean('restricted').default(false),
    relevance_score: decimal('relevance_score', { precision: 10, scale: 4 }).default('0'),
    start_date: timestamp('start_date', { withTimezone: true }),
    end_date: timestamp('end_date', { withTimezone: true }),
    last_ingested_at: timestamp('last_ingested_at', { withTimezone: true }),
    updated_at: timestamp('updated_at', { withTimezone: true }).defaultNow()
  },
  (table) => ({
    polymarketKey: uniqueIndex('events_polymarket_id_key').on(table.polymarket_id),
    slugIdx: index('idx_events_slug').on(table.slug),
    lastIngestedIdx: index('idx_events_last_ingested').on(table.last_ingested_at),
    relevanceIdx: index('idx_events_relevance').on(table.relevance_score, table.volume_24h)
  })
);

export const markets = pgTable(
  'markets',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    polymarket_id: text('polymarket_id').notNull(),
    event_id: uuid('event_id').references(() => events.id, { onDelete: 'cascade' }),
    question: text('question'),
    slug: text('slug'),
    description: text('description'),
    liquidity: decimal('liquidity', { precision: 20, scale: 8 }).default('0'),
    volume_24h: decimal('volume_24h', { precision: 20, scale: 8 }).default('0'),
    volume_total: decimal('volume_total', { precision: 20, scale: 8 }).default('0'),
    current_price: decimal('current_price', { precision: 20, scale: 8 }),
    last_trade_price: decimal('last_trade_price', { precision: 20, scale: 8 }),
    best_bid: decimal('best_bid', { precision: 20, scale: 8 }),
    best_ask: decimal('best_ask', { precision: 20, scale: 8 }),
    active: boolean('active').default(true),
    closed: boolean('closed').default(false),
    archived: boolean('archived').default(false),
    restricted: boolean('restricted').default(false),
    approved: boolean('approved').default(true),
    status: text('status'),
    resolved_at: timestamp('resolved_at', { withTimezone: true }),
    relevance_score: decimal('relevance_score', { precision: 10, scale: 4 }).default('0'),
    last_ingested_at: timestamp('last_ingested_at', { withTimezone: true }),
    updated_at: timestamp('updated_at', { withTimezone: true }).defaultNow()
  },
  (table) => ({
    polymarketKey: uniqueIndex('markets_polymarket_id_key').on(table.polymarket_id),
    eventIdx: index('idx_markets_event_id').on(table.event_id),
    relevanceIdx: index('idx_markets_relevance').on(table.relevance_score, table.volume_24h)
  })
);

export const outcomes = pgTable(
  'outcomes',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    polymarket_id: text('polymarket_id').notNull(),
    market_id: uuid('market_id').references(() => markets.id, { onDelete: 'cascade' }),
    title: text('title'),
    description: text('description'),
    price: decimal('price', { precision: 20, scale: 8 }).default('0'),
    probability: decimal('probability', { precision: 10, scale: 6 }).default('0'),
    volume: decimal('volume', { precision: 20, scale: 8 }).default('0'),
    status: text('status'),
    display_order: integer('display_order'),
    updated_at: timestamp('updated_at', { withTimezone: true }).defaultNow()
  },
  (table) => ({
    polymarketKey: uniqueIndex('outcomes_polymarket_id_key').on(table.polymarket_id),
    marketIdx: index('idx_outcomes_market_id').on(table.market_id)
  })
);

export const marketPricesRealtime = pgTable(
  'market_prices_realtime',
  {
    id: bigserial('id', { mode: 'bigint' }).primaryKey(),
    market_id: uuid('market_id').references(() => markets.id, { onDelete: 'cascade' }),
    price: decimal('price', { precision: 20, scale: 8 }),
    best_bid: decimal('best_bid', { precision: 20, scale: 8 }),
    best_ask: decimal('best_ask', { precision: 20, scale: 8 }),
    last_trade_price: decimal('last_trade_price', { precision: 20, scale: 8 }),
    liquidity: decimal('liquidity', { precision: 20, scale: 8 }),
    volume_24h: decimal('volume_24h', { precision: 20, scale: 8 }),
    updated_at: timestamp('updated_at', { withTimezone: true }).defaultNow()
  },
  (table) => ({
    marketIdx: index('idx_ticks_market_id').on(table.market_id)
  })
);

export type Event = typeof events.$inferInsert;
export type Market = typeof markets.$inferInsert;
export type Outcome = typeof outcomes.$inferInsert;
export type MarketPrice = typeof marketPricesRealtime.$inferInsert;

const rawConnectionString = process.env.DATABASE_URL;
if (!rawConnectionString) {
  throw new Error('DATABASE_URL environment variable is not set');
}
const connectionString: string = rawConnectionString;

let clientInstance: ReturnType<typeof postgres> | null = null;

function getClient() {
  if (!clientInstance) {
    clientInstance = postgres(connectionString, {
      max: Number(process.env.DB_POOL_MAX || 5),
      idle_timeout: Number(process.env.DB_IDLE_TIMEOUT || 30),
      connect_timeout: Number(process.env.DB_CONN_TIMEOUT || 15),
      max_lifetime: 60 * 30,
      prepare: false
    });
  }
  return clientInstance;
}

export const dbInstance = drizzle(getClient(), {
  schema: { events, markets, outcomes, marketPricesRealtime }
});
