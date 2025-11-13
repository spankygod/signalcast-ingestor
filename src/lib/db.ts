import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

import {
  pgTable,
  uuid,
  text,
  boolean,
  decimal,
  timestamp,
  smallint,
  integer,
  bigserial,
  index
} from 'drizzle-orm/pg-core';

export const events = pgTable('events', {
  id: uuid('id').primaryKey().defaultRandom(),
  polymarketId: text('polymarket_id').unique().notNull(),
  ticker: text('ticker'),
  slug: text('slug'),
  title: text('title').notNull(),
  description: text('description'),
  category: text('category'),
  subcategory: text('subcategory'),
  active: boolean('active').default(false),
  closed: boolean('closed').default(false),
  archived: boolean('archived').default(false),
  restricted: boolean('restricted').default(false),
  status: text('status').default('active'),
  startDate: timestamp('start_date', { withTimezone: true }),
  endDate: timestamp('end_date', { withTimezone: true }),
  resolutionDate: timestamp('resolution_date', { withTimezone: true }),
  liquidity: decimal('liquidity', { precision: 20, scale: 8 }).default('0'),
  volume24h: decimal('volume_24h', { precision: 20, scale: 8 }).default('0'),
  volumeTotal: decimal('volume_total', { precision: 20, scale: 8 }).default('0'),
  openInterest: decimal('open_interest', { precision: 20, scale: 8 }).default('0'),
  priceChange24hPercent: decimal('price_change_24h_percent', { precision: 10, scale: 2 }).default('0'),
  relevanceScore: smallint('relevance_score').default(1),
  imageUrl: text('image_url'),
  iconUrl: text('icon_url'),
  sourceUrl: text('source_url'),
  rules: text('rules'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow(),
  lastIngestedAt: timestamp('last_ingested_at', { withTimezone: true }).defaultNow()
}, (table) => ({
  statusIdx: index('idx_events_status').on(table.status),
  categoryIdx: index('idx_events_category').on(table.category),
  relevanceIdx: index('idx_events_relevance').on(table.relevanceScore, table.volume24h)
}));

export const markets = pgTable('markets', {
  id: uuid('id').primaryKey().defaultRandom(),
  polymarketId: text('polymarket_id').unique(),
  eventId: uuid('event_id').notNull().references(() => events.id, { onDelete: 'cascade' }),
  question: text('question').notNull(),
  description: text('description'),
  slug: text('slug'),
  marketType: text('market_type').default('binary'),
  active: boolean('active').default(false),
  closed: boolean('closed').default(false),
  archived: boolean('archived').default(false),
  restricted: boolean('restricted').default(false),
  approved: boolean('approved').default(true),
  liquidity: decimal('liquidity', { precision: 20, scale: 8 }).default('0'),
  liquidityAmm: decimal('liquidity_amm', { precision: 20, scale: 8 }).default('0'),
  liquidityClob: decimal('liquidity_clob', { precision: 20, scale: 8 }).default('0'),
  volume24h: decimal('volume_24h', { precision: 20, scale: 8 }).default('0'),
  volumeTotal: decimal('volume_total', { precision: 20, scale: 8 }).default('0'),
  currentPrice: decimal('current_price', { precision: 10, scale: 8 }).default('0'),
  yesProbability: decimal('yes_probability', { precision: 5, scale: 4 }).default('0'),
  noProbability: decimal('no_probability', { precision: 5, scale: 4 }).default('0'),
  lastTradePrice: decimal('last_trade_price', { precision: 10, scale: 8 }),
  bestBid: decimal('best_bid', { precision: 10, scale: 8 }),
  bestAsk: decimal('best_ask', { precision: 10, scale: 8 }),
  spread: decimal('spread', { precision: 10, scale: 8 }),
  status: text('status').default('active'),
  resolvedOutcomeId: uuid('resolved_outcome_id'),
  resolvedAt: timestamp('resolved_at', { withTimezone: true }),
  marketMakerAddress: text('market_maker_address'),
  relevanceScore: smallint('relevance_score').default(1),
  imageUrl: text('image_url'),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow(),
  lastIngestedAt: timestamp('last_ingested_at', { withTimezone: true }).defaultNow()
}, (table) => ({
  eventIdIdx: index('idx_markets_event_id').on(table.eventId),
  statusIdx: index('idx_markets_status').on(table.status),
  activeIdx: index('idx_markets_active').on(table.active),
  relevanceIdx: index('idx_markets_relevance').on(table.relevanceScore, table.volume24h)
}));

export const outcomes = pgTable('outcomes', {
  id: uuid('id').primaryKey().defaultRandom(),
  polymarketId: text('polymarket_id').unique().notNull(),
  marketId: uuid('market_id').notNull().references(() => markets.id, { onDelete: 'cascade' }),
  title: text('title').notNull(),
  description: text('description'),
  probability: decimal('probability', { precision: 5, scale: 4 }).default('0'),
  price: decimal('price', { precision: 10, scale: 8 }).default('0'),
  volume: decimal('volume', { precision: 20, scale: 8 }).default('0'),
  status: text('status').default('active'),
  displayOrder: integer('display_order').default(0),
  createdAt: timestamp('created_at', { withTimezone: true }).defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow()
}, (table) => ({
  polymarketIdx: index('idx_outcomes_polymarket').on(table.polymarketId)
}));

export const marketPricesRealtime = pgTable('market_prices_realtime', {
  id: bigserial('id', { mode: 'bigint' }).primaryKey(),
  marketId: uuid('market_id').notNull().references(() => markets.id, { onDelete: 'cascade' }),
  price: decimal('price', { precision: 10, scale: 8 }),
  bestBid: decimal('best_bid', { precision: 10, scale: 8 }),
  bestAsk: decimal('best_ask', { precision: 10, scale: 8 }),
  lastTradePrice: decimal('last_trade_price', { precision: 10, scale: 8 }),
  liquidity: decimal('liquidity', { precision: 20, scale: 8 }),
  volume24h: decimal('volume_24h', { precision: 20, scale: 8 }),
  updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow()
}, (table) => ({
  marketIdIdx: index('idx_mpr_market_id').on(table.marketId),
  updatedAtIdx: index('idx_mpr_updated_at').on(table.updatedAt)
}));

export type Event = typeof events.$inferSelect;
export type NewEvent = typeof events.$inferInsert;
export type Market = typeof markets.$inferSelect;
export type NewMarket = typeof markets.$inferInsert;
export type Outcome = typeof outcomes.$inferSelect;
export type NewOutcome = typeof outcomes.$inferInsert;
export type MarketPriceRealtime = typeof marketPricesRealtime.$inferSelect;
export type NewMarketPriceRealtime = typeof marketPricesRealtime.$inferInsert;

let dbInstance: ReturnType<typeof drizzle> | null = null;
let clientInstance: ReturnType<typeof postgres> | null = null;

function getDatabaseUrl(): string {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL environment variable is required');
  }
  return connectionString;
}

function createClient() {
  if (!clientInstance) {
    const connectionString = getDatabaseUrl();
    clientInstance = postgres(connectionString, {
      max: Number(process.env.DB_POOL_MAX || 10),
      idle_timeout: Number(process.env.DB_IDLE_TIMEOUT || 20),
      connect_timeout: Number(process.env.DB_CONN_TIMEOUT || 10),
      prepare: false
    });
  }
  return clientInstance;
}

function createDatabase() {
  if (!dbInstance) {
    const client = createClient();
    dbInstance = drizzle(client, { schema: { events, markets, outcomes, marketPricesRealtime } });
  }
  return dbInstance;
}

export const db = new Proxy({} as ReturnType<typeof drizzle>, {
  get(_, prop) {
    const actualDb = createDatabase();
    return Reflect.get(actualDb, prop);
  },
  has(_, prop) {
    const actualDb = createDatabase();
    return Reflect.has(actualDb, prop);
  },
  apply(_, thisArg, args) {
    const actualDb = createDatabase();
    return Reflect.apply(actualDb as any, thisArg, args);
  }
});

export const client = createClient();

export const schema = {
  events,
  markets,
  outcomes,
  marketPricesRealtime
};
