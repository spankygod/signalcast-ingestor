import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import dotenv from "dotenv";

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
  uniqueIndex,
} from "drizzle-orm/pg-core";

// EVENTS -----------------------------------------
export const events = pgTable(
  "events",
  {
    id: uuid("id").primaryKey().defaultRandom(),

    // camelCase field -> snake_case column
    polymarketId: text("polymarket_id").notNull(),

    slug: text("slug"),
    title: text("title").notNull(),
    description: text("description"),
    category: text("category"),
    subcategory: text("subcategory"),

    liquidity: decimal("liquidity", { precision: 20, scale: 8 }).default("0"),

    // camelCase in TS, snake_case in DB
    volume24h: decimal("volume_24h", { precision: 20, scale: 8 }).default("0"),
    volumeTotal: decimal("volume_total", { precision: 20, scale: 8 }).default("0"),

    active: boolean("active").default(false),
    closed: boolean("closed").default(false),
    archived: boolean("archived").default(false),
    restricted: boolean("restricted").default(false),

    relevanceScore: decimal("relevance_score", {
      precision: 10,
      scale: 4,
    }).default("0"),

    startDate: timestamp("start_date", { withTimezone: true }),
    endDate: timestamp("end_date", { withTimezone: true }),

    lastIngestedAt: timestamp("last_ingested_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (table) => ({
    polymarketKey: uniqueIndex("events_polymarket_id_key").on(
      table.polymarketId
    ),
    slugIdx: index("idx_events_slug").on(table.slug),
    lastIngestedIdx: index("idx_events_last_ingested").on(table.lastIngestedAt),
    relevanceIdx: index("idx_events_relevance").on(
      table.relevanceScore,
      table.volume24h
    ),
  })
);

// MARKETS ----------------------------------------
export const markets = pgTable(
  "markets",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    polymarketId: text("polymarket_id").notNull(),

    eventId: uuid("event_id").references(() => events.id, {
      onDelete: "cascade",
    }),

    question: text("question"),
    slug: text("slug"),
    description: text("description"),

    liquidity: decimal("liquidity", { precision: 20, scale: 8 }).default("0"),
    volume24h: decimal("volume_24h", { precision: 20, scale: 8 }).default("0"),
    volumeTotal: decimal("volume_total", { precision: 20, scale: 8 }).default(
      "0"
    ),

    currentPrice: decimal("current_price", { precision: 20, scale: 8 }),
    lastTradePrice: decimal("last_trade_price", {
      precision: 20,
      scale: 8,
    }),
    bestBid: decimal("best_bid", { precision: 20, scale: 8 }),
    bestAsk: decimal("best_ask", { precision: 20, scale: 8 }),

    active: boolean("active").default(true),
    closed: boolean("closed").default(false),
    archived: boolean("archived").default(false),
    restricted: boolean("restricted").default(false),
    approved: boolean("approved").default(true),

    status: text("status"),
    resolvedAt: timestamp("resolved_at", { withTimezone: true }),

    relevanceScore: decimal("relevance_score", {
      precision: 10,
      scale: 4,
    }).default("0"),

    lastIngestedAt: timestamp("last_ingested_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (table) => ({
    polymarketKey: uniqueIndex("markets_polymarket_id_key").on(
      table.polymarketId
    ),
    eventIdx: index("idx_markets_event_id").on(table.eventId),
    relevanceIdx: index("idx_markets_relevance").on(
      table.relevanceScore,
      table.volume24h
    ),
  })
);

// OUTCOMES ---------------------------------------
export const outcomes = pgTable(
  "outcomes",
  {
    id: uuid("id").primaryKey().defaultRandom(),

    polymarketId: text("polymarket_id").notNull(),

    marketId: uuid("market_id").references(() => markets.id, {
      onDelete: "cascade",
    }),

    title: text("title"),
    description: text("description"),

    price: decimal("price", { precision: 20, scale: 8 }).default("0"),
    probability: decimal("probability", {
      precision: 10,
      scale: 6,
    }).default("0"),
    volume: decimal("volume", { precision: 20, scale: 8 }).default("0"),

    status: text("status"),
    displayOrder: integer("display_order"),

    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (table) => ({
    polymarketKey: uniqueIndex("outcomes_polymarket_id_key").on(
      table.polymarketId
    ),
    marketIdx: index("idx_outcomes_market_id").on(table.marketId),
  })
);

// TICKS ------------------------------------------
export const marketPricesRealtime = pgTable(
  "market_prices_realtime",
  {
    id: bigserial("id", { mode: "bigint" }).primaryKey(),

    marketId: uuid("market_id").references(() => markets.id, {
      onDelete: "cascade",
    }),

    price: decimal("price", { precision: 20, scale: 8 }),
    bestBid: decimal("best_bid", { precision: 20, scale: 8 }),
    bestAsk: decimal("best_ask", { precision: 20, scale: 8 }),
    lastTradePrice: decimal("last_trade_price", {
      precision: 20,
      scale: 8,
    }),
    liquidity: decimal("liquidity", { precision: 20, scale: 8 }),
    volume24h: decimal("volume_24h", { precision: 20, scale: 8 }),

    updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
  },
  (table) => ({
    marketIdx: index("idx_ticks_market_id").on(table.marketId),
  })
);

// Types ------------------------------------------
export type Event = typeof events.$inferInsert;
export type Market = typeof markets.$inferInsert;
export type Outcome = typeof outcomes.$inferInsert;
export type MarketPrice = typeof marketPricesRealtime.$inferInsert;

// Connection -------------------------------------
const rawConnectionString = process.env.DATABASE_URL;
if (!rawConnectionString) {
  throw new Error("DATABASE_URL environment variable is not set");
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
      prepare: false,
    });
  }
  return clientInstance;
}

export const db = drizzle(getClient(), {
  schema: { events, markets, outcomes, marketPricesRealtime },
});
