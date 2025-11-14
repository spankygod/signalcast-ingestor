-- =====================================================
-- EXTENSIONS
-- =====================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =====================================================
-- TABLES
-- =====================================================

-- EVENTS -----------------------
CREATE TABLE IF NOT EXISTS public.events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    polymarket_id TEXT UNIQUE NOT NULL,
    slug TEXT,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    subcategory TEXT,
    liquidity NUMERIC DEFAULT 0,
    volume_24h NUMERIC DEFAULT 0,
    volume_total NUMERIC DEFAULT 0,
    active BOOLEAN DEFAULT FALSE,
    closed BOOLEAN DEFAULT FALSE,
    archived BOOLEAN DEFAULT FALSE,
    restricted BOOLEAN DEFAULT FALSE,
    relevance_score NUMERIC DEFAULT 0,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    last_ingested_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- MARKETS -----------------------
CREATE TABLE IF NOT EXISTS public.markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    polymarket_id TEXT UNIQUE NOT NULL,
    event_id UUID REFERENCES public.events(id) ON DELETE CASCADE,
    question TEXT,
    slug TEXT,
    description TEXT,
    liquidity NUMERIC DEFAULT 0,
    volume_24h NUMERIC DEFAULT 0,
    volume_total NUMERIC DEFAULT 0,
    current_price NUMERIC,
    last_trade_price NUMERIC,
    best_bid NUMERIC,
    best_ask NUMERIC,
    active BOOLEAN DEFAULT TRUE,
    closed BOOLEAN DEFAULT FALSE,
    archived BOOLEAN DEFAULT FALSE,
    restricted BOOLEAN DEFAULT FALSE,
    approved BOOLEAN DEFAULT TRUE,
    status TEXT,
    resolved_at TIMESTAMPTZ,
    relevance_score NUMERIC DEFAULT 0,
    last_ingested_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- OUTCOMES ----------------------
CREATE TABLE IF NOT EXISTS public.outcomes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    polymarket_id TEXT UNIQUE NOT NULL,
    market_id UUID REFERENCES public.markets(id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    price NUMERIC DEFAULT 0,
    probability NUMERIC DEFAULT 0,
    volume NUMERIC DEFAULT 0,
    status TEXT,
    display_order INT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- TICKS / REALTIME PRICES --------
-- (You said ticks are ephemeral via Redis, but this table
-- is kept because you said not to drop any existing tables.)
CREATE TABLE IF NOT EXISTS public.market_prices_realtime (
    id BIGSERIAL PRIMARY KEY,
    market_id UUID REFERENCES public.markets(id) ON DELETE CASCADE,
    price NUMERIC,
    best_bid NUMERIC,
    best_ask NUMERIC,
    last_trade_price NUMERIC,
    liquidity NUMERIC,
    volume_24h NUMERIC,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- INDEXES
-- =====================================================

-- EVENTS: existing indexes
CREATE UNIQUE INDEX IF NOT EXISTS events_polymarket_id_key
    ON public.events (polymarket_id);

CREATE INDEX IF NOT EXISTS idx_events_slug
    ON public.events (slug);

CREATE INDEX IF NOT EXISTS idx_events_last_ingested
    ON public.events (last_ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_relevance
    ON public.events (relevance_score DESC, volume_24h DESC);

CREATE INDEX IF NOT EXISTS idx_events_start_date
    ON public.events (start_date DESC);

CREATE INDEX IF NOT EXISTS idx_events_id
    ON public.events (id);

CREATE INDEX IF NOT EXISTS idx_events_category
    ON public.events (category);

-- EVENTS: optimization indexes
-- Composite for active/closed filters and recency
CREATE INDEX IF NOT EXISTS idx_events_active
    ON public.events (active, closed, updated_at DESC);

-- For fast ON CONFLICT (polymarket_id) + recency lookups
CREATE INDEX IF NOT EXISTS idx_events_polymarket_update
    ON public.events (polymarket_id, updated_at DESC);

-- Category + updated_at for filtered lists by category
CREATE INDEX IF NOT EXISTS idx_events_category_updated
    ON public.events (category, updated_at DESC);

-- Partial index for active & open events (most common query)
CREATE INDEX IF NOT EXISTS idx_events_active_only
    ON public.events (updated_at DESC)
    WHERE active = TRUE AND closed = FALSE;

-- MARKETS: existing indexes
CREATE UNIQUE INDEX IF NOT EXISTS markets_polymarket_id_key
    ON public.markets (polymarket_id);

CREATE INDEX IF NOT EXISTS idx_markets_event_id
    ON public.markets (event_id);

CREATE INDEX IF NOT EXISTS idx_markets_relevance
    ON public.markets (relevance_score DESC, volume_24h DESC);

-- MARKETS: optimization indexes
-- Active markets by recency
CREATE INDEX IF NOT EXISTS idx_markets_active
    ON public.markets (active, closed, updated_at DESC);

-- For ON CONFLICT + recency
CREATE INDEX IF NOT EXISTS idx_markets_polymarket_update
    ON public.markets (polymarket_id, updated_at DESC);

-- Markets under an event ordered by relevance
CREATE INDEX IF NOT EXISTS idx_markets_event_relevance
    ON public.markets (event_id, relevance_score DESC);

-- Partial index for active & open markets
CREATE INDEX IF NOT EXISTS idx_markets_active_only
    ON public.markets (updated_at DESC)
    WHERE active = TRUE AND closed = FALSE;

-- OUTCOMES: existing indexes
CREATE UNIQUE INDEX IF NOT EXISTS outcomes_polymarket_id_key
    ON public.outcomes (polymarket_id);

CREATE INDEX IF NOT EXISTS idx_outcomes_market_id
    ON public.outcomes (market_id);

-- OUTCOMES: optimization indexes
-- For ON CONFLICT + recency
CREATE INDEX IF NOT EXISTS idx_outcomes_polymarket_update
    ON public.outcomes (polymarket_id, updated_at DESC);

-- Fast retrieval of ordered outcomes per market
CREATE INDEX IF NOT EXISTS idx_outcomes_market_display
    ON public.outcomes (market_id, display_order ASC);

-- TICKS: existing index
CREATE INDEX IF NOT EXISTS idx_ticks_market_id
    ON public.market_prices_realtime (market_id);

-- TICKS: optimization index for "latest tick per market"
CREATE INDEX IF NOT EXISTS idx_ticks_market_time
    ON public.market_prices_realtime (market_id, updated_at DESC);
