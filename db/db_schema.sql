-- =====================================================
-- EXTENSIONS
-- =====================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =====================================================
-- DROP TABLES IN ORDER FOR RESET
-- =====================================================
DROP TABLE IF EXISTS market_prices_realtime CASCADE;
DROP TABLE IF EXISTS outcomes CASCADE;
DROP TABLE IF EXISTS markets CASCADE;
DROP TABLE IF EXISTS events CASCADE;

-- =====================================================
-- EVENTS TABLE
-- =====================================================
CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    polymarket_id TEXT UNIQUE NOT NULL,
    ticker TEXT,
    slug TEXT,
    title TEXT NOT NULL,
    description TEXT,
    category TEXT,
    subcategory TEXT,

    active BOOLEAN DEFAULT FALSE,
    closed BOOLEAN DEFAULT FALSE,
    archived BOOLEAN DEFAULT FALSE,
    restricted BOOLEAN DEFAULT FALSE,

    status TEXT DEFAULT 'active' CHECK (status IN ('upcoming', 'active', 'resolved', 'cancelled')),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    resolution_date TIMESTAMPTZ,

    liquidity DECIMAL(20,8) DEFAULT 0,
    volume_24h DECIMAL(20,8) DEFAULT 0,
    volume_total DECIMAL(20,8) DEFAULT 0,
    open_interest DECIMAL(20,8) DEFAULT 0,
    price_change_24h_percent DECIMAL(10,2) DEFAULT 0,

    relevance_score SMALLINT DEFAULT 1 CHECK (relevance_score BETWEEN 1 AND 10),

    image_url TEXT,
    icon_url TEXT,
    source_url TEXT,
    rules TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- MARKETS TABLE
-- =====================================================
CREATE TABLE markets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    polymarket_id TEXT UNIQUE,
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,

    question TEXT NOT NULL,
    description TEXT,
    slug TEXT,
    market_type TEXT DEFAULT 'binary' CHECK (market_type IN ('binary', 'categorical', 'numeric')),

    active BOOLEAN DEFAULT FALSE,
    closed BOOLEAN DEFAULT FALSE,
    archived BOOLEAN DEFAULT FALSE,
    restricted BOOLEAN DEFAULT FALSE,
    approved BOOLEAN DEFAULT TRUE,

    liquidity DECIMAL(20,8) DEFAULT 0,
    liquidity_amm DECIMAL(20,8) DEFAULT 0,
    liquidity_clob DECIMAL(20,8) DEFAULT 0,

    volume_24h DECIMAL(20,8) DEFAULT 0,
    volume_total DECIMAL(20,8) DEFAULT 0,

    current_price DECIMAL(10,8) DEFAULT 0,
    yes_probability DECIMAL(5,4) DEFAULT 0 CHECK (yes_probability BETWEEN 0 AND 1),
    no_probability DECIMAL(5,4) DEFAULT 0 CHECK (no_probability BETWEEN 0 AND 1),

    last_trade_price DECIMAL(10,8),
    best_bid DECIMAL(10,8),
    best_ask DECIMAL(10,8),
    spread DECIMAL(10,8),

    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'paused', 'resolved', 'cancelled')),
    resolved_outcome_id UUID,
    resolved_at TIMESTAMPTZ,

    market_maker_address TEXT,

    relevance_score SMALLINT DEFAULT 1 CHECK (relevance_score BETWEEN 1 AND 10),

    image_url TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- OUTCOMES TABLE
-- =====================================================
CREATE TABLE outcomes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    description TEXT,
    probability DECIMAL(5,4) DEFAULT 0 CHECK (probability BETWEEN 0 AND 1),
    price DECIMAL(10,8) DEFAULT 0,
    volume DECIMAL(20,8) DEFAULT 0,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'resolved_true', 'resolved_false', 'cancelled')),
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (market_id, title, status)
);

-- =====================================================
-- REALTIME PRICE/TICK TABLE
-- =====================================================
CREATE TABLE market_prices_realtime (
    id BIGSERIAL PRIMARY KEY,
    market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,

    price DECIMAL(10,8),
    best_bid DECIMAL(10,8),
    best_ask DECIMAL(10,8),
    last_trade_price DECIMAL(10,8),
    liquidity DECIMAL(20,8),
    volume_24h DECIMAL(20,8),

    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- TRIGGERS
-- =====================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_updated_at
    BEFORE UPDATE ON events
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_markets_updated_at
    BEFORE UPDATE ON markets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_outcomes_updated_at
    BEFORE UPDATE ON outcomes
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_events_status ON events(status);
CREATE INDEX idx_events_category ON events(category);
CREATE INDEX idx_events_relevance ON events(relevance_score DESC, volume_24h DESC);

CREATE INDEX idx_markets_event_id ON markets(event_id);
CREATE INDEX idx_markets_status ON markets(status);
CREATE INDEX idx_markets_active ON markets(active);
CREATE INDEX idx_markets_relevance ON markets(relevance_score DESC, volume_24h DESC);

CREATE INDEX idx_mpr_market_id ON market_prices_realtime(market_id);
CREATE INDEX idx_mpr_updated_at ON market_prices_realtime(updated_at);

-- =====================================================
-- ✅ SNAPSHOT UPDATER
-- Updates `markets` with latest tick only when needed
-- =====================================================
CREATE OR REPLACE FUNCTION refresh_market_snapshot()
RETURNS void AS $$
BEGIN
    UPDATE markets m
    SET
        current_price = r.price,
        best_bid = r.best_bid,
        best_ask = r.best_ask,
        last_trade_price = r.last_trade_price,
        liquidity = r.liquidity,
        volume_24h = r.volume_24h,
        updated_at = NOW()
    FROM (
        SELECT DISTINCT ON (market_id) *
        FROM market_prices_realtime
        ORDER BY market_id, updated_at DESC
    ) r
    WHERE m.id = r.market_id;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ✅ PRUNING FUNCTION (optional daily cron)
-- keeps realtime table small
-- =====================================================
CREATE OR REPLACE FUNCTION prune_old_market_ticks()
RETURNS void AS $$
BEGIN
    DELETE FROM market_prices_realtime
    WHERE updated_at < NOW() - INTERVAL '24 hours';
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- ROW LEVEL SECURITY
-- =====================================================
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE markets ENABLE ROW LEVEL SECURITY;
ALTER TABLE outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_prices_realtime ENABLE ROW LEVEL SECURITY;

CREATE POLICY events_select_all ON events FOR SELECT USING (true);
CREATE POLICY markets_select_all ON markets FOR SELECT USING (true);
CREATE POLICY outcomes_select_all ON outcomes FOR SELECT USING (true);
CREATE POLICY market_prices_realtime_select_all ON market_prices_realtime FOR SELECT USING (true);

CREATE POLICY events_write_service_role ON events FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY markets_write_service_role ON markets FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY outcomes_write_service_role ON outcomes FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY market_prices_realtime_write_service_role ON market_prices_realtime FOR ALL USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');

-- =====================================================
-- COMMENTS
-- =====================================================
COMMENT ON TABLE events IS 'Top-level events (sports, politics, finance) ingested from Polymarket';
COMMENT ON TABLE markets IS 'Prediction markets belonging to events';
COMMENT ON TABLE outcomes IS 'Outcome options with prices & probability';
COMMENT ON TABLE market_prices_realtime IS 'High-frequency price stream from WebSocket ticks';
COMMENT ON FUNCTION refresh_market_snapshot IS 'Updates markets with latest price tick (run every few seconds or on trigger)';
COMMENT ON FUNCTION prune_old_market_ticks IS 'Removes stale ticks older than 24 hours to save storage';
