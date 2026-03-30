-- ============================================
-- Trade Surveillance Platform - Database Init
-- ============================================

-- Alerts table: where Flink writes detected suspicious activity
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_type      VARCHAR(50) NOT NULL,  -- 'spoofing', 'wash_trading', 'front_running'
    severity        VARCHAR(20) NOT NULL,  -- 'low', 'medium', 'high', 'critical'
    trader_id       VARCHAR(50) NOT NULL,
    asset           VARCHAR(20) NOT NULL,
    description     TEXT NOT NULL,
    details         JSONB,                 -- flexible payload with detection specifics
    market_context  JSONB,                 -- volatility, spread, volume at time of alert
    detected_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status          VARCHAR(20) NOT NULL DEFAULT 'open',  -- 'open', 'investigating', 'escalated', 'dismissed'
    reviewed_by     VARCHAR(100),
    reviewed_at     TIMESTAMP WITH TIME ZONE,
    notes           TEXT
);

-- Indexes for common compliance analyst queries
CREATE INDEX idx_alerts_type ON alerts(alert_type);
CREATE INDEX idx_alerts_severity ON alerts(severity);
CREATE INDEX idx_alerts_trader ON alerts(trader_id);
CREATE INDEX idx_alerts_asset ON alerts(asset);
CREATE INDEX idx_alerts_status ON alerts(status);
CREATE INDEX idx_alerts_detected_at ON alerts(detected_at DESC);

-- Trader profiles: reference data for enrichment
CREATE TABLE IF NOT EXISTS trader_profiles (
    trader_id       VARCHAR(50) PRIMARY KEY,
    trader_name     VARCHAR(200) NOT NULL,
    trader_type     VARCHAR(50) NOT NULL,  -- 'retail', 'institutional', 'market_maker', 'broker'
    risk_rating     VARCHAR(20) NOT NULL DEFAULT 'normal',  -- 'low', 'normal', 'elevated', 'high'
    avg_daily_orders INTEGER DEFAULT 0,
    avg_order_size   NUMERIC(15,2) DEFAULT 0,
    registered_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_active     TIMESTAMP WITH TIME ZONE
);

-- Daily aggregation table (Gold layer summary)
CREATE TABLE IF NOT EXISTS trader_daily_activity (
    trader_id           VARCHAR(50) NOT NULL,
    activity_date       DATE NOT NULL,
    asset               VARCHAR(20) NOT NULL,
    total_orders        INTEGER DEFAULT 0,
    total_cancels       INTEGER DEFAULT 0,
    total_trades        INTEGER DEFAULT 0,
    cancel_rate         NUMERIC(5,4),
    total_volume        NUMERIC(15,2) DEFAULT 0,
    avg_time_to_cancel  INTERVAL,
    PRIMARY KEY (trader_id, activity_date, asset)
);

CREATE INDEX idx_daily_activity_date ON trader_daily_activity(activity_date DESC);
CREATE INDEX idx_daily_activity_cancel_rate ON trader_daily_activity(cancel_rate DESC);

DO $$ BEGIN RAISE NOTICE 'Trade surveillance database initialized successfully.'; END $$;
