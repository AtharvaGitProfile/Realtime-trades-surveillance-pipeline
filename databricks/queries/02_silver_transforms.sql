-- ============================================================
-- 02_silver_transforms.sql
-- Transform bronze Delta tables into cleaned silver tables.
-- Prerequisites: 01_create_schemas.sql, bronze tables loaded
--   via trade_data_ingestion.ipynb.
-- ============================================================

-- ── Alerts silver ────────────────────────────────────────────
DROP TABLE IF EXISTS workspace.silver.alerts;

CREATE TABLE workspace.silver.alerts
USING DELTA
AS
SELECT
    LOWER(alert_type)          AS alert_type,
    trader_id,
    UPPER(symbol)              AS symbol,
    cancel_count,
    total_orders,
    cancel_rate,
    CAST(match_count AS INT)   AS match_count,
    trader_id_1,
    trader_id_2,
    window_start,
    window_end,
    detected_at
FROM workspace.bronze.alerts
-- Remove duplicate detection windows (same alert fired by overlapping Flink windows)
WHERE (alert_type, trader_id, symbol, window_start, window_end) IN (
    SELECT alert_type, trader_id, symbol, window_start, window_end
    FROM workspace.bronze.alerts
    GROUP BY alert_type, trader_id, symbol, window_start, window_end
    HAVING COUNT(*) >= 1
);

-- ── Orders silver ─────────────────────────────────────────────
DROP TABLE IF EXISTS workspace.silver.orders;

CREATE TABLE workspace.silver.orders
USING DELTA
AS
SELECT DISTINCT
    order_id,
    trader_id,
    LOWER(trader_type)  AS trader_type,
    UPPER(symbol)       AS symbol,
    LOWER(side)         AS side,
    price,
    quantity,
    LOWER(status)       AS status,
    timestamp
FROM workspace.bronze.orders;

-- ── Trades silver ─────────────────────────────────────────────
DROP TABLE IF EXISTS workspace.silver.trades;

CREATE TABLE workspace.silver.trades
USING DELTA
AS
SELECT DISTINCT
    trade_id,
    order_id,
    trader_id,
    LOWER(trader_type)  AS trader_type,
    UPPER(symbol)       AS symbol,
    LOWER(side)         AS side,
    price,
    quantity,
    timestamp
FROM workspace.bronze.trades;
