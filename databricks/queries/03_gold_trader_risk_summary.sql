-- ============================================================
-- 03_gold_trader_risk_summary.sql
-- Gold table: per-trader risk ranking used by the compliance
-- dashboard "Trader Risk" panel.
--
-- Handles the schema asymmetry between alert types:
--   SPOOFING     → trader_id  (single trader)
--   WASH_TRADING → trader_id_1 + trader_id_2 (colluding pair)
-- Both are unioned into a common trader_id column before
-- aggregation so every flagged trader appears once.
-- ============================================================

DROP TABLE IF EXISTS workspace.gold.trader_risk_summary;

CREATE TABLE workspace.gold.trader_risk_summary
USING DELTA
AS
WITH all_flagged_traders AS (

    -- Spoofing: single trader per alert
    SELECT
        trader_id,
        symbol,
        alert_type,
        cancel_rate,
        cancel_count,
        detected_at
    FROM workspace.silver.alerts
    WHERE trader_id IS NOT NULL

    UNION ALL

    -- Wash trading: flag the buyer
    SELECT
        trader_id_1       AS trader_id,
        symbol,
        alert_type,
        cancel_rate,
        cancel_count,
        detected_at
    FROM workspace.silver.alerts
    WHERE trader_id_1 IS NOT NULL

    UNION ALL

    -- Wash trading: flag the seller / partner
    SELECT
        trader_id_2       AS trader_id,
        symbol,
        alert_type,
        cancel_rate,
        cancel_count,
        detected_at
    FROM workspace.silver.alerts
    WHERE trader_id_2 IS NOT NULL
)

SELECT
    trader_id,
    COUNT(*)                        AS total_alerts,
    COUNT(DISTINCT symbol)          AS distinct_symbols_flagged,
    COUNT(DISTINCT alert_type)      AS distinct_alert_types,
    AVG(cancel_rate)                AS avg_cancel_rate,
    MAX(cancel_count)               AS max_cancel_count_in_window,
    MIN(detected_at)                AS first_alert_at,
    MAX(detected_at)                AS latest_alert_at
FROM all_flagged_traders
GROUP BY trader_id
ORDER BY total_alerts DESC;
