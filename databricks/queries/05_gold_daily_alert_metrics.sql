-- ============================================================
-- 05_gold_daily_alert_metrics.sql
-- Gold table: daily time-series of alert volume used by the
-- "Live Alerts" trend chart on the compliance dashboard.
-- ============================================================

DROP TABLE IF EXISTS workspace.gold.daily_alert_metrics;

CREATE TABLE workspace.gold.daily_alert_metrics
USING DELTA
AS
SELECT
    DATE(detected_at)                           AS alert_date,
    alert_type,
    COUNT(*)                                    AS total_alerts,
    COUNT(DISTINCT COALESCE(trader_id,
                            trader_id_1))       AS unique_traders,
    COUNT(DISTINCT symbol)                      AS unique_symbols,
    AVG(cancel_rate)                            AS avg_cancel_rate
FROM workspace.silver.alerts
GROUP BY DATE(detected_at), alert_type
ORDER BY alert_date DESC, alert_type;
