-- ============================================================
-- 04_gold_asset_alert_activity.sql
-- Gold table: per-symbol alert breakdown used by the
-- "Asset Monitoring" panel of the compliance dashboard.
-- ============================================================

DROP TABLE IF EXISTS workspace.gold.asset_alert_activity;

CREATE TABLE workspace.gold.asset_alert_activity
USING DELTA
AS
SELECT
    symbol,
    alert_type,
    COUNT(*)                AS alert_count,
    AVG(cancel_rate)        AS avg_cancel_rate,
    MIN(detected_at)        AS first_detected,
    MAX(detected_at)        AS latest_detected
FROM workspace.silver.alerts
GROUP BY symbol, alert_type
ORDER BY alert_count DESC;
