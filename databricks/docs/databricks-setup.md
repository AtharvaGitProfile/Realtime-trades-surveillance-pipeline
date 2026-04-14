# Databricks Lakehouse — Setup & Architecture

This document covers the Unity Catalog structure, medallion layer semantics, and the SQL queries that back the compliance dashboard for the Realtime Trade Surveillance Pipeline.

---

## Medallion Architecture

Data flows through three progressive refinement layers, all stored as Delta tables under the `workspace` catalog.

```
MinIO surveillance-lake (S3-compatible)
        │
        │  CSV upload via Databricks Volume
        ▼
┌──────────────────────────────────┐
│  BRONZE  workspace.bronze.*      │  Raw CSV ingestion, no transforms
│  alerts / orders / trades        │
└─────────────────┬────────────────┘
                  │  02_silver_transforms.sql
                  ▼
┌──────────────────────────────────┐
│  SILVER  workspace.silver.*      │  Types cast, strings normalised,
│  alerts / orders / trades        │  duplicates removed
└─────────────────┬────────────────┘
                  │  03–05_gold_*.sql
                  ▼
┌──────────────────────────────────┐
│  GOLD    workspace.gold.*        │  Aggregated analytics tables
│  trader_risk_summary             │  powering the dashboard
│  asset_alert_activity            │
│  daily_alert_metrics             │
└──────────────────────────────────┘
```

---

## Catalog Structure

### `workspace.bronze` — Raw Ingestion

CSV files exported from MinIO (`databricks_upload/`) are uploaded to a Databricks Volume at `/Volumes/workspace/default/trade-lake/` and read directly into Delta tables via `trade_data_ingestion.ipynb`.

| Table | Source file | Rows loaded | Notes |
|---|---|---|---|
| `bronze.alerts` | `alerts.csv` | 5 000 | 2 500 SPOOFING + 2 500 WASH_TRADING, balanced sample |
| `bronze.orders` | `orders.csv` | 10 000 | All order lifecycle statuses (NEW / FILLED / CANCELLED) |
| `bronze.trades` | `trades.csv` | 5 000 | Expanded to two rows per execution (buy leg + sell leg) |

### `workspace.silver` — Cleaned Layer

Transformations applied uniformly across all tables:

| Transform | Detail |
|---|---|
| String normalisation | `alert_type`, `side`, `status`, `trader_type` → lowercase; `symbol` → uppercase |
| Type casting | `match_count` cast to `INT`; `window_start`, `window_end`, `detected_at` remain `TIMESTAMP` |
| Deduplication | Alerts: deduplicated on `(alert_type, trader_id, symbol, window_start, window_end)` — removes duplicate firings from overlapping Flink sliding windows. Orders: deduplicated on `(order_id, status)`. Trades: deduplicated on `trade_id`. |

Silver row counts after dedup (reference run):

| Table | Bronze rows | Silver rows | Dedup reduction |
|---|---|---|---|
| `silver.alerts` | 5 000 | ~4 249 | ~15% — overlapping 2-min Flink windows fire the same alert multiple times |
| `silver.orders` | 10 000 | 10 000 | No duplicates in order snapshot |
| `silver.trades` | 5 000 | 2 500 | Each raw trade was expanded into 2 rows; dedup collapses back to unique trade_ids |

### `workspace.gold` — Analytics Layer

Three aggregate tables, each rebuilt as a full overwrite from silver on each pipeline run.

---

## SQL Queries

### `01_create_schemas.sql`
Provisions the three Unity Catalog schemas (`bronze`, `silver`, `gold`). Safe to run repeatedly — all statements use `IF NOT EXISTS`.

### `02_silver_transforms.sql`
Rebuilds all three silver tables from bronze. Key detail: the `SPOOFING` alert schema uses `trader_id` (single trader), while `WASH_TRADING` uses `trader_id_1` + `trader_id_2` (colluding pair). Silver preserves this schema split; the union is handled at gold.

### `03_gold_trader_risk_summary.sql` — Trader Risk Panel

Powers the "Trader Risk Rankings" section of the Streamlit dashboard.

```sql
-- Flattens the trader_id / trader_id_1 / trader_id_2 split via UNION ALL,
-- then aggregates per trader.
SELECT
    trader_id,
    COUNT(*)                   AS total_alerts,
    COUNT(DISTINCT symbol)     AS distinct_symbols_flagged,
    COUNT(DISTINCT alert_type) AS distinct_alert_types,
    AVG(cancel_rate)           AS avg_cancel_rate,
    MAX(cancel_count)          AS max_cancel_count_in_window,
    MIN(detected_at)           AS first_alert_at,
    MAX(detected_at)           AS latest_alert_at
FROM all_flagged_traders   -- UNION of trader_id, trader_id_1, trader_id_2
GROUP BY trader_id
ORDER BY total_alerts DESC
```

Sample output (reference run):

| trader_id | total_alerts | avg_cancel_rate | first_alert_at |
|---|---|---|---|
| spoofer_003 | 1 083 | 0.841 | 2026-04-03 |
| spoofer_002 | 918 | 0.835 | 2026-04-03 |
| wash_002a | 775 | — | 2026-04-03 |
| wash_002b | 768 | — | 2026-04-03 |

### `04_gold_asset_alert_activity.sql` — Asset Monitoring Panel

Breaks down alert frequency by symbol and alert type.

```sql
SELECT
    symbol,
    alert_type,
    COUNT(*)         AS alert_count,
    AVG(cancel_rate) AS avg_cancel_rate,
    MIN(detected_at) AS first_detected,
    MAX(detected_at) AS latest_detected
FROM workspace.silver.alerts
GROUP BY symbol, alert_type
ORDER BY alert_count DESC
```

Sample output: `BINANCE:BTCUSDT` and `TSLA` are the highest-activity spoofing targets; `BINANCE:XRPUSDT` and `TSLA` dominate wash trading volume.

### `05_gold_daily_alert_metrics.sql` — Live Alerts Trend Chart

Daily time-series used to render the alert volume trend on the dashboard.

```sql
SELECT
    DATE(detected_at)                        AS alert_date,
    alert_type,
    COUNT(*)                                 AS total_alerts,
    COUNT(DISTINCT COALESCE(trader_id,
                            trader_id_1))    AS unique_traders,
    COUNT(DISTINCT symbol)                   AS unique_symbols,
    AVG(cancel_rate)                         AS avg_cancel_rate
FROM workspace.silver.alerts
GROUP BY DATE(detected_at), alert_type
ORDER BY alert_date DESC, alert_type
```

`COALESCE(trader_id, trader_id_1)` handles the schema split: spoofing alerts populate `trader_id`, wash trading alerts populate `trader_id_1`.

---

## Setup Steps

1. **Export CSVs** — run `python3 export_databricks.py` at the project root. Output lands in `databricks_upload/`.

2. **Upload to Volume** — in Databricks, upload the three CSV files to:
   ```
   /Volumes/workspace/default/trade-lake/
   ```

3. **Create schemas** — run `databricks/queries/01_create_schemas.sql` in the Databricks SQL editor.

4. **Load bronze + build silver/gold** — open and run all cells in `databricks/notebooks/trade_data_ingestion.ipynb`. The notebook reads the CSVs from the volume, writes bronze Delta tables, applies silver transforms, and builds all three gold aggregates.

5. **Run SQL files individually** (optional) — if you need to rebuild only one layer, run the corresponding numbered SQL file directly in the SQL editor. Files are ordered and idempotent (`DROP TABLE IF EXISTS` before each `CREATE`).

---

## Schema Reference

### `silver.alerts`

| Column | Type | Notes |
|---|---|---|
| `alert_type` | STRING | `spoofing` or `wash_trading` (lowercase) |
| `trader_id` | STRING | Populated for spoofing only |
| `symbol` | STRING | Uppercase, e.g. `AAPL`, `BINANCE:BTCUSDT` |
| `cancel_count` | INT | Orders cancelled in the detection window |
| `total_orders` | INT | Total orders in the detection window |
| `cancel_rate` | DOUBLE | `cancel_count / total_orders`; NULL for wash trading |
| `match_count` | INT | Matched buy/sell pairs; NULL for spoofing |
| `trader_id_1` | STRING | Populated for wash trading only (lexicographically min) |
| `trader_id_2` | STRING | Populated for wash trading only (lexicographically max) |
| `window_start` | TIMESTAMP | Flink detection window start |
| `window_end` | TIMESTAMP | Flink detection window end |
| `detected_at` | TIMESTAMP | Wall-clock time the alert was emitted |

### `silver.orders`

| Column | Type | Notes |
|---|---|---|
| `order_id` | STRING | UUID |
| `trader_id` | STRING | |
| `trader_type` | STRING | `normal`, `spoofer`, or `wash_trader` |
| `symbol` | STRING | Uppercase |
| `side` | STRING | `buy` or `sell` |
| `price` | DOUBLE | Limit price |
| `quantity` | INT | |
| `status` | STRING | `new`, `filled`, or `cancelled` |
| `timestamp` | STRING | ISO-8601 UTC |

### `silver.trades`

| Column | Type | Notes |
|---|---|---|
| `trade_id` | STRING | UUID — shared by both legs of the same execution |
| `order_id` | STRING | Empty — trade events are independent of order IDs in this pipeline |
| `trader_id` | STRING | One leg per row (buyer or seller) |
| `trader_type` | STRING | Inferred from trader ID prefix |
| `symbol` | STRING | Uppercase |
| `side` | STRING | `buy` or `sell` |
| `price` | DOUBLE | |
| `quantity` | INT | |
| `timestamp` | STRING | ISO-8601 UTC |
