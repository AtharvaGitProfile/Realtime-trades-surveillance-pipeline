"""
Silver / Gold Writer
====================
Reads bronze JSONL files from MinIO, applies transformations to produce
silver Parquet files, then computes gold aggregates.  Runs on a fixed
interval (default 5 minutes).

Pipeline per cycle
──────────────────
  Bronze JSONL                   Silver Parquet            Gold Parquet
  ──────────────────────────────────────────────────────────────────────
  bronze/orders/   ──(transform)──→  silver/orders/{date}/   ──┐
  bronze/trades/   ──(transform)──→  silver/trades/{date}/   ──┼──→  gold/{table}/{date}/part.parquet
  bronze/alerts/   ─────────────────────────────────────────── ┘
  (alerts have no silver layer; read directly from bronze for gold)

Silver transformations
  orders — price→decimal(2), quantity→int, timestamps→UTC ISO,
            status normalised (placed/cancelled/filled),
            trader_type lowercased, deduplicated by order_id
            (keep latest status); time_to_cancel_seconds computed
            from NEW→CANCELLED pairs present in the same batch.
  trades — same type/timestamp casting; deduplicated by trade_id.

Gold tables  (full rewrite for today's date partition each cycle)
  trader_daily_activity  — per (trader_id, symbol):
                           total_orders, total_cancels, total_trades,
                           cancel_rate, avg_order_size, avg_time_to_cancel
  asset_hourly_summary   — per (symbol, hour):
                           avg_price, price_volatility, total_volume,
                           total_orders, total_trades
  alert_summary          — per alert_type:
                           total_alerts, unique_traders_flagged, avg_cancel_rate

State: last successfully processed timestamp written back to MinIO at
  silver/_state/last_processed.json  after every successful cycle.
  On first run (or missing state) falls back to LOOKBACK_HOURS ago.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
from minio import Minio
from minio.error import S3Error

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("silver-gold-writer")

# ── Configuration ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "surveillance-lake")
INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "300"))
LOOKBACK_HOURS   = int(os.environ.get("LOOKBACK_HOURS",   "24"))

STATE_OBJECT = "silver/_state/last_processed.json"


# ── MinIO ─────────────────────────────────────────────────────────────────────

def build_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


# ── State ─────────────────────────────────────────────────────────────────────

def load_state(minio: Minio) -> datetime:
    try:
        resp = minio.get_object(MINIO_BUCKET, STATE_OBJECT)
        data = json.loads(resp.read())
        dt = datetime.fromisoformat(data["last_processed"]).replace(tzinfo=timezone.utc)
        logger.info(f"Resuming from state: {dt.isoformat()}")
        return dt
    except S3Error:
        dt = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        logger.info(f"No state found — starting {LOOKBACK_HOURS}h ago: {dt.isoformat()}")
        return dt


def save_state(minio: Minio, dt: datetime) -> None:
    content = json.dumps({"last_processed": dt.isoformat()}).encode()
    minio.put_object(
        MINIO_BUCKET, STATE_OBJECT,
        BytesIO(content), len(content),
        content_type="application/json",
    )


# ── Bronze helpers ────────────────────────────────────────────────────────────

def list_new_bronze_objects(
    minio: Minio, topic: str, since: datetime
) -> list[str]:
    objs = minio.list_objects(
        MINIO_BUCKET, prefix=f"bronze/{topic}/", recursive=True
    )
    return sorted(
        obj.object_name for obj in objs
        if obj.last_modified
        and obj.last_modified.replace(tzinfo=timezone.utc) > since
    )


def read_bronze_objects(minio: Minio, object_names: list[str]) -> pd.DataFrame:
    if not object_names:
        return pd.DataFrame()
    rows = []
    for name in object_names:
        try:
            resp = minio.get_object(MINIO_BUCKET, name)
            for line in resp.read().decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        except S3Error as exc:
            logger.warning(f"Skipping unreadable object {name}: {exc}")
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Silver: Orders ────────────────────────────────────────────────────────────

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required = {"order_id", "trader_id", "trader_type", "symbol",
                "side", "price", "quantity", "status", "timestamp"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        logger.warning(f"Orders missing columns {missing} — skipping")
        return pd.DataFrame()

    df = df.copy()
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_dt", "order_id"])

    # Compute time_to_cancel from NEW→CANCELLED pairs in this batch
    new_ts = (
        df[df["status"] == "NEW"][["order_id", "timestamp_dt"]]
        .rename(columns={"timestamp_dt": "placed_at"})
        .drop_duplicates("order_id")
    )
    can_ts = (
        df[df["status"] == "CANCELLED"][["order_id", "timestamp_dt"]]
        .rename(columns={"timestamp_dt": "cancelled_at"})
        .drop_duplicates("order_id")
    )
    if not new_ts.empty and not can_ts.empty:
        pairs = new_ts.merge(can_ts, on="order_id", how="inner")
        pairs["time_to_cancel_seconds"] = (
            (pairs["cancelled_at"] - pairs["placed_at"])
            .dt.total_seconds()
            .clip(lower=0)
        )
        ttc = pairs[["order_id", "time_to_cancel_seconds"]]
    else:
        ttc = pd.DataFrame(columns=["order_id", "time_to_cancel_seconds"])

    # Type normalisation
    df["price"]    = pd.to_numeric(df["price"],    errors="coerce").round(2)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    # Status & type normalisation
    df["status"] = (
        df["status"]
        .map({"NEW": "placed", "CANCELLED": "cancelled", "FILLED": "filled"})
        .fillna(df["status"].str.lower())
    )
    df["trader_type"] = df["trader_type"].str.lower()

    # Deduplicate: keep latest event per order_id
    df = (
        df.sort_values("timestamp_dt")
        .drop_duplicates(subset=["order_id"], keep="last")
    )

    # Attach time_to_cancel
    df = df.merge(ttc, on="order_id", how="left")

    df["timestamp"] = df["timestamp_dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["date"]      = df["timestamp_dt"].dt.strftime("%Y-%m-%d")
    return df.drop(columns=["timestamp_dt"]).reset_index(drop=True)


# ── Silver: Trades ────────────────────────────────────────────────────────────

def transform_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    required = {"trade_id", "buy_trader_id", "sell_trader_id",
                "symbol", "price", "quantity", "timestamp"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        logger.warning(f"Trades missing columns {missing} — skipping")
        return pd.DataFrame()

    df = df.copy()
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_dt", "trade_id"])

    df["price"]    = pd.to_numeric(df["price"],    errors="coerce").round(2)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

    df = (
        df.sort_values("timestamp_dt")
        .drop_duplicates(subset=["trade_id"], keep="last")
    )

    df["timestamp"] = df["timestamp_dt"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["date"]      = df["timestamp_dt"].dt.strftime("%Y-%m-%d")
    return df.drop(columns=["timestamp_dt"]).reset_index(drop=True)


# ── Silver I/O ────────────────────────────────────────────────────────────────

def write_silver(
    minio: Minio, df: pd.DataFrame, topic: str, run_ts: datetime
) -> None:
    if df.empty:
        return
    for date_str, grp in df.groupby("date"):
        payload = grp.drop(columns=["date"])
        buf  = BytesIO()
        payload.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        obj = (
            f"silver/{topic}/{date_str}/"
            f"{topic}_{int(run_ts.timestamp() * 1_000)}_{uuid.uuid4().hex[:8]}.parquet"
        )
        minio.put_object(
            MINIO_BUCKET, obj, buf, buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.info(f"Wrote silver {topic}: {len(grp):>6} rows → {obj}")


def read_silver_topic(minio: Minio, topic: str, date_str: str) -> pd.DataFrame:
    prefix = f"silver/{topic}/{date_str}/"
    try:
        objects = list(minio.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    except S3Error:
        return pd.DataFrame()
    dfs = []
    for obj in objects:
        try:
            resp = minio.get_object(MINIO_BUCKET, obj.object_name)
            dfs.append(pd.read_parquet(BytesIO(resp.read())))
        except Exception as exc:
            logger.warning(f"Could not read silver {obj.object_name}: {exc}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── Gold: trader_daily_activity ───────────────────────────────────────────────

def compute_trader_daily_activity(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or "order_id" not in orders.columns:
        return pd.DataFrame()

    if "time_to_cancel_seconds" not in orders.columns:
        orders = orders.copy()
        orders["time_to_cancel_seconds"] = float("nan")

    agg = (
        orders
        .groupby(["trader_id", "symbol"], as_index=False)
        .agg(
            total_orders       =("order_id",               "count"),
            total_cancels      =("status",    lambda s: (s == "cancelled").sum()),
            total_trades       =("status",    lambda s: (s == "filled").sum()),
            avg_order_size     =("quantity",               "mean"),
            avg_time_to_cancel =("time_to_cancel_seconds", "mean"),
        )
    )
    agg["cancel_rate"] = (
        (agg["total_cancels"] / agg["total_orders"])
        .where(agg["total_orders"] > 0)
        .round(4)
    )
    agg["avg_order_size"]     = agg["avg_order_size"].round(2)
    agg["avg_time_to_cancel"] = agg["avg_time_to_cancel"].round(2)
    return agg


# ── Gold: asset_hourly_summary ────────────────────────────────────────────────

def compute_asset_hourly_summary(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or "timestamp" not in orders.columns:
        return pd.DataFrame()

    df = orders.copy()
    df["hour"] = (
        pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.hour
    )
    df = df.dropna(subset=["hour"])
    df["hour"] = df["hour"].astype(int)

    agg = (
        df.groupby(["symbol", "hour"], as_index=False)
        .agg(
            avg_price        =("price",    "mean"),
            price_volatility =("price",    "std"),
            total_volume     =("quantity", "sum"),
            total_orders     =("order_id", "count"),
            total_trades     =("status",   lambda s: (s == "filled").sum()),
        )
    )
    agg["avg_price"]        = agg["avg_price"].round(4)
    agg["price_volatility"] = agg["price_volatility"].round(4)
    return agg


# ── Gold: alert_summary ───────────────────────────────────────────────────────

def compute_alert_summary(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty or "alert_type" not in alerts.columns:
        return pd.DataFrame()

    rows = []
    for alert_type, grp in alerts.groupby("alert_type"):
        traders: set = set()
        for col in ("trader_id", "trader_id_1", "trader_id_2"):
            if col in grp.columns:
                traders.update(grp[col].dropna())

        avg_cr = None
        if "cancel_rate" in grp.columns:
            numeric = pd.to_numeric(grp["cancel_rate"], errors="coerce")
            if numeric.notna().any():
                avg_cr = round(float(numeric.mean()), 4)

        rows.append({
            "alert_type":             alert_type,
            "total_alerts":           len(grp),
            "unique_traders_flagged": len(traders),
            "avg_cancel_rate":        avg_cr,
        })
    return pd.DataFrame(rows)


# ── Gold I/O ──────────────────────────────────────────────────────────────────

def write_gold(
    minio: Minio, df: pd.DataFrame, table_name: str, date_str: str
) -> None:
    if df.empty:
        logger.info(f"Gold {table_name}: no data for {date_str} — skipping")
        return
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    obj = f"gold/{table_name}/{date_str}/part.parquet"
    minio.put_object(
        MINIO_BUCKET, obj, buf, buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    logger.info(f"Wrote gold {table_name}: {len(df):>4} rows → {obj}")


# ── Cycle ─────────────────────────────────────────────────────────────────────

def run_cycle(minio: Minio, last_processed: datetime) -> datetime:
    run_ts   = datetime.now(timezone.utc)
    date_str = run_ts.strftime("%Y-%m-%d")
    logger.info(
        f"── Cycle start {run_ts.strftime('%H:%M:%S')} UTC "
        f"(since {last_processed.strftime('%H:%M:%S')}) ──"
    )

    # ── Silver ────────────────────────────────────────────────────────────────
    for topic, transform_fn in [("orders", transform_orders), ("trades", transform_trades)]:
        new_objs = list_new_bronze_objects(minio, topic, last_processed)
        logger.info(f"  bronze/{topic}: {len(new_objs)} new file(s)")
        if new_objs:
            raw = read_bronze_objects(minio, new_objs)
            silver = transform_fn(raw)
            write_silver(minio, silver, topic, run_ts)

    # ── Gold ──────────────────────────────────────────────────────────────────
    # Read ALL of today's silver for complete daily aggregates
    all_orders = read_silver_topic(minio, "orders", date_str)
    logger.info(f"  silver/orders today: {len(all_orders)} rows")

    # Read today's bronze alerts (no silver layer for alerts)
    alert_objs = [
        obj.object_name
        for obj in minio.list_objects(
            MINIO_BUCKET, prefix=f"bronze/alerts/{date_str}/", recursive=True
        )
    ]
    all_alerts = read_bronze_objects(minio, alert_objs)
    logger.info(f"  bronze/alerts today: {len(all_alerts)} rows across {len(alert_objs)} file(s)")

    write_gold(minio, compute_trader_daily_activity(all_orders),  "trader_daily_activity", date_str)
    write_gold(minio, compute_asset_hourly_summary(all_orders),   "asset_hourly_summary",  date_str)
    write_gold(minio, compute_alert_summary(all_alerts),          "alert_summary",         date_str)

    elapsed = (datetime.now(timezone.utc) - run_ts).total_seconds()
    logger.info(f"── Cycle complete in {elapsed:.1f}s ──")
    return run_ts


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 55)
    logger.info("Silver/Gold Writer starting")
    logger.info(f"  MinIO    : {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    logger.info(f"  Interval : {INTERVAL_SECONDS}s")
    logger.info(f"  Lookback : {LOOKBACK_HOURS}h (on first run)")
    logger.info("=" * 55)

    minio = build_minio_client()
    last_processed = load_state(minio)

    while True:
        try:
            last_processed = run_cycle(minio, last_processed)
            save_state(minio, last_processed)
        except Exception as exc:
            logger.error(f"Cycle failed: {exc}", exc_info=True)
        logger.info(f"Sleeping {INTERVAL_SECONDS}s…")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
