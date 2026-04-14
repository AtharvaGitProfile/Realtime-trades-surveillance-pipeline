#!/usr/bin/env python3
"""
Export bronze-layer data from MinIO to CSV files for Databricks upload.

Usage:
    python export_databricks.py

Output:
    databricks_upload/alerts.csv   — 5 000 most-recent alert records (all fields flattened)
    databricks_upload/orders.csv   — 10 000 most-recent order events
    databricks_upload/trades.csv   — 5 000 most-recent trade executions

NOTE – Trades schema adaptation:
    Bronze trade events have buy_trader_id / sell_trader_id rather than a single
    trader_id / side / trader_type / order_id.  This script expands each raw trade
    into TWO rows (one buy leg, one sell leg) so that trader_id, side, and
    trader_type columns are populated as requested.  order_id is empty (trades are
    independent events in this pipeline).
"""

import csv
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config

# ── Configuration ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_123")
BUCKET           = os.environ.get("MINIO_BUCKET",     "surveillance-lake")
OUTPUT_DIR       = Path(__file__).parent / "databricks_upload"

ALERT_LIMIT  = 5_000
ORDER_LIMIT  = 10_000
TRADE_LIMIT  = 5_000   # rows (each raw trade yields 2 rows → collect TRADE_LIMIT // 2 raw records)

# ── Column definitions ─────────────────────────────────────────────────────────
ALERT_FIELDS = [
    "alert_type", "trader_id", "symbol",
    "cancel_count", "total_orders", "cancel_rate",
    "match_count", "trader_id_1", "trader_id_2",
    "window_start", "window_end", "detected_at",
]
ORDER_FIELDS = [
    "order_id", "trader_id", "trader_type", "symbol",
    "side", "price", "quantity", "status", "timestamp",
]
TRADE_FIELDS = [
    "trade_id", "order_id", "trader_id", "trader_type",
    "symbol", "side", "price", "quantity", "timestamp",
]


# ── MinIO helpers ─────────────────────────────────────────────────────────────

def build_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_objects_sorted_desc(client, prefix: str) -> list[str]:
    """Return all object keys under *prefix* sorted newest-first.

    Keys are named  <topic>_{epoch_ms}_{uid}.jsonl  so lexicographic descending
    equals chronological descending.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    keys.sort(reverse=True)
    return keys


def iter_records(client, keys: list[str]):
    """Yield JSON dicts from JSONL files in newest-file-first, newest-line-first order."""
    for key in keys:
        resp = client.get_object(Bucket=BUCKET, Key=key)
        body = resp["Body"].read().decode("utf-8")
        lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
        for line in reversed(lines):          # within a file: latest first
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


# ── Exporters ─────────────────────────────────────────────────────────────────

def export_alerts(client) -> int:
    """Collect a balanced sample: SPOOFING_LIMIT spoofing + WASH_LIMIT wash-trading alerts."""
    SPOOFING_LIMIT = ALERT_LIMIT // 2       # 2 500
    WASH_LIMIT     = ALERT_LIMIT // 2       # 2 500

    print("  Scanning bronze/alerts/ ...")
    keys = list_objects_sorted_desc(client, "bronze/alerts/")
    if not keys:
        print("  [WARN] No alert files found in MinIO.")
        return 0

    spoofing: list[dict] = []
    wash:     list[dict] = []

    for rec in iter_records(client, keys):
        if len(spoofing) >= SPOOFING_LIMIT and len(wash) >= WASH_LIMIT:
            break
        alert_type = rec.get("alert_type", "")
        if alert_type == "SPOOFING" and len(spoofing) < SPOOFING_LIMIT:
            spoofing.append({f: rec.get(f, "") for f in ALERT_FIELDS})
        elif alert_type == "WASH_TRADING" and len(wash) < WASH_LIMIT:
            wash.append({f: rec.get(f, "") for f in ALERT_FIELDS})

    rows = spoofing + wash
    out = OUTPUT_DIR / "alerts.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=ALERT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  -> {len(spoofing):,} SPOOFING + {len(wash):,} WASH_TRADING = {len(rows):,} rows  =>  {out}")
    return len(rows)


def export_orders(client) -> int:
    print("  Scanning bronze/orders/ ...")
    keys = list_objects_sorted_desc(client, "bronze/orders/")
    if not keys:
        print("  [WARN] No order files found in MinIO.")
        return 0

    rows: list[dict] = []
    for rec in iter_records(client, keys):
        if len(rows) >= ORDER_LIMIT:
            break
        # Only FILLED events carry a matching trade; we include all statuses
        row = {f: rec.get(f, "") for f in ORDER_FIELDS}
        rows.append(row)

    out = OUTPUT_DIR / "orders.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=ORDER_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  -> {len(rows):,} rows  =>  {out}")
    return len(rows)


def export_trades(client) -> int:
    """Each raw trade event is expanded into two rows (buy leg + sell leg) so that
    trader_id, side, and trader_type columns are populated as the user requested.
    """
    print("  Scanning bronze/trades/ ...")
    keys = list_objects_sorted_desc(client, "bronze/trades/")
    if not keys:
        print("  [WARN] No trade files found in MinIO.")
        return 0

    rows: list[dict] = []
    raw_needed = (TRADE_LIMIT + 1) // 2   # each raw record → 2 rows

    for rec in iter_records(client, keys):
        if len(rows) >= TRADE_LIMIT:
            break

        trade_id  = rec.get("trade_id",       "")
        symbol    = rec.get("symbol",          "")
        price     = rec.get("price",           "")
        quantity  = rec.get("quantity",        "")
        timestamp = rec.get("timestamp",       "")
        buy_tid   = rec.get("buy_trader_id",   "")
        sell_tid  = rec.get("sell_trader_id",  "")

        # Infer trader_type from the ID prefix (spoofer/wash_trader/normal)
        def _type(tid: str) -> str:
            if not tid or tid == "market":
                return "market"
            if "spoofer" in tid:
                return "spoofer"
            if "wash" in tid:
                return "wash_trader"
            return "normal"

        # Buy leg
        rows.append({
            "trade_id":    trade_id,
            "order_id":    "",
            "trader_id":   buy_tid,
            "trader_type": _type(buy_tid),
            "symbol":      symbol,
            "side":        "buy",
            "price":       price,
            "quantity":    quantity,
            "timestamp":   timestamp,
        })
        if len(rows) >= TRADE_LIMIT:
            break

        # Sell leg
        rows.append({
            "trade_id":    trade_id,
            "order_id":    "",
            "trader_id":   sell_tid,
            "trader_type": _type(sell_tid),
            "symbol":      symbol,
            "side":        "sell",
            "price":       price,
            "quantity":    quantity,
            "timestamp":   timestamp,
        })

    out = OUTPUT_DIR / "trades.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=TRADE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  -> {len(rows):,} rows  =>  {out}")
    return len(rows)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    client = build_client()
    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception as exc:
        print(f"ERROR: Cannot reach MinIO bucket '{BUCKET}': {exc}", file=sys.stderr)
        print("  Make sure the Docker stack is running and MinIO is accessible at", file=sys.stderr)
        print(f"  {MINIO_ENDPOINT}", file=sys.stderr)
        sys.exit(1)

    print(f"Connected to MinIO ({MINIO_ENDPOINT})  bucket={BUCKET}")
    print(f"Output directory: {OUTPUT_DIR}\n")

    n_alerts = export_alerts(client)
    n_orders = export_orders(client)
    n_trades = export_trades(client)

    print(f"\nDone.  alerts={n_alerts:,}  orders={n_orders:,}  trades={n_trades:,}")
    print(f"Upload folder: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
