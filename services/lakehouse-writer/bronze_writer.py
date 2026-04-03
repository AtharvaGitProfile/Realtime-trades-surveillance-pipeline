"""
Bronze Writer
=============
Consumes raw events from all Kafka topics and writes them as newline-delimited
JSON (JSONL) files to the MinIO data lake under:

  surveillance-lake/bronze/{topic}/{YYYY-MM-DD}/{HH}/
      {topic}_{epoch_ms}_{uid}.jsonl

Partitioning uses wall-clock time at the moment each batch is flushed
(processing time), which is standard for a bronze / raw-ingestion layer —
the partition tells you when the data *arrived*, independent of event-time
skew across different topics.

Batching: events are buffered for BATCH_SECONDS (default 60 s), then one
file per topic is written in a single PUT.  Topics with no events in the
window are skipped.
"""

import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("bronze-writer")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS   = [t.strip() for t in os.environ.get(
    "KAFKA_TOPICS",
    "orders,trades,market-data-stocks,market-data-crypto,alerts",
).split(",")]

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio_secret_123")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "surveillance-lake")

BATCH_SECONDS    = int(os.environ.get("BATCH_SECONDS", "60"))
KAFKA_GROUP_ID   = os.environ.get("KAFKA_GROUP_ID",   "bronze-writer")


# ── MinIO Client ──────────────────────────────────────────────────────────────

def build_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    # Ensure the target bucket exists (minio-setup creates it, but guard here too)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket '{MINIO_BUCKET}'")
    else:
        logger.info(f"Bucket '{MINIO_BUCKET}' is ready")
    return client


# ── Kafka Consumer ────────────────────────────────────────────────────────────

def build_consumer(max_retries: int = 10, retry_delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: v.decode("utf-8"),
                # Poll returns quickly so the batch timer stays accurate
                consumer_timeout_ms=1_000,
            )
            logger.info(f"Consumer connected — topics: {KAFKA_TOPICS}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_delay}s…"
            )
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect to Kafka after {max_retries} attempts")


# ── Batch Flush ───────────────────────────────────────────────────────────────

def flush(
    buffer: dict[str, list[str]],
    minio: Minio,
) -> None:
    """Write one JSONL file per non-empty topic bucket to MinIO."""
    if not any(buffer.values()):
        return

    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    epoch_ms = int(now.timestamp() * 1_000)
    uid      = uuid.uuid4().hex[:8]

    for topic, lines in buffer.items():
        if not lines:
            continue

        content      = ("\n".join(lines) + "\n").encode("utf-8")
        object_name  = (
            f"bronze/{topic}/{date_str}/{hour_str}/"
            f"{topic}_{epoch_ms}_{uid}.jsonl"
        )

        try:
            minio.put_object(
                MINIO_BUCKET,
                object_name,
                BytesIO(content),
                length=len(content),
                content_type="application/x-ndjson",
            )
            logger.info(
                f"Flushed {len(lines):>5} events → "
                f"s3://{MINIO_BUCKET}/{object_name}"
            )
        except S3Error as exc:
            logger.error(f"MinIO write failed for topic '{topic}': {exc}")

    buffer.clear()


# ── Main Loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 55)
    logger.info("Bronze Writer starting")
    logger.info(f"  Kafka         : {KAFKA_SERVERS}")
    logger.info(f"  Topics        : {KAFKA_TOPICS}")
    logger.info(f"  MinIO         : {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    logger.info(f"  Batch window  : {BATCH_SECONDS}s")
    logger.info("=" * 55)

    minio    = build_minio_client()
    consumer = build_consumer()

    # topic → list of raw JSON strings (preserves the original message verbatim)
    buffer: dict[str, list[str]] = defaultdict(list)
    last_flush = time.monotonic()

    logger.info("Consuming — press Ctrl-C to stop")

    try:
        while True:
            # poll() returns within consumer_timeout_ms even if Kafka is quiet
            records = consumer.poll(timeout_ms=1_000)

            for _, messages in records.items():
                for msg in messages:
                    buffer[msg.topic].append(msg.value)

            if time.monotonic() - last_flush >= BATCH_SECONDS:
                flush(buffer, minio)
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing final batch…")
        flush(buffer, minio)
    finally:
        consumer.close()
        logger.info("Consumer closed. Goodbye.")


if __name__ == "__main__":
    main()
