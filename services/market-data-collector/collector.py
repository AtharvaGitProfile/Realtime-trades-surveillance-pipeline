"""
Market Data Collector
=====================
Connects to Finnhub WebSocket API for real-time trade data
and publishes each tick to the Kafka 'market-data' topic.

This is the entry point for real market data into our
surveillance pipeline.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("market-data-collector")

# ── Configuration ────────────────────────────────────────
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_MARKET_DATA_TOPIC", "market-data")
WATCH_SYMBOLS = os.environ.get("WATCH_SYMBOLS", "AAPL,TSLA,JPM,MSFT,GOOGL").split(",")

if not FINNHUB_API_KEY:
    raise ValueError("FINNHUB_API_KEY environment variable is required")


# ── Kafka Producer Setup ─────────────────────────────────
def create_kafka_producer(max_retries=10, retry_delay=5):
    """
    Create a Kafka producer with retry logic.
    Kafka might not be ready when this service starts,
    so we retry until it's available.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info(f"Connected to Kafka at {KAFKA_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}), "
                f"retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)

    raise ConnectionError(f"Could not connect to Kafka after {max_retries} attempts")


# ── Message Processing ───────────────────────────────────
def process_finnhub_message(ws, message, producer):
    """
    Finnhub sends trade data in this format:
    {
        "type": "trade",
        "data": [
            {"s": "AAPL", "p": 150.25, "v": 100, "t": 1234567890000, "c": ["1"]}
        ]
    }

    We transform each trade into a standardized event and
    publish it to Kafka with the symbol as the key (so all
    events for the same symbol go to the same partition).
    """
    try:
        payload = json.loads(message)

        if payload.get("type") != "trade":
            return

        trades = payload.get("data", [])
        for trade in trades:
            event = {
                "event_type": "market_tick",
                "symbol": trade["s"],
                "price": trade["p"],
                "volume": trade["v"],
                "timestamp_ms": trade["t"],
                "timestamp_iso": datetime.fromtimestamp(
                    trade["t"] / 1000, tz=timezone.utc
                ).isoformat(),
                "conditions": trade.get("c", []),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(
                KAFKA_TOPIC,
                key=trade["s"],
                value=event,
            )

        if trades:
            logger.debug(f"Published {len(trades)} ticks to Kafka")

    except Exception as e:
        logger.error(f"Error processing message: {e}")


# ── WebSocket Callbacks ──────────────────────────────────
def on_open(ws, producer):
    """Subscribe to all watched symbols when connection opens."""
    logger.info(f"WebSocket connected. Subscribing to {len(WATCH_SYMBOLS)} symbols...")
    for symbol in WATCH_SYMBOLS:
        subscribe_msg = json.dumps({"type": "subscribe", "symbol": symbol})
        ws.send(subscribe_msg)
        logger.info(f"  Subscribed to {symbol}")


def on_error(ws, error):
    logger.error(f"WebSocket error: {error}")


def on_close(ws, close_status, close_msg):
    logger.warning(f"WebSocket closed: {close_status} - {close_msg}")


# ── Main ─────────────────────────────────────────────────
def main():
    logger.info("=" * 50)
    logger.info("Market Data Collector starting...")
    logger.info(f"Symbols: {WATCH_SYMBOLS}")
    logger.info(f"Kafka: {KAFKA_SERVERS} → topic '{KAFKA_TOPIC}'")
    logger.info("=" * 50)

    # Connect to Kafka first
    producer = create_kafka_producer()

    # Connect to Finnhub WebSocket
    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    ws = websocket.WebSocketApp(
        ws_url,
        on_open=lambda ws: on_open(ws, producer),
        on_message=lambda ws, msg: process_finnhub_message(ws, msg, producer),
        on_error=on_error,
        on_close=on_close,
    )

    # Run forever with automatic reconnection
    while True:
        try:
            logger.info("Connecting to Finnhub WebSocket...")
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")

        logger.info("Reconnecting in 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    main()
