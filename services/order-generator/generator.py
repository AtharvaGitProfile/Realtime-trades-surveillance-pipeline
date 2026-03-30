"""
Order Generator
===============
Simulates a population of market participants submitting orders and
generating trades against a live price feed consumed from Kafka.

Topics consumed:
  - market-data-stocks   (equity ticks)
  - market-data-crypto   (crypto ticks)

Topics produced:
  - orders   (order lifecycle: NEW / CANCELLED / FILLED)
  - trades   (matched executions)
"""

import json
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from traders import Trader, create_trader_population

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("order-generator")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_STOCKS_TOPIC  = os.environ.get("KAFKA_STOCKS_TOPIC",      "market-data-stocks")
KAFKA_CRYPTO_TOPIC  = os.environ.get("KAFKA_CRYPTO_TOPIC",      "market-data-crypto")
KAFKA_ORDERS_TOPIC  = os.environ.get("KAFKA_ORDERS_TOPIC",      "orders")
KAFKA_TRADES_TOPIC  = os.environ.get("KAFKA_TRADES_TOPIC",      "trades")

# Simulation tick: how often each trader gets a chance to act (seconds)
LOOP_TICK_SECONDS = float(os.environ.get("LOOP_TICK_SECONDS", "1.0"))

# ── Shared Price State ────────────────────────────────────────────────────────
# Updated by the price-consumer thread; read by the main simulation loop.
latest_prices: dict[str, float] = {}
prices_lock = threading.Lock()


# ── Kafka Helpers ─────────────────────────────────────────────────────────────

def create_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info(f"Producer connected to Kafka at {KAFKA_SERVERS}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_retries}), retrying in {retry_delay}s...")
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect to Kafka after {max_retries} attempts")


def price_consumer_thread() -> None:
    """
    Background thread: consumes market-data-stocks and market-data-crypto,
    keeping latest_prices up to date.
    """
    consumer = KafkaConsumer(
        KAFKA_STOCKS_TOPIC,
        KAFKA_CRYPTO_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="order-generator-price-feed",
    )
    logger.info(f"Price consumer subscribed to {KAFKA_STOCKS_TOPIC}, {KAFKA_CRYPTO_TOPIC}")
    for msg in consumer:
        tick = msg.value
        symbol = tick.get("symbol")
        price  = tick.get("price")
        if symbol and price:
            with prices_lock:
                latest_prices[symbol] = price


# ── Event Builders ────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _order_event(
    trader: Trader,
    symbol: str,
    side: str,
    price: float,
    quantity: int,
    status: str,
    order_id: str | None = None,
) -> dict:
    return {
        "event_type": "order",
        "order_id":   order_id or str(uuid.uuid4()),
        "trader_id":  trader.trader_id,
        "trader_type": trader.trader_type,
        "symbol":     symbol,
        "side":       side,           # "buy" | "sell"
        "price":      price,
        "quantity":   quantity,
        "status":     status,         # "NEW" | "CANCELLED" | "FILLED"
        "timestamp":  _now_iso(),
    }


def _trade_event(
    buy_trader_id: str,
    sell_trader_id: str,
    symbol: str,
    price: float,
    quantity: int,
) -> dict:
    return {
        "event_type":      "trade",
        "trade_id":        str(uuid.uuid4()),
        "buy_trader_id":   buy_trader_id,
        "sell_trader_id":  sell_trader_id,
        "symbol":          symbol,
        "price":           price,
        "quantity":        quantity,
        "timestamp":       _now_iso(),
    }


# ── Price Helpers ─────────────────────────────────────────────────────────────

def get_price(symbol: str, rng: random.Random) -> float | None:
    """Return the latest known price for a symbol, or None if unavailable."""
    with prices_lock:
        price = latest_prices.get(symbol)
    return price


def jitter_price(price: float, bps: int, rng: random.Random) -> float:
    """Apply ±bps basis-points of random noise to a price."""
    factor = 1 + rng.uniform(-bps, bps) / 10_000
    return round(price * factor, 6)


# ── Trader Behaviours ─────────────────────────────────────────────────────────

def act_normal(trader: Trader, producer: KafkaProducer, rng: random.Random) -> None:
    symbol = rng.choice(trader.preferred_symbols)
    price  = get_price(symbol, rng)
    if price is None:
        return

    side     = rng.choice(["buy", "sell"])
    quantity = max(1, int(rng.gauss(trader.avg_order_size, trader.avg_order_size * 0.2)))
    limit    = jitter_price(price, 20, rng)
    order_id = str(uuid.uuid4())

    # Submit new order
    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=trader.trader_id,
        value=_order_event(trader, symbol, side, limit, quantity, "NEW", order_id),
    )

    # Decide: cancel or fill
    if rng.random() < trader.cancel_probability:
        time.sleep(rng.uniform(trader.cancel_speed_seconds * 0.5, trader.cancel_speed_seconds))
        producer.send(
            KAFKA_ORDERS_TOPIC,
            key=trader.trader_id,
            value=_order_event(trader, symbol, side, limit, quantity, "CANCELLED", order_id),
        )
    else:
        producer.send(
            KAFKA_ORDERS_TOPIC,
            key=trader.trader_id,
            value=_order_event(trader, symbol, side, limit, quantity, "FILLED", order_id),
        )
        producer.send(
            KAFKA_TRADES_TOPIC,
            key=symbol,
            value=_trade_event(
                buy_trader_id  = trader.trader_id if side == "buy"  else "market",
                sell_trader_id = trader.trader_id if side == "sell" else "market",
                symbol=symbol, price=limit, quantity=quantity,
            ),
        )
    logger.debug(f"[normal] {trader.trader_id} {side} {quantity}@{limit:.4f} {symbol}")


def act_spoofer(trader: Trader, producer: KafkaProducer, rng: random.Random) -> None:
    """
    Spoof pattern:
      1. Place a burst of large orders on one side to create the illusion
         of depth and nudge prices.
      2. Cancel almost all of them within cancel_speed_seconds.
      3. Occasionally slip in a small fill on the opposite side.
    """
    symbol = rng.choice(trader.preferred_symbols)
    price  = get_price(symbol, rng)
    if price is None:
        return

    # Determine the "fake" side (large orders) and the "real" trade side
    spoof_side = rng.choice(["buy", "sell"])
    real_side  = "sell" if spoof_side == "buy" else "buy"

    burst_count = rng.randint(3, 8)
    order_ids   = []

    for _ in range(burst_count):
        quantity = max(1, int(rng.gauss(trader.avg_order_size, trader.avg_order_size * 0.1)))
        limit    = jitter_price(price, 50, rng)
        order_id = str(uuid.uuid4())
        order_ids.append((order_id, limit, quantity))
        producer.send(
            KAFKA_ORDERS_TOPIC,
            key=trader.trader_id,
            value=_order_event(trader, symbol, spoof_side, limit, quantity, "NEW", order_id),
        )

    logger.debug(f"[spoofer] {trader.trader_id} burst {burst_count}x {spoof_side} on {symbol}")

    # Wait, then cancel most orders
    time.sleep(rng.uniform(trader.cancel_speed_seconds * 0.5, trader.cancel_speed_seconds))

    for order_id, limit, quantity in order_ids:
        if rng.random() < trader.cancel_probability:
            producer.send(
                KAFKA_ORDERS_TOPIC,
                key=trader.trader_id,
                value=_order_event(trader, symbol, spoof_side, limit, quantity, "CANCELLED", order_id),
            )

    # Small real trade on the opposite side to profit from the moved price
    real_qty   = max(1, int(trader.avg_order_size * 0.05))
    real_price = jitter_price(price, 10, rng)
    real_oid   = str(uuid.uuid4())
    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=trader.trader_id,
        value=_order_event(trader, symbol, real_side, real_price, real_qty, "FILLED", real_oid),
    )
    producer.send(
        KAFKA_TRADES_TOPIC,
        key=symbol,
        value=_trade_event(
            buy_trader_id  = trader.trader_id if real_side == "buy"  else "market",
            sell_trader_id = trader.trader_id if real_side == "sell" else "market",
            symbol=symbol, price=real_price, quantity=real_qty,
        ),
    )


def act_wash_trader(
    trader: Trader,
    partner: Trader,
    producer: KafkaProducer,
    rng: random.Random,
) -> None:
    """
    Wash trade pattern:
      Both legs use the same symbol, the same price, and matching sizes,
      creating artificial volume with no real change in ownership.
    """
    symbol = trader.preferred_symbols[0]
    price  = get_price(symbol, rng)
    if price is None:
        return

    # Match price exactly; tiny size variation stays within ~10 units
    wash_price = jitter_price(price, 5, rng)
    qty_a = max(1, int(rng.gauss(trader.avg_order_size, 5)))
    qty_b = max(1, int(rng.gauss(partner.avg_order_size, 5)))
    matched_qty = min(qty_a, qty_b)

    buy_oid  = str(uuid.uuid4())
    sell_oid = str(uuid.uuid4())

    # Trader A buys, Trader B sells (or vice versa — randomise to vary the pattern)
    if rng.random() < 0.5:
        buyer, seller = trader, partner
    else:
        buyer, seller = partner, trader

    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=buyer.trader_id,
        value=_order_event(buyer,  symbol, "buy",  wash_price, matched_qty, "NEW",    buy_oid),
    )
    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=seller.trader_id,
        value=_order_event(seller, symbol, "sell", wash_price, matched_qty, "NEW",    sell_oid),
    )
    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=buyer.trader_id,
        value=_order_event(buyer,  symbol, "buy",  wash_price, matched_qty, "FILLED", buy_oid),
    )
    producer.send(
        KAFKA_ORDERS_TOPIC,
        key=seller.trader_id,
        value=_order_event(seller, symbol, "sell", wash_price, matched_qty, "FILLED", sell_oid),
    )
    producer.send(
        KAFKA_TRADES_TOPIC,
        key=symbol,
        value=_trade_event(buyer.trader_id, seller.trader_id, symbol, wash_price, matched_qty),
    )
    logger.debug(
        f"[wash] {buyer.trader_id} <-> {seller.trader_id} "
        f"{matched_qty}@{wash_price:.4f} {symbol}"
    )


# ── Main Simulation Loop ──────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 50)
    logger.info("Order Generator starting...")
    logger.info(f"Kafka: {KAFKA_SERVERS}")
    logger.info(f"  consuming: {KAFKA_STOCKS_TOPIC}, {KAFKA_CRYPTO_TOPIC}")
    logger.info(f"  producing: {KAFKA_ORDERS_TOPIC}, {KAFKA_TRADES_TOPIC}")
    logger.info("=" * 50)

    # Build trader population and an index for wash-trader partner lookups
    population = create_trader_population()
    trader_index = {t.trader_id: t for t in population}

    counts = {"normal": 0, "spoofer": 0, "wash_trader": 0}
    for t in population:
        counts[t.trader_type] += 1
    logger.info(
        f"Population: {counts['normal']} normal, "
        f"{counts['spoofer']} spoofers, "
        f"{counts['wash_trader']} wash traders"
    )

    # Start background price consumer
    t = threading.Thread(target=price_consumer_thread, daemon=True)
    t.start()

    # Wait until we have at least some prices before generating orders
    logger.info("Waiting for initial price data...")
    while True:
        with prices_lock:
            n = len(latest_prices)
        if n > 0:
            break
        time.sleep(1)
    logger.info(f"Received prices for {n} symbol(s). Starting simulation.")

    producer = create_producer()
    rng = random.Random()  # non-deterministic for live simulation

    # Track which wash traders have already acted this tick (avoid double-firing pairs)
    already_acted: set[str] = set()

    while True:
        tick_start = time.monotonic()
        already_acted.clear()

        for trader in population:
            # Probability of acting this tick = rate / (60 / LOOP_TICK_SECONDS)
            ticks_per_minute = 60.0 / LOOP_TICK_SECONDS
            act_prob = trader.orders_per_minute / ticks_per_minute
            if rng.random() > act_prob:
                continue

            try:
                if trader.trader_type == "normal":
                    act_normal(trader, producer, rng)

                elif trader.trader_type == "spoofer":
                    act_spoofer(trader, producer, rng)

                elif trader.trader_type == "wash_trader":
                    if trader.trader_id in already_acted:
                        continue
                    partner = trader_index.get(trader.partner_id)
                    if partner is None:
                        continue
                    act_wash_trader(trader, partner, producer, rng)
                    already_acted.add(trader.trader_id)
                    already_acted.add(partner.trader_id)

            except Exception as e:
                logger.error(f"Error processing trader {trader.trader_id}: {e}")

        producer.flush()

        # Sleep for the remainder of the tick window
        elapsed = time.monotonic() - tick_start
        sleep_for = max(0.0, LOOP_TICK_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
