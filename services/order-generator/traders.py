"""
Trader Definitions
==================
Defines the Trader dataclass and a factory function that creates
a realistic population of market participants for simulation:
  - Normal traders  : typical retail/institutional behaviour
  - Spoofers        : place large orders then cancel before fill
  - Wash traders    : coordinated pairs that trade with each other
"""

import random
from dataclasses import dataclass, field
from typing import Optional


# ── Trader Dataclass ─────────────────────────────────────────────────────────

@dataclass
class Trader:
    trader_id: str
    trader_type: str                    # "normal" | "spoofer" | "wash_trader"
    preferred_symbols: list
    cancel_probability: float           # 0-1 probability of cancelling an order
    orders_per_minute: float            # average order submission rate
    avg_order_size: int                 # average quantity per order
    cancel_speed_seconds: float         # how quickly orders are cancelled
    partner_id: Optional[str] = None   # wash trader counterparty


# ── Population Factory ───────────────────────────────────────────────────────

_STOCK_SYMBOLS = ["AAPL", "TSLA", "JPM", "MSFT", "GOOGL"]
_CRYPTO_SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:XRPUSDT"]
_ALL_SYMBOLS = _STOCK_SYMBOLS + _CRYPTO_SYMBOLS


def create_trader_population() -> list[Trader]:
    """
    Create a fixed simulation population:
      - 20 normal traders
      -  3 spoofers
      -  4 wash traders (2 coordinated pairs)

    Random seeds are applied per-role so the population is
    deterministic across restarts.
    """
    traders: list[Trader] = []
    rng = random.Random(42)

    # ── Normal Traders ────────────────────────────────────────────────────────
    for i in range(1, 21):
        # Each normal trader focuses on 2-4 random symbols
        symbols = rng.sample(_ALL_SYMBOLS, k=rng.randint(2, 4))
        traders.append(Trader(
            trader_id=f"normal_{i:03d}",
            trader_type="normal",
            preferred_symbols=symbols,
            cancel_probability=round(rng.uniform(0.10, 0.35), 3),
            orders_per_minute=round(rng.uniform(0.2, 2.0), 2),
            avg_order_size=rng.randint(50, 500),
            cancel_speed_seconds=round(rng.uniform(10.0, 120.0), 1),
        ))

    # ── Spoofers ──────────────────────────────────────────────────────────────
    for i in range(1, 4):
        # Spoofers focus on high-volume symbols where large orders move prices
        symbols = rng.sample(_STOCK_SYMBOLS + ["BINANCE:BTCUSDT"], k=2)
        traders.append(Trader(
            trader_id=f"spoofer_{i:03d}",
            trader_type="spoofer",
            preferred_symbols=symbols,
            cancel_probability=round(rng.uniform(0.85, 0.95), 3),
            orders_per_minute=round(rng.uniform(8.0, 15.0), 2),
            avg_order_size=rng.randint(2000, 10000),
            cancel_speed_seconds=round(rng.uniform(1.0, 3.0), 2),
        ))

    # ── Wash Trader Pairs ─────────────────────────────────────────────────────
    # Two pairs; each pair shares the same symbol and similar order sizes.
    wash_configs = [
        {"symbol": rng.choice(_STOCK_SYMBOLS),   "size": rng.randint(200, 800)},
        {"symbol": rng.choice(_CRYPTO_SYMBOLS),  "size": rng.randint(100, 500)},
    ]
    for pair_idx, cfg in enumerate(wash_configs, start=1):
        id_a = f"wash_{pair_idx:03d}a"
        id_b = f"wash_{pair_idx:03d}b"
        # Slightly vary sizes so the orders don't look identical
        size_a = cfg["size"]
        size_b = max(1, cfg["size"] + rng.randint(-10, 10))
        traders.append(Trader(
            trader_id=id_a,
            trader_type="wash_trader",
            preferred_symbols=[cfg["symbol"]],
            cancel_probability=0.05,
            orders_per_minute=round(rng.uniform(1.0, 3.0), 2),
            avg_order_size=size_a,
            cancel_speed_seconds=round(rng.uniform(1.0, 5.0), 2),
            partner_id=id_b,
        ))
        traders.append(Trader(
            trader_id=id_b,
            trader_type="wash_trader",
            preferred_symbols=[cfg["symbol"]],
            cancel_probability=0.05,
            orders_per_minute=round(rng.uniform(1.0, 3.0), 2),
            avg_order_size=size_b,
            cancel_speed_seconds=round(rng.uniform(1.0, 5.0), 2),
            partner_id=id_a,
        ))

    return traders
