"""
Wash Trade Detector
===================
PyFlink streaming job that detects wash trading patterns in order flow.

Wash trading: two coordinated traders repeatedly buying and selling the same
instrument to each other at matching prices and quantities, creating artificial
volume with no real change in ownership.

Detection logic (tumbling window join):
  - Match pairs : FILLED buy/sell on same symbol, price within MAX_PRICE_DIFF_PCT,
                  quantity within MAX_QTY_DIFF_PCT, within MATCH_WINDOW_SEC of
                  each other, different trader IDs
  - Window      : WINDOW_SIZE_MIN tumbling, triggered by event-time watermark
  - Trigger     : match_count >= MIN_MATCH_COUNT in the window
  - Output      : alert event written to the 'alerts' Kafka topic

Implementation — self-join via intermediate views:
  Flink 1.18's SQL planner cannot resolve two TUMBLE(DESCRIPTOR(...)) TVFs
  written inline in the same JOIN clause when both reference a column with the
  same name (index-out-of-bounds in the DESCRIPTOR look-up).  The workaround is
  to materialise each windowed side as a named TEMPORARY VIEW before joining:

    orders_a  →  TUMBLE TVF (DESCRIPTOR(event_time_a))  →  windowed_buys  ─┐
                                                                             ├─ JOIN + GROUP BY → alerts
    orders_b  →  TUMBLE TVF (DESCRIPTOR(event_time_b))  →  windowed_sells ─┘

  Each source table uses a unique time-attribute name (event_time_a /
  event_time_b) and a separate Kafka consumer group so both sides start from
  earliest-offset with independent watermarks.

  Pair identity is normalised (lexicographic min/max of trader IDs) so that
  A-buys/B-sells and B-buys/A-sells are counted in the same group.

Run inside the Flink container:
  flink run -py /opt/flink/jobs/wash_trade_detector.py
"""

import logging
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("wash-trade-detector")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS        = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_ORDERS_TOPIC   = os.environ.get("KAFKA_ORDERS_TOPIC",      "orders")
KAFKA_ALERTS_TOPIC   = os.environ.get("KAFKA_ALERTS_TOPIC",      "alerts")
KAFKA_CONNECTOR_JAR  = os.environ.get(
    "KAFKA_CONNECTOR_JAR",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
)

# Detection thresholds (all env-configurable)
WINDOW_SIZE_MIN    = int(os.environ.get("WINDOW_SIZE_MIN",    "5"))    # minutes
MATCH_WINDOW_SEC   = int(os.environ.get("MATCH_WINDOW_SEC",   "30"))   # seconds
MIN_MATCH_COUNT    = int(os.environ.get("MIN_MATCH_COUNT",    "3"))
MAX_PRICE_DIFF_PCT = float(os.environ.get("MAX_PRICE_DIFF_PCT", "0.5"))  # percent
MAX_QTY_DIFF_PCT   = float(os.environ.get("MAX_QTY_DIFF_PCT",  "10.0")) # percent


# ── Environment Setup ─────────────────────────────────────────────────────────

def build_table_env() -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(KAFKA_CONNECTOR_JAR)
    settings = EnvironmentSettings.in_streaming_mode()
    return StreamTableEnvironment.create(env, environment_settings=settings)


# ── DDL Helpers ───────────────────────────────────────────────────────────────

def create_orders_source(t_env: StreamTableEnvironment, alias: str) -> None:
    """
    Create an orders source table under the given alias.

    Two instances (orders_a, orders_b) read the same Kafka topic with separate
    consumer groups so both sides of the self-join start from earliest-offset
    and advance their watermarks independently.

    Each alias gets a uniquely-named time attribute (event_time_a / event_time_b)
    to prevent DESCRIPTOR() ambiguity in the TVF planner.
    """
    time_col = f"event_time_{alias[-1]}"   # → event_time_a  or  event_time_b
    t_env.execute_sql(f"""
        CREATE TABLE {alias} (
            order_id    STRING,
            trader_id   STRING,
            trader_type STRING,
            symbol      STRING,
            side        STRING,
            price       DOUBLE,
            quantity    INT,
            status      STRING,
            `timestamp` STRING,
            {time_col}  AS TO_TIMESTAMP(
                REPLACE(LEFT(`timestamp`, 19), 'T', ' '),
                'yyyy-MM-dd HH:mm:ss'
            ),
            WATERMARK FOR {time_col} AS {time_col} - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_ORDERS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
            'properties.group.id'          = 'wash-trade-detector-{alias}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)
    logger.info(f"Source table '{alias}' created "
                f"(topic: {KAFKA_ORDERS_TOPIC}, time_col: {time_col})")


def create_windowed_views(t_env: StreamTableEnvironment) -> None:
    """
    Pre-apply the tumbling window TVF to each side of the self-join.

    Creating one view per side (each referencing a single source table) resolves
    the DESCRIPTOR index-out-of-bounds bug that arises when two TUMBLE TVFs on
    similarly-shaped tables are written inline in the same JOIN clause.

    window_time (the TVF-assigned rowtime attribute) is included so the planner
    can trace window lineage through the view into the outer window aggregate.
    order_ts holds the original event timestamp for the intra-pair proximity
    check (TIMESTAMPDIFF).
    """
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW windowed_buys AS
        SELECT
            trader_id,
            symbol,
            price,
            quantity,
            event_time_a    AS order_ts,
            window_start,
            window_end,
            window_time
        FROM TABLE(
            TUMBLE(TABLE orders_a, DESCRIPTOR(event_time_a),
                   INTERVAL '{WINDOW_SIZE_MIN}' MINUTE)
        )
        WHERE side = 'buy' AND status = 'FILLED'
    """)
    logger.info(f"View 'windowed_buys' created ({WINDOW_SIZE_MIN}m tumbling on orders_a)")

    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW windowed_sells AS
        SELECT
            trader_id,
            symbol,
            price,
            quantity,
            event_time_b    AS order_ts,
            window_start,
            window_end,
            window_time
        FROM TABLE(
            TUMBLE(TABLE orders_b, DESCRIPTOR(event_time_b),
                   INTERVAL '{WINDOW_SIZE_MIN}' MINUTE)
        )
        WHERE side = 'sell' AND status = 'FILLED'
    """)
    logger.info(f"View 'windowed_sells' created ({WINDOW_SIZE_MIN}m tumbling on orders_b)")


def create_alerts_sink(t_env: StreamTableEnvironment) -> None:
    """Alerts sink table — one JSON message per detected wash-trading window."""
    t_env.execute_sql(f"""
        CREATE TABLE alerts (
            alert_type         STRING,
            trader_id_1        STRING,
            trader_id_2        STRING,
            symbol             STRING,
            match_count        BIGINT,
            avg_price_diff_pct DOUBLE,
            window_start       TIMESTAMP(3),
            window_end         TIMESTAMP(3),
            detected_at        STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_ALERTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
            'format'                       = 'json'
        )
    """)
    logger.info(f"Sink table 'alerts' created (topic: {KAFKA_ALERTS_TOPIC})")


# ── Detection Query ───────────────────────────────────────────────────────────

def run_wash_trade_detection(t_env: StreamTableEnvironment) -> None:
    """
    Window join across the pre-windowed buy / sell views.

    Join condition:
      • Same tumbling window  (b.window_start = s.window_start, window_end)
      • Same symbol
      • Different trader IDs
      • Price midpoint deviation <= MAX_PRICE_DIFF_PCT %
      • Quantity deviation        <= MAX_QTY_DIFF_PCT %
      • Timestamp proximity       <= MATCH_WINDOW_SEC seconds

    The GROUP BY normalises trader-pair order so that (A buys, B sells) and
    (B buys, A sells) land in the same group, giving the true co-trading count.
    HAVING filters for pairs with MIN_MATCH_COUNT or more cross-trades.
    """
    price_tol = MAX_PRICE_DIFF_PCT / 100.0   # 0.5 % → 0.005
    qty_tol   = MAX_QTY_DIFF_PCT   / 100.0   # 10 % → 0.10

    logger.info(
        f"Starting wash trade detection: "
        f"window={WINDOW_SIZE_MIN}m proximity={MATCH_WINDOW_SEC}s "
        f"min_matches>={MIN_MATCH_COUNT} "
        f"price_tol<={MAX_PRICE_DIFF_PCT}% qty_tol<={MAX_QTY_DIFF_PCT}%"
    )

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(f"""
        INSERT INTO alerts
        SELECT
            'WASH_TRADING'                                            AS alert_type,
            CASE WHEN b.trader_id <= s.trader_id
                 THEN b.trader_id ELSE s.trader_id END                AS trader_id_1,
            CASE WHEN b.trader_id <= s.trader_id
                 THEN s.trader_id ELSE b.trader_id END                AS trader_id_2,
            b.symbol,
            COUNT(*)                                                  AS match_count,
            AVG(
                ABS(b.price - s.price) / ((b.price + s.price) / 2.0) * 100.0
            )                                                         AS avg_price_diff_pct,
            b.window_start,
            b.window_end,
            DATE_FORMAT(NOW(), 'yyyy-MM-dd HH:mm:ss')                AS detected_at
        FROM windowed_buys  b
        JOIN windowed_sells  s
        ON  b.window_start = s.window_start
        AND b.window_end   = s.window_end
        AND b.symbol       = s.symbol
        AND b.trader_id   <> s.trader_id
        AND ABS(b.price - s.price) / ((b.price + s.price) / 2.0)
                <= {price_tol}
        AND ABS(CAST(b.quantity AS DOUBLE) - CAST(s.quantity AS DOUBLE))
            / ((CAST(b.quantity AS DOUBLE) + CAST(s.quantity AS DOUBLE)) / 2.0)
                <= {qty_tol}
        AND TIMESTAMPDIFF(SECOND, b.order_ts, s.order_ts)
                BETWEEN -{MATCH_WINDOW_SEC} AND {MATCH_WINDOW_SEC}
        GROUP BY
            CASE WHEN b.trader_id <= s.trader_id
                 THEN b.trader_id ELSE s.trader_id END,
            CASE WHEN b.trader_id <= s.trader_id
                 THEN s.trader_id ELSE b.trader_id END,
            b.symbol,
            b.window_start,
            b.window_end
        HAVING COUNT(*) >= {MIN_MATCH_COUNT}
    """)

    table_result = statement_set.execute()
    job_client   = table_result.get_job_client()
    job_id       = job_client.get_job_id() if job_client else "unknown"
    logger.info(f"Job submitted — ID: {job_id}")
    logger.info("Wash trade detector running. Waiting for results...")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 55)
    logger.info("Wash Trade Detector starting")
    logger.info(f"  Kafka         : {KAFKA_SERVERS}")
    logger.info(f"  Source topic  : {KAFKA_ORDERS_TOPIC}")
    logger.info(f"  Sink topic    : {KAFKA_ALERTS_TOPIC}")
    logger.info(f"  Window        : {WINDOW_SIZE_MIN}m tumbling")
    logger.info(f"  Proximity     : {MATCH_WINDOW_SEC}s per matched pair")
    logger.info(f"  Thresholds    : matches>={MIN_MATCH_COUNT}, "
                f"price<={MAX_PRICE_DIFF_PCT}%, qty<={MAX_QTY_DIFF_PCT}%")
    logger.info("=" * 55)

    t_env = build_table_env()
    create_orders_source(t_env, "orders_a")
    create_orders_source(t_env, "orders_b")
    create_windowed_views(t_env)
    create_alerts_sink(t_env)
    run_wash_trade_detection(t_env)


if __name__ == "__main__":
    main()
