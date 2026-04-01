"""
Spoofing Detector
=================
PyFlink streaming job that detects spoofing patterns in order flow.

Spoofing: placing a burst of large orders on one side to create the
illusion of market depth, then cancelling them before execution to
profit from the induced price movement.

Detection logic (sliding window):
  - Window : 2 minutes, sliding every 30 seconds
  - Trigger : cancel_count > 10 AND cancel_rate > 0.80
  - Output  : alert event written to the 'alerts' Kafka topic

Run inside the Flink container:
  flink run -py /opt/flink/jobs/spoofing_detector.py
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
logger = logging.getLogger("spoofing-detector")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_SERVERS         = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_ORDERS_TOPIC    = os.environ.get("KAFKA_ORDERS_TOPIC",      "orders")
KAFKA_ALERTS_TOPIC    = os.environ.get("KAFKA_ALERTS_TOPIC",      "alerts")
KAFKA_CONNECTOR_JAR   = os.environ.get(
    "KAFKA_CONNECTOR_JAR",
    "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
)

# Window parameters
WINDOW_SIZE_MIN  = int(os.environ.get("WINDOW_SIZE_MIN",  "2"))   # minutes
WINDOW_SLIDE_SEC = int(os.environ.get("WINDOW_SLIDE_SEC", "30"))  # seconds
MIN_CANCEL_COUNT = int(os.environ.get("MIN_CANCEL_COUNT", "10"))
MIN_CANCEL_RATE  = float(os.environ.get("MIN_CANCEL_RATE", "0.8"))


# ── Environment Setup ─────────────────────────────────────────────────────────

def build_table_env() -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Ensure the Kafka connector JAR is on the classpath.
    # (setup.sh copies it to /opt/flink/lib/ which is auto-loaded by the
    # cluster, but adding it here makes local/standalone execution work too.)
    env.add_jars(KAFKA_CONNECTOR_JAR)

    settings = EnvironmentSettings.in_streaming_mode()
    return StreamTableEnvironment.create(env, environment_settings=settings)


# ── DDL Helpers ───────────────────────────────────────────────────────────────

def create_orders_source(t_env: StreamTableEnvironment) -> None:
    """
    Orders source table.

    The `timestamp` column arrives as an ISO-8601 string
    (e.g. "2026-03-30T21:34:02.743134+00:00").  We strip it to
    second precision for TO_TIMESTAMP, then declare it as the
    event-time attribute with a 5-second watermark.
    """
    t_env.execute_sql(f"""
        CREATE TABLE orders (
            order_id   STRING,
            trader_id  STRING,
            trader_type STRING,
            symbol     STRING,
            side       STRING,
            price      DOUBLE,
            quantity   INT,
            status     STRING,
            `timestamp` STRING,
            event_time AS TO_TIMESTAMP(
                REPLACE(LEFT(`timestamp`, 19), 'T', ' '),
                'yyyy-MM-dd HH:mm:ss'
            ),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = '{KAFKA_ORDERS_TOPIC}',
            'properties.bootstrap.servers'  = '{KAFKA_SERVERS}',
            'properties.group.id'           = 'spoofing-detector',
            'scan.startup.mode'             = 'earliest-offset',
            'format'                        = 'json',
            'json.ignore-parse-errors'      = 'true'
        )
    """)
    logger.info(f"Source table 'orders' created (topic: {KAFKA_ORDERS_TOPIC})")


def create_alerts_sink(t_env: StreamTableEnvironment) -> None:
    """Alerts sink table — one JSON message per detected spoofing window."""
    t_env.execute_sql(f"""
        CREATE TABLE alerts (
            alert_type   STRING,
            trader_id    STRING,
            symbol       STRING,
            cancel_count BIGINT,
            total_orders BIGINT,
            cancel_rate  DOUBLE,
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            detected_at  STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = '{KAFKA_ALERTS_TOPIC}',
            'properties.bootstrap.servers'  = '{KAFKA_SERVERS}',
            'format'                        = 'json'
        )
    """)
    logger.info(f"Sink table 'alerts' created (topic: {KAFKA_ALERTS_TOPIC})")


# ── Detection Query ───────────────────────────────────────────────────────────

def run_spoofing_detection(t_env: StreamTableEnvironment) -> None:
    """
    Sliding-window aggregation over the orders stream.

    HOP TVF syntax: HOP(TABLE t, DESCRIPTOR(timecol), slide, size)
      - slide : INTERVAL '30' SECOND
      - size  : INTERVAL '2'  MINUTE

    A (trader_id, symbol) pair triggers an alert when, within any
    window, they submit more than {MIN_CANCEL_COUNT} cancellations
    that account for more than {MIN_CANCEL_RATE*100:.0f}% of their
    total order activity.
    """
    logger.info(
        f"Starting spoofing detection: "
        f"window={WINDOW_SIZE_MIN}m slide={WINDOW_SLIDE_SEC}s "
        f"cancel_count>{MIN_CANCEL_COUNT} cancel_rate>{MIN_CANCEL_RATE}"
    )

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(f"""
        INSERT INTO alerts
        SELECT
            'SPOOFING'                                      AS alert_type,
            trader_id,
            symbol,
            SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)
                                                            AS cancel_count,
            SUM(CASE WHEN status IN ('CANCELLED', 'FILLED') THEN 1 ELSE 0 END)
                                                            AS total_orders,
            CAST(
                SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)
                AS DOUBLE
            ) / CAST(
                SUM(CASE WHEN status IN ('CANCELLED', 'FILLED') THEN 1 ELSE 0 END)
                AS DOUBLE
            )                                              AS cancel_rate,
            window_start,
            window_end,
            DATE_FORMAT(NOW(), 'yyyy-MM-dd HH:mm:ss')      AS detected_at
        FROM TABLE(
            HOP(
                TABLE orders,
                DESCRIPTOR(event_time),
                INTERVAL '{WINDOW_SLIDE_SEC}' SECOND,
                INTERVAL '{WINDOW_SIZE_MIN}'  MINUTE
            )
        )
        GROUP BY
            trader_id,
            symbol,
            window_start,
            window_end
        HAVING
            SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)
                > {MIN_CANCEL_COUNT}
            AND
            CAST(
                SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)
                AS DOUBLE
            ) / CAST(
                SUM(CASE WHEN status IN ('CANCELLED', 'FILLED') THEN 1 ELSE 0 END)
                AS DOUBLE
            )
                > {MIN_CANCEL_RATE}
    """)

    table_result = statement_set.execute()
    job_client = table_result.get_job_client()
    job_id = job_client.get_job_id() if job_client else "unknown"
    logger.info(f"Job submitted — ID: {job_id}")
    logger.info("Spoofing detector running. Waiting for results...")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 55)
    logger.info("Spoofing Detector starting")
    logger.info(f"  Kafka         : {KAFKA_SERVERS}")
    logger.info(f"  Source topic  : {KAFKA_ORDERS_TOPIC}")
    logger.info(f"  Sink topic    : {KAFKA_ALERTS_TOPIC}")
    logger.info(f"  Window        : {WINDOW_SIZE_MIN}m / {WINDOW_SLIDE_SEC}s slide")
    logger.info(f"  Thresholds    : count>{MIN_CANCEL_COUNT}, rate>{MIN_CANCEL_RATE}")
    logger.info("=" * 55)

    t_env = build_table_env()
    create_orders_source(t_env)
    create_alerts_sink(t_env)
    run_spoofing_detection(t_env)


if __name__ == "__main__":
    main()
