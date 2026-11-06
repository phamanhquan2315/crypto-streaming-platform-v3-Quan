"""Batch-vs-streaming VWAP reconciliation across all four exchanges."""

import logging
import sys
from datetime import datetime, timedelta, timezone

import clickhouse_driver
from pyflink.table import EnvironmentSettings, TableEnvironment

from shared.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    NESSIE_CATALOG_PROPERTIES,
    DIFF_THRESHOLD_PCT,
)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def get_time_window():
    """Return the last two-hour UTC reconciliation window."""
    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    return start, now


def compute_batch_vwap(t_env, start: datetime, end: datetime) -> dict:
    """Compute batch VWAP by exchange and symbol from the unified Iceberg table."""
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    result = t_env.execute_sql(f"""
        SELECT
            exchange,
            symbol,
            SUM(price * quantity) / NULLIF(SUM(quantity), 0.0) AS batch_vwap
        FROM nessie_catalog.crypto.market_trades_raw
        WHERE trade_time >= {start_ms}
          AND trade_time < {end_ms}
        GROUP BY exchange, symbol
    """)

    values = {}
    with result.collect() as rows:
        for row in rows:
            key = (row[0], row[1])
            values[key] = float(row[2])
            logger.info("Batch VWAP %s/%s = %.8f", row[0], row[1], row[2])
    return values


def compute_streaming_vwap(start: datetime, end: datetime) -> dict:
    """Read matching exchange/symbol VWAPs from the ClickHouse serving table."""
    client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    try:
        rows = client.execute("""
            SELECT
                exchange,
                symbol,
                SUM(vwap * total_volume) / NULLIF(SUM(total_volume), 0) AS streaming_vwap
            FROM vwap_aggregations_multi
            WHERE window_start >= %(start)s
              AND window_end <= %(end)s
            GROUP BY exchange, symbol
        """, {"start": start.replace(tzinfo=None), "end": end.replace(tzinfo=None)})

        values = {}
        for exchange, symbol, vwap in rows:
            values[(exchange, symbol)] = float(vwap)
            logger.info("Streaming VWAP %s/%s = %.8f", exchange, symbol, vwap)
        return values
    finally:
        client.disconnect()


def save_results(results: list) -> None:
    """Write reconciliation output to the new multi-exchange result table."""
    client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    try:
        client.execute("""
            INSERT INTO recon_results_multi
                (run_time, window_start, window_end, exchange, symbol,
                 streaming_vwap, batch_vwap, diff_pct, status, version)
            VALUES
        """, results)
    finally:
        client.disconnect()


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    start, end = get_time_window()
    logger.info("Reconciliation UTC window: %s -> %s", start, end)

    batch = compute_batch_vwap(t_env, start, end)
    streaming = compute_streaming_vwap(start, end)

    if not batch and not streaming:
        logger.warning("No batch or streaming VWAP data found; skipping.")
        return

    run_time = datetime.now(timezone.utc)
    results = []

    for exchange, symbol in sorted(set(batch) | set(streaming)):
        batch_vwap = batch.get((exchange, symbol), 0.0)
        streaming_vwap = streaming.get((exchange, symbol), 0.0)

        # A missing side is a data completeness failure, not a numeric zero.
        if batch_vwap == 0.0 or streaming_vwap == 0.0:
            status = "ALERT"
            diff_pct = 100.0
        else:
            diff_pct = abs(batch_vwap - streaming_vwap) / batch_vwap * 100.0
            status = "ALERT" if diff_pct > DIFF_THRESHOLD_PCT else "OK"

        logger.info(
            "%s/%s | streaming=%.8f | batch=%.8f | diff=%.5f%% | %s",
            exchange, symbol, streaming_vwap, batch_vwap, diff_pct, status,
        )
        results.append((
            run_time.replace(tzinfo=None),
            start.replace(tzinfo=None),
            end.replace(tzinfo=None),
            exchange,
            symbol,
            streaming_vwap,
            batch_vwap,
            diff_pct,
            status,
            int(run_time.timestamp()),
        ))

    if results:
        save_results(results)
        logger.info("Saved %s multi-exchange reconciliation records.", len(results))


if __name__ == "__main__":
    main()
