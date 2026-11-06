"""ClickHouse helpers used by the Airflow reconciliation DAG."""

import logging
import clickhouse_driver

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""


def get_client() -> clickhouse_driver.Client:
    """Create a ClickHouse client using the existing Docker network settings."""
    return clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def get_recon_results(window_start=None) -> list:
    """Return the latest four-exchange reconciliation rows.

    Keep the original five-column return shape so reconciliation_dag.py does
    not need to change its alert indexing. The first column is exchange/symbol.
    """
    client = get_client()
    try:
        return client.execute("""
            SELECT
                concat(exchange, '/', symbol) AS exchange_symbol,
                streaming_vwap,
                batch_vwap,
                diff_pct,
                status
            FROM recon_results_multi
            WHERE run_time >= now() - INTERVAL 10 MINUTE
            ORDER BY diff_pct DESC
        """)
    finally:
        client.disconnect()
