"""Create the Nessie database and Iceberg tables required by the pipeline."""

import logging
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment
from shared.config import NESSIE_CATALOG_PROPERTIES

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    logger.info("Registering Nessie catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)
    t_env.execute_sql("USE CATALOG nessie_catalog")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS crypto")
    t_env.execute_sql("USE crypto")

    # Existing Binance Iceberg table.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_raw (
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT
        )
    """)

    # Existing Coinbase Iceberg table.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_raw (
            trade_id STRING,
            product_id STRING,
            price DOUBLE,
            size DOUBLE,
            side STRING,
            event_time BIGINT,
            ingestion_time BIGINT
        )
    """)

    # New OKX Iceberg table.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS okx_trades_raw (
            trade_id STRING,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            side STRING,
            trade_time BIGINT,
            ingestion_time BIGINT
        )
    """)

    # New Bitget Iceberg table.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS bitget_trades_raw (
            trade_id STRING,
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            side STRING,
            trade_time BIGINT,
            ingestion_time BIGINT
        )
    """)

    # Unified table contains all four exchanges in one canonical structure.
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS market_trades_raw (
            exchange STRING,
            symbol STRING,
            trade_id STRING,
            price DOUBLE,
            quantity DOUBLE,
            side STRING,
            trade_time BIGINT,
            ingestion_time BIGINT
        )
    """)

    logger.info("All original and four-exchange Iceberg tables are ready.")


if __name__ == "__main__":
    main()
