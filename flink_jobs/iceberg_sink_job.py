"""Persist original and normalized four-exchange trades into Iceberg/Nessie."""

import logging

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment

from shared.config import NESSIE_CATALOG_PROPERTIES
from shared.market_sources import create_market_sources

logger = logging.getLogger(__name__)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Checkpointing is enabled so the Kafka source offsets and Iceberg commits
    # can be coordinated by Flink's checkpoint lifecycle.
    env.enable_checkpointing(300000)
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_checkpoint_timeout(120000)
    checkpoint_config.set_min_pause_between_checkpoints(60000)
    checkpoint_config.set_tolerable_checkpoint_failure_number(2)
    checkpoint_config.set_max_concurrent_checkpoints(1)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    t_env.get_config().set(
        "pipeline.name", "Iceberg Sink Job - Four Exchange Raw Trades"
    )

    # Register the same Nessie catalog as the original project.
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)
    t_env.execute_sql("USE CATALOG nessie_catalog")
    t_env.execute_sql("USE crypto")

    # Register original and additional Kafka sources; the original topics and
    # their Avro schemas remain untouched.
    create_market_sources(t_env, "flink-iceberg")

    statements = t_env.create_statement_set()

    # Existing Binance target is retained exactly as before.
    statements.add_insert_sql("""
        INSERT INTO binance_trades_raw
        SELECT symbol, CAST(price AS DOUBLE), CAST(quantity AS DOUBLE),
               trade_time, is_buyer_maker, ingestion_time
        FROM binance_market_source
    """)

    # Existing Coinbase target is retained exactly as before.
    statements.add_insert_sql("""
        INSERT INTO coinbase_trades_raw
        SELECT trade_id, product_id, CAST(price AS DOUBLE), CAST(size AS DOUBLE),
               side, event_time, ingestion_time
        FROM coinbase_market_source
    """)

    # New exchange-specific tables keep the source payload and exchange identity.
    statements.add_insert_sql("""
        INSERT INTO okx_trades_raw
        SELECT trade_id, symbol, CAST(price AS DOUBLE), CAST(quantity AS DOUBLE),
               side, trade_time, ingestion_time
        FROM okx_market_source
    """)
    statements.add_insert_sql("""
        INSERT INTO bitget_trades_raw
        SELECT trade_id, symbol, CAST(price AS DOUBLE), CAST(quantity AS DOUBLE),
               side, trade_time, ingestion_time
        FROM bitget_market_source
    """)

    # Unified table makes cross-exchange batch analytics straightforward.
    statements.add_insert_sql("""
        INSERT INTO market_trades_raw
        SELECT exchange, symbol, trade_id, CAST(price AS DOUBLE),
               CAST(quantity AS DOUBLE), side, trade_time, ingestion_time
        FROM normalized_market_trades
    """)

    statements.execute()


if __name__ == "__main__":
    main()
