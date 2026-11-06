"""Detect large spot trades from all four exchanges and write ClickHouse alerts."""

import logging
from datetime import datetime, timezone

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from shared.config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB,
    CLICKHOUSE_USER, CLICKHOUSE_PASSWORD,
)
from shared.clickhouse_sink import ClickHouseSink
from shared.market_sources import create_market_sources

logger = logging.getLogger(__name__)


class WhaleClickHouseSink(ClickHouseSink):
    """ClickHouse sink for whale alerts; table schema is unchanged."""

    @property
    def table_name(self):
        return "whale_alerts_multi"

    @property
    def columns(self):
        return ["symbol", "price", "quantity", "trade_value",
                "exchange", "side", "event_time", "version"]

    def to_record(self, value):
        event_time = datetime.strptime(value[6][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], value[1], value[2], value[3], value[4], value[5],
            event_time,
            int(event_time.replace(tzinfo=timezone.utc).timestamp()),
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    create_market_sources(t_env, "flink-whale")

    # Thresholds preserve the original project's base-asset quantity rules.
    # The exchange name now remains attached to each alert.
    result = t_env.sql_query("""
        SELECT
            symbol,
            CAST(price AS DOUBLE) AS price,
            CAST(quantity AS DOUBLE) AS quantity,
            CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE) AS trade_value,
            exchange,
            side,
            CAST(trade_time_ts AS STRING) AS event_time
        FROM normalized_market_trades
        WHERE
            (symbol = 'BTCUSDT' AND quantity >= 5.0)
            OR (symbol = 'ETHUSDT' AND quantity >= 50.0)
            OR (symbol = 'SOLUSDT' AND quantity >= 500.0)
    """)

    stream = t_env.to_append_stream(
        result,
        Types.ROW([
            Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
            Types.STRING(), Types.STRING(), Types.STRING(),
        ]),
    )
    stream.map(WhaleClickHouseSink(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        batch_size=1, flush_interval_sec=1.0,
    ))
    env.execute("Four-Exchange Whale Detection Job")


if __name__ == "__main__":
    main()
