"""Five-minute VWAP aggregation across Binance, Coinbase, OKX and Bitget."""

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


class VwapClickHouseSink(ClickHouseSink):
    """Backward-compatible sink retained for existing tests/consumers."""

    @property
    def table_name(self):
        return "vwap_aggregations"

    @property
    def columns(self):
        return ["symbol", "window_start", "window_end", "vwap",
                "total_volume", "trade_count", "interval_label", "version"]

    def to_record(self, value):
        start = datetime.strptime(value[1][:19], "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(value[2][:19], "%Y-%m-%d %H:%M:%S")
        return (value[0], start, end, value[3], value[4], value[5], value[6],
                int(start.replace(tzinfo=timezone.utc).timestamp()))


class MultiExchangeVwapSink(ClickHouseSink):
    """Persist a separate VWAP per exchange, symbol and time window."""

    @property
    def table_name(self):
        return "vwap_aggregations_multi"

    @property
    def columns(self):
        return ["exchange", "symbol", "window_start", "window_end", "vwap",
                "total_volume", "trade_count", "interval_label", "version"]

    def to_record(self, value):
        start = datetime.strptime(value[2][:19], "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(value[3][:19], "%Y-%m-%d %H:%M:%S")
        return (value[0], value[1], start, end, value[4], value[5], value[6],
                value[7], int(start.replace(tzinfo=timezone.utc).timestamp()))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    create_market_sources(t_env, "flink-vwap")

    result = t_env.sql_query("""
        SELECT
            exchange,
            symbol,
            CAST(TUMBLE_START(trade_time_ts, INTERVAL '5' MINUTE) AS STRING) AS window_start,
            CAST(TUMBLE_END(trade_time_ts, INTERVAL '5' MINUTE) AS STRING) AS window_end,
            SUM(CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE))
                / NULLIF(SUM(CAST(quantity AS DOUBLE)), 0.0) AS vwap,
            SUM(CAST(quantity AS DOUBLE)) AS total_volume,
            COUNT(*) AS trade_count,
            '5m' AS interval_label
        FROM normalized_market_trades
        GROUP BY exchange, symbol, TUMBLE(trade_time_ts, INTERVAL '5' MINUTE)
    """)

    stream = t_env.to_append_stream(
        result,
        Types.ROW([
            Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.DOUBLE(), Types.LONG(), Types.STRING(),
        ]),
    )
    stream.map(MultiExchangeVwapSink(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        batch_size=1, flush_interval_sec=2.0,
    ))
    env.execute("Multi-Exchange VWAP Aggregation Job")


if __name__ == "__main__":
    main()
