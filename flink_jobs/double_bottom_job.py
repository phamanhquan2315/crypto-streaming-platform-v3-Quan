"""Double-bottom CEP-like pattern detection, isolated by exchange and symbol."""

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


class MultiExchangeDoubleBottomSink(ClickHouseSink):
    """Write detected patterns to a new table including exchange identity."""

    @property
    def table_name(self):
        return "double_bottom_signals_multi"

    @property
    def columns(self):
        return ["exchange", "symbol", "first_dip", "second_dip",
                "peak_price", "breakout_price", "confirmed_at", "version"]

    def to_record(self, value):
        confirmed_at = datetime.strptime(value[6][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], value[1], value[2], value[3], value[4], value[5],
            confirmed_at,
            int(confirmed_at.replace(tzinfo=timezone.utc).timestamp()),
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    t_env.get_config().set("table.exec.state.ttl", "15min")
    create_market_sources(t_env, "flink-double-bottom")

    # Partition by exchange as well as symbol. Otherwise a BTC trade from OKX
    # could incorrectly become the second dip after a BTC trade from Binance.
    pattern_input = t_env.sql_query("""
        SELECT
            exchange,
            symbol,
            CAST(price AS DOUBLE) AS price_d,
            trade_time_ts
        FROM normalized_market_trades
    """)
    t_env.create_temporary_view("market_prices_for_pattern", pattern_input)

    result = t_env.sql_query("""
        SELECT
            exchange,
            symbol,
            first_dip,
            second_dip,
            peak_price,
            breakout_price,
            confirmed_at
        FROM market_prices_for_pattern
        MATCH_RECOGNIZE (
            PARTITION BY exchange, symbol
            ORDER BY trade_time_ts
            MEASURES
                FIRST(FIRST_DIP.price_d) AS first_dip,
                LAST(SECOND_DIP.price_d) AS second_dip,
                LAST(PEAK.price_d) AS peak_price,
                LAST(BREAK.price_d) AS breakout_price,
                CAST(LAST(BREAK.trade_time_ts) AS STRING) AS confirmed_at
            ONE ROW PER MATCH
            PATTERN (FIRST_DIP+ PEAK+ SECOND_DIP+ BREAK)
            WITHIN INTERVAL '10' MINUTE
            DEFINE
                FIRST_DIP AS price_d < PREV(price_d, 1),
                PEAK AS price_d > PREV(price_d, 1),
                SECOND_DIP AS price_d < PREV(price_d, 1),
                BREAK AS price_d > FIRST(PEAK.price_d)
        )
    """)

    stream = t_env.to_append_stream(
        result,
        Types.ROW([
            Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
            Types.DOUBLE(), Types.DOUBLE(), Types.STRING(),
        ]),
    )
    stream.map(MultiExchangeDoubleBottomSink(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        batch_size=1, flush_interval_sec=10.0,
    ))
    env.execute("Four-Exchange Double Bottom Pattern Job")


if __name__ == "__main__":
    main()
