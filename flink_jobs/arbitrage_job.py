"""Cross-exchange last-trade arbitrage spread monitor.

This is a market-data signal, not an executable arbitrage guarantee. It compares
last traded prices in aligned five-second event-time windows and does not include
fees, slippage, order-book depth, funding or transfer latency.
"""

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


class ArbitrageClickHouseSink(ClickHouseSink):
    """Legacy mapping kept so existing tests and old table consumers still work."""

    @property
    def table_name(self):
        return "arbitrage_signals"

    @property
    def columns(self):
        return ["symbol", "binance_price", "coinbase_price", "spread",
                "spread_pct", "direction", "event_time", "version"]

    def to_record(self, value):
        event_time = datetime.strptime(value[6][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], value[1], value[2], value[3], value[4], value[5],
            event_time,
            int(event_time.replace(tzinfo=timezone.utc).timestamp()),
        )


class MultiExchangeArbitrageSink(ClickHouseSink):
    """Store generic buy/sell exchange pairs without hard-coded exchange columns."""

    @property
    def table_name(self):
        return "arbitrage_signals_multi"

    @property
    def columns(self):
        return ["symbol", "buy_exchange", "sell_exchange", "buy_price",
                "sell_price", "spread", "spread_pct", "event_time", "version"]

    def to_record(self, value):
        event_time = datetime.strptime(value[7][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], value[1], value[2], value[3], value[4], value[5], value[6],
            event_time,
            int(event_time.replace(tzinfo=timezone.utc).timestamp()),
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    # Keep short-lived join state bounded.
    t_env.get_config().set("table.exec.state.ttl", "2min")
    create_market_sources(t_env, "flink-arbitrage")

    result = t_env.sql_query("""
        WITH exchange_prices AS (
            SELECT
                symbol,
                exchange,
                window_start,
                LAST_VALUE(price) AS last_price
            FROM TABLE(
                TUMBLE(
                    TABLE normalized_market_trades,
                    DESCRIPTOR(trade_time_ts),
                    INTERVAL '5' SECOND
                )
            )
            GROUP BY symbol, exchange, window_start, window_end
        )
        SELECT
            buy_side.symbol,
            buy_side.exchange AS buy_exchange,
            sell_side.exchange AS sell_exchange,
            CAST(buy_side.last_price AS DOUBLE) AS buy_price,
            CAST(sell_side.last_price AS DOUBLE) AS sell_price,
            CAST(sell_side.last_price - buy_side.last_price AS DOUBLE) AS spread,
            (
                (CAST(sell_side.last_price AS DOUBLE)
                 - CAST(buy_side.last_price AS DOUBLE))
                / CAST(buy_side.last_price AS DOUBLE)
            ) * 100.0 AS spread_pct,
            CAST(buy_side.window_start AS STRING) AS event_time
        FROM exchange_prices AS buy_side
        JOIN exchange_prices AS sell_side
          ON buy_side.symbol = sell_side.symbol
         AND buy_side.window_start = sell_side.window_start
         AND buy_side.exchange <> sell_side.exchange
        WHERE buy_side.last_price < sell_side.last_price
          AND buy_side.last_price > 0
          AND (
            (CAST(sell_side.last_price AS DOUBLE)
             - CAST(buy_side.last_price AS DOUBLE))
            / CAST(buy_side.last_price AS DOUBLE)
          ) * 100.0 > 0.1
    """)

    stream = t_env.to_append_stream(
        result,
        Types.ROW([
            Types.STRING(), Types.STRING(), Types.STRING(),
            Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
            Types.STRING(),
        ]),
    )
    stream.map(MultiExchangeArbitrageSink(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        batch_size=100, flush_interval_sec=5.0,
    ))
    env.execute("Four-Exchange Arbitrage Monitor Job")


if __name__ == "__main__":
    main()
