-- New multi-exchange result tables.
-- The original tables in 001_create_tables.sql are intentionally not altered.
-- This makes the migration safe for an existing ClickHouse volume.

CREATE TABLE IF NOT EXISTS vwap_aggregations_multi
(
    exchange String,
    symbol String,
    window_start DateTime,
    window_end DateTime,
    vwap Float64,
    total_volume Float64,
    trade_count UInt64,
    interval_label String,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (exchange, symbol, window_start);

CREATE TABLE IF NOT EXISTS whale_alerts_multi
(
    symbol String,
    price Float64,
    quantity Float64,
    trade_value Float64,
    exchange String,
    side String,
    event_time DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (exchange, symbol, event_time);

CREATE TABLE IF NOT EXISTS arbitrage_signals_multi
(
    symbol String,
    buy_exchange String,
    sell_exchange String,
    buy_price Float64,
    sell_price Float64,
    spread Float64,
    spread_pct Float64,
    event_time DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, event_time, buy_exchange, sell_exchange);

CREATE TABLE IF NOT EXISTS double_bottom_signals_multi
(
    exchange String,
    symbol String,
    first_dip Float64,
    second_dip Float64,
    peak_price Float64,
    breakout_price Float64,
    confirmed_at DateTime,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (exchange, symbol, confirmed_at);


CREATE TABLE IF NOT EXISTS recon_results_multi
(
    run_time DateTime,
    window_start DateTime,
    window_end DateTime,
    exchange String,
    symbol String,
    streaming_vwap Float64,
    batch_vwap Float64,
    diff_pct Float64,
    status String,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (exchange, symbol, window_start, run_time);
