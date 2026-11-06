"""Shared Flink SQL DDL for a normalized four-exchange trade stream.

The original Binance and Coinbase Kafka topics and Avro schemas are not changed.
This module creates four source tables and a TEMPORARY VIEW that maps them to
one common row shape, allowing downstream Flink jobs to process all exchanges.
"""

def create_market_sources(t_env, group_prefix: str) -> None:
    """Register four Kafka/Avro source tables and normalized_market_trades view."""

    # Binance source keeps its original schema and topic.
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS binance_market_source (
            event_type STRING,
            event_time BIGINT,
            symbol STRING,
            trade_id BIGINT,
            price DECIMAL(18, 8),
            quantity DECIMAL(18, 8),
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT,
            trade_time_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
            WATERMARK FOR trade_time_ts AS trade_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = '{group_prefix}-binance',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # Coinbase source keeps its original schema and topic.
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS coinbase_market_source (
            trade_id STRING,
            product_id STRING,
            price DECIMAL(18, 8),
            size DECIMAL(18, 8),
            side STRING,
            event_time BIGINT,
            ingestion_time BIGINT,
            event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-coinbase-match',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = '{group_prefix}-coinbase',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # OKX and Bitget use the shared MarketTrade Avro schema.
    for exchange in ("okx", "bitget"):
        t_env.execute_sql(f"""
            CREATE TEMPORARY TABLE IF NOT EXISTS {exchange}_market_source (
                exchange STRING,
                symbol STRING,
                trade_id STRING,
                price DECIMAL(18, 8),
                quantity DECIMAL(18, 8),
                side STRING,
                trade_time BIGINT,
                ingestion_time BIGINT,
                trade_time_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
                WATERMARK FOR trade_time_ts AS trade_time_ts - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'raw-{exchange}-trades',
                'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
                'properties.group.id' = '{group_prefix}-{exchange}',
                'scan.startup.mode' = 'group-offsets',
                'properties.auto.offset.reset' = 'latest',
                'format' = 'avro-confluent',
                'avro-confluent.url' = 'http://schema-registry:8081'
            )
        """)

    # Normalize the source-specific field names without altering original topics.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW normalized_market_trades AS
        SELECT
            'binance' AS exchange,
            UPPER(symbol) AS symbol,
            CAST(trade_id AS STRING) AS trade_id,
            price,
            quantity,
            CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS side,
            trade_time,
            ingestion_time,
            trade_time_ts
        FROM binance_market_source

        UNION ALL

        SELECT
            'coinbase' AS exchange,
            REPLACE(UPPER(product_id), '-USD', 'USDT') AS symbol,
            trade_id,
            price,
            size AS quantity,
            UPPER(side) AS side,
            event_time AS trade_time,
            ingestion_time,
            event_time_ts AS trade_time_ts
        FROM coinbase_market_source

        UNION ALL

        SELECT
            'okx' AS exchange,
            UPPER(symbol) AS symbol,
            trade_id,
            price,
            quantity,
            UPPER(side) AS side,
            trade_time,
            ingestion_time,
            trade_time_ts
        FROM okx_market_source

        UNION ALL

        SELECT
            'bitget' AS exchange,
            UPPER(symbol) AS symbol,
            trade_id,
            price,
            quantity,
            UPPER(side) AS side,
            trade_time,
            ingestion_time,
            trade_time_ts
        FROM bitget_market_source
    """)
