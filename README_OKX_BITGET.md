# OKX + Bitget integration patch

This patch extends the existing `crypto-streaming-platform` repository without
changing the original Binance and Coinbase Avro schemas or Kafka topic names.

## New files
- `producers/okx_producer.py`
- `producers/bitget_producer.py`
- `producers/schemas/market_trade.avsc`
- `flink_jobs/shared/market_sources.py`
- `clickhouse/init/002_multi_exchange_tables.sql`
- `tests/test_okx_bitget_producers.py`
- `tests/test_market_trade_schema.py`
- `tests/test_market_sources.py`
- `scripts/smoke_test_exchange_topics.py`

## Replaced/updated files
- `producers/main.py` starts four producers.
- `scripts/create_topics.py` creates OKX and Bitget topics in addition to the
  original topics.
- `flink_jobs/vwap_job.py` computes exchange-specific VWAP.
- `flink_jobs/whale_job.py` detects whales on all four exchanges.
- `flink_jobs/arbitrage_job.py` compares every exchange pair in aligned
  five-second windows.
- `flink_jobs/double_bottom_job.py` isolates pattern state by exchange+symbol.
- `flink_jobs/iceberg_sink_job.py` writes old and new sources to Iceberg.
- `flink_jobs/init_iceberg.py` creates the new Iceberg tables.
- `flink_jobs/check_iceberg_data.py` checks the unified Iceberg table.
- `flink_jobs/reconciliation_job.py` reconciles per exchange and symbol.
- `flink_jobs/shared/config.py` lists the new Iceberg tables.
- `clickhouse/init/002_multi_exchange_tables.sql` creates new result tables.
- `airflow/operators/clickhouse_operator.py` reads the multi-exchange results.
- `monitoring/grafana/dashboards/crypto_analytics.json` uses the new tables.
- `docker/flink/Dockerfile` uses the Java path from the official Flink image,
  avoiding the hard-coded ARM64 path on x86_64 WSL systems.
- `Makefile` updates the producer help text.

## Infrastructure/configuration notes

- `docker-compose.yml` does not need a new service: producers continue to run
  on the WSL host, just like the original Makefile workflow.
- Kafka broker configuration is unchanged. Topics are created by
  `scripts/create_topics.py`.
- Schema Registry configuration is unchanged. Existing `AvroSerializer`
  automatically registers subjects for `raw-okx-trades` and
  `raw-bitget-trades`.
- The existing Docker Compose ClickHouse init-directory mount already includes
  `clickhouse/init/002_multi_exchange_tables.sql`. On a fresh ClickHouse
  volume it is executed automatically. On an existing volume, apply it once
  manually as shown below.
- No Prometheus exporter change is required because Kafka Exporter already
  discovers topics and consumer groups from the brokers.

## Data contract

OKX and Bitget both publish:
`exchange, symbol, trade_id, price, quantity, side, trade_time, ingestion_time`

Symbols are normalized to `BTCUSDT`, `ETHUSDT`, and `SOLUSDT`.
Prices and quantities are Avro decimal(18,8); timestamps are Unix milliseconds.
The existing Binance and Coinbase records remain unchanged. Flink registers
all four source topics and exposes a normalized temporary view.

## Apply and run

From the repository root:

```bash
# 1. Rebuild Flink image because JAVA_HOME in its Dockerfile is corrected.
docker compose build flink-jobmanager flink-taskmanager-1 flink-taskmanager-2

# 2. Start or recreate the infrastructure.
docker compose up -d

# 3. Create the additional Kafka topics (safe to run repeatedly).
python scripts/create_topics.py

# 4. Apply ClickHouse migration if ClickHouse volume already existed.
docker exec -i clickhouse clickhouse-client --multiquery \
  < clickhouse/init/002_multi_exchange_tables.sql

# 5. Run unit tests.
pytest -q

# 6. Start all four producers in a separate terminal.
make producers

# 7. In another terminal, submit the Flink jobs.
cd flink_jobs && python main.py

# 8. Validate that OKX and Bitget topics contain Avro records.
cd .. && python scripts/smoke_test_exchange_topics.py --timeout 60
```

If this is a clean deployment, the original Makefile flow can be used after
building the Flink image: `make up`, then `make producers`.

## Check results

Kafka UI: http://localhost:8090
Flink UI: http://localhost:8080
ClickHouse HTTP: http://localhost:8123
Grafana: http://localhost:3000
Airflow: http://localhost:8082

Example ClickHouse queries:

```sql
SELECT exchange, symbol, count()
FROM market_trades_raw
GROUP BY exchange, symbol
ORDER BY exchange, symbol;

SELECT exchange, symbol, vwap, window_start
FROM vwap_aggregations_multi FINAL
ORDER BY window_start DESC
LIMIT 30;

SELECT symbol, buy_exchange, sell_exchange, spread_pct, event_time
FROM arbitrage_signals_multi FINAL
ORDER BY event_time DESC
LIMIT 30;

SELECT exchange, symbol, side, trade_value, event_time
FROM whale_alerts_multi FINAL
ORDER BY event_time DESC
LIMIT 30;
```

## Important interpretation notes

- This implementation uses public SPOT USDT trades, not perpetual/futures.
- The arbitrage signal compares last traded prices, not executable bid/ask
  order-book prices. It does not deduct fees, slippage or market impact.
- A WebSocket disconnect can cause gaps because these public streams do not
  provide guaranteed replay for every missed trade. Kafka producer idempotence
  does not make the exchange WebSocket exactly-once.
- Bitget may send an initial recent-trades snapshot as well as incremental
  updates. Deduplicate downstream using `(exchange, symbol, trade_id)` if the
  feed/API behavior creates repeated IDs in your observed data.
- The `market_sources.py` view uses event-time watermarks with a 10-second
  out-of-orderness allowance. Very late records may be dropped by windowed
  analytics depending on the Flink operator.
- For production use, add authentication/network hardening, secret management,
  retention sizing and alerting based on measured traffic.
