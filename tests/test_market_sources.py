"""Tests for the shared Flink SQL source registration without starting Flink."""

from pathlib import Path

from shared.market_sources import create_market_sources


class FakeTableEnvironment:
    """Capture SQL strings so tests do not require a running Flink cluster."""

    def __init__(self):
        self.statements = []

    def execute_sql(self, statement):
        self.statements.append(statement)


def test_registers_original_and_new_exchange_topics():
    env = FakeTableEnvironment()
    create_market_sources(env, "test-group")
    sql = "\n".join(env.statements)

    # Existing source topics are still registered.
    assert "'topic' = 'raw-binance-trades'" in sql
    assert "'topic' = 'raw-coinbase-match'" in sql

    # New source topics are registered using the new shared Avro contract.
    assert "'topic' = 'raw-okx-trades'" in sql
    assert "'topic' = 'raw-bitget-trades'" in sql
    assert "normalized_market_trades" in sql
    assert "'format' = 'avro-confluent'" in sql


def test_multi_exchange_clickhouse_migration_has_four_result_tables():
    root = Path(__file__).resolve().parents[1]
    sql = (root / "clickhouse" / "init" / "002_multi_exchange_tables.sql").read_text()
    for table in (
        "vwap_aggregations_multi",
        "whale_alerts_multi",
        "arbitrage_signals_multi",
        "double_bottom_signals_multi",
        "recon_results_multi",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
