"""Contract tests for the shared OKX/Bitget Avro record."""

import json
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("fastavro", reason="install producers/requirements.txt first")

from fastavro import parse_schema
from fastavro.validation import validate


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "producers" / "schemas" / "market_trade.avsc"


def test_market_trade_avro_schema_accepts_normalized_record():
    schema = parse_schema(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))
    record = {
        "exchange": "okx",
        "symbol": "BTCUSDT",
        "trade_id": "123456",
        "price": Decimal("65000.12345678"),
        "quantity": Decimal("0.00123456"),
        "side": "BUY",
        "trade_time": 1793952000000,
        "ingestion_time": 1793952000005,
    }
    assert validate(record, schema)


def test_market_trade_schema_has_fields_required_by_flink_union():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    names = {field["name"] for field in schema["fields"]}
    assert names == {
        "exchange", "symbol", "trade_id", "price", "quantity",
        "side", "trade_time", "ingestion_time",
    }
