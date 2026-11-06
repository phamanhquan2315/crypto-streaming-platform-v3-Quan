"""Offline unit tests for the new OKX and Bitget adapters."""

from decimal import Decimal

import pytest

pytest.importorskip("confluent_kafka", reason="install producers/requirements.txt first")
pytest.importorskip("websockets", reason="install producers/requirements.txt first")

from okx_producer import OKXProducer
from bitget_producer import BitgetProducer


def test_okx_trade_parser_normalizes_symbol_and_numbers():
    raw = {
        "instId": "BTC-USDT",
        "tradeId": "12345678901234567890",
        "px": "67123.45000000",
        "sz": "0.01250000",
        "side": "buy",
        "ts": "1793952000123",
    }
    trade = OKXProducer.parse_trade(raw)

    assert trade.exchange == "okx"
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == "12345678901234567890"
    assert trade.price == Decimal("67123.45000000")
    assert trade.quantity == Decimal("0.01250000")
    assert trade.side == "BUY"
    assert trade.trade_time == 1793952000123
    assert trade.ingestion_time > 0


def test_bitget_trade_parser_maps_size_to_quantity():
    raw = {
        "tradeId": "987654321",
        "price": "3500.12000000",
        "size": "0.25000000",
        "side": "sell",
        "ts": "1793952000456",
    }
    trade = BitgetProducer.parse_trade(raw, "ETHUSDT")

    assert trade.exchange == "bitget"
    assert trade.symbol == "ETHUSDT"
    assert trade.trade_id == "987654321"
    assert trade.price == Decimal("3500.12000000")
    assert trade.quantity == Decimal("0.25000000")
    assert trade.side == "SELL"
    assert trade.trade_time == 1793952000456


def test_okx_symbol_normalization_for_all_supported_assets():
    assert OKXProducer.normalize_symbol("BTC-USDT") == "BTCUSDT"
    assert OKXProducer.normalize_symbol("ETH-USDT") == "ETHUSDT"
    assert OKXProducer.normalize_symbol("SOL-USDT") == "SOLUSDT"
