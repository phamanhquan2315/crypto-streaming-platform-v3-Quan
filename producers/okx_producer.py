"""OKX Spot public-trades WebSocket -> Kafka (Avro).

This producer intentionally follows the existing BaseProducer contract:
- one Kafka topic per exchange;
- Avro serialization through the existing Schema Registry;
- symbol-keyed Kafka partitioning;
- centralized delivery callback and DLQ handling.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import websockets

from base_producer import BaseProducer

OKX_WS_URL = "wss://ws.okx.com:443/ws/v5/public"
OKX_INSTRUMENTS = ("BTC-USDT", "ETH-USDT", "SOL-USDT")


@dataclass
class OKXTrade:
    """Canonical trade model stored in raw-okx-trades."""
    exchange: str
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: str
    trade_time: int
    ingestion_time: int


class OKXProducer(BaseProducer):
    """Subscribe to OKX public Spot trades and publish normalized Avro events."""

    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        # Resolve schema relative to this source file, not the current directory.
        schema_path = Path(__file__).resolve().parent / "schemas" / "market_trade.avsc"
        schema_str = schema_path.read_text(encoding="utf-8")
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            topic="raw-okx-trades",
            schema_str=schema_str,
        )

    def to_dict(self, trade: OKXTrade, ctx) -> dict:
        """AvroSerializer callback: convert the dataclass to schema fields."""
        return {
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "trade_id": trade.trade_id,
            "price": trade.price,
            "quantity": trade.quantity,
            "side": trade.side,
            "trade_time": trade.trade_time,
            "ingestion_time": trade.ingestion_time,
        }

    @staticmethod
    def normalize_symbol(inst_id: str) -> str:
        """Convert OKX's BTC-USDT notation to the platform's BTCUSDT notation."""
        return inst_id.replace("-", "").upper()

    @classmethod
    def parse_trade(cls, raw: dict) -> OKXTrade:
        """Parse one item from the OKX `trades` channel data array."""
        return OKXTrade(
            exchange="okx",
            symbol=cls.normalize_symbol(raw["instId"]),
            trade_id=str(raw["tradeId"]),
            price=Decimal(raw["px"]),
            quantity=Decimal(raw["sz"]),
            side=raw["side"].upper(),
            trade_time=int(raw["ts"]),
            ingestion_time=int(time.time() * 1000),
        )

    async def handle_message(self, raw_message: str) -> None:
        """Parse control frames and publish every trade in an OKX message."""
        if raw_message == "pong":
            return

        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            self.logger.warning("Ignoring invalid OKX JSON frame: %s", raw_message)
            return

        # Subscription acknowledgements and errors are control messages.
        if "event" in message:
            if message.get("event") == "error":
                self.logger.error("OKX subscription error: %s", message)
            else:
                self.logger.info("OKX WebSocket event: %s", message)
            return

        if message.get("arg", {}).get("channel") != "trades":
            return

        for raw_trade in message.get("data", []):
            try:
                trade = self.parse_trade(raw_trade)
                # BaseProducer routes BTC/ETH/SOL to the existing 3 partitions.
                self.send(key=trade.symbol, value=trade)
            except (KeyError, ValueError, ArithmeticError, TypeError) as exc:
                self.logger.exception("Invalid OKX trade %s: %s", raw_trade, exc)

    async def start(self) -> None:
        """Connect, subscribe, heartbeat and reconnect with bounded backoff."""
        asyncio.create_task(self._poll_loop())

        subscribe = {
            # "id": "crypto-platform-okx-trades",
            "op": "subscribe",
            "args": [
                {"channel": "trades", "instId": instrument}
                for instrument in OKX_INSTRUMENTS
            ],
        }

        delay = 1
        while True:
            try:
                async with websockets.connect(
                    OKX_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as websocket:
                    await websocket.send(json.dumps(subscribe))
                    self.logger.info("OKX subscribed: %s", OKX_INSTRUMENTS)
                    delay = 1

                    while True:
                        try:
                            frame = await asyncio.wait_for(websocket.recv(), timeout=25)
                            await self.handle_message(frame)
                        except asyncio.TimeoutError:
                            # OKX documents application-level text ping/pong.
                            await websocket.send("ping")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("OKX WebSocket disconnected; retry in %ss", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
