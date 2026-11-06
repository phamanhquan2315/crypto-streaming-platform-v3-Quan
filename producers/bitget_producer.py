"""Bitget Classic V2 Spot public-trades WebSocket -> Kafka (Avro)."""

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import websockets

from base_producer import BaseProducer

BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"
BITGET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")


@dataclass
class BitgetTrade:
    """Canonical trade model stored in raw-bitget-trades."""
    exchange: str
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: str
    trade_time: int
    ingestion_time: int


class BitgetProducer(BaseProducer):
    """Subscribe to Bitget's public Spot trade channel."""

    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        schema_path = Path(__file__).resolve().parent / "schemas" / "market_trade.avsc"
        schema_str = schema_path.read_text(encoding="utf-8")
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            topic="raw-bitget-trades",
            schema_str=schema_str,
        )

    def to_dict(self, trade: BitgetTrade, ctx) -> dict:
        """Convert a BitgetTrade to the Avro record expected by Schema Registry."""
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
    def parse_trade(raw: dict, symbol: str) -> BitgetTrade:
        """Map Bitget's tradeId/price/size/side/ts into canonical field names."""
        return BitgetTrade(
            exchange="bitget",
            symbol=symbol.upper(),
            trade_id=str(raw["tradeId"]),
            price=Decimal(raw["price"]),
            quantity=Decimal(raw["size"]),
            side=raw["side"].upper(),
            trade_time=int(raw["ts"]),
            ingestion_time=int(time.time() * 1000),
        )

    async def handle_message(self, raw_message: str) -> None:
        """Handle Bitget control frames and trade messages."""
        if raw_message == "pong":
            return

        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            self.logger.warning("Ignoring invalid Bitget JSON frame: %s", raw_message)
            return

        if message.get("event"):
            if message.get("event") == "error":
                self.logger.error("Bitget subscription error: %s", message)
            else:
                self.logger.info("Bitget WebSocket event: %s", message)
            return

        arg = message.get("arg", {})
        if arg.get("channel") != "trade":
            return

        symbol = arg.get("instId", "").upper()
        for raw_trade in message.get("data", []):
            try:
                trade = self.parse_trade(raw_trade, symbol)
                self.send(key=trade.symbol, value=trade)
            except (KeyError, ValueError, ArithmeticError, TypeError):
                self.logger.exception("Invalid Bitget trade payload: %s", raw_trade)

    async def start(self) -> None:
        """Connect and resubscribe after disconnect, with a Bitget text heartbeat."""
        asyncio.create_task(self._poll_loop())

        subscribe = {
            "op": "subscribe",
            "args": [
                {"instType": "SPOT", "channel": "trade", "instId": symbol}
                for symbol in BITGET_SYMBOLS
            ],
        }

        delay = 1
        while True:
            try:
                async with websockets.connect(
                    BITGET_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as websocket:
                    await websocket.send(json.dumps(subscribe))
                    self.logger.info("Bitget subscribed: %s", BITGET_SYMBOLS)
                    delay = 1

                    while True:
                        try:
                            frame = await asyncio.wait_for(websocket.recv(), timeout=25)
                            await self.handle_message(frame)
                        except asyncio.TimeoutError:
                            # Bitget requires application-level text ping frames.
                            await websocket.send("ping")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Bitget WebSocket disconnected; retry in %ss", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
