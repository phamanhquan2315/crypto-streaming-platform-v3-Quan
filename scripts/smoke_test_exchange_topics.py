"""Integration smoke test for OKX/Bitget Kafka topics.

Start Docker infrastructure and producers first, then run:
    python scripts/smoke_test_exchange_topics.py --timeout 45

The script uses a fresh consumer group and reads retained messages from the
beginning, so it can validate messages produced shortly before the test starts.
"""

import argparse
import logging
import time
import uuid

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("exchange-smoke-test")

TOPICS = ("raw-okx-trades", "raw-bitget-trades")
EXPECTED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument(
        "--brokers", default="localhost:29092,localhost:29093,localhost:29094"
    )
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    args = parser.parse_args()

    registry = SchemaRegistryClient({"url": args.schema_registry})
    deserializer = AvroDeserializer(registry)

    consumer = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": f"exchange-smoke-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPICS))

    seen = {topic: set() for topic in TOPICS}
    deadline = time.time() + args.timeout

    try:
        while time.time() < deadline and any(not symbols for symbols in seen.values()):
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("Kafka consumer message error: %s", message.error())
                continue

            topic = message.topic()
            record = deserializer(
                message.value(),
                SerializationContext(topic, MessageField.VALUE),
            )
            if not record:
                continue

            # Contract checks: these are the exact fields consumed by Flink.
            required = {
                "exchange", "symbol", "trade_id", "price", "quantity",
                "side", "trade_time", "ingestion_time",
            }
            missing = required.difference(record.keys())
            if missing:
                raise AssertionError(f"{topic} missing fields: {sorted(missing)}")

            if record["exchange"] not in {"okx", "bitget"}:
                raise AssertionError(f"Unexpected exchange value: {record['exchange']}")
            if record["symbol"] not in EXPECTED_SYMBOLS:
                raise AssertionError(f"Unexpected symbol: {record['symbol']}")
            if record["side"] not in {"BUY", "SELL"}:
                raise AssertionError(f"Unexpected side: {record['side']}")

            seen[topic].add(record["symbol"])
            logger.info(
                "Validated %s | %s | %s | price=%s | qty=%s",
                topic, record["exchange"], record["symbol"],
                record["price"], record["quantity"],
            )
    finally:
        consumer.close()

    for topic, symbols in seen.items():
        missing = EXPECTED_SYMBOLS.difference(symbols)
        if missing:
            raise SystemExit(
                f"FAIL: {topic} did not produce symbols: {sorted(missing)}. "
                "Check producer logs and exchange market activity."
            )

    logger.info("PASS: OKX and Bitget topics contain all three symbols.")


if __name__ == "__main__":
    main()
