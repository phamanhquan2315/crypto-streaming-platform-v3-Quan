"""Launch the four exchange WebSocket producers concurrently."""

import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from binance_producer import BinanceProducer
from coinbase_producer import CoinbaseProducer
from okx_producer import OKXProducer
from bitget_producer import BitgetProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Load configuration from the repository root regardless of the shell's cwd.
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092,localhost:29093,localhost:29094",
)
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


async def main() -> None:
    """Run Binance, Coinbase, OKX and Bitget without blocking each other."""
    producers = [
        BinanceProducer(KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL),
        CoinbaseProducer(KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL),
        OKXProducer(KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL),
        BitgetProducer(KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL),
    ]

    tasks = [
        asyncio.create_task(producer.start(), name=producer.__class__.__name__)
        for producer in producers
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Producer tasks cancelled.")
        raise
    except Exception:
        logger.exception("An exchange producer stopped unexpectedly.")
        raise
    finally:
        # Stop sibling producers and flush every Kafka buffer on shutdown.
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        for producer in producers:
            producer.flush()
        logger.info("All four producers stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
