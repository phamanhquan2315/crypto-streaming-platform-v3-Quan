"""Create raw-trade topics for all exchanges and the shared dead-letter topic."""

import logging
import os

from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092,localhost:29093,localhost:29094",
)
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))
MIN_ISR = "2"


def create_topics(admin_client: AdminClient, topics: list[NewTopic]) -> None:
    """Create topics; an already-existing topic is an expected idempotent state."""
    futures = admin_client.create_topics(topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Created Kafka topic: %s", topic_name)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.info("Kafka topic already exists: %s", topic_name)
            else:
                logger.exception("Could not create topic %s", topic_name)
                raise


def raw_topic(name: str, retention_ms: str) -> NewTopic:
    """Use the same three-partition layout as the original Binance topic."""
    return NewTopic(
        topic=name,
        num_partitions=3,
        replication_factor=REPLICATION_FACTOR,
        config={
            "min.insync.replicas": MIN_ISR,
            "retention.ms": retention_ms,
            "compression.type": "lz4",
            "cleanup.policy": "delete",
        },
    )


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topics = [
        raw_topic("raw-binance-trades", "86400000"),
        raw_topic("raw-coinbase-match", "21600000"),
        raw_topic("raw-okx-trades", "21600000"),
        raw_topic("raw-bitget-trades", "21600000"),
        NewTopic(
            topic="dlq-events",
            num_partitions=1,
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "604800000",
                "compression.type": "zstd",
                "cleanup.policy": "delete",
            },
        ),
    ]
    create_topics(admin, topics)
    logger.info("All exchange topics are ready.")


if __name__ == "__main__":
    main()
