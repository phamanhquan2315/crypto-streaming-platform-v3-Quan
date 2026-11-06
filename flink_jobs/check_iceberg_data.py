"""Count all-exchange Iceberg trade rows in the last two-hour reconciliation window."""

import logging
import sys
from datetime import datetime, timedelta, timezone

from pyflink.table import EnvironmentSettings, TableEnvironment
from shared.config import NESSIE_CATALOG_PROPERTIES

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    result = t_env.execute_sql(f"""
        SELECT COUNT(*)
        FROM nessie_catalog.crypto.market_trades_raw
        WHERE trade_time >= {start_ms}
          AND trade_time < {end_ms}
    """)
    with result.collect() as rows:
        count = next(iter(rows))[0]

    logger.info("Four-exchange Iceberg rows in reconciliation window: %s", count)
    # Airflow's existing operator reads the final stdout line as an integer.
    print(count)


if __name__ == "__main__":
    main()
