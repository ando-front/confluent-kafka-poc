"""01 basic pub/sub — consumer.

Subscribes to ``pubsub.orders.v1`` and prints each message to stdout.

Run:  python use_cases/01_basic_pubsub/consumer.py [--group NAME]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # project root for `core`

from confluent_kafka import Message  # noqa: E402

from core import BaseConsumer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "pubsub.orders.v1"


class PrintingConsumer(BaseConsumer):
    """Prints each received order in a readable one-liner."""

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        payload = value.get("payload", {}) if isinstance(value, dict) else value
        ts = value.get("timestamp") if isinstance(value, dict) else "?"
        print(
            f"<- key={key} ts={ts} "
            f"partition={message.partition()} offset={message.offset()} "
            f"payload={payload}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Basic pub/sub consumer")
    parser.add_argument("--group", default="pubsub-demo", help="consumer group id")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)

    consumer = PrintingConsumer(TOPIC, group_id=args.group)
    consumer.run()


if __name__ == "__main__":
    main()
