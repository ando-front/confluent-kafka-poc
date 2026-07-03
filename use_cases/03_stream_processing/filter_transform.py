"""03 stream processing — filter & transform pipeline.

Reads ``stream.transactions.v1`` and forwards only high-value transactions
(amount > 10000 JPY) to ``stream.high_value.v1``, enriching each with a
``tier`` field. Demonstrates the classic filter + map streaming pattern.

It can also generate sample input so the whole use case is runnable standalone.

Run:
  # 1) generate 200 random transactions into the input topic
  python use_cases/03_stream_processing/filter_transform.py --produce 200

  # 2) run the filter/transform pipeline
  python use_cases/03_stream_processing/filter_transform.py
"""

from __future__ import annotations

import argparse
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Message  # noqa: E402

from core import BaseConsumer, BaseProducer, TopicAdmin, configure_logging  # noqa: E402

IN_TOPIC = "stream.transactions.v1"
OUT_TOPIC = "stream.high_value.v1"
THRESHOLD = 10000.0


def _tier(amount: float) -> str:
    if amount > 100000:
        return "platinum"
    if amount > 50000:
        return "gold"
    return "silver"


class FilterTransform(BaseConsumer):
    """Forward only transactions above THRESHOLD, adding a tier label."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(IN_TOPIC, group_id="stream-filter", **kwargs)
        self._producer = BaseProducer(default_topic=OUT_TOPIC)
        self.forwarded = 0

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        if not isinstance(value, dict):
            return
        amount = float(value.get("amount", 0))
        if amount <= THRESHOLD:
            return
        enriched = {**value, "tier": _tier(amount), "filtered_at": time.time()}
        self._producer.produce(value=enriched, key=key)
        self._producer.poll(0)
        self.forwarded += 1
        print(f"-> forwarded amount={amount} tier={enriched['tier']} (total={self.forwarded})")


def produce_sample(n: int) -> None:
    """Generate n random transactions into the input topic."""
    TopicAdmin().ensure_topic_exists(IN_TOPIC)
    with BaseProducer(default_topic=IN_TOPIC) as p:
        for _ in range(n):
            txn = {
                "txn_id": str(uuid.uuid4()),
                "amount": round(random.choice([500, 3000, 8000, 15000, 60000, 120000])
                                * random.uniform(0.8, 1.2), 2),
                "event_epoch": time.time(),
            }
            p.produce(value=txn, key=txn["txn_id"])
        print(f"Produced {n} sample transactions to {IN_TOPIC}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter & transform pipeline")
    parser.add_argument("--produce", type=int, metavar="N", help="generate N sample txns and exit")
    args = parser.parse_args()

    configure_logging()
    if args.produce:
        produce_sample(args.produce)
        return

    admin = TopicAdmin()
    admin.ensure_topic_exists(IN_TOPIC)
    admin.ensure_topic_exists(OUT_TOPIC)
    FilterTransform().run(poll_timeout=1.0)


if __name__ == "__main__":
    main()
