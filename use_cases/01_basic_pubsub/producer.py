"""01 basic pub/sub — producer.

Publishes a JSON message every second to ``pubsub.orders.v1``. Each message
carries a timestamp, a monotonic id and a small payload.

Run:  python use_cases/01_basic_pubsub/producer.py [--count N] [--interval S]
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # project root for `core`

from core import BaseProducer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "pubsub.orders.v1"


def build_message(seq: int) -> dict[str, object]:
    """Construct one order message."""
    return {
        "id": seq,
        "order_id": str(uuid.uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": {
            "product": f"SKU-{seq % 10:03d}",
            "quantity": (seq % 5) + 1,
            "amount": round(1000 + (seq % 7) * 1234.5, 2),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Basic pub/sub producer")
    parser.add_argument("--count", type=int, default=0, help="messages to send (0 = infinite)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between messages")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)

    sent = 0
    producer = BaseProducer(default_topic=TOPIC)
    with producer:
        try:
            while args.count == 0 or sent < args.count:
                msg = build_message(sent)
                producer.produce(value=msg, key=str(msg["id"]))
                print(f"-> sent id={msg['id']} order_id={msg['order_id']}")
                sent += 1
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nInterrupted — flushing...")
    print(f"Done. sent={sent} metrics={producer.metrics.as_dict()}")


if __name__ == "__main__":
    main()
