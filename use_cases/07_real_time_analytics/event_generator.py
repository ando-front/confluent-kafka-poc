"""07 real-time analytics — event generator.

Generates a realistic-ish stream of e-commerce events (page_view, add_to_cart,
purchase) into ``analytics.events.v1``. Purchases carry an amount so the
downstream aggregator can compute GMV and conversion rate.

Run:  python use_cases/07_real_time_analytics/event_generator.py [--rate 20]
"""

from __future__ import annotations

import argparse
import random
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core import BaseProducer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "analytics.events.v1"

# Weighted funnel: most events are views, fewer carts, fewer purchases.
EVENT_WEIGHTS = [("page_view", 0.70), ("add_to_cart", 0.22), ("purchase", 0.08)]
PRODUCTS = [f"SKU-{i:03d}" for i in range(20)]


def _pick_event() -> str:
    r = random.random()
    cumulative = 0.0
    for name, weight in EVENT_WEIGHTS:
        cumulative += weight
        if r <= cumulative:
            return name
    return "page_view"


def build_event() -> dict[str, object]:
    etype = _pick_event()
    event: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "type": etype,
        "user_id": f"user-{random.randint(1, 500)}",
        "product": random.choice(PRODUCTS),
        "ts": datetime.now(UTC).isoformat(),
        "event_epoch": time.time(),
    }
    if etype == "purchase":
        event["amount"] = round(random.uniform(500, 30000), 2)
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time analytics event generator")
    parser.add_argument("--rate", type=float, default=20.0, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="total events (0 = infinite)")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    with BaseProducer(default_topic=TOPIC) as producer:
        try:
            while args.count == 0 or sent < args.count:
                event = build_event()
                producer.produce(value=event, key=str(event["user_id"]))
                sent += 1
                if sent % 50 == 0:
                    print(f"generated {sent} events...")
                if interval:
                    time.sleep(interval)
        except KeyboardInterrupt:
            print("\nInterrupted")
    print(f"Done. generated {sent} events")


if __name__ == "__main__":
    main()
