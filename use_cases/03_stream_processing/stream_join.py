"""03 stream processing — stream-stream join.

Joins two streams — ``stream.order_events.v1`` and ``stream.payment_events.v1`` —
on ``order_id`` within a 5-minute window. When both an order and its matching
payment are seen inside the window, the joined record is emitted to
``stream.order_payment_joined.v1``.

This is an in-memory windowed join (state is a dict keyed by order_id with an
expiry). It mirrors what Kafka Streams / ksqlDB would do with a windowed join.

Run:
  # 1) generate correlated order + payment events
  python use_cases/03_stream_processing/stream_join.py --produce 50

  # 2) run the join
  python use_cases/03_stream_processing/stream_join.py
"""

from __future__ import annotations

import argparse
import random
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, Message  # noqa: E402

from core import BaseProducer, TopicAdmin, configure_logging, get_settings  # noqa: E402

ORDER_TOPIC = "stream.order_events.v1"
PAYMENT_TOPIC = "stream.payment_events.v1"
OUT_TOPIC = "stream.order_payment_joined.v1"
JOIN_WINDOW_S = 300  # 5 minutes


@dataclass
class Pending:
    payload: dict[str, Any]
    seen_at: float


class WindowedJoiner:
    """In-memory 5-minute windowed join of orders and payments by order_id."""

    def __init__(self) -> None:
        settings = get_settings()
        self._consumer = Consumer(
            settings.consumer_config(group_id="stream-joiner", **{"auto.offset.reset": "earliest"})
        )
        self._producer = BaseProducer(default_topic=OUT_TOPIC)
        self.orders: dict[str, Pending] = {}
        self.payments: dict[str, Pending] = {}
        self.joined = 0

    def _try_join(self, order_id: str) -> None:
        o = self.orders.get(order_id)
        p = self.payments.get(order_id)
        if not (o and p):
            return
        if abs(o.seen_at - p.seen_at) > JOIN_WINDOW_S:
            return  # outside the join window
        record = {
            "order_id": order_id,
            "order": o.payload,
            "payment": p.payload,
            "joined_at": time.time(),
        }
        self._producer.produce(value=record, key=order_id)
        self._producer.poll(0)
        self.joined += 1
        print(f"[joined] order_id={order_id} amount={p.payload.get('amount')} total={self.joined}")
        # matched — remove from both sides
        self.orders.pop(order_id, None)
        self.payments.pop(order_id, None)

    def _expire(self) -> None:
        now = time.time()
        for store in (self.orders, self.payments):
            for oid in [k for k, v in store.items() if now - v.seen_at > JOIN_WINDOW_S]:
                store.pop(oid, None)

    def run(self) -> None:
        self._consumer.subscribe([ORDER_TOPIC, PAYMENT_TOPIC])
        print(f"Joining {ORDER_TOPIC} + {PAYMENT_TOPIC} on order_id (window={JOIN_WINDOW_S}s)")
        try:
            while True:
                msg: Message | None = self._consumer.poll(1.0)
                if msg is None:
                    self._expire()
                    continue
                if msg.error():
                    continue
                import json

                value = json.loads(msg.value().decode("utf-8"))
                order_id = value.get("order_id")
                if not order_id:
                    continue
                now = time.time()
                if msg.topic() == ORDER_TOPIC:
                    self.orders[order_id] = Pending(value, now)
                else:
                    self.payments[order_id] = Pending(value, now)
                self._try_join(order_id)
                self._consumer.commit(msg, asynchronous=False)
        except KeyboardInterrupt:
            print(f"\nStopped. joined={self.joined}")
        finally:
            self._consumer.close()


def produce_sample(n: int) -> None:
    """Emit n orders and their (slightly delayed) payments."""
    admin = TopicAdmin()
    admin.ensure_topic_exists(ORDER_TOPIC)
    admin.ensure_topic_exists(PAYMENT_TOPIC)
    with BaseProducer() as p:
        for _ in range(n):
            oid = str(uuid.uuid4())[:8]
            amount = round(random.uniform(1000, 50000), 2)
            p.produce(ORDER_TOPIC, value={"order_id": oid, "amount": amount}, key=oid)
            # 10% of orders never get paid (won't join)
            if random.random() > 0.1:
                p.produce(
                    PAYMENT_TOPIC,
                    value={"order_id": oid, "amount": amount, "method": "card"},
                    key=oid,
                )
        print(f"Produced {n} orders (+~90% payments) to {ORDER_TOPIC}/{PAYMENT_TOPIC}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream-stream windowed join")
    parser.add_argument("--produce", type=int, metavar="N", help="generate N order/payment pairs")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(OUT_TOPIC)
    if args.produce:
        produce_sample(args.produce)
        return
    WindowedJoiner().run()


if __name__ == "__main__":
    main()
