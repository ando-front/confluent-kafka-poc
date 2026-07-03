"""02 event sourcing — the event store.

Kafka is the source of truth. ``append`` writes an event to the log keyed by the
aggregate id (so all events for one order land in the same partition, preserving
order). ``get_events`` reads the whole log and filters by aggregate; and
``rebuild_state`` folds the events into the current state of the aggregate.

Run (demo):  python use_cases/02_event_sourcing/event_store.py
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

# Make `core` (project root) and sibling modules importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from confluent_kafka import Consumer, TopicPartition  # noqa: E402
from events import (  # noqa: E402
    TOPIC,
    Event,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    event_from_dict,
)

from core import BaseProducer, TopicAdmin, configure_logging, get_settings  # noqa: E402


class EventStore:
    """Append-only event store backed by a single Kafka topic."""

    def __init__(self, topic: str = TOPIC) -> None:
        self.topic = topic
        TopicAdmin().ensure_topic_exists(topic)
        self._producer = BaseProducer(default_topic=topic)

    def append(self, event: Event) -> None:
        """Publish an event, keyed by aggregate id to preserve per-order order."""
        self._producer.produce(value=event.to_dict(), key=event.aggregate_id)
        self._producer.flush(10)

    def get_events(self, aggregate_id: str | None = None, *, timeout: float = 5.0) -> list[Event]:
        """Read all events (optionally filtered by aggregate id), in log order."""
        settings = get_settings()
        consumer = Consumer(
            settings.consumer_config(
                group_id=f"eventstore-reader-{uuid.uuid4()}",
                **{"auto.offset.reset": "earliest"},
            )
        )
        events: list[Event] = []
        try:
            # Assign all partitions from the beginning for a full replay.
            md = consumer.list_topics(self.topic, timeout=timeout).topics[self.topic]
            partitions = [TopicPartition(self.topic, p, 0) for p in md.partitions]
            consumer.assign(partitions)

            idle = 0.0
            while idle < timeout:
                msg = consumer.poll(0.5)
                if msg is None:
                    idle += 0.5
                    continue
                if msg.error():
                    continue
                idle = 0.0
                data: dict[str, Any] = json.loads(msg.value().decode("utf-8"))
                if aggregate_id is None or data.get("aggregate_id") == aggregate_id:
                    events.append(event_from_dict(data))
        finally:
            consumer.close()
        return events

    def rebuild_state(self, aggregate_id: str) -> dict[str, Any]:
        """Fold the event history of one aggregate into its current state."""
        state: dict[str, Any] = {"order_id": aggregate_id, "status": "UNKNOWN", "paid": 0.0}
        for event in self.get_events(aggregate_id):
            if isinstance(event, OrderCreated):
                state.update(status="CREATED", customer=event.customer, amount=event.amount)
            elif isinstance(event, PaymentReceived):
                state["paid"] = state.get("paid", 0.0) + event.amount
                if state["paid"] >= state.get("amount", 0):
                    state["status"] = "PAID"
            elif isinstance(event, OrderShipped):
                state.update(status="SHIPPED", carrier=event.carrier, tracking=event.tracking)
            elif isinstance(event, OrderCancelled):
                state.update(status="CANCELLED", reason=event.reason)
        return state


def _demo() -> None:
    configure_logging()
    store = EventStore()
    order_id = str(uuid.uuid4())[:8]
    print(f"Emitting event history for order {order_id} ...")
    store.append(OrderCreated(aggregate_id=order_id, customer="alice", amount=5000.0))
    store.append(PaymentReceived(aggregate_id=order_id, amount=5000.0, method="card"))
    store.append(OrderShipped(aggregate_id=order_id, carrier="yamato", tracking="TRK123"))

    print("\nFull event history:")
    for e in store.get_events(order_id):
        print(f"  {e.timestamp}  {e.event_type}")

    print("\nRebuilt current state:")
    print(f"  {store.rebuild_state(order_id)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Event store demo / reader")
    parser.add_argument("--aggregate", help="print rebuilt state for this order id")
    args = parser.parse_args()
    configure_logging()
    if args.aggregate:
        print(EventStore().rebuild_state(args.aggregate))
    else:
        _demo()


if __name__ == "__main__":
    main()
