"""05 dead letter queue — consumer that routes failures to a DLQ.

Consumes ``dlq.orders.v1`` and validates each message. Anything that fails to
parse as JSON or is missing required fields is forwarded to ``dlq.orders.dlq.v1``
with error metadata in the headers, instead of blocking the main flow.

Because it validates raw bytes itself (not via the base consumer's auto-decode),
it subscribes with a raw poll loop.

Run:  python use_cases/05_dead_letter_queue/consumer.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError  # noqa: E402

from core import BaseProducer, TopicAdmin, configure_logging, get_settings  # noqa: E402

TOPIC = "dlq.orders.v1"
DLQ_TOPIC = "dlq.orders.dlq.v1"
REQUIRED_FIELDS = ("order_id", "customer", "amount")


def validate(raw: bytes) -> tuple[dict[str, Any] | None, str | None]:
    """Return (parsed, error). error is None when the message is valid."""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"invalid_json: {exc}"
    if not isinstance(data, dict):
        return None, "not_an_object"
    missing = [f for f in REQUIRED_FIELDS if f not in data]
    if missing:
        return None, f"missing_fields: {missing}"
    return data, None


def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ-routing consumer")
    parser.add_argument("--max", type=int, default=None, help="stop after N messages")
    args = parser.parse_args()

    configure_logging()
    admin = TopicAdmin()
    admin.ensure_topic_exists(TOPIC)
    admin.ensure_topic_exists(DLQ_TOPIC)

    settings = get_settings()
    consumer = Consumer(settings.consumer_config(group_id="dlq-main-consumer"))
    consumer.subscribe([TOPIC])
    dlq = BaseProducer(default_topic=DLQ_TOPIC)

    ok = routed = 0
    print(f"Consuming {TOPIC}; routing failures to {DLQ_TOPIC} (Ctrl-C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            data, error = validate(msg.value())
            if error is None:
                ok += 1
                print(f"[ok] key={key} amount={data['amount']}")
            else:
                routed += 1
                # Forward the ORIGINAL bytes plus error context to the DLQ.
                dlq.produce(
                    value=msg.value(),
                    key=key,
                    headers={
                        "error": error,
                        "source_topic": TOPIC,
                        "source_offset": str(msg.offset()),
                    },
                )
                dlq.poll(0)
                print(f"[DLQ] key={key} reason={error}")

            consumer.commit(msg, asynchronous=False)
            if args.max and (ok + routed) >= args.max:
                break
    except KeyboardInterrupt:
        pass
    finally:
        dlq.flush(10)
        consumer.close()
    print(f"Done. ok={ok} routed_to_dlq={routed}")


if __name__ == "__main__":
    main()
