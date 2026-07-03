"""05 dead letter queue — DLQ processor / recovery.

Monitors ``dlq.orders.dlq.v1``. For each dead-lettered message it inspects the
error header and decides:

* **Recoverable** (e.g. missing 'amount' — we can default it): fix the payload
  and republish to the main topic ``dlq.orders.v1`` for reprocessing.
* **Unrecoverable** (e.g. invalid JSON / bad bytes): log it to a "poison"
  ledger for human inspection and move on.

Run:  python use_cases/05_dead_letter_queue/dlq_processor.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError  # noqa: E402

from core import BaseProducer, TopicAdmin, configure_logging, get_settings  # noqa: E402

MAIN_TOPIC = "dlq.orders.v1"
DLQ_TOPIC = "dlq.orders.dlq.v1"


def _headers_to_dict(headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    if not headers:
        return {}
    return {k: (v.decode("utf-8") if isinstance(v, bytes) else str(v)) for k, v in headers}


def try_recover(raw: bytes, error: str) -> dict | None:
    """Attempt to repair a dead-lettered message. Return fixed dict or None."""
    if error.startswith("missing_fields"):
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        # Fill sensible defaults for the fields the validator requires.
        data.setdefault("customer", "unknown")
        data.setdefault("amount", 0.0)
        data.setdefault("order_id", "recovered")
        return data
    # invalid_json / bad bytes -> cannot recover automatically.
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ processor / recovery")
    parser.add_argument("--max", type=int, default=None, help="stop after N DLQ messages")
    args = parser.parse_args()

    configure_logging()
    admin = TopicAdmin()
    admin.ensure_topic_exists(DLQ_TOPIC)
    admin.ensure_topic_exists(MAIN_TOPIC)

    settings = get_settings()
    consumer = Consumer(settings.consumer_config(group_id="dlq-processor"))
    consumer.subscribe([DLQ_TOPIC])
    republisher = BaseProducer(default_topic=MAIN_TOPIC)

    recovered = poison = 0
    print(f"Monitoring {DLQ_TOPIC} (Ctrl-C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue

            hdrs = _headers_to_dict(msg.headers())
            error = hdrs.get("error", "unknown")
            key = msg.key().decode("utf-8") if msg.key() else None
            fixed = try_recover(msg.value(), error)

            if fixed is not None:
                recovered += 1
                republisher.produce(value=fixed, key=key)
                republisher.poll(0)
                print(f"[recovered] key={key} error={error} -> republished")
            else:
                poison += 1
                # In production this would go to durable storage / alerting.
                print(f"[POISON] key={key} error={error} raw={msg.value()!r}")

            consumer.commit(msg, asynchronous=False)
            if args.max and (recovered + poison) >= args.max:
                break
    except KeyboardInterrupt:
        pass
    finally:
        republisher.flush(10)
        consumer.close()
    print(f"Done. recovered={recovered} poison={poison}")


if __name__ == "__main__":
    main()
