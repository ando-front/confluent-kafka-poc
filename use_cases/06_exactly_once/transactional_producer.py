"""06 exactly-once — transactional producer.

Uses Kafka transactions so a batch of messages is committed atomically: either
all messages in a transaction become visible to read-committed consumers, or
none do. A configurable failure demonstrates ``abort_transaction()``.

Key config:
    transactional.id      unique per logical producer (enables transactions)
    enable.idempotence    implied true when transactional.id is set

Run:
  # commit a batch of 5 messages atomically
  python use_cases/06_exactly_once/transactional_producer.py --batch 5

  # simulate a mid-transaction failure -> the whole batch is aborted
  python use_cases/06_exactly_once/transactional_producer.py --batch 5 --fail
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Producer  # noqa: E402

from core import TopicAdmin, configure_logging, get_settings  # noqa: E402

TOPIC = "eos.transfers.v1"


def build_producer(txn_id: str) -> Producer:
    settings = get_settings()
    config = settings.producer_config(
        **{
            "transactional.id": txn_id,
            "enable.idempotence": True,
        }
    )
    return Producer(config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Transactional (EOS) producer")
    parser.add_argument("--batch", type=int, default=5, help="messages per transaction")
    parser.add_argument("--fail", action="store_true", help="abort the transaction mid-way")
    parser.add_argument(
        "--txn-id",
        default="eos-poc-txn-1",
        help="transactional.id (stable across restarts for fencing)",
    )
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)

    producer = build_producer(args.txn_id)
    producer.init_transactions()
    print(f"Initialized transactions (txn.id={args.txn_id})")

    try:
        producer.begin_transaction()
        print("BEGIN transaction")
        for i in range(args.batch):
            record = {"transfer_id": str(uuid.uuid4())[:8], "seq": i, "amount": 100 * (i + 1)}
            producer.produce(TOPIC, value=json.dumps(record).encode("utf-8"), key=str(i))
            print(f"  produced seq={i} amount={record['amount']}")
            if args.fail and i == args.batch // 2:
                raise RuntimeError("Simulated failure mid-transaction")

        producer.commit_transaction()
        print(f"COMMIT transaction — {args.batch} message(s) are now visible")
    except Exception as exc:  # noqa: BLE001
        print(f"ABORT transaction — {exc}")
        producer.abort_transaction()
        print("Aborted: read-committed consumers will see NONE of these messages")


if __name__ == "__main__":
    main()
