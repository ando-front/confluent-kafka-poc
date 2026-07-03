"""06 exactly-once — idempotent consumer.

Reads only committed messages (``isolation.level=read_committed``) from
``eos.transfers.v1`` and processes each transfer id exactly once, even if the
same message is redelivered. Processed ids are persisted to SQLite, so a dedup
survives restarts — giving effectively exactly-once *processing* on top of
at-least-once delivery.

Run:  python use_cases/06_exactly_once/idempotent_consumer.py [--db processed.db]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Message  # noqa: E402

from core import BaseConsumer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "eos.transfers.v1"
DEFAULT_DB = str(Path(__file__).resolve().parent / "processed.db")


class ProcessedLedger:
    """SQLite-backed set of already-processed ids (survives restarts)."""

    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS processed (id TEXT PRIMARY KEY, amount REAL)"
        )
        self.conn.commit()

    def seen(self, transfer_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM processed WHERE id = ?", (transfer_id,))
        return cur.fetchone() is not None

    def mark(self, transfer_id: str, amount: float) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO processed (id, amount) VALUES (?, ?)", (transfer_id, amount)
        )
        self.conn.commit()

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM processed").fetchone()[0]


class IdempotentConsumer(BaseConsumer):
    """Deduplicates on transfer_id so each transfer is applied once."""

    def __init__(self, db_path: str, **kwargs: Any) -> None:
        # read_committed hides messages from aborted transactions.
        super().__init__(
            TOPIC,
            group_id="eos-idempotent-consumer",
            config_overrides={"isolation.level": "read_committed"},
            **kwargs,
        )
        self.ledger = ProcessedLedger(db_path)
        self.processed_unique = 0
        self.duplicates = 0

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        if not isinstance(value, dict):
            return
        transfer_id = value.get("transfer_id")
        if not transfer_id:
            return
        if self.ledger.seen(transfer_id):
            self.duplicates += 1
            print(f"[dup ignored] transfer_id={transfer_id}")
            return
        # ---- business logic would go here (apply the transfer) ----
        self.ledger.mark(transfer_id, float(value.get("amount", 0)))
        self.processed_unique += 1
        print(f"[applied] transfer_id={transfer_id} amount={value.get('amount')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Idempotent (EOS) consumer")
    parser.add_argument("--db", default=DEFAULT_DB, help="dedup ledger SQLite path")
    parser.add_argument("--max", type=int, default=None, help="stop after N messages")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)
    consumer = IdempotentConsumer(args.db)
    consumer.run(max_messages=args.max)
    print(
        f"Done. unique={consumer.processed_unique} duplicates_ignored={consumer.duplicates} "
        f"ledger_total={consumer.ledger.count()}"
    )


if __name__ == "__main__":
    main()
