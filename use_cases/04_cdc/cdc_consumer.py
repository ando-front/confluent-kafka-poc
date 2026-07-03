"""04 CDC — change event consumer / replicator.

Consumes Debezium-style change events from ``cdc.orders.v1`` and applies them to
a *separate* SQLite database, keeping it a live replica of the source table.
Demonstrates database replication driven entirely by the Kafka change log.

Run:
  # In one terminal, run the replicator:
  python use_cases/04_cdc/cdc_consumer.py [--db replica.db]
  # In another, generate changes:
  python use_cases/04_cdc/simulator.py --changes 30
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

TOPIC = "cdc.orders.v1"
DEFAULT_DB = str(Path(__file__).resolve().parent / "replica.db")


def init_replica(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer TEXT,
            status TEXT,
            amount REAL
        )
        """
    )
    conn.commit()
    return conn


class CDCReplicator(BaseConsumer):
    """Applies create/update/delete change events to a replica table."""

    def __init__(self, db_path: str, **kwargs: Any) -> None:
        super().__init__(TOPIC, group_id="cdc-replicator", **kwargs)
        self.conn = init_replica(db_path)
        self.applied = 0

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        if not isinstance(value, dict):
            return
        op = value.get("op")
        after = value.get("after")
        before = value.get("before")

        if op == "c" and after:
            self.conn.execute(
                "INSERT OR REPLACE INTO orders (id, customer, status, amount) VALUES (?,?,?,?)",
                (after["id"], after["customer"], after["status"], after["amount"]),
            )
        elif op == "u" and after:
            self.conn.execute(
                "INSERT OR REPLACE INTO orders (id, customer, status, amount) VALUES (?,?,?,?)",
                (after["id"], after["customer"], after["status"], after["amount"]),
            )
        elif op == "d" and before:
            self.conn.execute("DELETE FROM orders WHERE id = ?", (before["id"],))
        else:
            return

        self.conn.commit()
        self.applied += 1
        count = self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        print(f"[apply {op}] key={key} -> replica now has {count} row(s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="CDC replicator")
    parser.add_argument("--db", default=DEFAULT_DB, help="replica SQLite db path")
    parser.add_argument("--max", type=int, default=None, help="stop after N events")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)
    replicator = CDCReplicator(args.db)
    replicator.run(max_messages=args.max)
    print(f"Applied {replicator.applied} change(s) to {args.db}")


if __name__ == "__main__":
    main()
