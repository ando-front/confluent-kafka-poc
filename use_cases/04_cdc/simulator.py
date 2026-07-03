"""04 CDC — change data capture simulator.

Simulates a source database: it applies INSERT / UPDATE / DELETE operations to a
local SQLite ``orders`` table and, for each change, emits a Debezium-style
change event to ``cdc.orders.v1``.

Debezium envelope (simplified):
    {
      "op": "c" | "u" | "d",          # create / update / delete
      "before": {...} | null,
      "after":  {...} | null,
      "source": {"table": "orders", "ts_ms": ...},
      "ts_ms": ...
    }

Run:
  python use_cases/04_cdc/simulator.py [--changes 20] [--db source.db]
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core import BaseProducer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "cdc.orders.v1"
DEFAULT_DB = str(Path(__file__).resolve().parent / "source.db")


def _now_ms() -> int:
    return int(time.time() * 1000)


def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer TEXT NOT NULL,
            status TEXT NOT NULL,
            amount REAL NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _envelope(op: str, before: dict | None, after: dict | None) -> dict[str, Any]:
    return {
        "op": op,
        "before": before,
        "after": after,
        "source": {"table": "orders", "ts_ms": _now_ms()},
        "ts_ms": _now_ms(),
    }


class ChangeEmitter:
    """Apply a change to SQLite and emit the corresponding CDC event."""

    def __init__(self, conn: sqlite3.Connection, producer: BaseProducer) -> None:
        self.conn = conn
        self.producer = producer

    def _read(self, order_id: int) -> dict | None:
        cur = self.conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        return _row_to_dict(cur.fetchone())

    def _emit(self, order_id: int, envelope: dict) -> None:
        self.producer.produce(value=envelope, key=str(order_id))
        self.producer.poll(0)
        print(f"[{envelope['op']}] id={order_id} after={envelope['after']}")

    def insert(self, order_id: int) -> None:
        after = {
            "id": order_id,
            "customer": random.choice(["alice", "bob", "carol", "dave"]),
            "status": "NEW",
            "amount": round(random.uniform(1000, 50000), 2),
        }
        self.conn.execute(
            "INSERT INTO orders (id, customer, status, amount) VALUES (?, ?, ?, ?)",
            (after["id"], after["customer"], after["status"], after["amount"]),
        )
        self.conn.commit()
        self._emit(order_id, _envelope("c", None, after))

    def update(self, order_id: int) -> None:
        before = self._read(order_id)
        if before is None:
            return
        new_status = random.choice(["PAID", "SHIPPED", "CANCELLED"])
        self.conn.execute("UPDATE orders SET status = ? WHERE id = ?", (new_status, order_id))
        self.conn.commit()
        self._emit(order_id, _envelope("u", before, self._read(order_id)))

    def delete(self, order_id: int) -> None:
        before = self._read(order_id)
        if before is None:
            return
        self.conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        self.conn.commit()
        self._emit(order_id, _envelope("d", before, None))


def main() -> None:
    parser = argparse.ArgumentParser(description="CDC simulator")
    parser.add_argument("--changes", type=int, default=20, help="number of changes to emit")
    parser.add_argument("--db", default=DEFAULT_DB, help="source SQLite db path")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)
    conn = init_db(args.db)

    existing: list[int] = [r["id"] for r in conn.execute("SELECT id FROM orders")]
    next_id = (max(existing) + 1) if existing else 1

    with BaseProducer(default_topic=TOPIC) as producer:
        emitter = ChangeEmitter(conn, producer)
        for _ in range(args.changes):
            # Bias towards inserts early, updates/deletes once rows exist.
            if not existing or random.random() < 0.5:
                emitter.insert(next_id)
                existing.append(next_id)
                next_id += 1
            elif random.random() < 0.8:
                emitter.update(random.choice(existing))
            else:
                victim = random.choice(existing)
                emitter.delete(victim)
                existing.remove(victim)
            time.sleep(0.1)
    conn.close()
    print(f"Emitted {args.changes} change events to {TOPIC}")


if __name__ == "__main__":
    main()
