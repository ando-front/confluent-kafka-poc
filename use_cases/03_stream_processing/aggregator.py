"""03 stream processing — tumbling-window aggregation.

Consumes ``stream.transactions.v1`` and aggregates the total sales amount in
fixed 30-second tumbling windows. When a window closes, its total is printed and
published to ``stream.sales_30s.v1``.

Windows are keyed by ``floor(event_time / 30s)`` so late-ish events still land in
the right bucket (event-time semantics, not wall-clock).

Run:
  # generate input first (see filter_transform.py --produce or a manual producer)
  python use_cases/03_stream_processing/aggregator.py [--window 30]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Message  # noqa: E402

from core import BaseConsumer, BaseProducer, TopicAdmin, configure_logging  # noqa: E402

IN_TOPIC = "stream.transactions.v1"
OUT_TOPIC = "stream.sales_30s.v1"


class WindowedAggregator(BaseConsumer):
    """Tumbling-window sum of transaction amounts."""

    def __init__(self, window_s: int, **kwargs: Any) -> None:
        super().__init__(IN_TOPIC, group_id="stream-aggregator", **kwargs)
        self.window_s = window_s
        self.windows: dict[int, dict[str, float]] = {}
        self._producer = BaseProducer(default_topic=OUT_TOPIC)
        self._last_flush = time.monotonic()

    def _window_key(self, epoch_s: float) -> int:
        return int(epoch_s // self.window_s) * self.window_s

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        if not isinstance(value, dict):
            return
        amount = float(value.get("amount", 0))
        # Prefer event time (ms in message); fall back to broker timestamp.
        event_epoch = value.get("event_epoch")
        if event_epoch is None:
            _, ts_ms = message.timestamp()
            event_epoch = ts_ms / 1000.0
        wk = self._window_key(float(event_epoch))
        agg = self.windows.setdefault(wk, {"count": 0, "total": 0.0})
        agg["count"] += 1
        agg["total"] += amount
        self._maybe_close_windows()

    def _maybe_close_windows(self) -> None:
        """Emit and drop windows whose end is safely in the past."""
        now = time.time()
        for wk in sorted(self.windows):
            window_end = wk + self.window_s
            # small grace period for out-of-order arrivals
            if now > window_end + 2:
                agg = self.windows.pop(wk)
                result = {
                    "window_start": wk,
                    "window_end": window_end,
                    "count": agg["count"],
                    "total_amount": round(agg["total"], 2),
                }
                print(f"[window closed] {result}")
                self._producer.produce(value=result, key=str(wk))
                self._producer.poll(0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Tumbling-window sales aggregator")
    parser.add_argument("--window", type=int, default=30, help="window size in seconds")
    args = parser.parse_args()

    configure_logging()
    admin = TopicAdmin()
    admin.ensure_topic_exists(IN_TOPIC)
    admin.ensure_topic_exists(OUT_TOPIC)

    WindowedAggregator(window_s=args.window).run(poll_timeout=1.0)


if __name__ == "__main__":
    main()
