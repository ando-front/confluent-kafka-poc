"""07 real-time analytics — console dashboard (Rich).

Consumes ``analytics.events.v1`` directly and maintains a rolling 60-second
window of KPIs, refreshing a live Rich dashboard once per second:

    * Page views / Add-to-carts / Purchases (last 60s)
    * GMV (gross merchandise value, last 60s)
    * CVR (conversion rate)
    * Events/sec throughput
    * Top products

Runs standalone (does not require the aggregator). Poll happens on a background
thread so the UI stays responsive.

Run:  python use_cases/07_real_time_analytics/dashboard.py
"""

from __future__ import annotations

import sys
import threading
import time
from collections import Counter, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from confluent_kafka import Consumer, KafkaError  # noqa: E402
from rich.console import Group  # noqa: E402
from rich.live import Live  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402

from core import TopicAdmin, configure_logging, get_settings  # noqa: E402

TOPIC = "analytics.events.v1"
WINDOW_S = 60


class RollingStats:
    """Thread-safe rolling 60-second event window."""

    def __init__(self) -> None:
        self._events: deque[tuple[float, str, float, str]] = deque()
        self._lock = threading.Lock()
        self.total = 0

    def add(self, epoch: float, etype: str, amount: float, product: str) -> None:
        with self._lock:
            self._events.append((epoch, etype, amount, product))
            self.total += 1

    def snapshot(self) -> dict[str, object]:
        cutoff = time.time() - WINDOW_S
        with self._lock:
            while self._events and self._events[0][0] < cutoff:
                self._events.popleft()
            events = list(self._events)
        pv = sum(1 for _, t, _, _ in events if t == "page_view")
        cart = sum(1 for _, t, _, _ in events if t == "add_to_cart")
        purch = sum(1 for _, t, _, _ in events if t == "purchase")
        gmv = sum(a for _, t, a, _ in events if t == "purchase")
        products = Counter(p for _, t, _, p in events if t == "purchase")
        return {
            "page_views": pv,
            "add_to_carts": cart,
            "purchases": purch,
            "gmv": gmv,
            "cvr": (purch / pv) if pv else 0.0,
            "eps": len(events) / WINDOW_S,
            "window_count": len(events),
            "top": products.most_common(5),
            "total": self.total,
        }


def render(stats: RollingStats) -> Panel:
    s = stats.snapshot()
    table = Table(title="KPIs (rolling 60s)", expand=True)
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", justify="right", style="bold green")
    table.add_row("Page views", f"{s['page_views']}")
    table.add_row("Add to carts", f"{s['add_to_carts']}")
    table.add_row("Purchases", f"{s['purchases']}")
    table.add_row("GMV (¥)", f"{s['gmv']:,.0f}")
    table.add_row("CVR", f"{s['cvr'] * 100:.2f}%")
    table.add_row("Events/sec", f"{s['eps']:.1f}")
    table.add_row("Total consumed", f"{s['total']}")

    top = Table(title="Top products (purchases, 60s)", expand=True)
    top.add_column("Product", style="magenta")
    top.add_column("Count", justify="right")
    for product, count in s["top"]:  # type: ignore[union-attr]
        top.add_row(str(product), str(count))
    if not s["top"]:
        top.add_row("(none yet)", "-")

    return Panel(Group(table, top), title="Real-time Analytics Dashboard", border_style="blue")


def consume_loop(stats: RollingStats, stop: threading.Event) -> None:
    settings = get_settings()
    consumer = Consumer(settings.consumer_config(group_id="analytics-dashboard"))
    consumer.subscribe([TOPIC])
    import json

    try:
        while not stop.is_set():
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                if msg and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                    continue
                continue
            try:
                v = json.loads(msg.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            stats.add(
                float(v.get("event_epoch", time.time())),
                v.get("type", "page_view"),
                float(v.get("amount", 0)),
                str(v.get("product", "?")),
            )
    finally:
        consumer.close()


def main() -> None:
    configure_logging("WARNING")  # keep the console clean for the dashboard
    TopicAdmin().ensure_topic_exists(TOPIC)

    stats = RollingStats()
    stop = threading.Event()
    thread = threading.Thread(target=consume_loop, args=(stats, stop), daemon=True)
    thread.start()

    try:
        with Live(render(stats), refresh_per_second=1, screen=False) as live:
            while True:
                time.sleep(1)
                live.update(render(stats))
    except KeyboardInterrupt:
        print("\nStopping dashboard...")
    finally:
        stop.set()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
