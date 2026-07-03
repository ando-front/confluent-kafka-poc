"""07 real-time analytics — KPI aggregator.

Consumes ``analytics.events.v1`` and computes KPIs over 1-minute tumbling
windows: page views, add-to-carts, purchases, GMV (gross merchandise value) and
CVR (conversion rate = purchases / page_views). Closed windows are published to
``analytics.kpi_1m.v1`` so any downstream (dashboard, warehouse) can consume
pre-aggregated KPIs.

Run:  python use_cases/07_real_time_analytics/aggregator.py
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

IN_TOPIC = "analytics.events.v1"
OUT_TOPIC = "analytics.kpi_1m.v1"
WINDOW_S = 60


class KpiAggregator(BaseConsumer):
    """1-minute tumbling KPI aggregation."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(IN_TOPIC, group_id="analytics-kpi-aggregator", **kwargs)
        self.windows: dict[int, dict[str, float]] = {}
        self._producer = BaseProducer(default_topic=OUT_TOPIC)

    def _wk(self, epoch: float) -> int:
        return int(epoch // WINDOW_S) * WINDOW_S

    def process_message(self, key: Any, value: Any, message: Message) -> None:
        if not isinstance(value, dict):
            return
        epoch = float(value.get("event_epoch", time.time()))
        wk = self._wk(epoch)
        agg = self.windows.setdefault(
            wk, {"page_view": 0, "add_to_cart": 0, "purchase": 0, "gmv": 0.0}
        )
        etype = value.get("type", "page_view")
        if etype in agg:
            agg[etype] += 1
        if etype == "purchase":
            agg["gmv"] += float(value.get("amount", 0))
        self._maybe_close()

    def _maybe_close(self) -> None:
        now = time.time()
        for wk in sorted(self.windows):
            if now <= wk + WINDOW_S + 2:
                continue
            agg = self.windows.pop(wk)
            pv = agg["page_view"]
            cvr = (agg["purchase"] / pv) if pv else 0.0
            kpi = {
                "window_start": wk,
                "page_views": int(pv),
                "add_to_carts": int(agg["add_to_cart"]),
                "purchases": int(agg["purchase"]),
                "gmv": round(agg["gmv"], 2),
                "cvr": round(cvr, 4),
            }
            print(f"[KPI window closed] {kpi}")
            self._producer.produce(value=kpi, key=str(wk))
            self._producer.poll(0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time KPI aggregator")
    parser.parse_args()
    configure_logging()
    admin = TopicAdmin()
    admin.ensure_topic_exists(IN_TOPIC)
    admin.ensure_topic_exists(OUT_TOPIC)
    KpiAggregator().run(poll_timeout=1.0)


if __name__ == "__main__":
    main()
