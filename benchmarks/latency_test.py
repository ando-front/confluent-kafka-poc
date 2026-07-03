"""Benchmark — end-to-end latency.

Measures producer-to-consumer latency. The producer embeds a high-resolution
send timestamp in each message; a consumer (run in a background thread) records
the delta on receipt. Reports P50 / P95 / P99 / max latency in milliseconds.

This runs both sides in one process for convenience (still crosses the broker).

Run:  python benchmarks/latency_test.py --count 2000 --rate 200
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError, Producer  # noqa: E402

from core import TopicAdmin, configure_logging, get_settings  # noqa: E402

TOPIC = "bench.latency.v1"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[k]


def consumer_thread(count: int, latencies: list[float], stop: threading.Event) -> None:
    settings = get_settings()
    consumer = Consumer(
        settings.consumer_config(
            group_id=f"bench-latency-{int(time.time())}",
            **{"auto.offset.reset": "latest"},
        )
    )
    consumer.subscribe([TOPIC])
    received = 0
    try:
        while received < count and not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if msg and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                    continue
                continue
            recv_ns = time.perf_counter_ns()
            try:
                data = json.loads(msg.value().decode("utf-8"))
                sent_ns = int(data["sent_ns"])
                latencies.append((recv_ns - sent_ns) / 1e6)  # ms
                received += 1
            except (KeyError, ValueError, UnicodeDecodeError):
                continue
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="E2E latency benchmark")
    parser.add_argument("--count", type=int, default=2000, help="messages to measure")
    parser.add_argument("--rate", type=float, default=200.0, help="messages per second")
    args = parser.parse_args()

    configure_logging("WARNING")
    TopicAdmin().ensure_topic_exists(TOPIC)

    latencies: list[float] = []
    stop = threading.Event()
    thread = threading.Thread(target=consumer_thread, args=(args.count, latencies, stop))
    thread.start()
    time.sleep(2)  # let the consumer join the group and seek to latest

    settings = get_settings()
    producer = Producer(settings.producer_config(**{"linger.ms": 0}))
    interval = 1.0 / args.rate if args.rate > 0 else 0
    for _ in range(args.count):
        payload = json.dumps({"sent_ns": time.perf_counter_ns()}).encode("utf-8")
        producer.produce(TOPIC, value=payload)
        producer.poll(0)
        if interval:
            time.sleep(interval)
    producer.flush(30)

    thread.join(timeout=30)
    stop.set()

    result = {
        "measured": len(latencies),
        "p50_ms": round(percentile(latencies, 50), 3),
        "p95_ms": round(percentile(latencies, 95), 3),
        "p99_ms": round(percentile(latencies, 99), 3),
        "max_ms": round(max(latencies), 3) if latencies else 0.0,
    }
    print(f"Latency: {result}")

    out_dir = Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "latency.json").write_text(json.dumps(result, indent=2))
    print(f"Saved -> {out_dir / 'latency.json'}")


if __name__ == "__main__":
    main()
