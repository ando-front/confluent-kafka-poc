"""Benchmark — consumer throughput.

Consumes messages from ``bench.throughput.v1`` as fast as possible and reports
consume rate (messages/sec, MB/sec). Run the producer benchmark first to fill
the topic.

Run:  python benchmarks/throughput_consumer.py --count 100000
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Consumer, KafkaError  # noqa: E402

from core import configure_logging, get_settings  # noqa: E402

TOPIC = "bench.throughput.v1"


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumer throughput benchmark")
    parser.add_argument("--count", type=int, default=100000, help="messages to consume")
    parser.add_argument("--timeout", type=float, default=30.0, help="idle timeout seconds")
    args = parser.parse_args()

    configure_logging("WARNING")
    settings = get_settings()
    consumer = Consumer(
        settings.consumer_config(
            group_id=f"bench-consumer-{int(time.time())}",
            **{"auto.offset.reset": "earliest", "fetch.min.bytes": 65536},
        )
    )
    consumer.subscribe([TOPIC])

    consumed = 0
    total_bytes = 0
    start: float | None = None
    idle = 0.0
    try:
        while consumed < args.count and idle < args.timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1.0
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            if start is None:
                start = time.monotonic()
            idle = 0.0
            consumed += 1
            total_bytes += len(msg.value() or b"")
    finally:
        consumer.close()

    elapsed = (time.monotonic() - start) if start else 0.0
    result = {
        "count": consumed,
        "elapsed_s": round(elapsed, 3),
        "msg_per_s": round(consumed / elapsed, 1) if elapsed else 0.0,
        "mb_per_s": round((total_bytes / (1024 * 1024)) / elapsed, 2) if elapsed else 0.0,
    }
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
