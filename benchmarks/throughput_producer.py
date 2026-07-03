"""Benchmark — producer throughput.

Sends N messages as fast as possible and reports messages/sec and MB/sec. Can
sweep producer tunables (batch.size, linger.ms, compression.type) to compare
their impact.

Run:
  # single run
  python benchmarks/throughput_producer.py --count 100000

  # sweep compression + linger and print a comparison table
  python benchmarks/throughput_producer.py --count 100000 --sweep
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from confluent_kafka import Producer  # noqa: E402

from core import TopicAdmin, configure_logging, get_settings  # noqa: E402

TOPIC = "bench.throughput.v1"
PAYLOAD = b"x" * 256  # 256-byte payload


def run_once(count: int, overrides: dict[str, Any]) -> dict[str, float]:
    """Produce *count* messages with the given config overrides; return metrics."""
    settings = get_settings()
    config = settings.producer_config(**overrides)
    producer = Producer(config)

    delivered = {"n": 0, "bytes": 0}

    def _cb(err: Any, msg: Any) -> None:
        if err is None:
            delivered["n"] += 1
            delivered["bytes"] += len(msg)

    start = time.monotonic()
    for i in range(count):
        while True:
            try:
                producer.produce(TOPIC, value=PAYLOAD, key=str(i), on_delivery=_cb)
                break
            except BufferError:
                producer.poll(0.1)
        if i % 10000 == 0:
            producer.poll(0)
    producer.flush(60)
    elapsed = time.monotonic() - start

    return {
        "count": delivered["n"],
        "elapsed_s": round(elapsed, 3),
        "msg_per_s": round(delivered["n"] / elapsed, 1) if elapsed else 0.0,
        "mb_per_s": round((delivered["bytes"] / (1024 * 1024)) / elapsed, 2) if elapsed else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer throughput benchmark")
    parser.add_argument("--count", type=int, default=100000, help="messages to send")
    parser.add_argument("--sweep", action="store_true", help="sweep tuning params")
    args = parser.parse_args()

    configure_logging("WARNING")
    TopicAdmin().ensure_topic_exists(TOPIC, partitions=6)

    if not args.sweep:
        result = run_once(args.count, {"linger.ms": 5, "compression.type": "lz4"})
        print(f"Result: {result}")
        return

    configs = [
        ("baseline", {"linger.ms": 0, "compression.type": "none"}),
        ("linger20", {"linger.ms": 20, "compression.type": "none"}),
        ("lz4", {"linger.ms": 20, "compression.type": "lz4"}),
        ("snappy", {"linger.ms": 20, "compression.type": "snappy"}),
        ("gzip", {"linger.ms": 20, "compression.type": "gzip"}),
        ("big-batch", {"linger.ms": 50, "batch.size": 1_000_000, "compression.type": "lz4"}),
    ]
    print(f"Sweeping {len(configs)} configs x {args.count} msgs...\n")
    rows = []
    for name, overrides in configs:
        res = run_once(args.count, overrides)
        res["config"] = name
        rows.append(res)
        print(f"  {name:10s} -> {res['msg_per_s']:>12,.0f} msg/s  {res['mb_per_s']:>8.2f} MB/s")

    # Persist for report.py
    out_dir = Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    import json

    (out_dir / "throughput.json").write_text(json.dumps(rows, indent=2))
    print(f"\nSaved -> {out_dir / 'throughput.json'}")


if __name__ == "__main__":
    main()
