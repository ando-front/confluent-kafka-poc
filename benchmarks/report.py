"""Benchmark — report generator.

Reads the JSON results written by ``throughput_producer.py --sweep`` and
``latency_test.py`` from ``benchmarks/results/`` and renders a Markdown report to
``benchmarks/results/REPORT.md`` (also printed to stdout).

Run:  python benchmarks/report.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def _load(name: str) -> object | None:
    path = RESULTS_DIR / name
    if not path.exists():
        return None
    return json.loads(path.read_text())


def build_report() -> str:
    lines: list[str] = ["# Benchmark Report", ""]

    throughput = _load("throughput.json")
    lines.append("## Producer throughput (sweep)")
    lines.append("")
    if isinstance(throughput, list) and throughput:
        lines.append("| Config | Messages | Elapsed (s) | msg/s | MB/s |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in throughput:
            lines.append(
                f"| {row.get('config', '?')} | {row.get('count', 0):,} | "
                f"{row.get('elapsed_s', 0)} | {row.get('msg_per_s', 0):,.0f} | "
                f"{row.get('mb_per_s', 0):.2f} |"
            )
    else:
        lines.append("_No throughput results. Run `throughput_producer.py --sweep` first._")
    lines.append("")

    latency = _load("latency.json")
    lines.append("## End-to-end latency")
    lines.append("")
    if isinstance(latency, dict):
        lines.append("| Metric | Value (ms) |")
        lines.append("|---|---:|")
        lines.append(f"| measured | {latency.get('measured', 0)} |")
        lines.append(f"| P50 | {latency.get('p50_ms', 0)} |")
        lines.append(f"| P95 | {latency.get('p95_ms', 0)} |")
        lines.append(f"| P99 | {latency.get('p99_ms', 0)} |")
        lines.append(f"| max | {latency.get('max_ms', 0)} |")
    else:
        lines.append("_No latency results. Run `latency_test.py` first._")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    report = build_report()
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / "REPORT.md"
    out.write_text(report)
    print(report)
    print(f"\nSaved -> {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
