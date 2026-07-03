"""02 event sourcing — event replay & what-if simulation.

Replays events from the log starting at a given point in time and re-folds them
into state. Because the log is immutable, we can also apply a *hypothetical*
transformation to the event stream ("what if the payment had been declined?")
without touching the real data — the essence of event sourcing.

Run:
  # Replay everything and show final states per order
  python use_cases/02_event_sourcing/replay.py

  # Replay only events at/after an ISO timestamp
  python use_cases/02_event_sourcing/replay.py --since 2026-07-03T00:00:00+00:00

  # What-if: drop all PaymentReceived events and see the impact
  python use_cases/02_event_sourcing/replay.py --whatif drop-payments
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from event_store import EventStore  # noqa: E402
from events import Event, PaymentReceived  # noqa: E402

from core import configure_logging  # noqa: E402


def _fold(events: list[Event]) -> dict[str, dict]:
    """Fold a flat event list into per-aggregate state."""
    states: dict[str, dict] = defaultdict(lambda: {"status": "UNKNOWN", "paid": 0.0})
    for e in events:
        st = states[e.aggregate_id]
        st["order_id"] = e.aggregate_id
        etype = e.event_type
        if etype == "OrderCreated":
            st.update(status="CREATED", amount=getattr(e, "amount", 0.0))
        elif etype == "PaymentReceived":
            st["paid"] += getattr(e, "amount", 0.0)
            if st["paid"] >= st.get("amount", 0):
                st["status"] = "PAID"
        elif etype == "OrderShipped":
            st["status"] = "SHIPPED"
        elif etype == "OrderCancelled":
            st["status"] = "CANCELLED"
    return dict(states)


def _apply_whatif(events: list[Event], scenario: str) -> list[Event]:
    """Return a transformed event list for a hypothetical scenario."""
    if scenario == "drop-payments":
        return [e for e in events if not isinstance(e, PaymentReceived)]
    if scenario == "none":
        return events
    raise ValueError(f"Unknown what-if scenario: {scenario}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Event replay / what-if")
    parser.add_argument("--since", help="ISO timestamp; replay events at/after this time")
    parser.add_argument(
        "--whatif",
        default="none",
        choices=["none", "drop-payments"],
        help="apply a hypothetical transformation before folding",
    )
    args = parser.parse_args()
    configure_logging()

    events = EventStore().get_events()
    if args.since:
        events = [e for e in events if e.timestamp >= args.since]
        print(f"Replaying {len(events)} event(s) since {args.since}")
    else:
        print(f"Replaying {len(events)} event(s) (full history)")

    actual = _fold(events)
    print("\n=== Actual state ===")
    for oid, st in actual.items():
        print(f"  {oid}: {st}")

    if args.whatif != "none":
        hypo = _fold(_apply_whatif(events, args.whatif))
        print(f"\n=== What-if ({args.whatif}) ===")
        for oid, st in hypo.items():
            changed = "  <-- changed" if actual.get(oid, {}).get("status") != st["status"] else ""
            print(f"  {oid}: {st}{changed}")


if __name__ == "__main__":
    main()
