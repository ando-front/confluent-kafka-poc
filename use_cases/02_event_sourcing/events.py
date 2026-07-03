"""02 event sourcing — event definitions.

Domain events for a simple order lifecycle. Every event is immutable and carries
an ``aggregate_id`` (the order id) so the full history of one order can be
replayed from the event log.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

TOPIC = "eventsourcing.orders.v1"


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Event:
    """Base event. ``event_type`` is used to reconstruct the concrete class."""

    aggregate_id: str
    event_type: str = field(init=False, default="Event")
    timestamp: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["event_type"] = self.event_type
        return data


@dataclass
class OrderCreated(Event):
    customer: str = ""
    amount: float = 0.0

    def __post_init__(self) -> None:
        self.event_type = "OrderCreated"


@dataclass
class PaymentReceived(Event):
    amount: float = 0.0
    method: str = "card"

    def __post_init__(self) -> None:
        self.event_type = "PaymentReceived"


@dataclass
class OrderShipped(Event):
    carrier: str = ""
    tracking: str = ""

    def __post_init__(self) -> None:
        self.event_type = "OrderShipped"


@dataclass
class OrderCancelled(Event):
    reason: str = ""

    def __post_init__(self) -> None:
        self.event_type = "OrderCancelled"


_EVENT_CLASSES: dict[str, type[Event]] = {
    "OrderCreated": OrderCreated,
    "PaymentReceived": PaymentReceived,
    "OrderShipped": OrderShipped,
    "OrderCancelled": OrderCancelled,
}


def event_from_dict(data: dict[str, Any]) -> Event:
    """Reconstruct a concrete Event subclass from a decoded message."""
    etype = data.get("event_type", "Event")
    cls = _EVENT_CLASSES.get(etype, Event)
    known = {k: v for k, v in data.items() if k != "event_type"}
    # Filter to fields the dataclass accepts (aggregate_id, timestamp + own).
    valid = {f for f in cls.__dataclass_fields__ if cls.__dataclass_fields__[f].init}
    return cls(**{k: v for k, v in known.items() if k in valid})
