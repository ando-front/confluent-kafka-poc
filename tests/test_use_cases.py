"""Smoke tests for the use cases.

Unit-level checks that each use case's core logic is importable and behaves
correctly without a broker (event folding, CDC envelopes, DLQ validation,
schema validation, KPI math). Heavier round-trips live in the integration tests.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[1]


def _load(path: str, name: str):  # noqa: ANN202
    """Import a module by file path (use cases are not a package)."""
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # Ensure sibling imports inside the module can resolve.
    sys.path.insert(0, str((ROOT / path).parent))
    # Register before exec: dataclasses with `from __future__ import annotations`
    # resolve field types via sys.modules[cls.__module__].
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------- event sourcing
def test_event_sourcing_roundtrip_dict() -> None:
    events = _load("use_cases/02_event_sourcing/events.py", "es_events")
    e = events.OrderCreated(aggregate_id="o1", customer="alice", amount=100.0)
    d = e.to_dict()
    assert d["event_type"] == "OrderCreated"
    back = events.event_from_dict(d)
    assert isinstance(back, events.OrderCreated)
    assert back.customer == "alice" and back.amount == 100.0


# ------------------------------------------------------------------ CDC envelope
def test_cdc_envelope_shape() -> None:
    sim = _load("use_cases/04_cdc/simulator.py", "cdc_sim")
    env = sim._envelope("c", None, {"id": 1, "customer": "x", "status": "NEW", "amount": 1.0})
    assert env["op"] == "c"
    assert env["before"] is None
    assert env["after"]["id"] == 1
    assert env["source"]["table"] == "orders"


# --------------------------------------------------------------------- DLQ logic
def test_dlq_validation() -> None:
    consumer = _load("use_cases/05_dead_letter_queue/consumer.py", "dlq_consumer")
    ok, err = consumer.validate(b'{"order_id":"1","customer":"a","amount":10}')
    assert err is None and ok["amount"] == 10
    bad, err2 = consumer.validate(b"not-json{")
    assert bad is None and err2.startswith("invalid_json")
    miss, err3 = consumer.validate(b'{"order_id":"1"}')
    assert miss is None and err3.startswith("missing_fields")


def test_dlq_recovery() -> None:
    proc = _load("use_cases/05_dead_letter_queue/dlq_processor.py", "dlq_proc")
    fixed = proc.try_recover(b'{"order_id":"1"}', "missing_fields: ['amount']")
    assert fixed is not None and fixed["amount"] == 0.0
    assert proc.try_recover(b"not-json", "invalid_json: x") is None


# ------------------------------------------------------------------ schema utils
def test_schema_local_validation() -> None:
    from core.schema import validate_against_schema

    schema = {
        "type": "object",
        "required": ["id", "amount"],
        "properties": {"id": {"type": "integer"}, "amount": {"type": "number"}},
    }
    assert validate_against_schema({"id": 1, "amount": 9.5}, schema) == []
    errors = validate_against_schema({"id": "x"}, schema)
    assert any("id" in e for e in errors)
    assert any("amount" in e for e in errors)  # missing required


def test_schema_rejects_bool_as_int() -> None:
    from core.schema import validate_against_schema

    schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
    assert validate_against_schema({"n": True}, schema)  # bool must not pass integer
