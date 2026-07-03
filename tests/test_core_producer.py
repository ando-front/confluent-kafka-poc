"""Tests for core.producer.

Unit tests (no broker) cover serialization and metrics. The integration test
(marked, auto-skipped without Kafka) does a real round-trip produce+consume.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import BaseProducer, get_settings  # noqa: E402
from core.producer import ProducerMetrics, _default_serializer  # noqa: E402


def test_serializer_dict_to_json() -> None:
    assert _default_serializer({"a": 1}) == b'{"a": 1}'


def test_serializer_str_and_bytes() -> None:
    assert _default_serializer("hello") == b"hello"
    assert _default_serializer(b"raw") == b"raw"
    assert _default_serializer(None) == b""


def test_serializer_handles_non_ascii() -> None:
    assert _default_serializer({"name": "安藤"}).decode("utf-8") == '{"name": "安藤"}'


def test_metrics_error_rate_and_throughput() -> None:
    m = ProducerMetrics()
    m.delivered = 8
    m.failed = 2
    assert m.error_rate == pytest.approx(0.2)
    assert m.throughput_msg_s > 0
    d = m.as_dict()
    assert d["delivered"] == 8 and d["failed"] == 2


def test_producer_config_defaults() -> None:
    cfg = get_settings().producer_config()
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True


@pytest.mark.integration
def test_round_trip(test_topic: str) -> None:
    """Produce a message and confirm delivery metrics increment."""
    from confluent_kafka import Consumer

    with BaseProducer(default_topic=test_topic) as p:
        p.produce(value={"hello": "world"}, key="k1")
        remaining = p.flush(15)
        assert remaining == 0
        assert p.metrics.delivered == 1

    settings = get_settings()
    consumer = Consumer(
        settings.consumer_config(group_id="test-rt", **{"auto.offset.reset": "earliest"})
    )
    consumer.subscribe([test_topic])
    try:
        msg = None
        for _ in range(20):
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                break
        assert msg is not None and not msg.error()
        import json

        assert json.loads(msg.value().decode("utf-8")) == {"hello": "world"}
    finally:
        consumer.close()
