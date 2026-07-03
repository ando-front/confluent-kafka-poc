"""Tests for core.consumer and core.admin.

Unit tests cover deserialization and config. Integration tests (auto-skipped
without Kafka) exercise admin topic lifecycle and a real consume loop.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import BaseConsumer, BaseProducer, get_settings  # noqa: E402
from core.consumer import _deserialize  # noqa: E402


def test_deserialize_json() -> None:
    assert _deserialize(b'{"x": 1}') == {"x": 1}


def test_deserialize_plain_string() -> None:
    assert _deserialize(b"not json") == "not json"


def test_deserialize_none() -> None:
    assert _deserialize(None) is None


def test_consumer_config_manual_commit() -> None:
    cfg = get_settings().consumer_config(group_id="g")
    assert cfg["enable.auto.commit"] is False
    assert cfg["group.id"] == "g"


def test_consumer_accepts_string_topic() -> None:
    # Does not connect until run(); just checks topic normalization.
    c = BaseConsumer("single-topic", install_signal_handlers=False)
    assert c.topics == ["single-topic"]


@pytest.mark.integration
def test_admin_topic_lifecycle(topic_admin, require_kafka) -> None:  # noqa: ANN001
    name = "test-lifecycle-abc123"
    topic_admin.delete_topic(name)  # ensure clean
    created = topic_admin.create_topic(name, partitions=2, replication=1)
    assert created is True
    assert name in topic_admin.list_topics()
    # idempotent create returns False
    assert topic_admin.create_topic(name) is False
    assert topic_admin.delete_topic(name) is True


@pytest.mark.integration
def test_consume_max_messages(test_topic: str) -> None:
    received: list[tuple] = []

    with BaseProducer(default_topic=test_topic) as p:
        for i in range(3):
            p.produce(value={"i": i}, key=str(i))
        p.flush(15)

    consumer = BaseConsumer(
        test_topic,
        group_id="test-consume",
        config_overrides={"auto.offset.reset": "earliest"},
        on_message=lambda k, v, m: received.append((k, v)),
        install_signal_handlers=False,
    )
    processed = consumer.run(poll_timeout=1.0, max_messages=3)
    assert processed == 3
    assert len(received) == 3
