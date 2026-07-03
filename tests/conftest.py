"""pytest fixtures for the Kafka PoC.

Provides:

* ``kafka_available``  — session-scoped bool; True if a broker is reachable.
* ``require_kafka``    — skips the test when no broker is available.
* ``topic_admin``      — a :class:`core.admin.TopicAdmin`.
* ``test_topic``       — a freshly-created, uniquely-named topic that is deleted
                          automatically after the test.

Integration tests (those using ``test_topic`` / ``require_kafka``) are skipped
cleanly when Kafka is not running, so the unit tests still pass in CI without a
broker.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

# Make `core` importable when running pytest from anywhere.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import TopicAdmin, get_settings  # noqa: E402


@pytest.fixture(scope="session")
def kafka_available() -> bool:
    """Return True if a Kafka broker responds to a metadata request."""
    try:
        admin = TopicAdmin()
        admin.list_topics(timeout=3.0)
        return True
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture
def require_kafka(kafka_available: bool) -> None:
    """Skip the test if no broker is reachable."""
    if not kafka_available:
        pytest.skip("No Kafka broker reachable (start it with ./scripts/start.sh)")


@pytest.fixture(scope="session")
def topic_admin() -> TopicAdmin:
    return TopicAdmin()


@pytest.fixture
def test_topic(require_kafka: None, topic_admin: TopicAdmin) -> Iterator[str]:
    """Create a unique test topic; delete it after the test."""
    name = f"test-{uuid.uuid4().hex[:12]}"
    topic_admin.create_topic(name, partitions=1, replication=1)
    try:
        yield name
    finally:
        topic_admin.delete_topic(name)


@pytest.fixture
def settings():  # noqa: ANN201 - pytest fixture
    return get_settings()
