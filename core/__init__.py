"""Shared infrastructure layer for the Confluent/Kafka PoC.

Public API:

* :class:`~core.config.Settings` / :func:`~core.config.get_settings`
* :class:`~core.producer.BaseProducer`
* :class:`~core.consumer.BaseConsumer`
* :class:`~core.admin.TopicAdmin`
* :mod:`core.schema` helpers
"""

from __future__ import annotations

import logging

from core.admin import TopicAdmin
from core.config import Settings, get_settings, reload_settings
from core.consumer import BaseConsumer
from core.producer import BaseProducer, ProducerMetrics

__all__ = [
    "Settings",
    "get_settings",
    "reload_settings",
    "BaseProducer",
    "ProducerMetrics",
    "BaseConsumer",
    "TopicAdmin",
    "configure_logging",
]


def configure_logging(level: str | None = None) -> None:
    """Configure root logging once, honoring ``LOG_LEVEL`` when *level* is None."""
    resolved = (level or get_settings().log_level).upper()
    logging.basicConfig(
        level=resolved,
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
