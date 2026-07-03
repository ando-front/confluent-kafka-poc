"""Base producer with delivery reporting, in-memory metrics and retries.

Wraps ``confluent_kafka.Producer`` so use-case code gets consistent JSON
serialization, delivery callbacks, throughput/error tracking and a context
manager that guarantees a final ``flush()``.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

from confluent_kafka import KafkaException, Producer

from core.config import Settings, get_settings

logger = logging.getLogger("core.producer")


@dataclass
class ProducerMetrics:
    """In-memory counters tracked over the life of a producer."""

    produced: int = 0
    delivered: int = 0
    failed: int = 0
    bytes_sent: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def error_rate(self) -> float:
        total = self.delivered + self.failed
        return (self.failed / total) if total else 0.0

    @property
    def elapsed_s(self) -> float:
        return max(time.monotonic() - self.started_at, 1e-9)

    @property
    def throughput_msg_s(self) -> float:
        return self.delivered / self.elapsed_s

    @property
    def throughput_mb_s(self) -> float:
        return (self.bytes_sent / (1024 * 1024)) / self.elapsed_s

    def as_dict(self) -> dict[str, float | int]:
        return {
            "produced": self.produced,
            "delivered": self.delivered,
            "failed": self.failed,
            "bytes_sent": self.bytes_sent,
            "error_rate": round(self.error_rate, 4),
            "throughput_msg_s": round(self.throughput_msg_s, 1),
            "throughput_mb_s": round(self.throughput_mb_s, 3),
        }


def _default_serializer(value: Any) -> bytes:
    """Serialize a value to bytes. dict/list -> JSON, str -> utf-8, bytes as-is."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")


class BaseProducer:
    """A reusable Kafka producer.

    Example
    -------
    >>> with BaseProducer() as p:
    ...     p.produce("pubsub.orders.v1", value={"id": 1}, key="1")
    ...     p.flush()
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        config_overrides: dict[str, Any] | None = None,
        default_topic: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.default_topic = default_topic
        self.metrics = ProducerMetrics()
        config = self.settings.producer_config(**(config_overrides or {}))
        self._producer = Producer(config)
        logger.debug("Producer initialized: %s", config.get("bootstrap.servers"))

    # ------------------------------------------------------------------ context
    def __enter__(self) -> BaseProducer:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Ensure everything is on the wire before the process exits.
        self.flush()

    # ------------------------------------------------------------------ delivery
    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Called once per message from librdkafka's background thread."""
        if err is not None:
            self.metrics.failed += 1
            logger.error("Delivery failed [%s]: %s", msg.topic(), err)
            return
        self.metrics.delivered += 1
        self.metrics.bytes_sent += len(msg) if msg else 0
        logger.debug(
            "Delivered -> %s [partition=%s offset=%s]",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

    # ------------------------------------------------------------------ produce
    def produce(
        self,
        topic: str | None = None,
        *,
        value: Any = None,
        key: str | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Enqueue a message for asynchronous delivery.

        Applies backpressure: if the local queue is full we ``poll`` to drain
        delivery callbacks and retry once, rather than dropping the message.
        """
        target = topic or self.default_topic
        if not target:
            raise ValueError("No topic given and no default_topic configured")

        payload = _default_serializer(value)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key
        hdrs = [(k, v.encode("utf-8")) for k, v in headers.items()] if headers else None

        for attempt in (1, 2):
            try:
                self._producer.produce(
                    topic=target,
                    value=payload,
                    key=key_bytes,
                    headers=hdrs,
                    on_delivery=self._delivery_report,
                )
                self.metrics.produced += 1
                # Serve delivery callbacks without blocking.
                self._producer.poll(0)
                return
            except BufferError:
                # Local queue full: block briefly to let it drain, then retry.
                logger.warning("Producer queue full, draining (attempt %d)", attempt)
                self._producer.poll(0.5)
        # If still failing after a drain, surface it.
        raise KafkaException("Producer queue full after retry; message dropped")

    def flush(self, timeout: float = 30.0) -> int:
        """Block until all messages are delivered. Returns # still in queue."""
        remaining = self._producer.flush(timeout)
        if remaining:
            logger.warning("flush() timed out with %d message(s) undelivered", remaining)
        else:
            logger.info("flush() complete: %s", self.metrics.as_dict())
        return remaining

    def poll(self, timeout: float = 0.0) -> int:
        """Serve delivery callbacks. Returns # events processed."""
        return self._producer.poll(timeout)
