"""Base consumer with graceful shutdown, manual commits and error handling.

Subclass and override :meth:`BaseConsumer.process_message` to implement a
use case. The :meth:`run` loop handles polling, JSON deserialization, manual
offset commits and clean shutdown on SIGINT/SIGTERM.
"""

from __future__ import annotations

import json
import logging
import signal
from types import FrameType, TracebackType
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from core.config import Settings, get_settings

logger = logging.getLogger("core.consumer")


def _deserialize(raw: bytes | None) -> Any:
    """Best-effort deserialize: JSON if possible, else utf-8 str, else bytes."""
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw


class BaseConsumer:
    """A reusable Kafka consumer with manual commit and graceful shutdown.

    Override :meth:`process_message` in a subclass, or pass ``on_message`` for
    a simple callback style.
    """

    def __init__(
        self,
        topics: list[str] | str,
        *,
        group_id: str | None = None,
        settings: Settings | None = None,
        config_overrides: dict[str, Any] | None = None,
        on_message: Any = None,
        install_signal_handlers: bool = True,
    ) -> None:
        self.settings = settings or get_settings()
        self.topics = [topics] if isinstance(topics, str) else list(topics)
        self._on_message = on_message
        self._running = False
        self.processed = 0

        config = self.settings.consumer_config(group_id=group_id, **(config_overrides or {}))
        self._consumer = Consumer(config)
        logger.debug("Consumer group=%s topics=%s", config.get("group.id"), self.topics)

        if install_signal_handlers:
            self._install_signal_handlers()

    # ------------------------------------------------------------------ signals
    def _install_signal_handlers(self) -> None:
        def _handler(signum: int, _frame: FrameType | None) -> None:
            logger.info("Received signal %s, shutting down gracefully...", signum)
            self.stop()

        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except ValueError:
            # signal only works in the main thread; ignore otherwise.
            logger.debug("Signal handlers not installed (not main thread)")

    # ------------------------------------------------------------------ context
    def __enter__(self) -> BaseConsumer:
        self._consumer.subscribe(self.topics)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------ override
    def process_message(self, key: Any, value: Any, message: Message) -> None:
        """Handle a single decoded message. Override in a subclass.

        Default behavior delegates to the ``on_message`` callback if provided,
        otherwise logs the message.
        """
        if self._on_message is not None:
            self._on_message(key, value, message)
        else:
            logger.info("[%s] key=%s value=%s", message.topic(), key, value)

    # ------------------------------------------------------------------ loop
    def run(self, poll_timeout: float = 1.0, max_messages: int | None = None) -> int:
        """Poll and process until stopped, signalled, or *max_messages* reached.

        Returns the number of messages successfully processed.
        """
        self._consumer.subscribe(self.topics)
        self._running = True
        logger.info("Consuming from %s ... (Ctrl-C to stop)", self.topics)
        try:
            while self._running:
                msg = self._consumer.poll(poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition is normal — not an error.
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    raise KafkaException(msg.error())

                key = msg.key().decode("utf-8") if msg.key() else None
                value = _deserialize(msg.value())
                try:
                    self.process_message(key, value, msg)
                    self._consumer.commit(msg, asynchronous=False)
                    self.processed += 1
                except Exception:  # noqa: BLE001 - keep the loop alive
                    # Do NOT commit — the message will be redelivered.
                    logger.exception("Failed to process message; will retry")
                if max_messages is not None and self.processed >= max_messages:
                    logger.info("Reached max_messages=%d, stopping", max_messages)
                    break
        finally:
            self.close()
        return self.processed

    # ------------------------------------------------------------------ control
    def stop(self) -> None:
        """Request the run loop to exit after the current iteration."""
        self._running = False

    def close(self) -> None:
        """Commit final offsets and leave the group cleanly."""
        try:
            self._consumer.close()
            logger.info("Consumer closed. processed=%d", self.processed)
        except RuntimeError:
            # Already closed.
            pass
