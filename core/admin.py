"""Topic administration helpers built on ``confluent_kafka.admin.AdminClient``."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    NewTopic,
)

from core.config import Settings, get_settings

logger = logging.getLogger("core.admin")


class TopicAdmin:
    """Thin wrapper around AdminClient for create/delete/list/describe."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        config_overrides: dict[str, Any] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._admin = AdminClient(self.settings.admin_config(**(config_overrides or {})))

    # ------------------------------------------------------------------ create
    def create_topic(
        self,
        name: str,
        *,
        partitions: int | None = None,
        replication: int | None = None,
        config: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> bool:
        """Create a topic. Returns True if created, False if it already existed."""
        num_partitions = partitions or self.settings.default_topic_partitions
        repl = replication or self.settings.default_topic_replication
        new_topic = NewTopic(
            topic=name,
            num_partitions=num_partitions,
            replication_factor=repl,
            config=config or {},
        )
        futures = self._admin.create_topics([new_topic])
        try:
            futures[name].result(timeout=timeout)
            logger.info("Created topic '%s' (partitions=%d, rf=%d)", name, num_partitions, repl)
            return True
        except Exception as exc:  # noqa: BLE001
            if "TopicExistsError" in type(exc).__name__ or "already exists" in str(exc):
                logger.debug("Topic '%s' already exists", name)
                return False
            raise

    def ensure_topic_exists(
        self,
        name: str,
        *,
        partitions: int | None = None,
        replication: int | None = None,
        config: dict[str, str] | None = None,
    ) -> None:
        """Create the topic only if it does not exist yet (idempotent)."""
        if name in self.list_topics():
            logger.debug("ensure_topic_exists: '%s' present, skipping", name)
            return
        self.create_topic(
            name, partitions=partitions, replication=replication, config=config
        )

    # ------------------------------------------------------------------ delete
    def delete_topic(self, name: str, *, timeout: float = 30.0) -> bool:
        """Delete a topic. Returns True on success, False if it did not exist."""
        futures = self._admin.delete_topics([name], operation_timeout=timeout)
        try:
            futures[name].result(timeout=timeout)
            logger.info("Deleted topic '%s'", name)
            return True
        except Exception as exc:  # noqa: BLE001
            if "UnknownTopic" in type(exc).__name__ or "does not exist" in str(exc):
                logger.debug("Topic '%s' does not exist", name)
                return False
            raise

    # ------------------------------------------------------------------ list
    def list_topics(self, *, timeout: float = 10.0, include_internal: bool = False) -> list[str]:
        """Return the names of all topics on the cluster."""
        metadata = self._admin.list_topics(timeout=timeout)
        names = [
            t
            for t in metadata.topics
            if include_internal or not t.startswith("_")
        ]
        return sorted(names)

    # ------------------------------------------------------------------ describe
    def get_topic_config(self, name: str, *, timeout: float = 10.0) -> dict[str, str]:
        """Return the (non-default) configuration of a topic."""
        resource = ConfigResource(ConfigResource.Type.TOPIC, name)
        futures = self._admin.describe_configs([resource])
        config = futures[resource].result(timeout=timeout)
        return {k: entry.value for k, entry in config.items()}

    def alter_topic_config(
        self, name: str, config: dict[str, str], *, timeout: float = 10.0
    ) -> None:
        """Incrementally change one or more topic configs."""
        # incremental_alter_configs is preferred; fall back to alter_configs.
        resource = ConfigResource(ConfigResource.Type.TOPIC, name)
        for key, value in config.items():
            resource.set_config(key, value)
        try:
            futures = self._admin.incremental_alter_configs([resource])
        except AttributeError:  # older client
            futures = self._admin.alter_configs([resource])
        futures[resource].result(timeout=timeout)
        logger.info("Altered config for '%s': %s", name, config)


if __name__ == "__main__":
    # Quick manual check:  python -m core.admin
    import sys

    logging.basicConfig(level="INFO")
    admin = TopicAdmin()
    try:
        print("Topics:", admin.list_topics())
    except Exception as exc:  # noqa: BLE001
        print(f"Could not reach broker: {exc}", file=sys.stderr)
        sys.exit(1)
