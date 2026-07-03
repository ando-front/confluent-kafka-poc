"""Central configuration for the Kafka/Confluent PoC.

Settings are read from environment variables and `.env` / `.env.local` files via
pydantic-settings. Two target environments are supported and switched with
``KAFKA_ENV``:

* ``local``     — docker-compose broker, PLAINTEXT, no auth.
* ``confluent`` — Confluent Cloud, SASL_SSL / PLAIN with API key + secret.

The :meth:`Settings.producer_config`, :meth:`Settings.consumer_config` and
:meth:`Settings.admin_config` helpers return dicts that can be passed straight
into ``confluent_kafka`` clients, so use-case code never hardcodes connection
details.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve project root so we can find .env files regardless of the current CWD.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Prefer an explicit `.env` (may hold secrets, git-ignored). Fall back to the
# checked-in secret-free `.env.local`. Both are optional — real env vars win.
_ENV_FILES = (
    str(_PROJECT_ROOT / ".env.local"),
    str(_PROJECT_ROOT / ".env"),
)


class Settings(BaseSettings):
    """Typed application settings loaded from the environment."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILES,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Target environment --------------------------------------------------
    kafka_env: Literal["local", "confluent"] = "local"

    # --- Connection ----------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"

    # --- Client identity -----------------------------------------------------
    kafka_client_id: str = "confluent-kafka-poc"
    kafka_group_id: str = "confluent-kafka-poc-group"
    kafka_auto_offset_reset: str = "earliest"

    # --- Security ------------------------------------------------------------
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    schema_registry_api_key: str = ""
    schema_registry_api_secret: str = ""

    # --- Topic defaults ------------------------------------------------------
    default_topic_partitions: int = 3
    default_topic_replication: int = 1

    # --- Logging -------------------------------------------------------------
    log_level: str = "INFO"

    # ------------------------------------------------------------------ helpers
    @property
    def is_confluent_cloud(self) -> bool:
        return self.kafka_env == "confluent"

    def _security_config(self) -> dict[str, Any]:
        """Security-related client properties, empty for local PLAINTEXT."""
        if self.kafka_security_protocol.upper() == "PLAINTEXT":
            return {"security.protocol": "PLAINTEXT"}

        cfg: dict[str, Any] = {"security.protocol": self.kafka_security_protocol}
        if self.kafka_sasl_mechanism:
            cfg["sasl.mechanism"] = self.kafka_sasl_mechanism
            cfg["sasl.username"] = self.kafka_sasl_username
            cfg["sasl.password"] = self.kafka_sasl_password
        return cfg

    def base_config(self) -> dict[str, Any]:
        """Connection + security config common to all client types."""
        cfg: dict[str, Any] = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "client.id": self.kafka_client_id,
        }
        cfg.update(self._security_config())
        return cfg

    def producer_config(self, **overrides: Any) -> dict[str, Any]:
        """Config dict for ``confluent_kafka.Producer``."""
        cfg = self.base_config()
        cfg.update(
            {
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "linger.ms": 5,
            }
        )
        cfg.update(overrides)
        return cfg

    def consumer_config(self, group_id: str | None = None, **overrides: Any) -> dict[str, Any]:
        """Config dict for ``confluent_kafka.Consumer``."""
        cfg = self.base_config()
        cfg.update(
            {
                "group.id": group_id or self.kafka_group_id,
                "auto.offset.reset": self.kafka_auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        cfg.update(overrides)
        return cfg

    def admin_config(self, **overrides: Any) -> dict[str, Any]:
        """Config dict for ``confluent_kafka.admin.AdminClient``."""
        cfg = self.base_config()
        cfg.update(overrides)
        return cfg

    def schema_registry_config(self) -> dict[str, Any]:
        """Config dict for the Schema Registry client."""
        cfg: dict[str, Any] = {"url": self.schema_registry_url}
        if self.schema_registry_api_key and self.schema_registry_api_secret:
            cfg["basic.auth.user.info"] = (
                f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}"
            )
        return cfg


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached, process-wide :class:`Settings` instance."""
    return Settings()


def reload_settings() -> Settings:
    """Clear the cache and re-read the environment (useful in tests)."""
    get_settings.cache_clear()
    return get_settings()


if __name__ == "__main__":
    # Quick manual inspection:  python -m core.config
    s = get_settings()
    print(f"KAFKA_ENV                = {s.kafka_env}")
    print(f"bootstrap.servers        = {s.kafka_bootstrap_servers}")
    print(f"schema.registry.url      = {s.schema_registry_url}")
    print(f"security.protocol        = {s.kafka_security_protocol}")
    print(f"cloud?                   = {s.is_confluent_cloud}")
    # Never print secrets.
    redacted = dict(s.producer_config())
    if "sasl.password" in redacted:
        redacted["sasl.password"] = "***"
    print(f"producer_config          = {redacted}")
