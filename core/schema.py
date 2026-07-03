"""Schema management helpers for JSON Schema and Avro.

Provides:

* :func:`json_serializer` / :func:`json_deserializer` — Schema-Registry-backed
  JSON Schema (de)serializers when a registry is reachable.
* :func:`avro_serializer` / :func:`avro_deserializer` — Avro variants.
* :func:`get_schema_registry_client` — a cached SR client.
* :func:`validate_against_schema` — lightweight local JSON Schema validation
  (type/required checks) that works WITHOUT a registry, handy for tests.

All Schema-Registry-dependent functions import lazily so that the module can be
imported (and the local validator used) even without a running registry.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Any

from core.config import Settings, get_settings

logger = logging.getLogger("core.schema")


# --------------------------------------------------------------------------- SR
@lru_cache(maxsize=1)
def get_schema_registry_client(settings: Settings | None = None) -> Any:
    """Return a cached SchemaRegistryClient (raises if the lib is missing)."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    resolved = settings or get_settings()
    return SchemaRegistryClient(resolved.schema_registry_config())


def json_serializer(schema_str: str, settings: Settings | None = None) -> Any:
    """Return a confluent JSONSerializer bound to the registry."""
    from confluent_kafka.schema_registry.json_schema import JSONSerializer

    client = get_schema_registry_client(settings)
    return JSONSerializer(schema_str, client, lambda obj, ctx: obj)


def json_deserializer(schema_str: str, settings: Settings | None = None) -> Any:
    """Return a confluent JSONDeserializer bound to the registry."""
    from confluent_kafka.schema_registry.json_schema import JSONDeserializer

    return JSONDeserializer(schema_str, from_dict=lambda obj, ctx: obj)


def avro_serializer(schema_str: str, settings: Settings | None = None) -> Any:
    """Return a confluent AvroSerializer bound to the registry."""
    from confluent_kafka.schema_registry.avro import AvroSerializer

    client = get_schema_registry_client(settings)
    return AvroSerializer(client, schema_str, lambda obj, ctx: obj)


def avro_deserializer(schema_str: str, settings: Settings | None = None) -> Any:
    """Return a confluent AvroDeserializer bound to the registry."""
    from confluent_kafka.schema_registry.avro import AvroDeserializer

    client = get_schema_registry_client(settings)
    return AvroDeserializer(client, schema_str, lambda obj, ctx: obj)


def register_schema(
    subject: str, schema_str: str, schema_type: str = "JSON", settings: Settings | None = None
) -> int:
    """Register a schema under *subject* and return the schema id."""
    from confluent_kafka.schema_registry import Schema

    client = get_schema_registry_client(settings)
    schema_id = client.register_schema(subject, Schema(schema_str, schema_type=schema_type))
    logger.info("Registered %s schema under '%s' -> id=%s", schema_type, subject, schema_id)
    return schema_id


# ---------------------------------------------------------- local (no registry)
_JSON_TYPE_MAP: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "object": (dict,),
    "array": (list,),
    "null": (type(None),),
}


def validate_against_schema(instance: Any, schema: dict[str, Any]) -> list[str]:
    """Validate *instance* against a minimal subset of JSON Schema.

    Supports ``type``, ``required`` and nested ``properties``. Returns a list of
    human-readable error strings (empty == valid). This intentionally has no
    external dependency so it can run in unit tests without a Schema Registry.
    """
    errors: list[str] = []
    _validate(instance, schema, path="$", errors=errors)
    return errors


def _validate(instance: Any, schema: dict[str, Any], *, path: str, errors: list[str]) -> None:
    expected = schema.get("type")
    if expected:
        expected_types = _JSON_TYPE_MAP.get(expected, ())
        # bool is a subclass of int — guard against it matching "integer".
        if expected in ("integer", "number") and isinstance(instance, bool):
            errors.append(f"{path}: expected {expected}, got boolean")
            return
        if expected_types and not isinstance(instance, expected_types):
            errors.append(f"{path}: expected {expected}, got {type(instance).__name__}")
            return

    if expected == "object" and isinstance(instance, dict):
        for key in schema.get("required", []):
            if key not in instance:
                errors.append(f"{path}: missing required property '{key}'")
        for key, subschema in schema.get("properties", {}).items():
            if key in instance:
                _validate(instance[key], subschema, path=f"{path}.{key}", errors=errors)

    if expected == "array" and isinstance(instance, list):
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for i, item in enumerate(instance):
                _validate(item, item_schema, path=f"{path}[{i}]", errors=errors)


def load_schema(path: str) -> dict[str, Any]:
    """Load a JSON Schema document from disk."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
