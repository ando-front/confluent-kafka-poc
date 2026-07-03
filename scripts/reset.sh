#!/usr/bin/env bash
# =============================================================================
# reset.sh — wipe ALL Kafka data: stop containers AND remove volumes.
#
# Also deletes local SQLite artifacts created by the use cases.
# Usage:  ./scripts/reset.sh [minimal]
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

MODE="${1:-full}"
if [[ "$MODE" == "minimal" ]]; then
  COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.minimal.yml"
else
  COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"
fi

if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
else
  DC="docker-compose"
fi

echo "!!  This will DELETE all Kafka topics, messages and volumes for '$MODE'."
read -r -p "    Continue? [y/N] " reply
if [[ ! "$reply" =~ ^[Yy]$ ]]; then
  echo "Aborted."
  exit 0
fi

echo "==> Removing containers and volumes..."
$DC -f "$COMPOSE_FILE" down -v

echo "==> Removing local SQLite artifacts..."
rm -f "$ROOT_DIR"/use_cases/04_cdc/*.db \
      "$ROOT_DIR"/use_cases/06_exactly_once/*.db 2>/dev/null || true
rm -rf "$ROOT_DIR"/benchmarks/results 2>/dev/null || true

echo "==> Reset complete. Run ./scripts/start.sh to begin fresh."
