#!/usr/bin/env bash
# =============================================================================
# start.sh — bring up the local Kafka stack and wait until it is ready.
#
# Usage:
#   ./scripts/start.sh            # full Confluent Platform stack
#   ./scripts/start.sh minimal    # ZooKeeper + Kafka + kafka-ui only
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

MODE="${1:-full}"
if [[ "$MODE" == "minimal" ]]; then
  COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.minimal.yml"
  BROKER_CONTAINER="poc-broker-min"
else
  COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"
  BROKER_CONTAINER="poc-broker"
fi

# --- Preconditions -----------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed or not on PATH." >&2
  echo "       Install Docker Desktop: https://www.docker.com/products/docker-desktop/" >&2
  exit 1
fi

# Resolve 'docker compose' (v2) vs 'docker-compose' (v1).
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "ERROR: neither 'docker compose' nor 'docker-compose' is available." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker daemon is not running. Start Docker Desktop and retry." >&2
  exit 1
fi

echo "==> Starting Kafka stack ($MODE) from $COMPOSE_FILE"
$DC -f "$COMPOSE_FILE" up -d

# --- Wait for the broker to answer kafka-topics --list -----------------------
echo "==> Waiting for the broker to become ready (up to 60s)..."
READY=0
for i in $(seq 1 60); do
  if docker exec "$BROKER_CONTAINER" kafka-topics --bootstrap-server broker:29092 --list \
       >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
  printf '.'
done
echo

if [[ "$READY" -ne 1 ]]; then
  echo "ERROR: Kafka broker did not become ready within 60s." >&2
  echo "       Check logs with: $DC -f $COMPOSE_FILE logs broker" >&2
  exit 1
fi

echo "==> Kafka is ready. Current topics:"
docker exec "$BROKER_CONTAINER" kafka-topics --bootstrap-server broker:29092 --list \
  | sed 's/^/    /' || true

echo
echo "==> UIs:"
echo "    kafka-ui        : http://localhost:8080"
if [[ "$MODE" != "minimal" ]]; then
  echo "    Control Center  : http://localhost:9021"
  echo "    Schema Registry : http://localhost:8081"
  echo "    Kafka Connect   : http://localhost:8083"
fi
echo
echo "Bootstrap servers: localhost:9092"
echo "Done. Try:  python use_cases/01_basic_pubsub/consumer.py"
