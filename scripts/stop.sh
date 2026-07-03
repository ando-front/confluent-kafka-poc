#!/usr/bin/env bash
# =============================================================================
# stop.sh — cleanly shut down the local Kafka stack (keeps volumes/data).
#
# Usage:
#   ./scripts/stop.sh            # stop full stack
#   ./scripts/stop.sh minimal    # stop minimal stack
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

echo "==> Stopping Kafka stack ($MODE)..."
$DC -f "$COMPOSE_FILE" down
echo "==> Stopped. Data volumes are preserved (use scripts/reset.sh to wipe)."
