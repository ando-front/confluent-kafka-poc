#!/usr/bin/env bash
# =============================================================================
# run_all_demos.sh — run each use case end-to-end in sequence (non-interactive).
#
# For every use case it produces a bounded amount of data and runs the consumer
# with a message/time cap so the whole thing finishes on its own. Great as a
# smoke test that the platform works after `./scripts/start.sh`.
#
# Usage:  ./scripts/run_all_demos.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

PY="${PYTHON:-python}"

banner() { echo; echo "============================================================"; echo "  $1"; echo "============================================================"; }

# Fail fast if the broker is unreachable.
if ! $PY -c "from core.admin import TopicAdmin; TopicAdmin().list_topics(timeout=3.0)" \
     >/dev/null 2>&1; then
  echo "ERROR: Kafka broker not reachable. Run ./scripts/start.sh first." >&2
  exit 1
fi

banner "01 basic pub/sub"
$PY use_cases/01_basic_pubsub/producer.py --count 10 --interval 0.05
timeout 15 $PY use_cases/01_basic_pubsub/consumer.py --group demo-runner &
CONS=$!; sleep 5; kill "$CONS" 2>/dev/null || true

banner "02 event sourcing"
$PY use_cases/02_event_sourcing/event_store.py
$PY use_cases/02_event_sourcing/replay.py --whatif drop-payments

banner "03 stream processing (filter/transform)"
$PY use_cases/03_stream_processing/filter_transform.py --produce 200
timeout 12 $PY use_cases/03_stream_processing/filter_transform.py &
P=$!; sleep 8; kill "$P" 2>/dev/null || true

banner "04 CDC"
$PY use_cases/04_cdc/simulator.py --changes 20
$PY use_cases/04_cdc/cdc_consumer.py --max 20

banner "05 dead letter queue"
$PY use_cases/05_dead_letter_queue/producer.py --count 40
$PY use_cases/05_dead_letter_queue/consumer.py --max 40
$PY use_cases/05_dead_letter_queue/dlq_processor.py --max 20 &
D=$!; sleep 5; kill "$D" 2>/dev/null || true

banner "06 exactly-once"
$PY use_cases/06_exactly_once/transactional_producer.py --batch 5
$PY use_cases/06_exactly_once/transactional_producer.py --batch 5 --fail || true
$PY use_cases/06_exactly_once/idempotent_consumer.py --max 5

banner "07 real-time analytics"
$PY use_cases/07_real_time_analytics/event_generator.py --rate 100 --count 500
timeout 12 $PY use_cases/07_real_time_analytics/aggregator.py &
A=$!; sleep 8; kill "$A" 2>/dev/null || true

banner "ALL DEMOS COMPLETE"
echo "Tip: open http://localhost:8080 (kafka-ui) to inspect the topics."
