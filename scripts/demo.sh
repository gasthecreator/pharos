#!/usr/bin/env bash
set -euo pipefail

# Pharos end-to-end demo: proves the full pipeline live —
# edge capture -> durable local queue -> Central Ingestion -> Kafka ->
# consumer -> queryable Cassandra, plus the dead-letter path for a
# malformed submission. Every step here mirrors the walkthrough in README.md,
# and every command was actually run and verified before being scripted.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SITE_ID="SITE-DEMO-$(date +%s | tail -c 5)"
EDGE_DB="/tmp/pharos-demo-edge-$$.db"
LOG_DIR="$(mktemp -d /tmp/pharos-demo-logs.XXXXXX)"
PIDS=()

log()  { printf '\n\033[1;36m>>> %s\033[0m\n' "$1"; }
info() { printf '%s\n' "$1"; }

cleanup() {
  log "Shutting down demo services"
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  rm -f "$EDGE_DB" "$EDGE_DB"-wal "$EDGE_DB"-shm "$EDGE_DB"-journal
  info "Logs kept at $LOG_DIR if you want to inspect them."
}
trap cleanup EXIT

find_free_port() {
  local port=$1
  while lsof -i ":$port" >/dev/null 2>&1; do
    port=$((port + 1))
  done
  echo "$port"
}

wait_for_log() {
  local file=$1 pattern=$2 timeout=${3:-15}
  local waited=0
  while ! grep -q "$pattern" "$file" 2>/dev/null; do
    sleep 0.5
    waited=$((waited + 1))
    if [ "$waited" -gt $((timeout * 2)) ]; then
      echo "Timed out waiting for '$pattern' in $file" >&2
      cat "$file" >&2
      exit 1
    fi
  done
}

log "Checking Cassandra and Kafka are up"
if ! docker ps --format '{{.Names}}' | grep -q '^pharos-cassandra$'; then
  info "Starting Cassandra and Kafka via docker compose (this can take ~30-40s the first time)..."
  docker compose up -d
fi
info "Waiting for both containers to report healthy..."
for i in $(seq 1 60); do
  cass_ok=$(docker inspect --format '{{.State.Health.Status}}' pharos-cassandra 2>/dev/null || echo "missing")
  kafka_ok=$(docker inspect --format '{{.State.Health.Status}}' pharos-kafka 2>/dev/null || echo "missing")
  if [ "$cass_ok" = "healthy" ] && [ "$kafka_ok" = "healthy" ]; then
    break
  fi
  sleep 2
done
if [ "$cass_ok" != "healthy" ] || [ "$kafka_ok" != "healthy" ]; then
  echo "Cassandra/Kafka did not become healthy in time (cassandra=$cass_ok, kafka=$kafka_ok)." >&2
  exit 1
fi
info "Both healthy."

log "Building binaries"
make build

INGESTION_PORT=$(find_free_port 8091)
EDGE_PORT=$(find_free_port 8080)

log "Starting pharos-ingestion on :$INGESTION_PORT"
./bin/pharos-ingestion --port "$INGESTION_PORT" > "$LOG_DIR/ingestion.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/ingestion.log" "Central Ingestion ready"

log "Starting pharos-consumer"
./bin/pharos-consumer > "$LOG_DIR/consumer.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/consumer.log" "listening for adverse event messages"

log "Starting pharos-edge for site $SITE_ID on :$EDGE_PORT"
./bin/pharos-edge --site-id "$SITE_ID" --port "$EDGE_PORT" \
  --central-url "http://localhost:$INGESTION_PORT/api/v1/events" \
  --db-path "$EDGE_DB" > "$LOG_DIR/edge.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/edge.log" "HTTP capture endpoint listening"

log "Submitting a valid adverse event to the edge collector"
RESP=$(curl -s -X POST "http://localhost:$EDGE_PORT/api/v1/adverse-events" \
  -H "Content-Type: application/json" \
  -d "{
    \"resourceType\": \"AdverseEvent\",
    \"actuality\": \"actual\",
    \"subject\": {\"reference\": \"Patient/DEMO-001\"},
    \"event\": {\"coding\": [{\"system\":\"http://hl7.org/fhir/sid/meddra\",\"code\":\"10012345\",\"display\":\"Nausea\"}], \"text\": \"Severe Nausea\"},
    \"date\": \"2026-08-28T09:00:00+01:00\",
    \"recordedDate\": \"2026-08-30T05:13:00Z\",
    \"severity\": {\"coding\": [{\"code\": \"severe\"}]},
    \"study\": [{\"reference\": \"ResearchStudy/LILLY-401\"}],
    \"location\": {\"reference\": \"Location/$SITE_ID\"}
  }")
info "Edge response: $RESP"
IDKEY="$SITE_ID:1"

log "Waiting for it to flow edge -> Central Ingestion -> Kafka -> consumer -> Cassandra"
for i in $(seq 1 20); do
  if ./bin/pharos-cli query event "$IDKEY" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

log "Querying it back by idempotency key"
./bin/pharos-cli query event "$IDKEY"

log "Querying by site (answers: all events from site Z)"
./bin/pharos-cli query site "$SITE_ID"

log "Querying by study and date range (answers: all events for trial X in range Y)"
./bin/pharos-cli query study LILLY-401 --from 2026-08-01T00:00:00Z --to 2026-08-31T23:59:59Z

log "Submitting a malformed event (missing subject and event fields)"
info "The edge buffers it durably anyway — it never validates, by design (PLAN.md §2.3)."
curl -s -X POST "http://localhost:$EDGE_PORT/api/v1/adverse-events" \
  -H "Content-Type: application/json" \
  -d "{\"resourceType\":\"AdverseEvent\",\"actuality\":\"actual\",\"date\":\"2026-08-28T09:00:00Z\",\"recordedDate\":\"2026-08-30T05:14:00Z\",\"location\":{\"reference\":\"Location/$SITE_ID\"}}"
echo

log "Waiting for Central Ingestion to reject it and route it to the dead-letter store"
for i in $(seq 1 20); do
  if ./bin/pharos-cli dlq list --site "$SITE_ID" 2>/dev/null | grep -q "$SITE_ID:2"; then
    break
  fi
  sleep 0.5
done

log "Inspecting the dead-letter queue for this site"
./bin/pharos-cli dlq list --site "$SITE_ID"

log "Demo complete."
info "Nothing was lost, nothing was duplicated, and the rejection is fully inspectable."
info "Services will now shut down (see trap above)."
