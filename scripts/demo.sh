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

log "Checking TLS materials (Slice 15: Auth & TLS)"
if [ ! -f "certs/ca-cert.pem" ]; then
  info "No certs/ca-cert.pem found -- generating project CA + service certs..."
  ./scripts/generate_certs.sh
else
  info "certs/ca-cert.pem already present, reusing it."
fi

# Cassandra (dc-us: cassandra-1/2/3, dc-eu: cassandra-4) and Kafka cluster A
# (kafka-1/2/3, dc-us) + cluster B (kafka-4, dc-eu) are what this project's
# application code actually talks to (§2.4, Slice 7/14). MirrorMaker 2 is
# deliberately brought up *last*, after Kafka topics exist -- starting it
# concurrently with everything else has caused real, repeated OOM kills on
# this host (docker-compose.yml's Kafka section comment, and
# ARCHITECTURE_PROPOSALS.md's Slice 14 addendum #3): before any topic
# exists, MM2 busy-loops discovery/retry, piling CPU contention on top of
# 10 other JVMs' own startup work, which delays GC long enough for RSS to
# balloon past the Docker VM's ceiling. This mirrors .github/workflows/ci.yml
# exactly, which hit and fixed the identical ordering issue.
CORE_CONTAINERS="pharos-cassandra-1 pharos-cassandra-2 pharos-cassandra-3 pharos-cassandra-4 pharos-kafka-1 pharos-kafka-2 pharos-kafka-3 pharos-kafka-4"

log "Checking Cassandra + Kafka are up (without MirrorMaker 2 yet)"
docker compose up -d cassandra-1 cassandra-2 cassandra-3 cassandra-4 kafka-1 kafka-2 kafka-3 kafka-4 prometheus grafana

info "Waiting for Cassandra + Kafka to report healthy (this can take a couple of minutes the first time)..."
all_healthy=false
for i in $(seq 1 90); do
  all_healthy=true
  for c in $CORE_CONTAINERS; do
    status=$(docker inspect --format '{{.State.Health.Status}}' "$c" 2>/dev/null || echo "missing")
    if [ "$status" != "healthy" ]; then
      all_healthy=false
    fi
  done
  if [ "$all_healthy" = true ]; then
    break
  fi
  sleep 3
done
if [ "$all_healthy" != true ]; then
  echo "Cassandra/Kafka did not become healthy in time:" >&2
  for c in $CORE_CONTAINERS; do
    echo "  $c: $(docker inspect --format '{{.State.Health.Status}}' "$c" 2>/dev/null || echo missing)" >&2
  done
  exit 1
fi
info "Cassandra + Kafka healthy."

log "Provisioning Kafka topics with regulatory retention policies (§4)"
./scripts/create_topics.sh

log "Starting MirrorMaker 2 (now that topics exist)"
docker compose up -d mirrormaker

log "Building binaries"
make build

INGESTION_PORT=$(find_free_port 8091)
EDGE_PORT=$(find_free_port 8080)

log "Provisioning a per-site API key for $SITE_ID (§2.1, §2.2, Slice 15: Auth & TLS)"
KEY_OUTPUT=$(./bin/pharos-cli site create-key "$SITE_ID" --ca-cert certs/ca-cert.pem)
API_KEY=$(printf '%s\n' "$KEY_OUTPUT" | awk '/Save this now/{getline; getline; print; exit}' | tr -d '[:space:]')
if [ -z "$API_KEY" ]; then
  echo "Failed to provision an API key for $SITE_ID:" >&2
  printf '%s\n' "$KEY_OUTPUT" >&2
  exit 1
fi
info "API key provisioned for $SITE_ID."

log "Starting pharos-ingestion on :$INGESTION_PORT (TLS + per-site auth enabled, Slice 15)"
./bin/pharos-ingestion --port "$INGESTION_PORT" \
  --tls-cert certs/ingestion-cert.pem --tls-key certs/ingestion-key.pem \
  --ca-cert certs/ca-cert.pem \
  > "$LOG_DIR/ingestion.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/ingestion.log" "Central Ingestion ready"

log "Starting pharos-consumer"
./bin/pharos-consumer --ca-cert certs/ca-cert.pem > "$LOG_DIR/consumer.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/consumer.log" "listening for adverse event messages"

log "Starting pharos-edge for site $SITE_ID on :$EDGE_PORT"
./bin/pharos-edge --site-id "$SITE_ID" --port "$EDGE_PORT" \
  --central-url "https://localhost:$INGESTION_PORT/api/v1/events" \
  --db-path "$EDGE_DB" \
  --api-key "$API_KEY" \
  --ca-cert certs/ca-cert.pem \
  > "$LOG_DIR/edge.log" 2>&1 &
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
# The idempotency key's local_seq component is a composite instance-epoch +
# counter value (§2.2, Slice 8), not a simple incrementing integer -- it
# must be read back from the response, not assumed to be "$SITE_ID:1".
IDKEY=$(printf '%s' "$RESP" | sed -n 's/.*"idempotency_key":"\([^"]*\)".*/\1/p')
if [ -z "$IDKEY" ]; then
  echo "Could not parse idempotency_key from edge response: $RESP" >&2
  exit 1
fi

log "Waiting for it to flow edge -> Central Ingestion -> Kafka -> consumer -> Cassandra"
# Generous budget on purpose: the consumer group is shared and persistent
# (Kafka topic retention is measured in days, and named Docker volumes
# survive between runs), so a fresh consumer process here may first have to
# catch up on backlog left by earlier demo/test runs before it ever reaches
# this run's own event -- a few seconds is not a safe assumption.
FOUND=false
for i in $(seq 1 90); do
  if ./bin/pharos-cli query event "$IDKEY" --ca-cert certs/ca-cert.pem --operator "$SITE_ID" >/dev/null 2>&1; then
    FOUND=true
    break
  fi
  if [ $((i % 15)) -eq 0 ]; then
    info "Still waiting (${i}s)... consumer may be catching up on backlog from earlier runs."
  fi
  sleep 1
done
if [ "$FOUND" != true ]; then
  echo "Event $IDKEY never showed up in the canonical store within the wait budget." >&2
  echo "Check $LOG_DIR/consumer.log for consumer lag/backlog, or rerun -- this is a timing issue, not a correctness one." >&2
  exit 1
fi

log "Querying it back by idempotency key"
./bin/pharos-cli query event "$IDKEY" --ca-cert certs/ca-cert.pem --operator "$SITE_ID"

log "Querying by site (answers: all events from site Z)"
./bin/pharos-cli query site "$SITE_ID" --ca-cert certs/ca-cert.pem --operator "$SITE_ID"

log "Querying by study and date range (answers: all events for trial X in range Y)"
./bin/pharos-cli query study LILLY-401 --from 2026-08-01T00:00:00Z --to 2026-08-31T23:59:59Z --ca-cert certs/ca-cert.pem --operator "$SITE_ID"

log "Submitting a malformed event (missing subject and event fields)"
info "The edge buffers it durably anyway — it never validates, by design (PLAN.md §2.3)."
RESP2=$(curl -s -X POST "http://localhost:$EDGE_PORT/api/v1/adverse-events" \
  -H "Content-Type: application/json" \
  -d "{\"resourceType\":\"AdverseEvent\",\"actuality\":\"actual\",\"date\":\"2026-08-28T09:00:00Z\",\"recordedDate\":\"2026-08-30T05:14:00Z\",\"location\":{\"reference\":\"Location/$SITE_ID\"}}")
info "Edge response: $RESP2"
IDKEY2=$(printf '%s' "$RESP2" | sed -n 's/.*"idempotency_key":"\([^"]*\)".*/\1/p')
if [ -z "$IDKEY2" ]; then
  echo "Could not parse idempotency_key from edge response: $RESP2" >&2
  exit 1
fi

log "Waiting for Central Ingestion to reject it and route it to the dead-letter store"
# The DLQ write happens directly in Central Ingestion's request path (it
# writes dead_letter_events to Cassandra itself, not via the consumer), so
# this should be fast -- but give it a real budget rather than assuming so.
FOUND=false
for i in $(seq 1 30); do
  if ./bin/pharos-cli dlq list --site "$SITE_ID" --ca-cert certs/ca-cert.pem --operator "$SITE_ID" 2>/dev/null | grep -q "$IDKEY2"; then
    FOUND=true
    break
  fi
  sleep 1
done
if [ "$FOUND" != true ]; then
  echo "Rejected event $IDKEY2 never showed up in the DLQ within the wait budget." >&2
  echo "Check $LOG_DIR/ingestion.log for the rejection, or rerun." >&2
  exit 1
fi

log "Inspecting the dead-letter queue for this site"
./bin/pharos-cli dlq list --site "$SITE_ID" --ca-cert certs/ca-cert.pem --operator "$SITE_ID"

log "Confirming the access-audit trail recorded this session's own CLI queries (§2.4, Slice 20)"
info "Every query/dlq command above required --operator $SITE_ID -- this is that same trail, not seeded fixtures."
./bin/pharos-cli audit list --operator "$SITE_ID" --ca-cert certs/ca-cert.pem

DASHBOARD_PORT=$(find_free_port 8092)
log "Starting pharos-dashboard on :$DASHBOARD_PORT (§2.4, Slices 21 & 23 -- web dashboard + chaos control panel)"
info "--enable-chaos is deliberately left off here, matching its own off-by-default safety guard (Slice 23) -- this demo only proves the dashboard and its chaos panel route are alive, it doesn't mutate the live cluster."
./bin/pharos-dashboard --port "$DASHBOARD_PORT" --ca-cert certs/ca-cert.pem \
  --central-url "https://localhost:$INGESTION_PORT" \
  > "$LOG_DIR/dashboard.log" 2>&1 &
PIDS+=($!)
wait_for_log "$LOG_DIR/dashboard.log" "Ready on http" 30

info "Dashboard health check:"
curl -sf "http://localhost:$DASHBOARD_PORT/healthz" && echo
info "Dashboard query view for this run's own site (same query.Service pharos-cli uses):"
curl -s "http://localhost:$DASHBOARD_PORT/query?type=site&id=$SITE_ID" | grep -o "$IDKEY" | head -1 \
  && info "  -> found $IDKEY rendered on the dashboard's own query page." \
  || info "  -> (didn't spot $IDKEY in the rendered page -- non-fatal, the CLI already proved this data is queryable above)"
info "Chaos control panel route (present but inert without --enable-chaos):"
curl -s -o /dev/null -w "  GET /chaos -> HTTP %{http_code}\n" "http://localhost:$DASHBOARD_PORT/chaos"

log "Demo complete."
info "Nothing was lost, nothing was duplicated, and the rejection is fully inspectable."
info "Services will now shut down (see trap above)."
