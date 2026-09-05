#!/usr/bin/env bash
set -euo pipefail

# Pharos load-test site provisioning (§2.4, PLAN.md Slice 16: Load testing).
#
# Creates N fresh sites via `pharos-cli site create-key` and writes their
# site_id/api_key pairs to loadtest/sites.json for k6 to load. Re-running
# this regenerates every key from scratch (old ones are implicitly replaced
# server-side -- CreateKey overwrites the stored hash for a given site_id),
# matching scripts/generate_certs.sh's "idempotent for local/CI use" model.
# The output file is gitignored: it holds live plaintext API keys.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NUM_SITES="${LOADTEST_NUM_SITES:-10}"
CASSANDRA_HOSTS="${LOADTEST_CASSANDRA_HOSTS:-127.0.0.1}"
CA_CERT="${LOADTEST_CA_CERT:-$ROOT_DIR/certs/ca-cert.pem}"
OUT_FILE="$ROOT_DIR/loadtest/sites.json"

if [ ! -x "$ROOT_DIR/bin/pharos-cli" ]; then
  echo "bin/pharos-cli not found -- run 'make build' first." >&2
  exit 1
fi

echo "Provisioning $NUM_SITES load-test sites..."
mkdir -p "$ROOT_DIR/loadtest"

entries=()
for i in $(seq 1 "$NUM_SITES"); do
  site_id="SITE-LOAD-$i"
  out=$("$ROOT_DIR/bin/pharos-cli" --hosts "$CASSANDRA_HOSTS" --ca-cert "$CA_CERT" site create-key "$site_id")
  key=$(echo "$out" | grep -oE 'phk_[A-Za-z0-9_-]+')
  if [ -z "$key" ]; then
    echo "Failed to parse API key for $site_id from CLI output:" >&2
    echo "$out" >&2
    exit 1
  fi
  entries+=("  {\"site_id\": \"$site_id\", \"api_key\": \"$key\"}")
  echo "  $site_id: provisioned"
done

printf '[\n' > "$OUT_FILE"
for i in "${!entries[@]}"; do
  if [ "$i" -eq $((${#entries[@]} - 1)) ]; then
    printf '%s\n' "${entries[$i]}" >> "$OUT_FILE"
  else
    printf '%s,\n' "${entries[$i]}" >> "$OUT_FILE"
  fi
done
printf ']\n' >> "$OUT_FILE"

echo "Wrote $NUM_SITES site credentials to $OUT_FILE"
