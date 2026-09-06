#!/usr/bin/env bash
set -euo pipefail

# Pharos Cassandra restore (§2.4, PLAN.md Slice 18: Backup & disaster
# recovery). Restores a keyspace's SSTables from a backup taken by
# scripts/backup_cassandra.sh, into each node's *own* live table
# directories (not a fresh/replacement node) via `nodetool refresh` --
# the standard, no-downtime way to load new SSTables into an already
# running node.
#
# Deliberately scoped to "the keyspace's data was lost or corrupted, but
# node identity/tokens are intact" (e.g. an accidental DROP KEYSPACE, a bad
# migration, on-disk corruption limited to this keyspace) -- restoring a
# fully-destroyed node from snapshot alone is a different, harder problem:
# Cassandra 5.0's default vnodes assign RANDOM tokens to a fresh node on
# first bootstrap, so a brand-new node generally does NOT get the same
# token ranges the original node owned, and blindly copying its old
# SSTables in would silently misplace data. That scenario is handled by
# Cassandra's own node-replacement procedure (streaming from surviving
# replicas), which is what RF>1 and this project's own multi-node
# topology (Slice 7/14) exist to make possible -- not something a backup
# restores you from. This script restores the case backups actually exist
# for: getting a keyspace's data back when live replicas can't help
# because the data itself, not a node, is what's gone.
#
# Usage: ./restore_cassandra.sh <backup-timestamp-dir> [keyspace]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_TS="${1:?usage: restore_cassandra.sh <backup-timestamp-dir> [keyspace]}"
KEYSPACE="${2:-${PHAROS_BACKUP_KEYSPACE:-pharos}}"
BACKUP_DIR="${PHAROS_BACKUP_DIR:-${ROOT_DIR}/backup}/${BACKUP_TS}"

if [ ! -d "${BACKUP_DIR}" ]; then
  echo "Backup directory not found: ${BACKUP_DIR}" >&2
  exit 1
fi

echo "Restoring keyspace '${KEYSPACE}' from ${BACKUP_DIR}"

for node_dir in "${BACKUP_DIR}"/pharos-cassandra-*; do
  [ -d "${node_dir}" ] || continue
  node="$(basename "${node_dir}")"

  if ! docker inspect -f '{{.State.Running}}' "${node}" >/dev/null 2>&1; then
    echo "  ${node}: WARNING -- container not running, skipping"
    continue
  fi

  echo "  ${node}: restoring..."
  for table_backup_dir in "${node_dir}"/*/; do
    table_name="$(basename "${table_backup_dir}")"

    # Find the live table directory by asking Cassandra for the table's
    # *current* id (system_schema.tables), not by globbing
    # <table_name>-* on disk: DROP TABLE does not delete the old
    # directory immediately (Cassandra leaves it as an orphan pending
    # background cleanup), so a table that was dropped and recreated
    # during this drill has TWO <table_name>-<uuid> directories on disk --
    # the stale pre-drop one and the new live one -- and a glob picking
    # "the first match" has no way to know which is current. Found by
    # restoring, seeing no errors, and then finding zero rows: the glob
    # had silently written into the orphaned directory nodetool refresh
    # never looks at.
    # Matched by pattern, not a fixed line number: cqlsh's output has a
    # leading blank line before the header (found by piping this to a
    # file and actually counting, not by eye -- a terminal render made it
    # easy to miscount and grab the "----" separator line instead).
    table_id=$(docker exec "${node}" sh -c "SSL_CERTFILE=/tls/ca-cert.pem cqlsh 127.0.0.1 9042 --ssl -e \"SELECT id FROM system_schema.tables WHERE keyspace_name='${KEYSPACE}' AND table_name='${table_name}';\" --no-color" 2>/dev/null | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | tr -d '-')
    if [ -z "${table_id}" ]; then
      echo "    WARNING: could not look up current table id for ${KEYSPACE}.${table_name} on ${node} -- is the table created? (run migrations first)"
      continue
    fi
    live_table_dir="/var/lib/cassandra/data/${KEYSPACE}/${table_name}-${table_id}"
    if ! docker exec "${node}" sh -c "[ -d '${live_table_dir}' ]"; then
      echo "    WARNING: no live table directory found for ${KEYSPACE}.${table_name} on ${node} -- is the keyspace/table created? (run migrations first)"
      continue
    fi

    # Copy every backed-up SSTable component into the live table
    # directory, then nodetool refresh to load it -- no restart needed.
    docker cp "${table_backup_dir}." "${node}:${live_table_dir}/" >/dev/null
    docker exec "${node}" nodetool refresh "${KEYSPACE}" "${table_name}" >/dev/null
    echo "    restored ${table_name}"
  done
done

echo "Restore complete. Verify with a query against the restored table(s)."
