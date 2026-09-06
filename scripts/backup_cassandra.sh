#!/usr/bin/env bash
set -euo pipefail

# Pharos Cassandra backup (§2.4, PLAN.md Slice 18: Backup & disaster
# recovery). Takes a `nodetool snapshot` on every node (a fast, consistent,
# hard-linked point-in-time copy of the on-disk SSTables -- Cassandra's own
# standard backup primitive, not a custom one), then copies each node's
# snapshot out of its container into a host-side backup directory tagged
# with the same timestamp, so a restore doesn't depend on the original
# container/volume still existing.
#
# This backs up the pharos keyspace only (system keyspaces are recreated
# fresh by any new node/cluster bootstrap, per every earlier slice's own
# schema-bootstrap-on-connect pattern) across every node this project's
# docker-compose.yml actually runs (dc-us: cassandra-1/2/3, dc-eu:
# cassandra-4) -- restoring a subset of nodes' snapshots is legitimate for
# NetworkTopologyStrategy (each node only holds its own token ranges), so
# there's no need to coordinate a single-node snapshot differently from a
# whole-cluster one; this script always does the whole cluster.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEYSPACE="${PHAROS_BACKUP_KEYSPACE:-pharos}"
NODES="${PHAROS_BACKUP_NODES:-pharos-cassandra-1 pharos-cassandra-2 pharos-cassandra-3 pharos-cassandra-4}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SNAPSHOT_TAG="pharos-backup-${TIMESTAMP}"
BACKUP_DIR="${PHAROS_BACKUP_DIR:-${ROOT_DIR}/backup}/${TIMESTAMP}"

echo "Backing up keyspace '${KEYSPACE}' from [${NODES}] -> ${BACKUP_DIR}"
mkdir -p "${BACKUP_DIR}"

for node in ${NODES}; do
  echo "  ${node}: taking snapshot '${SNAPSHOT_TAG}'..."
  docker exec "${node}" nodetool snapshot -t "${SNAPSHOT_TAG}" "${KEYSPACE}" >/dev/null

  node_dir="${BACKUP_DIR}/${node}"
  mkdir -p "${node_dir}"

  # Snapshot layout: /var/lib/cassandra/data/<keyspace>/<table-uuid>/snapshots/<tag>/*
  # Copy each table's snapshot out, preserving the table directory name so
  # restore knows which table each set of SSTables belongs to.
  table_dirs=$(docker exec "${node}" sh -c "ls -d /var/lib/cassandra/data/${KEYSPACE}/*/snapshots/${SNAPSHOT_TAG} 2>/dev/null" || true)
  if [ -z "${table_dirs}" ]; then
    echo "  ${node}: WARNING -- no snapshot data found for keyspace ${KEYSPACE} (empty keyspace?)"
    continue
  fi
  while IFS= read -r table_snapshot_dir; do
    # Cassandra table directories are named <table_name>-<32-hex-char-uuid>;
    # strip that suffix so the backup is keyed by plain table name, not one
    # specific table incarnation's UUID -- a DROP+recreate (or a restore
    # into a fresh keyspace) gets a brand-new UUID, and restore_cassandra.sh
    # looks up the *current* live directory by table name, not by this
    # backup's now-stale one. Found by watching restore fail to match any
    # live table on the very first drill run, not anticipated up front.
    table_dir_name=$(basename "$(dirname "$(dirname "${table_snapshot_dir}")")")
    table_name=$(echo "${table_dir_name}" | sed -E 's/-[0-9a-f]{32}$//')
    dest="${node_dir}/${table_name}"
    mkdir -p "${dest}"
    docker cp "${node}:${table_snapshot_dir}/." "${dest}/" >/dev/null
  done <<< "${table_dirs}"

  # Clear the snapshot from the node's own disk now that it's safely
  # copied out -- nodetool snapshots are hard links (near-zero space when
  # taken), but they pin SSTables from being compacted away, so they
  # shouldn't accumulate indefinitely on the live nodes.
  docker exec "${node}" nodetool clearsnapshot -t "${SNAPSHOT_TAG}" >/dev/null

  echo "  ${node}: backed up $(find "${node_dir}" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ') table(s)"
done

echo "${SNAPSHOT_TAG}" > "${BACKUP_DIR}/SNAPSHOT_TAG"
echo "Backup complete: ${BACKUP_DIR}"
