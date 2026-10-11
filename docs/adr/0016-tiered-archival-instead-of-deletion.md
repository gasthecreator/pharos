---
status: accepted
date: 2026-09-05
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0016. File-based cold-tier archival instead of deletion

## Context and Problem Statement

Canonical and DLQ tables grow forever. The regulatory framing implies long retention of clinical safety data, so the problem is tiering inactive data, not deleting it.

## Decision Drivers

- Never delete clinical safety data
- Mirror the proven partition-key-first layout
- Zero cloud spend (local disk)
- Cold point lookups need not be fast

## Considered Options

- Gzip-JSONL files partitioned by key and month, with a hot-miss fallback in the query layer (chosen)
- Cassandra TTL
- Second Cassandra table/keyspace as cold tier
- SQLite/Cassandra index of key -> file offset

## Decision Outcome

`pharos-cli archive run [--older-than 90d] [--archive-dir] [--dry-run]` exports rows older than the threshold to `archive/by_study|by_site|dlq_by_site/<key>/<YYYY-MM>.jsonl.gz` and deletes from Cassandra only after the export is flushed (never delete-then-write). Range queries always merge the archive; point lookups consult it only after a hot miss. `event_outbox`/`pending_outbox` were excluded as operational bookkeeping.

### Consequences

- Outbox pruning was later added as a separate retention-policy decision (`RunOutboxPrune`; `PLAN.md` Slice 11 resolution) deleting bookkeeping rows outright.

### Confirmation

A flagship integration test seeds old data, archives for real, confirms deletion from Cassandra and confirms the data remains queryable via fallback; also exercised live through the CLI.

## Pros and Cons of the Options

### Gzip-JSONL files partitioned by key and month, with a hot-miss fallback in the query layer (chosen)

- Good, because same logical layout as the hot tier; no second source of truth
- Bad, because archived point lookups scan a site's files

### Cassandra TTL

- Good, because native expiry
- Bad, because TTL deletes data, violating the retention requirement

### Second Cassandra table/keyspace as cold tier

- Good, because uniform querying
- Bad, because still hot, always-on infrastructure with no added benefit

### SQLite/Cassandra index of key -> file offset

- Good, because O(1) cold lookups
- Bad, because a second source of truth that must stay consistent, for a non-critical path

## More Information

Evidence: [proposal "Slice 11"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md Slice 11 (../../PLAN.md); worklog ["Slice 11 — data retention & lifecycle (tiered archival)"](../history/WORKLOG.md).
