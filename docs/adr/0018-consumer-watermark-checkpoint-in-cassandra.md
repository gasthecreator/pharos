---
status: accepted
date: 2026-09-05
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0018. Persist the consumer watermark as a periodic Cassandra checkpoint

## Context and Problem Statement

PLAN.md assumed the watermark could not regress across a process restart, but `WatermarkTracker` kept its guard state (`previousEmitted`, per-partition high watermark and last activity) in memory only. A restart zeroed the guard, and Kafka resumes from the last committed offset, so the exported watermark could visibly regress.

## Decision Drivers

- Make the monotonic guarantee true across crashes
- Avoid a write on every message
- Avoid scanning data-of-record tables

## Considered Options

- `consumer_watermark_checkpoints` row per consumer group, saved every 10s and on graceful shutdown, restored before consuming (chosen)
- Checkpoint on every message
- Derive from `canonical_events`

## Decision Outcome

`WatermarkTracker` gains `Snapshot()` and `Restore()` (initialization bypassing the guarded advance path). `cmd/pharos-consumer` restores at startup and saves on a 10-second ticker and at shutdown. All three fields are restored because idle/active classification depends on `partitionLastActivity`.

### Consequences

- Slice 19 later changed saves to merge per-partition (CQL map addition) and read-then-max for `previous_emitted` so multiple instances in one group cannot overwrite each other.

### Confirmation

A restart test restores the checkpoint and asserts no regression; multi-instance merging tested against `MemoryCanonicalStore` and real Cassandra.

## Pros and Cons of the Options

### `consumer_watermark_checkpoints` row per consumer group, saved every 10s and on graceful shutdown, restored before consuming (chosen)

- Good, because small purpose-built state using native collection types; bounded exposure
- Bad, because a crash can lose up to ~10s of watermark progress

### Checkpoint on every message

- Good, because no exposure window
- Bad, because extra Cassandra write on the hot path

### Derive from `canonical_events`

- Good, because no new table
- Bad, because no efficient per-Kafka-partition query; needs a full scan or `ALLOW FILTERING`

## More Information

Evidence: [proposal "Slice 13"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.4 and Slice 13/19 (../../PLAN.md); worklog ["Slice 13 — consumer crash/restart watermark continuity"](../history/WORKLOG.md).
