---
status: accepted
date: 2026-08-31
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0013. Encode a per-instance epoch into `local_seq` to survive edge disk replacement

## Context and Problem Statement

The idempotency key is `site_id:local_seq`, and `local_seq` came only from the edge's local `site_sequence` table. Replacing a failed disk under the same `--site-id` restarts the counter at 1; Central Ingestion then treats the new event as an already-published duplicate and silently drops a genuinely new adverse event.

## Decision Drivers

- Zero operator action during a hardware failure
- No change to the wire contract or downstream parsing
- `local_seq` is stored as a signed 64-bit Cassandra `bigint`

## Considered Options

- Epoch in the high bits of `local_seq`: `(instance_epoch << 32) | counter` (chosen)
- Three-part key `site_id:instance_id:local_seq`
- Ask Central Ingestion for the last-known sequence at startup
- Manual operator bootstrap

## Decision Outcome

A new `instance_epoch INTEGER NOT NULL DEFAULT 0` column is stamped only when a site's row is first inserted into a database file, as minutes since 2026-01-01 UTC; the key's `local_seq` is `(instance_epoch << 32) | counter` (31 + 32 = 63 bits, so the sign bit is never set). Minutes-since-2026 give roughly 4,083 years of headroom versus raw Unix seconds overflowing in 2038.

### Consequences

- `ParseIdempotencyKey` and every consumer treat `local_seq` as an opaque `uint64`.
- A site's keys now look like large numbers in `pharos-cli dlq list`.

### Confirmation

A fault-injection test replaces the edge's SQLite file mid-stream and asserts new events get `new_claim`, never `duplicate_hit`.

## Pros and Cons of the Options

### Epoch in the high bits of `local_seq`: `(instance_epoch << 32) | counter` (chosen)

- Good, because collisions are structurally impossible; touches only `sqlite_store.go`; legacy rows (epoch 0) are numerically unchanged
- Bad, because keys for new sites are large opaque numbers

### Three-part key `site_id:instance_id:local_seq`

- Good, because explicit
- Bad, because ambiguous with the existing last-colon parsing and would require dual-format parsing everywhere

### Ask Central Ingestion for the last-known sequence at startup

- Good, because detects resets
- Bad, because needs a new API, creates a race, and requires a policy for mismatches

### Manual operator bootstrap

- Good, because simple
- Bad, because manual steps get skipped under the pressure of a real failure

## More Information

Evidence: [proposal "Slice 8"](../history/ARCHITECTURE_PROPOSALS.md) (authored directly by Claude Code); PLAN.md §2.2 (../../PLAN.md); worklog ["Slice 8 implementation verified and closed out"](../history/WORKLOG.md).
