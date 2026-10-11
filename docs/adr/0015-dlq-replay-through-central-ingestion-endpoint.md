---
status: accepted
date: 2026-09-04
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0015. DLQ replay through a server-side Central Ingestion endpoint

## Context and Problem Statement

Rejected events are inspectable but not recoverable. A replay path must re-run validation honestly, must not corrupt the original record on failure, and must respect the project boundary that Central Ingestion owns every outbox/DLQ write while `pharos-cli` is a read path.

## Decision Drivers

- Keep Central Ingestion as the sole writer
- One shared implementation of validate-claim-publish, not two drifting copies
- Preserve the rejection audit trail

## Considered Options

- `POST /api/v1/dlq/{key}/replay` reusing `processOneEvent`; CLI is a thin HTTP client (chosen)
- CLI fetches the payload and POSTs it to `/api/v1/events`

## Decision Outcome

A successful replay moves the original record from `PUBLISHED` to `REPLAYED` via the same CAS pattern as `MarkDLQPublished`, preserving `rejection_reason` and the stored payload. A failed replay structurally cannot alter the original because `InsertDLQClaim` against an existing key is a no-op. `HandleEvents`'s loop was extracted into `processOneEvent` first, behavior-identical, so both callers share it.

### Consequences

- `EnsureSchema` gained an idempotent column check (`system_schema.columns`) so `replayed_at` reaches already-bootstrapped keyspaces.
- Verification also exposed that `pharos-cli`'s global flags were never parsed; fixed in the same slice.

### Confirmation

Verified live: replaying a still-invalid payload returned the identical rejection and left the record untouched; correcting the stored payload then replayed successfully and marked `REPLAYED`.

## Pros and Cons of the Options

### `POST /api/v1/dlq/{key}/replay` reusing `processOneEvent`; CLI is a thin HTTP client (chosen)

- Good, because single fault-injection-tested code path; CLI never mutates Cassandra
- Bad, because CLI now needs the site's credentials and `--central-url`

### CLI fetches the payload and POSTs it to `/api/v1/events`

- Good, because no new endpoint
- Bad, because cannot mark the original DLQ record `REPLAYED` without a second direct Cassandra write, violating the write boundary

## More Information

Evidence: [proposal "Slice 10"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.3 (../../PLAN.md); worklog ["Slice 10 — DLQ replay & reprocessing"](../history/WORKLOG.md).
