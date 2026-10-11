---
status: accepted
date: 2026-09-04
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0014. Explicit `schemaVersion` stamped at the edge, validated through a dispatch map

## Context and Problem Statement

Edge binaries at different sites will be upgraded at different times over years. Without an explicit version, a site's events silently claim whatever Central Ingestion's current default is, and a future v2 would force an invasive change.

## Decision Drivers

- Backward compatibility with every event already captured
- Adding v2 should be a pure addition
- The version must reflect what the submitting binary understood at capture time

## Considered Options

- `schemaVersion` field stamped by the edge; `Validate()` dispatches via a `map[int]func(*AdverseEvent) error` (chosen)
- Bare `if version > 1 { reject }` check
- Dedicated `schema_version` Cassandra/DLQ column

## Decision Outcome

`model.AdverseEvent` gains `schemaVersion int` (`omitempty`), stamped by `SQLiteStore.Enqueue` with `model.CurrentSchemaVersion` unless already set. `Validate()` looks up the validator by version; absent means `SchemaVersionV1`; anything else returns `ErrUnsupportedSchemaVersion`, which is dead-lettered like any rejection.

### Consequences

- No Cassandra schema or consumer change.
- Forms the basis of `docs/upgrade-and-compat-policy.md`.

### Confirmation

Model tests for dispatch and the default; the DLQ path is unchanged and already tested.

## Pros and Cons of the Options

### `schemaVersion` field stamped by the edge; `Validate()` dispatches via a `map[int]func(*AdverseEvent) error` (chosen)

- Good, because absent/zero defaults to v1 so nothing is retroactively invalidated; unknown versions flow into the existing DLQ path with a typed error
- Bad, because a dispatch map with a single entry today

### Bare `if version > 1 { reject }` check

- Good, because simplest today
- Bad, because becomes a dispatch table the moment v2 exists, so the simplicity was only deferred

### Dedicated `schema_version` Cassandra/DLQ column

- Good, because queryable
- Bad, because duplicates data already preserved in the verbatim `payload` column

## More Information

Evidence: [proposal "Slice 9"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.3 (../../PLAN.md); worklog ["Slice 9 — wire-format schema versioning"](../history/WORKLOG.md).
