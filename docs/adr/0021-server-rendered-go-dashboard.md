---
status: accepted
date: 2026-08-31
decision-makers: Claude Code (scoping and build); Gideon Sanni (project owner)
---

# 0021. Server-rendered Go dashboard instead of a JavaScript frontend

## Context and Problem Statement

Everything was operated via `curl`, `pharos-cli` and Grafana. A non-technical viewer without a terminal needs a browser page, but a web frontend proves nothing about partition tolerance that the CLI and fault-injection suite do not already prove better.

## Decision Drivers

- Keep the "Go, Kafka, Cassandra" story and a single toolchain
- Reuse the already-tested query logic
- Not a production-hardening item

## Considered Options

- `pharos-dashboard`: stdlib `html/template` + `net/http` over `internal/query.Service` (chosen)
- React/Next.js frontend

## Decision Outcome

A `pharos-dashboard` binary renders a recent-events feed, DLQ view with replay, site/study/event query, a test-event form posting to Central Ingestion, and a Grafana link-out. A new `events_recent` table (bucketed by the UTC hour of `ConsumedAt`) answers "what came in recently across all sites" without `ALLOW FILTERING`. Read views were later gated by a self-declared operator cookie feeding the audit trail (ADR 0025).

### Consequences

- The dashboard has no authentication of its own; network reachability is the access boundary (see `docs/security-threat-model.md`).

### Confirmation

Handler tests in `internal/dashboard` and a live run through `scripts/demo.sh`.

## Pros and Cons of the Options

### `pharos-dashboard`: stdlib `html/template` + `net/http` over `internal/query.Service` (chosen)

- Good, because a presentation layer over existing logic; no Node toolchain
- Bad, because limited client-side interactivity

### React/Next.js frontend

- Good, because richer interactivity
- Bad, because adds an npm/Node build and dependency system that dilutes the project's single-toolchain story

## More Information

Evidence: PLAN.md Slice 21 (../../PLAN.md); worklog ["Slice 21 — Web dashboard"](../history/WORKLOG.md) and ["scoped Slice 14 — web dashboard"](../history/WORKLOG.md).
