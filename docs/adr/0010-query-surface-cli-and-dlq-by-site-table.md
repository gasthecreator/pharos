---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0010. CLI-backed query surface, and a dedicated DLQ-by-site table instead of a secondary index

## Context and Problem Statement

Operators need to inspect canonical events and rejected submissions. The first Slice 5 draft indexed DLQ lookups with `CREATE INDEX dead_letter_site_idx ON dead_letter_events (site_id)`, contradicting the partition-key-first principle the project had established one slice earlier.

## Decision Drivers

- Zero-friction inspection of live state
- Consistency with the partition-key-first modeling principle
- Scalability as the number of sites grows

## Considered Options

- `pharos-cli` over a reusable `query.Service`, plus `dead_letter_events_by_site` table (chosen)
- Standalone HTTP API only
- Cassandra secondary index on `site_id`

## Decision Outcome

`pharos-cli` exposes `query study|site|event` and `dlq list|get` over `internal/query.Service`. DLQ site lookups use `dead_letter_events_by_site` (`PRIMARY KEY ((site_id), rejected_at, idempotency_key)`, newest first), written alongside `dead_letter_events` and updated on publish.

### Consequences

- The same `query.Service` later backs the web dashboard (ADR 0021).
- Review noted the by-site row can be momentarily stale after a lease steal until publish converges both tables.

### Confirmation

Reviewers checked the live cluster: `cqlsh` showed the by-site table and the absence of the index (migration `003_dlq_site_table.cql`).

## Pros and Cons of the Options

### `pharos-cli` over a reusable `query.Service`, plus `dead_letter_events_by_site` table (chosen)

- Good, because immediate demo and operations surface; site lookups hit one partition
- Bad, because extra write per rejection

### Standalone HTTP API only

- Good, because bindable to handlers
- Bad, because another process, ports and curl scripts to demo (the service layer can still back handlers)

### Cassandra secondary index on `site_id`

- Good, because no extra table
- Bad, because every query fans out to every node and degrades as cardinality grows

## More Information

Evidence: [proposal "Slice 5 Architecture"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.3/§2.4 (../../PLAN.md); worklog ["Claude Code review: Slice 5 sent back"](../history/WORKLOG.md) and ["Resolve Slice 5 review findings"](../history/WORKLOG.md).
