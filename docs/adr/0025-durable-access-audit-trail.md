---
status: accepted
date: 2026-09-06
decision-makers: Claude Code (scoping and build); Gideon Sanni (project owner)
---

# 0025. One durable access-audit trail for CLI and dashboard reads

## Context and Problem Statement

`query.Service` has no identity concept, so real access to adverse-event data carried no attributable identity. The design principle was to extend the existing late-arrival audit pattern rather than invent a second audit mechanism.

## Decision Drivers

- Attribute every view of clinical data to an operator
- One shared trail, not per-tool logs
- Do not claim authentication that does not exist

## Considered Options

- `internal/audit` `Store` (`RecordAccess`, `ListByOperator`) in Cassandra `access_audit_log`, required `--operator` on the CLI and a self-declared session-cookie operator on the dashboard (chosen)
- Gate every dashboard page behind a real login
- Leave the dashboard read path unaudited

## Decision Outcome

`access_audit_log` is partitioned by `operator`, clustered by a `timeuuid` descending. `pharos-cli query/dlq` require `--operator` and record every call (outcome derived from the underlying error). Dashboard reads redirect to `/operator` until a no-expiry session cookie is set, then record the same action strings. `safeNextPath` prevents open redirects. DLQ replay and event submission are audited by Central Ingestion because they proxy through its authenticated endpoint.

### Consequences

- The cookie is accountability, not access control (see `docs/security-threat-model.md`).

### Confirmation

`internal/audit` unit and Cassandra integration tests; `internal/dashboard/operator_test.go` covers redirects, cookie attributes and recording on failure.

## Pros and Cons of the Options

### `internal/audit` `Store` (`RecordAccess`, `ListByOperator`) in Cassandra `access_audit_log`, required `--operator` on the CLI and a self-declared session-cookie operator on the dashboard (chosen)

- Good, because one durable trail inspectable via `pharos-cli audit list`; errors are recorded too
- Bad, because identity is self-declared, not verified

### Gate every dashboard page behind a real login

- Good, because verified identity
- Bad, because the project has no identity system; deferred as hardening, with the cookie closing the accountability gap meanwhile

### Leave the dashboard read path unaudited

- Good, because no friction
- Bad, because an asymmetry with the CLI that left real views unattributed

## More Information

Evidence: PLAN.md Slices 20-21 (../../PLAN.md); worklog ["Slice 20 — Compliance / access-audit logging"](../history/WORKLOG.md) and ["Slice 21 — dashboard read-side access-audit logging"](../history/WORKLOG.md).
