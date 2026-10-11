---
status: accepted
date: 2026-08-31
decision-makers: Claude Code (scoping and build); Gideon Sanni (project owner)
---

# 0023. Chaos Control Panel gated behind an explicit opt-in flag

## Context and Problem Statement

"The system survives a network partition" was a claim a reader had to take on faith. A panel that causes real failures makes it observable, but it mutates real infrastructure and the dashboard has no authentication.

## Decision Drivers

- Make resilience watchable
- Never let a reachable dashboard silently allow stopping a database node
- Expose only failure modes the project already knows how to cause

## Considered Options

- Dashboard-driven chaos actions, disabled unless `--enable-chaos` is passed (chosen)
- Always-on chaos actions
- Reuse the test-only partition transport

## Decision Outcome

`internal/chaos` stops/starts Cassandra containers and partitions `dc-us`/`dc-eu` with the proven `tc netem` mechanism (region partitions auto-heal after 60 seconds); site partition uses the edge admin routes; duplicate and clock-skew reuse the submission proxy (data-level skew, not OS clock manipulation). A Correctness Ledger reads existing `/metrics` endpoints. `--enable-chaos` defaults to false everywhere and logs a warning when set.

### Consequences

- The panel's actual safety boundary is not passing the flag outside local/demo instances; this is stated in `docs/security-threat-model.md`.
- The `internal/chaos` tests run as a separate CI step because they disrupt the cluster.

### Confirmation

`internal/chaos` real-Docker tests and dashboard handler tests asserting every action refuses when disabled.

## Pros and Cons of the Options

### Dashboard-driven chaos actions, disabled unless `--enable-chaos` is passed (chosen)

- Good, because observable demo with an explicit safety guard enforced per action handler
- Bad, because the only gate is the flag, not an in-app login

### Always-on chaos actions

- Good, because no setup
- Bad, because anyone able to reach the dashboard could kill a Cassandra node

### Reuse the test-only partition transport

- Good, because no new code
- Bad, because it is an in-process test double with no hook a running edge could expose; a real `edge.ChaosClient` plus `/admin/chaos/*` routes behind the edge's own `--enable-chaos` was built instead

## More Information

Evidence: PLAN.md Slice 23 (../../PLAN.md); worklog ["Slice 23 — Chaos Control Panel (final scoped slice)"](../history/WORKLOG.md).
