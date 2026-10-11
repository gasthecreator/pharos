---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0009. Event-time watermark with idle-partition exclusion and a monotonic guard

## Context and Problem Statement

There is no single global order across sites in different time zones. A naive watermark `min(T_p) - L` freezes for everyone if one site is offline for days. The first proposal was sent back because the watermark starves in exactly that scenario; the revision then regressed when an idle partition reawakened with an older backlog.

## Decision Drivers

- Completeness signal must keep moving when a site is offline
- Never regress when a site reconnects with backlogged data
- Late data must be kept and audited, never dropped (patient safety)
- Past reports must not be silently rewritten (21 CFR Part 11 auditability)

## Considered Options

- Idle-aware minimum with an outer `max(previous_emitted, candidate)` and a `COMPLETE` -> `REVISED` window lifecycle (chosen)
- Static watermark without idle detection
- Discard late data on reconnected partitions
- Silently modify closed windows

## Decision Outcome

For each partition track the highest event time `T_p` and last-activity wall clock `U_p`; a partition is idle after `IdleTimeout` (default 10m). `W_candidate = min over active partitions of T_p, minus lateness L`, or `max T_p - L` if all are idle; the emitted watermark is `max(W_previous_emitted, W_candidate)`. A window with `W >= t2` becomes `COMPLETE`; late records are stored with `is_late = true` and the window becomes `REVISED` with a deduplicated `LateArrivalAudit` entry.

### Consequences

- Watermark state needed crash persistence (ADR 0018) and multi-instance merging (see Slice 19 in PLAN.md).
- The late-arrival audit was later made durable (`consumer_late_arrival_audits`).

### Confirmation

`internal/consumer` unit tests including the exact reawakening-with-backlog scenario, property tests over generated event sequences (ADR 0022), and real end-to-end tests.

## Pros and Cons of the Options

### Idle-aware minimum with an outer `max(previous_emitted, candidate)` and a `COMPLETE` -> `REVISED` window lifecycle (chosen)

- Good, because progresses despite offline sites, monotonic by construction, preserves what was originally reported
- Bad, because more state to track per partition

### Static watermark without idle detection

- Good, because simple
- Bad, because a single offline site freezes the global watermark indefinitely

### Discard late data on reconnected partitions

- Good, because keeps windows closed
- Bad, because violates the clinical-safety premise of never dropping adverse events

### Silently modify closed windows

- Good, because no status churn
- Bad, because violates regulatory auditability

## More Information

Evidence: [proposal "Slice 4 Architecture"](../history/ARCHITECTURE_PROPOSALS.md) (status: one required revision, then cleared to implement); PLAN.md §2.4 (../../PLAN.md); worklog ["Claude Code review: Slice 4 proposal sent back"](../history/WORKLOG.md) and ["...Slice 4 watermark almost there"](../history/WORKLOG.md).
