---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0007. Edge forwarder retries with exponential backoff and full jitter

## Context and Problem Statement

PLAN.md required retry/backoff and allowed the edge to lag indefinitely without data loss, but did not fix the algorithm or parameters. Plain exponential backoff synchronizes retry waves across sites when Central Ingestion recovers.

## Decision Drivers

- Avoid thundering-herd retries after an outage
- Back off during multi-day partitions
- Respect server-directed throttling

## Considered Options

- Exponential backoff with full jitter (chosen)
- Constant retry interval
- Equal or decorrelated jitter

## Decision Outcome

`backoff = random_between(0, min(MaxBackoff, BaseBackoff * 2^attempts))` with `BaseBackoff` 500ms, `MaxBackoff` 30s, `BatchSize` 50, `PollInterval` 1s, `RequestTimeout` 5s. HTTP 429 `Retry-After` is honored (clamped 1s-60s); timeouts, connection refused and 5xx increment the attempt count.

### Consequences

- Parameters live in `internal/edge/forwarder.go`.
- Retries are safe because of idempotency keys (ADR 0004, ADR 0013).

### Confirmation

Forwarder unit tests and the asymmetric-partition fault-injection test (Central Ingestion completes a write but the edge never sees the response).

## Pros and Cons of the Options

### Exponential backoff with full jitter (chosen)

- Good, because best desynchronization at low average client latency
- Bad, because random delays are less predictable to reason about

### Constant retry interval

- Good, because simple
- Bad, because overloads a recovering service and does not back off across long partitions

### Equal or decorrelated jitter

- Good, because some desynchronization
- Bad, because full jitter performs better per the cited AWS architecture research

## More Information

Evidence: [proposal "Edge Forwarder Retry and Exponential Backoff with Full Jitter"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.1 (../../PLAN.md).
