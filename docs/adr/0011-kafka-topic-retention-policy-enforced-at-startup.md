---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0011. Kafka retention of 7 days (main) and 14 days (DLQ), enforced at startup

## Context and Problem Statement

Kafka is the streaming backbone between ingestion and durable persistence, not permanent storage (Cassandra is). The retention values were documented in a Go file but never applied; the review found `kafka-configs.sh --describe` returned zero dynamic configs on the live broker.

## Decision Drivers

- Buffer for consumer maintenance and replay
- Time for clinical data managers to investigate rejections
- Unbounded broker disk is unacceptable
- A decision written in code but not applied is not a decision

## Considered Options

- `pharos.events.adverse` 7 days / 10 GB per partition; `pharos.events.dlq` 14 days / 5 GB, applied by `EnsureTopics()` (chosen)
- Permanent Kafka retention
- 24-hour retention

## Decision Outcome

Retention is `retention.ms=604800000`/`retention.bytes=10737418240` on the main topic and `1209600000`/`5368709120` on the DLQ topic, created or reconciled by `EnsureTopics()` (`internal/kafka/topics.go`) at ingestion and consumer startup and by `scripts/create_topics.sh`. Rejected records stay in Cassandra `dead_letter_events` indefinitely.

### Consequences

- Kubernetes provisions the same values in `deploy/k8s/04-migrations-job.yaml`.

### Confirmation

Verified against the running broker with `kafka-configs.sh --describe` after the fix, not only by reading the source.

## Pros and Cons of the Options

### `pharos.events.adverse` 7 days / 10 GB per partition; `pharos.events.dlq` 14 days / 5 GB, applied by `EnsureTopics()` (chosen)

- Good, because 7 days aligns with expedited-reporting windows (21 CFR 312.32(c)(2)) and covers holiday-weekend outages; DLQ gets double the operational buffer
- Bad, because consumers that are down longer than the window lose replay ability (Cassandra still holds the record)

### Permanent Kafka retention

- Good, because never loses a message
- Bad, because wasteful; Cassandra is already the long-term record store

### 24-hour retention

- Good, because small
- Bad, because a Friday-evening consumer failure could cause unrecoverable loss by Monday

## More Information

Evidence: [proposal "Slice 5 Architecture"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §4 (../../PLAN.md); worklog ["Resolve Slice 5 review findings"](../history/WORKLOG.md).
