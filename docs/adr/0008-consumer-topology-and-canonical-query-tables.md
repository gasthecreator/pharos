---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0008. Dedicated consumer binary and purpose-built canonical Cassandra query tables

## Context and Problem Statement

After the outbox publishes to Kafka, events must land in a durable, queryable store. The two named query patterns are "all events for trial X in a date range" and "all events from site Z"; Cassandra performs only with partition-key-first modeling. `event_outbox` was designed for transactional publishing, not analytical querying.

## Decision Drivers

- Scale ingestion (HTTP-bound) independently from consumption (partition-bound)
- Fast range queries by study and by site
- Exactly-once effect downstream despite Kafka redelivery
- Avoid coordinator overhead for guarantees the consumer does not need

## Considered Options

- Separate `pharos-consumer` binary plus three query tables, written with concurrent idempotent upserts (chosen)
- Query `event_outbox` directly
- CQL logged batches across the three tables
- Secondary indexes on one table

## Decision Outcome

`pharos-consumer` (group `pharos-canonical-sink`, manual commit after durable write) writes `canonical_events` (key `idempotency_key`), `events_by_study` (`((study_id), event_time, idempotency_key)`, newest first) and `events_by_site` (`((site_id), local_seq, idempotency_key)`) as concurrent upserts at `LOCAL_QUORUM`; the Kafka offset is committed only after all three succeed. `study_id` comes from `study[0].reference`, defaulting to `UNKNOWN_STUDY`.

### Consequences

- Redelivered messages rewrite identical rows, so no duplicates.
- A fourth table, `events_recent`, was added later for the dashboard feed.

### Confirmation

Integration tests assert single-row idempotency on redelivery and per-site ordering against real Kafka and Cassandra.

## Pros and Cons of the Options

### Separate `pharos-consumer` binary plus three query tables, written with concurrent idempotent upserts (chosen)

- Good, because isolates failures and scaling; each query pattern has its own partition key; redelivery is harmless
- Bad, because three writes per event instead of one

### Query `event_outbox` directly

- Good, because no extra tables
- Bad, because requires full-table scans for study/site queries

### CQL logged batches across the three tables

- Good, because atomic-looking
- Bad, because heavy coordinator batch-log replication across different partition keys for a guarantee the consumer does not need

### Secondary indexes on one table

- Good, because one table
- Bad, because fan out to every node and degrade with cardinality

## More Information

Evidence: [proposal "Slice 4 Architecture"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.4 (../../PLAN.md); worklog ["Implement Slice 4"](../history/WORKLOG.md).
