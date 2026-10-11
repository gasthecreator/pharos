---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0004. Deduplicate with Cassandra lightweight transactions and a transactional outbox

## Context and Problem Statement

Retries are guaranteed, so a duplicate submission must never become a second record, and a crash must never silently drop one. The first proposal inserted a dedup key and then published to Kafka as two steps, which has a fatal gap: if the insert succeeds and the process dies before publishing, every retry looks like an already-processed duplicate and the event is lost forever.

## Decision Drivers

- Exactly-once effect under retries and crashes
- A dedup record that outlives long network partitions
- Keep the local stack to Kafka plus Cassandra
- Match the store-then-forward shape already used at the edge

## Considered Options

- Cassandra LWT `INSERT ... IF NOT EXISTS` as a transactional outbox (chosen)
- Redis with TTL
- In-memory Go map
- Insert dedup key, then publish (original proposal)

## Decision Outcome

Dedup is a Cassandra outbox: insert the event row with its idempotency key and an unpublished state in one LWT statement (the `IF NOT EXISTS` makes a retry a no-op); a separate retriable step publishes to Kafka and then marks the row published. The mechanism is detailed in ADR 0005.

### Consequences

- PLAN.md §2.2 was written with the outbox pattern, not the original check-then-publish text.
- A `DedupStore` interface stays pluggable. A later load test measured the Paxos insert as the dominant latency cost (`docs/benchmark-results.md`).

### Confirmation

Fault-injection tests crash between claim and publish and assert exactly one Kafka publish per key; property tests (ADR 0022) check the same invariant over generated sequences.

## Pros and Cons of the Options

### Cassandra LWT `INSERT ... IF NOT EXISTS` as a transactional outbox (chosen)

- Good, because the insert is the dedup check, and an interrupted publish remains resumable; permanent and auditable
- Bad, because LWTs cost more than plain writes (Paxos round trip)

### Redis with TTL

- Good, because extremely fast (<1ms)
- Bad, because keys must expire, so a partitioned site reconnecting after the TTL could be processed as a duplicate

### In-memory Go map

- Good, because trivial
- Bad, because lost on restart and ineffective across multiple ingestion replicas

### Insert dedup key, then publish (original proposal)

- Good, because simple
- Bad, because crash between the two steps silently drops the event permanently

## More Information

Evidence: [proposal "Dedup Store Choice"](../history/ARCHITECTURE_PROPOSALS.md) (approved with a required modification); PLAN.md §2.2 (../../PLAN.md).
