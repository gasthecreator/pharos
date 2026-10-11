---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0005. Three-state LWT claim/lease for the outbox and a symmetric DLQ path

## Context and Problem Statement

With a plain outbox flag, two concurrent requests (or a retry racing a slow first attempt) could both observe "not published" and both publish to Kafka. A crashed claimant could also leave a row stuck forever. The first Slice 3 draft was sent back for exactly this concurrent-publish race.

## Decision Drivers

- Exactly one Kafka publish per idempotency key
- Deterministic recovery from a crashed publisher
- The rejection (DLQ) path must be as crash-safe as the accept path
- Store raw wire payloads without re-serialization

## Considered Options

- Three-state claim (`PUBLISHING` -> `PUBLISHED`) won via LWT, with a `claimed_at` lease (chosen)
- In-process mutex only
- Outbox without a lease timeout

## Decision Outcome

`event_outbox` rows are inserted with `status='PUBLISHING'` and `claimed_at` via `IF NOT EXISTS`; only the winner publishes to Kafka (partition key `site_id`, `RequiredAcks: all`) and finalizes with a conditional update. An expired lease (default 30s) is stolen with `IF status='PUBLISHING' AND claimed_at=?`. A background sweeper over `pending_outbox` reclaims abandoned claims. Rejected events use the same pattern in `dead_letter_events`, written before the edge is told "rejected". Payloads are stored as raw JSON bytes. LWT uses `LOCAL_SERIAL`; reads/writes use `LOCAL_QUORUM`.

### Consequences

- The design review noted `PENDING` is vestigial because the winning insert is the claim.
- Later property testing found that finalization needed fencing on `claimed_at` (`ErrClaimSuperseded`), see ADR 0022.

### Confirmation

Real-Cassandra tests cover the crash window, sequential and concurrent duplicates (asserting exactly one Kafka publish), stale-lease reclamation, DLQ durability and raw-payload preservation.

## Pros and Cons of the Options

### Three-state claim (`PUBLISHING` -> `PUBLISHED`) won via LWT, with a `claimed_at` lease (chosen)

- Good, because the winner of the LWT insert holds an exclusive claim; stale leases are stolen with a conditional update; a sweeper is the backstop
- Bad, because more states and a lease to reason about

### In-process mutex only

- Good, because cheap
- Bad, because unsafe across multiple Central Ingestion instances (kept only as an optimization to save Paxos round trips)

### Outbox without a lease timeout

- Good, because simple
- Bad, because rows stuck in `PUBLISHING` forever after a worker crash

## More Information

Evidence: [proposal "Slice 3 Architecture"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.2/§2.3 (../../PLAN.md); worklog ["Claude Code review: Slice 3 proposal sent back for revision"](../history/WORKLOG.md) and ["Slice 3 architecture revised"](../history/WORKLOG.md).
