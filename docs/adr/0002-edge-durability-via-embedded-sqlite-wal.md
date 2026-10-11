---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0002. Edge collector durability via embedded SQLite in WAL mode

## Context and Problem Statement

A trial site can lose connectivity for hours or days and must not lose or corrupt captured events. The edge collector needs a local durable queue with ordered pending records, state transitions and atomic sequence numbers, deployable as one binary on a resource-constrained site workstation.

## Decision Drivers

- Single-binary deployment with no external daemons
- ACID durability across crashes and power loss
- FIFO ordered pending records and status tracking without hand-written indexing
- Small footprint on hospital workstations

## Considered Options

- Embedded SQLite in WAL mode (chosen)
- Custom append-only file WAL
- Embedded key-value store (bbolt, Pebble, Badger)
- Embedded Redpanda/Kafka at the edge

## Decision Outcome

`pharos-edge` is a standalone Go binary per site using embedded SQLite with `PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;` as its durable queue. It exposes a local HTTP capture endpoint, persists records as `PENDING` with an atomic local sequence number, and a background forwarder streams batches onward.

### Consequences

- Resolved the then-open PLAN.md questions on edge storage and queue contract.
- The pure-Go `modernc.org/sqlite` driver keeps the binary cgo-free.
- One file is a single point of local loss; ADR 0013 and ADR 0017 harden that.

### Confirmation

`internal/edge` tests and the network-partition fault-injection test show zero loss while partitioned.

## Pros and Cons of the Options

### Embedded SQLite in WAL mode (chosen)

- Good, because transactional, crash-resilient, atomic sequence generation and status tracking come for free, single binary
- Bad, because a single un-replicated file per site (addressed later, see ADR 0017)

### Custom append-only file WAL

- Good, because very lightweight
- Bad, because requires hand-implementing indexing, crash recovery, compaction and retry-state tracking

### Embedded key-value store (bbolt, Pebble, Badger)

- Good, because good for key-value access
- Bad, because FIFO queries over pending records and state transitions need extra indexing SQLite handles natively

### Embedded Redpanda/Kafka at the edge

- Good, because a real log
- Bad, because overkill for an edge agent; too much RAM and deployment complexity on hospital workstations

## More Information

Evidence: [proposal "Edge Collector Local Durability via Embedded SQLite in WAL Mode"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.1 (../../PLAN.md); worklog ["Slice 1 built"](../history/WORKLOG.md).
