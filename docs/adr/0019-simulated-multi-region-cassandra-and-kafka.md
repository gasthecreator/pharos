---
status: accepted
date: 2026-09-05
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0019. Simulated multi-region topology: dc-us and dc-eu with MirrorMaker 2

## Context and Problem Statement

Multi-region behavior needed to be proven without real geographic infrastructure, on a host whose Docker VM is capped near 6.3GB. A real correctness gap also surfaced: no gocql config used DC-aware host selection, so a `dc-us` service could coordinate `LOCAL_QUORUM` through a `dc-eu` node.

## Decision Drivers

- Prove regional-partition tolerance with real software
- Correct `LOCAL_QUORUM` semantics across DCs
- Fit the memory budget
- No live migration procedure was in scope

## Considered Options

- `NetworkTopologyStrategy` across `dc-us`/`dc-eu`; independent Kafka clusters A and B with one-way MirrorMaker 2; `tc netem` for latency and partition (chosen)
- Bidirectional MirrorMaker 2
- In-place `SimpleStrategy` to `NetworkTopologyStrategy` migration
- Docker network disconnect for partitions

## Decision Outcome

Keyspace `NetworkTopologyStrategy` with DC-aware, token-aware gocql host selection (`LocalDC`, default `dc-us`) in dedup, consumer and query. `dc-eu` ultimately shrank to one node/RF=1 and one broker as memory evidence accumulated (RF=2 on a single node would be under-replicated by construction). MirrorMaker 2 is started last, after topics exist, because starting it concurrently caused repeated OOM kills.

### Consequences

- Cassandra heap ended at 176M after a 128M setting produced flaky LWT races; race test retries only the 0-winner case.
- Final shipped topology is 4 Cassandra nodes and 4 Kafka brokers.

### Confirmation

`internal/faultinjection/regional_partition_test.go` proves no loss or duplication across a `tc`-induced partition; confirmed OOM kills via `docker inspect`.

## Pros and Cons of the Options

### `NetworkTopologyStrategy` across `dc-us`/`dc-eu`; independent Kafka clusters A and B with one-way MirrorMaker 2; `tc netem` for latency and partition (chosen)

- Good, because real DCs, rack-aware brokers, one tool for steady-state latency and partitions
- Bad, because `dc-eu` had to shrink to fit memory

### Bidirectional MirrorMaker 2

- Good, because symmetric
- Bad, because needs loop-prevention and renaming for a property not needed

### In-place `SimpleStrategy` to `NetworkTopologyStrategy` migration

- Good, because exercises migration
- Bad, because a delicate separate procedure; volumes held only test data

### Docker network disconnect for partitions

- Good, because simple
- Bad, because a second mechanism beside `tc`

## More Information

Evidence: [proposal "Slice 14"](../history/ARCHITECTURE_PROPOSALS.md) and addenda in ["Slice 15"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md Slice 14 (../../PLAN.md); worklog ["Slice 14 — multi-region Cassandra + Kafka (simulated)"](../history/WORKLOG.md).
