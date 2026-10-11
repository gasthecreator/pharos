---
status: accepted
date: 2026-08-30
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0012. Three-node Cassandra and three-broker KRaft Kafka with LOCAL_QUORUM

## Context and Problem Statement

Single-node Cassandra and Kafka cannot demonstrate replication or node loss. The first overnight draft used three KRaft controllers and exposed one broker port; live debugging superseded it. The shipped design is what the status line approves.

## Decision Drivers

- Real replication (RF 3) and `LOCAL_QUORUM` consistency
- Kafka clients must reach every partition leader
- Reliable startup on a memory-constrained host

## Considered Options

- 3-node Cassandra (RF 3), 3-broker Kafka with a single KRaft controller (chosen)
- Three KRaft controllers (first draft)
- Expose only broker 1 (first draft)
- `QUORUM` instead of `LOCAL_QUORUM`

## Decision Outcome

Cassandra nodes `datacenter1` with `GossipingPropertyFileSnitch`, sequential `depends_on: service_healthy`, `MAX_HEAP_SIZE` 256M plus `-XX:MaxDirectMemorySize=256M`; Kafka brokers with one controller voter, every broker advertising its own `EXTERNAL://127.0.0.1:<port>`, topics at RF 3 with `min.insync.replicas=2`; health checks with JVM-scale timeouts. Application stores use `LOCAL_QUORUM`; LWTs keep `LOCAL_SERIAL`.

### Consequences

- The real OOM root cause was a concurrently running Kubernetes cluster from another project consuming the shared Docker VM; this is a host-capacity constraint, not a defect.
- Topology was later reshaped by ADR 0019.

### Confirmation

Checked against the live cluster: `nodetool status` all `UN`; `kafka-topics.sh --describe` showed ISR covering all brokers; full test suite passed twice.

## Pros and Cons of the Options

### 3-node Cassandra (RF 3), 3-broker Kafka with a single KRaft controller (chosen)

- Good, because stable elections; all brokers are data-plane members with their own host port
- Bad, because controller is a single point of metadata

### Three KRaft controllers (first draft)

- Good, because controller redundancy
- Bad, because the default election timeout kept expiring under CPU contention, a live-lock of candidate elections that never converged

### Expose only broker 1 (first draft)

- Good, because mirrors Cassandra's single contact point
- Bad, because Kafka clients dial each partition leader's advertised address directly, so unreachable brokers silently stall writes

### `QUORUM` instead of `LOCAL_QUORUM`

- Good, because same guarantee in one DC
- Bad, because `LOCAL_QUORUM` is correct once real datacenters exist (ADR 0019) and has lower latency

## More Information

Evidence: [proposal "Slice 7 Architecture"](../history/ARCHITECTURE_PROPOSALS.md) (approved with corrections); PLAN.md Slice 7 (../../PLAN.md); worklog ["Claude Code review + finish: Phase 2 Slice 7"](../history/WORKLOG.md).
