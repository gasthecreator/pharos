---
status: accepted
date: 2026-08-31
decision-makers: Claude Code (scoping and build); Gideon Sanni (project owner)
---

# 0022. Property-based testing with rapid plus deterministic simulation

## Context and Problem Statement

Hand-written fault-injection scenarios cover only what a human thought to write down. Every real bug caught in review had been an interleaving nobody wrote a test for.

## Decision Drivers

- Explore state space automatically
- Reproduce any failure from a seed
- Reuse existing in-memory test doubles; do not rewrite the pipeline

## Considered Options

- `pgregory.net/rapid` state-machine properties (Stage A) plus a seeded simulation harness (Stage B) (chosen)
- `testing/quick` or native fuzzing
- More hand-written fault-injection tests

## Decision Outcome

Property tests in `internal/dedup` and `internal/consumer` check exactly one publish per key, a never-decreasing watermark and no duplicate late-arrival audits; CI sets `RAPID_CHECKS=5000`. `internal/simulation` runs the real `ingestion.Handler`, `Sweeper` and `consumer.Engine` against a simulated broker and clock (500 seeds x 150 ticks).

### Consequences

- The first generated sequences found two real bugs: no fencing on `MarkPublished`/`MarkDLQPublished` (fixed with an `expectedClaimedAt` check and `ErrClaimSuperseded`) and a terminal `REPLAYED` DLQ record that was steal-able in the in-memory store.
- The watermark property found nothing, a useful negative result.

### Confirmation

`TestSimulation_SameSeedIsDeterministic`, the property suites, and CI's `RAPID_CHECKS=5000` run.

## Pros and Cons of the Options

### `pgregory.net/rapid` state-machine properties (Stage A) plus a seeded simulation harness (Stage B) (chosen)

- Good, because generates operation sequences with shrinking; simulation swaps only I/O boundaries (`internal/clock`, in-memory broker) so a seed reproduces one interleaving
- Bad, because extra test infrastructure

### `testing/quick` or native fuzzing

- Good, because stdlib
- Bad, because no precedent here; rapid's `t.Repeat` fits "random sequences of operations" directly

### More hand-written fault-injection tests

- Good, because simple
- Bad, because covers a much smaller slice of the state space

## More Information

Evidence: PLAN.md Slice 22 (../../PLAN.md); worklog ["Slice 22 — Property-based & deterministic simulation testing"](../history/WORKLOG.md).
