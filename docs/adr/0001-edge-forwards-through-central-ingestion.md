---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0001. Edge collectors forward through Central Ingestion, not directly to Kafka

## Context and Problem Statement

PLAN.md described forwarding "to the central Kafka backbone" in one place and an insert-if-not-exists dedup step plus per-site rate limiting and FHIR validation "at the central ingestion service" in others. Those cannot all be true if edge collectors publish to Kafka themselves. The question: what does an edge collector talk to across the WAN?

## Decision Drivers

- Hospital firewalls frequently block raw Kafka ports
- Exposing Kafka brokers over the WAN widens the attack surface
- Per-site rate limiting, schema validation and dedup must happen before anything is enqueued

## Considered Options

- Edge forwards over HTTPS to a Central Ingestion API (chosen)
- Edge connects directly to Kafka brokers over the WAN with SASL/TLS
- Kafka REST Proxy (Confluent-style)

## Decision Outcome

The edge collector forwards queued batches to Central Ingestion via `POST /api/v1/events`. Central Ingestion rate-limits, validates, deduplicates, and only then publishes to Kafka (valid events) or the DLQ (rejected events). It is the only service that talks to Kafka on the write path.

### Consequences

- PLAN.md §2.1/§3/§4 wording was clarified to "forwarding to Central Ingestion API with retry/backoff".
- Central Ingestion becomes a required, scalable component (see ADR 0024 for multi-instance operation).

### Confirmation

The wire contract is exercised by the fault-injection suite (`internal/faultinjection`) against a real edge, real Central Ingestion, real Cassandra and real Kafka, and documented in `docs/api/ingestion-openapi.yaml`.

## Pros and Cons of the Options

### Edge forwards over HTTPS to a Central Ingestion API (chosen)

- Good, because one choke point enforces rate limits, validation and dedup before Kafka, and only HTTPS crosses the WAN
- Bad, because adds a service that must itself be available and scaled

### Edge connects directly to Kafka brokers over the WAN with SASL/TLS

- Good, because no extra hop
- Bad, because blocked by hospital firewalls, exposes brokers, and bypasses rate limiting and pre-Kafka dedup

### Kafka REST Proxy (Confluent-style)

- Good, because exposes an HTTP endpoint in front of Kafka
- Bad, because bypasses the custom FHIR validation, per-site token buckets and application-level dedup

## More Information

Evidence: [proposal "Clarify Ingress Topology"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.1 (../../PLAN.md); worklog ["Architecture Review, Requirements Re-Derivation, and Ingress/Storage Proposals"](../history/WORKLOG.md).
