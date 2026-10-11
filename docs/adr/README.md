# Architecture Decision Records

Significant or hard-to-reverse decisions are recorded here as short
[MADR](https://adr.github.io/madr/)-style records: the context, the real
alternatives considered, the decision, and how it is confirmed. They are
distilled from the dated proposal/review trail in
[`../history/ARCHITECTURE_PROPOSALS.md`](../history/ARCHITECTURE_PROPOSALS.md)
(and, for later slices, the slice write-ups in [`../../PLAN.md`](../../PLAN.md)),
and each one links to that evidence and the matching
[`../history/WORKLOG.md`](../history/WORKLOG.md) entry. The history files
remain the unedited source of record; the ADRs are the readable index of
*why*.

To add one, copy [`0000-template.md`](0000-template.md), use the next number,
and add a row below. Records are not rewritten after acceptance: a changed
decision gets a new ADR that supersedes the old one (set the old one's
status to `superseded by NNNN`).

| # | Title | Status | Date |
|---|-------|--------|------|
| 0001 | [Edge collectors forward through Central Ingestion, not directly to Kafka](0001-edge-forwards-through-central-ingestion.md) | accepted | 2026-08-29 |
| 0002 | [Edge collector durability via embedded SQLite in WAL mode](0002-edge-durability-via-embedded-sqlite-wal.md) | accepted | 2026-08-29 |
| 0003 | [Scoped FHIR R4 AdverseEvent profile as the payload contract](0003-scoped-fhir-r4-adverse-event-profile.md) | accepted | 2026-08-29 |
| 0004 | [Deduplicate with Cassandra lightweight transactions and a transactional outbox](0004-dedup-via-cassandra-lwt-transactional-outbox.md) | accepted | 2026-08-29 |
| 0005 | [Three-state LWT claim/lease for the outbox and a symmetric DLQ path](0005-three-state-lwt-claim-lease-outbox.md) | accepted | 2026-08-30 |
| 0006 | [Per-site token-bucket rate limiting that meters HTTP batch requests](0006-per-site-token-bucket-rate-limiting.md) | accepted | 2026-08-29 |
| 0007 | [Edge forwarder retries with exponential backoff and full jitter](0007-edge-retry-exponential-backoff-full-jitter.md) | accepted | 2026-08-29 |
| 0008 | [Dedicated consumer binary and purpose-built canonical Cassandra query tables](0008-consumer-topology-and-canonical-query-tables.md) | accepted | 2026-08-30 |
| 0009 | [Event-time watermark with idle-partition exclusion and a monotonic guard](0009-event-time-watermark-idle-exclusion-monotonic-guard.md) | accepted | 2026-08-30 |
| 0010 | [CLI-backed query surface, and a dedicated DLQ-by-site table instead of a secondary index](0010-query-surface-cli-and-dlq-by-site-table.md) | accepted | 2026-08-30 |
| 0011 | [Kafka retention of 7 days (main) and 14 days (DLQ), enforced at startup](0011-kafka-topic-retention-policy-enforced-at-startup.md) | accepted | 2026-08-30 |
| 0012 | [Three-node Cassandra and three-broker KRaft Kafka with LOCAL_QUORUM](0012-multi-node-cassandra-and-kafka-topology.md) | accepted | 2026-08-30 |
| 0013 | [Encode a per-instance epoch into `local_seq` to survive edge disk replacement](0013-idempotency-key-epoch-in-local-seq.md) | accepted | 2026-08-31 |
| 0014 | [Explicit `schemaVersion` stamped at the edge, validated through a dispatch map](0014-wire-format-schema-version-dispatch.md) | accepted | 2026-09-04 |
| 0015 | [DLQ replay through a server-side Central Ingestion endpoint](0015-dlq-replay-through-central-ingestion-endpoint.md) | accepted | 2026-09-04 |
| 0016 | [File-based cold-tier archival instead of deletion](0016-tiered-archival-instead-of-deletion.md) | accepted | 2026-09-05 |
| 0017 | [Edge backup with periodic `VACUUM INTO` and restore-on-missing-primary](0017-edge-periodic-vacuum-into-backup-and-restore.md) | accepted | 2026-09-05 |
| 0018 | [Persist the consumer watermark as a periodic Cassandra checkpoint](0018-consumer-watermark-checkpoint-in-cassandra.md) | accepted | 2026-09-05 |
| 0019 | [Simulated multi-region topology: dc-us and dc-eu with MirrorMaker 2](0019-simulated-multi-region-cassandra-and-kafka.md) | accepted | 2026-09-05 |
| 0020 | [Per-site API keys and a project-owned CA for TLS](0020-per-site-api-keys-and-project-ca-tls.md) | accepted | 2026-09-05 |
| 0021 | [Server-rendered Go dashboard instead of a JavaScript frontend](0021-server-rendered-go-dashboard.md) | accepted | 2026-08-31 |
| 0022 | [Property-based testing with rapid plus deterministic simulation](0022-property-based-and-deterministic-simulation-testing.md) | accepted | 2026-08-31 |
| 0023 | [Chaos Control Panel gated behind an explicit opt-in flag](0023-chaos-control-panel-explicit-opt-in.md) | accepted | 2026-08-31 |
| 0024 | [Redis-backed token bucket and TCP passthrough load balancing for multiple ingestion instances](0024-distributed-rate-limiter-and-ingestion-load-balancing.md) | accepted | 2026-09-06 |
| 0025 | [One durable access-audit trail for CLI and dashboard reads](0025-durable-access-audit-trail.md) | accepted | 2026-09-06 |

Dates are the dates recorded in the source proposal or, for the later slices
that were scoped and built directly in `PLAN.md`, the date recorded there
(ADRs 0021-0023 use the scoping date; ADRs 0024-0025 use the completion date).
There are no rejected ADRs: every proposal in the source file was resolved
approved (some with a required revision); the rejected alternatives are
recorded inside each ADR's options.
