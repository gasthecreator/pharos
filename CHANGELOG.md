# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once tagged releases begin. Until then, `[Unreleased]` tracks `main`.

## [Unreleased]

Every item below is implemented and verified against real infrastructure
(real Cassandra, real Kafka, real Docker/Kubernetes) — not proposed or
mocked. Full reasoning, live-verification evidence, and the real bugs found
along the way for every item live in [`WORKLOG.md`](WORKLOG.md) and are
tracked to completion in [`PLAN.md`](PLAN.md)'s own Slice list, the source
of truth for scope.

### Added

- **Edge collector** (Slice 1-2): local durable buffering via an embedded
  SQLite WAL queue, forwarding to Central Ingestion with exponential
  backoff + full jitter, and client-side idempotency key generation at
  capture time — a site can lose connectivity for hours or days without
  losing or duplicating data.
- **Central ingestion service** (Slice 2-3): HTTP intake
  (`POST /api/v1/events`), per-site token-bucket rate limiting, scoped FHIR
  validation, and a Cassandra-backed transactional outbox with a
  three-state claim/lease (lightweight-transaction insert) before anything
  reaches Kafka — closing the "insert dedup key, then publish" crash window
  a naive implementation gets wrong.
- **Dead-letter handling** (Slice 3, 5, 10): a durable DLQ in both Cassandra
  (`dead_letter_events`) and Kafka (`pharos.events.dlq`), inspectable and
  replayable via `pharos-cli dlq list/get/replay`.
- **Stream processing / event ordering** (Slice 4): `pharos-consumer` reads
  `pharos.events.adverse`, tracks per-partition watermarks with
  idle-source exclusion and a monotonic max guard (a reawakening site with
  backlogged old data can't push the clock backward), and manages
  `OPEN`/`COMPLETE`/`REVISED` window lifecycle with a deduplicated
  late-arrival audit trail.
- **Fault-injection test suite** (`internal/faultinjection`): real
  network-partition, redelivery, and crash-recovery scenarios against the
  full real stack, not mocks.
- **Observability** (Slice 6): Prometheus + Grafana, with a pre-provisioned
  "Pharos Overview" dashboard (request rates/latency, rate-limit/
  validation/dedup/DLQ counters, Kafka consumer lag and watermark
  freshness, Cassandra write latency, edge queue depth).
- **Multi-node, multi-region cluster** (Slice 7, 14): a genuine two-datacenter
  topology — `dc-us`: 3-node Cassandra (RF=3) + 3-broker Kafka, `dc-eu`:
  1-node Cassandra (RF=1) + 1-broker Kafka, MirrorMaker 2 replication,
  `LOCAL_QUORUM` reads/writes.
- **Idempotency key resilience** (Slice 8), **wire-format schema
  versioning** (Slice 9), **data retention & lifecycle pruning** (Slice 11),
  **edge collector durability hardening** (Slice 12), and **consumer
  crash/restart fault-injection** (Slice 13) — hardening passes on the four
  core pipeline pieces above, each verified against real infrastructure
  restarts and crashes.
- **Auth & TLS** (Slice 15): per-site API key authentication (SHA-256
  hashed at rest) required by default on Central Ingestion, and
  project-owned-CA TLS across every service and every Cassandra/Kafka
  connection, including internode/inter-broker traffic (Docker Compose
  deployment).
- **Load testing** (Slice 16): real k6 load runs against the full live
  stack under realistic multi-site volume, with per-site burst isolation
  verified and the actual bottleneck (the Cassandra outbox's Paxos LWT
  insert) identified from real metrics, not assumed.
- **Kubernetes deployment** (Slice 17): manifests for every service,
  multi-stage scratch-image Dockerfiles (`deploy/docker/Dockerfile.*`),
  health/readiness probes wired to the same metrics Slice 6 exposes.
- **Backup & disaster recovery** (Slice 18): a real Cassandra
  snapshot/restore drill, schema-agnostic backup/restore scripts.
- **Multi-instance scaling** (Slice 19): 2+ `pharos-consumer` instances
  sharing one Kafka consumer group, verified for correct partition
  rebalancing.
- **Compliance / access-audit logging** (Slice 20-21): every
  `pharos-cli` query/DLQ/replay action, and every real view of
  adverse-event data through the web dashboard, recorded in one durable,
  shared access-audit trail.
- **Web dashboard** (Slice 21): a server-rendered `pharos-dashboard`
  binary (stdlib `html/template`, no JS framework) reusing the exact same
  `internal/query.Service` interface `pharos-cli` already uses.
- **Property-based & deterministic simulation testing** (Slice 22):
  generator-driven checks of state-machine and zero-value invariants
  across the input space, not just fixed cases.
- **Chaos Control Panel** (Slice 23): a live, dashboard-driven
  infrastructure control panel for demoing partition/duplicate/clock-skew
  recovery against the real cluster — explicit opt-in only
  (`--enable-chaos`, off by default).
- **Repo hygiene**: `LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md` plus a dedicated `docs/security-threat-model.md`,
  `CODEOWNERS`, this changelog, GitHub issue templates, Dependabot, and CI
  running the full real-infrastructure test suite, `govulncheck`, and
  CodeQL on every push.

### Known gaps

Tracked explicitly, not hidden — see [`SECURITY.md`](SECURITY.md) and
[`docs/security-threat-model.md`](docs/security-threat-model.md) for the
current list (no secrets-management system, no Kubernetes RBAC/
NetworkPolicy yet, Kubernetes internode/inter-broker traffic still
plaintext) and [`PLAN.md`](PLAN.md)'s roadmap section for what's deferred
vs. what was found and fixed during review.
