# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Full reasoning, live-verification evidence, and implementation detail for
every item below live in [`WORKLOG.md`](WORKLOG.md) and are tracked in
[`PLAN.md`](PLAN.md)'s own Slice list, the source of truth for scope.

## [Unreleased]

Everything below is implemented and verified against real infrastructure
(real Cassandra, real Kafka, real Docker/Kubernetes) since `v0.2.0`.

### Added

- Idempotency key resilience, wire-format schema versioning, DLQ replay,
  data retention/archival, edge collector durability hardening, and
  consumer crash/restart fault-injection (Slices 8-13).
- Per-site API key auth and project-owned-CA TLS across every service and
  Cassandra/Kafka connection, including internode/inter-broker traffic
  (Slice 15).
- Real k6 load testing against the full live stack (Slice 16); results in
  `docs/benchmark-results.md`.
- Kubernetes deployment manifests for every service (Slice 17).
- Cassandra backup/restore drill (Slice 18).
- Multi-instance `pharos-consumer` sharing one Kafka consumer group
  (Slice 19).
- Durable access-audit logging for every `pharos-cli` and dashboard query/
  DLQ/replay action (Slice 20-21).
- Server-rendered web dashboard (Slice 21).
- Property-based and deterministic simulation testing (Slice 22).
- Chaos Control Panel for live partition/duplicate/clock-skew demos,
  explicit opt-in only (Slice 23).
- `CODE_OF_CONDUCT.md`, `CODEOWNERS`, `docs/security-threat-model.md`,
  this changelog, GitHub issue templates, Dependabot, and CI running
  `govulncheck` and CodeQL on every push.
- Signed container image publishing for every service
  (`.github/workflows/publish-image.yml`): cosign keyless signatures, SBOM,
  and SLSA provenance, published to GHCR.

### Fixed

- A concurrent-publish race in the Cassandra outbox, a watermark-regression
  bug on partition reawakening, an audit-trail duplication bug under Kafka
  redelivery, a Cassandra secondary-index modeling inconsistency, and an
  unapplied Kafka retention policy — see `README.md`'s "How this was
  actually verified" for detail.
- A missing `cassandra-truststore.jks` in the Kubernetes TLS Secret/volume
  mounts that would have broken a fresh cluster deploy.
- Several stale claims in `PLAN.md` describing already-fixed gaps as open,
  and several `docs/api/*.yaml` inaccuracies (missing response codes, a
  misattributed field, a too-broad security scheme).

## [0.2.0] - 2026-08-30

### Added

- Prometheus + Grafana observability, with a pre-provisioned dashboard
  (Slice 6).
- A genuine multi-node cluster: 3-node Cassandra (RF=3) + 3-broker Kafka
  (Slice 7).

## [0.1.0] - 2026-08-30

### Added

- Edge collector: local durable buffering (SQLite WAL), forwarding to
  Central Ingestion with exponential backoff and full jitter, client-side
  idempotency keys.
- Central Ingestion: HTTP intake, per-site token-bucket rate limiting,
  scoped FHIR validation, and a Cassandra-backed transactional outbox with
  a claim/lease pattern before anything reaches Kafka.
- Dead-letter handling in both Cassandra and Kafka, inspectable via
  `pharos-cli`.
- Stream processing (`pharos-consumer`): per-partition watermarking with
  idle-source exclusion and a monotonic max guard, deduplicated
  late-arrival audit trail.
- Fault-injection test suite against the real stack (network partition,
  asymmetric partition healing, out-of-order delivery).
- README, working demo (`scripts/demo.sh`), and initial repo scaffolding.
