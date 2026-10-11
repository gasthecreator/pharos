# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Full reasoning, live-verification evidence, and implementation detail for
every item below live in [`docs/history/WORKLOG.md`](docs/history/WORKLOG.md) and are tracked in
[`PLAN.md`](PLAN.md)'s own Slice list, the source of truth for scope.

## [Unreleased]

## [0.3.0] - 2026-10-11

Everything in Slices 8-23 below was implemented and verified against real
infrastructure (real Cassandra, real Kafka, real Docker) since `v0.2.0`; the
supply-chain, container and operations items are called out where they are
statically validated only.

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

- Architecture decision records (`docs/adr/`, 25 records distilled from the
  proposal trail), runbooks for nine failure modes (`docs/runbooks/`), Prometheus
  alert rules as code (`observability/prometheus/alerts.yml`), proposed SLOs
  (`docs/slo.md`), an upgrade and compatibility policy, and a configuration
  reference (`docs/configuration.md`).
- Release automation: `release.yml` builds a GitHub Release from the matching
  CHANGELOG section and publishes the signed images by digest; OpenSSF Scorecard
  workflow; per-package coverage summary and `go mod tidy` drift check in CI.
- Kubernetes `NetworkPolicy` (default-deny plus explicit allows) and
  `PodDisruptionBudget` manifests.

### Changed

- The five Go-service images run as non-root (`65532:65532`) on a digest-pinned base, and
  their Kubernetes workloads set `securityContext` (non-root, no privilege
  escalation, dropped capabilities, read-only root filesystem, `RuntimeDefault`
  seccomp). Statically validated in CI; not yet run on a cluster.
- Every GitHub Action is pinned to a commit SHA; Dependabot groups updates and
  covers Dockerfiles.
- `make test` now mirrors CI (serialized packages, separate chaos run,
  `RAPID_CHECKS=5000`); `make lint` runs golangci-lint at CI's pinned version;
  `make up` brings the stack up in the safe order.
- Compose Prometheus scrapes Central Ingestion over HTTPS (it serves TLS by default).
- Project history moved to `docs/history/` (`WORKLOG.md`, `ARCHITECTURE_PROPOSALS.md`);
  `CODEOWNERS` moved to `.github/`; sample study ID is `STUDY-401`.

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

[Unreleased]: https://github.com/gasthecreator/pharos/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/gasthecreator/pharos/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/gasthecreator/pharos/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/gasthecreator/pharos/releases/tag/v0.1.0
