# Upgrade and compatibility policy

Status: policy for the current 0.x line. Items that are conventions today (the
code already behaves this way) are marked *current*; items that are commitments
for future changes are marked *rule*. Pre-1.0, the public surfaces below may
still change in a minor release, but only following this process and with a
CHANGELOG entry.

## Public surfaces

1. **Event wire format**: the FHIR-derived `AdverseEvent` JSON accepted by the
   edge and by `POST /api/v1/events` (`docs/api/*.yaml`, ADR 0003).
2. **Edge-to-Central batch envelope** (`internal/wire`).
3. **Cassandra schema** (`migrations/*.cql` and the in-code `EnsureSchema`).
4. **Kafka topics**: `pharos.events.adverse`, `pharos.events.dlq` and their
   message format.
5. **CLI flags** and **HTTP routes** (`docs/configuration.md`, `docs/api/`).

## Wire-format versioning

- *Current*: every event carries `schemaVersion`, stamped by the edge at capture
  time (ADR 0014). An absent or zero value means v1, so events captured before
  the field existed stay valid forever. `model.CurrentSchemaVersion` is what the
  running edge stamps; `Validate()` dispatches on the version.
- *Current*: an unsupported version is not a crash: it is rejected with
  `ErrUnsupportedSchemaVersion`, written to the DLQ (raw payload preserved), and
  replayable once Central Ingestion is upgraded.
- *Rule*: a new version is an **addition** to the validator map. A released
  validator is never edited in a way that rejects previously accepted events.
  New fields are optional (`omitempty`) in the version that introduces them.
- *Rule*: **upgrade order is Central Ingestion first, then edges.** Central
  must understand a version before any edge stamps it; otherwise events dead
  letter. The reverse (new Central, old edges) always works.
- *Rule*: support for a version is dropped only after a deprecation period (below)
  and only once no site still stamps it; the raw payloads of already-stored events
  are never rewritten.
- The idempotency-key format (`site_id:local_seq`, ADR 0013) is part of the wire
  contract: `local_seq` is opaque to every consumer, and any change to its
  meaning needs an ADR.

## Schema and migration rules

- *Current*: schemas are bootstrapped idempotently at service start
  (`CREATE ... IF NOT EXISTS`, plus idempotent column checks via
  `system_schema.columns` where a column was added later), and `migrations/NNN_*.cql`
  mirrors that state for `cqlsh` and CI. Numbers are sequential and never reused.
- *Rule*: migrations are **forward-only and backward-compatible**: use
  expand-migrate-contract.
  1. *Expand*: add the new table/column (nullable, no destructive default); ship code
     that writes both shapes and still reads the old one.
  2. *Migrate*: backfill existing rows with a job (for example the way
     `pharos-cli archive run` moves data), verified against real Cassandra.
  3. *Contract*: only in a later release, stop reading the old shape and remove it.
- *Rule*: never `DROP`/rename a column or table that a deployed older binary still
  uses; rolling upgrades mean old and new binaries run side by side.
- *Rule*: a change to partition or clustering keys is a new table plus a
  backfill, not an `ALTER` (Cassandra cannot alter keys), recorded in an ADR.
- Kafka: topic names and the message value (the raw event JSON plus its
  headers) are append-only; retention changes go through `EnsureTopics` and
  `scripts/create_topics.sh` together (ADR 0011).

## Supported dependency versions

As pinned in this repository; versions not listed here have not been exercised.

| Component | Pinned version | Where |
|---|---|---|
| Go | `go 1.25.0` in `go.mod` is the minimum language version; CI and the image builder use the 1.26 toolchain (1.25.x stdlib has unfixed `net/http` advisories GO-2026-6611 to 6617, fixed in 1.26.9) | `go.mod`, `.github/workflows/ci.yml` |
| Cassandra | 5.0 | `docker-compose.yml`, `deploy/k8s/01-cassandra.yaml` |
| Kafka | `apache/kafka` 3.8.0 (KRaft) | `docker-compose.yml`, `deploy/k8s/02-kafka.yaml` |
| Redis | 7.4 (optional, distributed rate limiter) | `docker-compose.yml` |
| nginx | 1.27 (optional, ingestion load balancer) | `docker-compose.yml` |
| Prometheus / Grafana | v2.55.1 / 11.3.1 | `docker-compose.yml`, `deploy/k8s/08-observability.yaml` |
| Kubernetes | manifests use only stable APIs (`apps/v1`, `batch/v1`, `networking.k8s.io/v1`, `policy/v1`) and are schema-validated in CI against 1.30.0; **no cluster version is pinned and none has been exercised in CI** (runtime verification is by hand on `kind`, see `deploy/k8s/README.md`) | `.github/workflows/ci.yml` |

Upgrading Cassandra or Kafka is a change to a pinned image tag in a PR that runs
the full real-infrastructure suite (`make test`/CI) and, for Cassandra, the
backup/restore drill (`scripts/backup_cassandra.sh`, `scripts/restore_cassandra.sh`)
first. Dependabot proposes Go module, GitHub Actions and Dockerfile base-image
updates; the compose-file image tags are updated by hand.

## Releases and deprecation

- Versions follow [Semantic Versioning](https://semver.org/); the CHANGELOG
  follows Keep a Changelog. Tags `vX.Y.Z` trigger the release workflow, which
  publishes signed images (`.github/workflows/publish-image.yml`).
- *Rule*: a breaking change to a public surface above (remove or rename a flag, route,
  field, table or topic) requires: an ADR, an entry under "Deprecated" in the
  CHANGELOG for at least **one minor release** while both behaviors work (with a
  startup warning where the surface is a flag), then removal under "Removed". Before
  1.0, the removal may land in the next minor release; from 1.0, only in a major.
- Security fixes may shorten this window; see `SECURITY.md`.
- Rolling back: schema changes follow expand-migrate-contract precisely so that
  the previous release still works against the migrated schema until the contract
  step ships; a release that includes the contract step documents that rollback
  past it requires a restore from backup.
