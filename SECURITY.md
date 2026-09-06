# Security Policy

Pharos is a portfolio project demonstrating distributed-systems engineering
patterns using a clinical-trial-adverse-event payload shape. **It is not a
production system, is not deployed anywhere with real data, and should not
be used to handle real patient or clinical trial information.** All example
data in this repo (patient references, adverse event codes, study
identifiers) is synthetic.

## What's actually in place

As of this writing:
- Every Cassandra/Kafka client connection and Central Ingestion's own HTTP
  listener require TLS by default, against a project-owned CA
  (`scripts/generate_certs.sh`) — see PLAN.md Slice 15.
- Central Ingestion requires per-site API key authentication by default
  (`--enable-auth` defaults `true`): a site must be provisioned via
  `pharos-cli site create-key` before its edge collector can submit
  anything. Keys are shown once at creation time and stored only as a
  SHA-256 hash (`internal/auth/keystore.go`), never in plaintext.
- Kubernetes deployment manifests (`deploy/k8s/`, PLAN.md Slice 17), a real
  backup/restore drill (Slice 18), and 2+ `pharos-consumer` instances
  sharing one Kafka consumer group (Slice 19) are all built and verified,
  not just proposed.
- Cassandra internode traffic and Kafka inter-broker traffic are also TLS
  (Docker Compose deployment) — re-enabled and verified live 2026-09-06
  after an earlier attempt had narrowed this to client-facing connections
  only under real memory constraints; see PLAN.md Slice 15's addendum for
  what changed. Not yet carried into the Kubernetes deployment (`deploy/k8s/`
  keeps its original client-only TLS scope).
- Every `pharos-cli` query/DLQ/replay action is recorded in a durable
  access-audit trail, keyed by a required `--operator` (Slice 20); the
  web dashboard's DLQ replay/submit actions inherit this too, since those
  proxy through the same authenticated Central Ingestion endpoint. Its
  read-only query/DLQ views do not (see PLAN.md's Slice 21 "Read-side audit
  asymmetry" for why, and what closing it would need).

## Known, deliberate gaps

This is tracked explicitly, not hidden. As of this writing there is:
- No secrets-management system (Vault, KMS, or similar) — API key hashes
  live in a Cassandra table, and TLS private keys live as plain files under
  `certs/`/`deploy/k8s/certs/` (gitignored). Adequate for a local/portfolio
  deployment, not for a real secret-rotation or least-privilege-access story.
- The edge collector's own local HTTP capture endpoint is deliberately
  unauthenticated and plaintext (§2.1: scoped to the trusted site network,
  not a connection that leaves it) — everything that does leave the site
  network (edge → Central Ingestion, and every Cassandra/Kafka connection)
  is TLS + authenticated, per the list above.
- The Kubernetes deployment (`deploy/k8s/`) still runs Cassandra
  internode/Kafka inter-broker traffic in plaintext, unlike the Docker
  Compose deployment (see above) — carrying that re-enablement into K8s,
  with its own separate resource ceiling to re-check, hasn't been done yet.
- Kubernetes deployment automation exists (see above), and was attempted at
  the plan's originally scoped full scale (2 datacenters / 4 Cassandra
  nodes / 4-broker Kafka) on 2026-09-06 — Cassandra and Kafka both came up
  cleanly, but the one-shot migrations Job triggered a genuine CPU-contention
  cascading failure on this specific 8-core host once a single-node `kind`
  cluster's own K8s control-plane overhead was added on top of the identical
  application workload. Retried the same day with a multi-node `kind`
  cluster (1 control-plane + 3 workers) specifically to test that mitigation
  — Cassandra came up further/cleaner than the single-node attempt, but
  once Kafka's 4 brokers began bootstrapping, host load hit the same ~11-13
  peak and `kubectl`/`docker` themselves became unresponsive. Not a manifest
  bug either time: this host's 8 physical cores are the actual ceiling,
  which changing the `kind` node topology alone can't raise — see PLAN.md
  Slice 17's addendum for the full account.

Don't run this outside a local/dev environment, and don't feed it real
personal or clinical data.

## Reporting a vulnerability

This is a solo-maintained portfolio repository. If you find a genuine
security issue in the code itself (not the already-documented gaps above —
those are known and tracked in `PLAN.md`), please open a GitHub issue or
reach out to the maintainer directly rather than a public disclosure, so
there's time to assess it before it's public. Given the project's current
scope (no production deployment, no real data), response time is
best-effort, not SLA-bound.
