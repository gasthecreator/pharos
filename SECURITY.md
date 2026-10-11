# Security Policy

Pharos is a distributed adverse-event ingestion product built on real
distributed-systems engineering patterns, using a clinical-trial-adverse-event
payload shape. **It has not yet been operated in production with real data,
and should not currently be used to handle real patient or clinical trial
information.** All example data in this repo (patient references, adverse
event codes, study identifiers) is synthetic.

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
- The five Go-service images run as the non-root numeric user `65532:65532` on a
  digest-pinned builder base, and the Kubernetes manifests for them set
  `runAsNonRoot`, dropped capabilities, a read-only root filesystem, the
  `RuntimeDefault` seccomp profile, and default-deny NetworkPolicies. CI asserts the
  image user and validates the manifests against the Kubernetes schemas; **the pod
  settings and NetworkPolicies have not yet been exercised on a running cluster**.
  Releases publish keylessly signed images with SBOM and provenance.
- Cassandra internode traffic and Kafka inter-broker traffic are also TLS
  (Docker Compose deployment) — re-enabled and verified live 2026-09-06
  after an earlier attempt had narrowed this to client-facing connections
  only under real memory constraints; see PLAN.md Slice 15's addendum for
  what changed. Not yet carried into the Kubernetes deployment (`deploy/k8s/`
  keeps its original client-only TLS scope).
- Every `pharos-cli` query/DLQ/replay action, and every real view of
  adverse-event data through the web dashboard (recent-events feed, query,
  DLQ list/detail), is recorded in one durable, shared access-audit trail —
  keyed by a required `--operator` for the CLI, and by a self-declared
  operator name captured once per browser via a session cookie for the
  dashboard (no `Expires`/`MaxAge` — not a persistent login). See PLAN.md's
  Slice 21 addendum for how this closed the asymmetry an earlier pass here
  had left open.

See [`docs/security-threat-model.md`](docs/security-threat-model.md) for a
per-component breakdown of what each service can read/write, its trust
boundaries, and these gaps restated in those terms.

## Known, deliberate gaps

This is tracked explicitly, not hidden. As of this writing there is:
- No secrets-management system (Vault, KMS, or similar) — API key hashes
  live in a Cassandra table, and TLS private keys live as plain files under
  `certs/`/`deploy/k8s/certs/` (gitignored). Adequate for this project's
  current single-tenant deployment model, not for a real secret-rotation or
  least-privilege-access story.
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

Please report suspected vulnerabilities **privately**, not in a public issue or
pull request:

- Preferred: GitHub's private vulnerability reporting for this repository
  (Security tab, "Report a vulnerability").
- Alternatively, email **gideonsanni2023@gmail.com** with "Pharos security" in
  the subject.

Include the affected component and commit or image tag, steps to reproduce,
and the impact you believe it has. Please do not include real patient or
clinical-trial data. Findings that are already documented as known gaps below
and in [`PLAN.md`](PLAN.md) and [`docs/security-threat-model.md`](docs/security-threat-model.md)
are tracked work, but a new angle on one is still welcome.

This is a solo-maintained project, so timelines are targets, not an SLA:

| Stage | Target |
|---|---|
| Acknowledge your report | within 3 business days |
| Triage (confirm, assess severity, tell you the plan) | within 7 days |
| Fix or mitigation | within 90 days of the report, sooner for critical issues |
| Coordinated disclosure | we agree on a date with you; by default after a fix or mitigation is available, and no later than 90 days after the report |

We will credit you in the advisory and `CHANGELOG.md` if you wish (tell us how
you would like to be named, or that you prefer to stay anonymous). If a fix is
delayed past 90 days we will tell you why and agree on the next step with you
rather than going silent.

Given the project's current scope (no production deployment, no real data),
there is no bug bounty.
