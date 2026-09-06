# Security threat model

This is a factual account of what each Pharos component can read, what it
can write, and where its trust boundaries actually are — written the same
way this project's `WORKLOG.md` entries are: state what was checked, not
what "should" be true. See [SECURITY.md](../SECURITY.md) for the current
gap list and how to report a vulnerability; this document is about design,
not process.

## What each component can do

| Component | Reads | Writes | Inbound network surface |
|---|---|---|---|
| `pharos-edge` | Local HTTP capture requests | Local SQLite WAL queue | Plaintext HTTP, deliberately unauthenticated — scoped to the trusted site network, never exposed beyond it (§2.1) |
| `pharos-ingestion` | HTTP requests (site-authenticated), `internal/auth` keystore | Cassandra (outbox, dedup, DLQ, audit, keystore), Kafka | TLS, requires a per-site API key by default (`--enable-auth=true`) |
| `pharos-consumer` | Kafka | Cassandra (canonical events, late-arrival audit) | TLS to Kafka/Cassandra only; `/metrics` and `/healthz` are its only inbound HTTP surface, no request path accepts adverse-event data |
| `pharos-cli` | Cassandra, Kafka (direct client, same TLS certs as the services) | Cassandra (keystore on `site create-key`, audit trail on every query/DLQ/replay action) | None — a local operator tool, not a server |
| `pharos-dashboard` | Cassandra (via `internal/query.Service`, the same interface `pharos-cli` uses) | Cassandra (access-audit trail) | TLS/plaintext per deployment; a session cookie captures a self-declared operator name, **not an authentication credential** — see below |

Confirmed by reading the actual route registrations and flag definitions
directly (`internal/dashboard/dashboard.go`, `cmd/pharos-*/main.go`), not
assumed from README/PLAN.md's descriptions of intent.

## Trust boundaries

1. **The site network / Central Ingestion boundary is the real authentication
   boundary.** `pharos-edge`'s local capture endpoint is intentionally
   unauthenticated plaintext HTTP — anything that can reach it is already
   inside the trusted site network. Everything crossing out of that
   boundary (edge → Central Ingestion, and every Cassandra/Kafka
   connection) requires TLS against the project-owned CA
   (`scripts/generate_certs.sh`) plus, for ingestion, a per-site API key
   hashed with SHA-256 at rest (`internal/auth/keystore.go`) — never stored
   or logged in plaintext after the one-time `pharos-cli site create-key`
   display.
2. **The dashboard's operator identification is accountability, not
   access control.** The `/operator` cookie (no `Expires`/`MaxAge`) records
   *who* looked at what in the durable audit trail — it does not verify
   *that* the person is who they claim, and it gates nothing that wasn't
   already reachable. Anyone who can reach the dashboard's network can view
   adverse-event data; the cookie only makes that view attributable
   afterward. This is a deliberate, documented tradeoff
   (`internal/dashboard/dashboard.go`'s own package doc), not an oversight —
   but it means the dashboard's own network reachability (which network it's
   bound to, whether it sits behind a real auth proxy in front of it) is the
   actual access-control boundary, not anything inside the Go binary.
3. **`/submit` and `/chaos` are deliberately not operator-gated, for two
   different reasons.** `/submit` proxies through Central Ingestion's own
   authenticated HTTP endpoint using the real owning site's credentials per
   request — a stronger, per-action accountability than the dashboard's own
   self-declared operator identity would add. `/chaos` is an infrastructure
   action, not a view of patient data, so it isn't in the access-audit
   trail's scope at all — but its actions are real (stopping/restarting
   Cassandra containers, partitioning `dc-us`/`dc-eu`) and gated instead by
   a separate mechanism: `pharos-dashboard --enable-chaos` defaults to
   `false`, refuses every chaos action at the handler level when unset
   (confirmed uniform across all seven action handlers in
   `internal/dashboard/chaos.go`), and logs a loud warning at startup when
   set. **The chaos panel's actual safety boundary is "don't pass
   `--enable-chaos` to anything but a local/demo instance," not any
   in-app authentication** — a deployment that flips this flag in a
   reachable environment has no further gate behind it.
4. **The Kubernetes deployment (`deploy/k8s/`) currently grants no
   narrower RBAC than the cluster's own defaults.** There is no
   `ServiceAccount`, `Role`, `ClusterRole`, `RoleBinding`, or
   `NetworkPolicy` manifest anywhere under `deploy/k8s/` — confirmed by
   grepping the whole directory, not assumed. Every pod runs under its
   namespace's (`pharos`) default `ServiceAccount` with whatever the
   cluster grants that identity, and nothing restricts which pods in the
   `pharos` namespace (or beyond, absent a cluster-wide default-deny
   policy) can reach which other pods on which ports. This is a real,
   current gap, not a "someone forgot to write a doc" situation — no code
   or manifest anywhere implements pod-level network or RBAC restriction
   for this deployment target.
5. **Cassandra internode and Kafka inter-broker traffic is TLS in the
   Docker Compose deployment, but plaintext in the Kubernetes deployment.**
   The two deployment targets have genuinely diverged on this point (see
   `SECURITY.md`'s gap list and `PLAN.md` Slice 15's addendum for why) —
   anyone relying on this project's Kubernetes manifests as-is should not
   assume the same internode TLS posture the Compose topology has.

## Known gaps (stated plainly, not hidden)

These are the security-relevant items from `SECURITY.md`'s gap list,
restated here in trust-boundary terms:

- **No secrets-management system.** API key hashes live in a Cassandra
  table; TLS private keys are plain files under `certs/`/
  `deploy/k8s/certs/` (gitignored, not committed). A compromise of the
  host or cluster filesystem exposes every private key at once — there is
  no per-key rotation or least-privilege scoping today.
- **No Kubernetes RBAC or NetworkPolicy**, per trust boundary 4 above —
  the single largest concrete gap between this project's current
  Kubernetes manifests and a deployment a security-conscious buyer would
  accept as-is.
- **The dashboard has no real authentication**, per trust boundary 2 —
  whoever can reach it on the network can read adverse-event data; the
  self-declared operator cookie is an audit-trail convenience, not a
  login.
- **Kubernetes internode/inter-broker traffic is plaintext**, per trust
  boundary 5.

## What's intentionally out of scope for this document

Threats from a fully compromised host or Kubernetes control plane, a
malicious cluster-admin, or supply-chain compromise of this project's own
dependencies (`go.mod`/`go.sum`) — `govulncheck` and CodeQL (both run in
CI on every push) and Dependabot already continuously check the latter,
not something a static document adds value restating.
