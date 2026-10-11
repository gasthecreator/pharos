---
status: accepted
date: 2026-09-05
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0020. Per-site API keys and a project-owned CA for TLS

## Context and Problem Statement

Anything that could reach a port could claim to be any site just by setting a header, and every hop was plaintext. PLAN.md flagged Slice 15 as needing real review before building.

## Decision Drivers

- Prove a request comes from a specific authorized site
- Operational fit for a resource-constrained trial site without a PKI team
- No cloud spend and no publicly resolvable domain
- Encrypt every hop that matters, within a fixed memory budget

## Considered Options

- Per-site API key (SHA-256 hashed at rest) over TLS (chosen)
- mTLS for edge to ingestion
- JWT session tokens
- A real ACME/public CA
- Per-node Cassandra/Kafka identity certificates

## Decision Outcome

Keys are random 256-bit values shown once by `pharos-cli site create-key <site-id>` and stored only as a SHA-256 hash in `site_api_keys` (no per-key salt needed for a high-entropy secret). `internal/auth` middleware resolves the site from `X-API-Key` and rejects a mismatched `X-Site-ID`/envelope site (401, 403). `scripts/generate_certs.sh` issues a single project CA and the certificates for ingestion, Cassandra and Kafka; every Go service trusts that CA. `--enable-auth` defaults to `true`.

### Consequences

- Shipped scope was first narrowed to client-facing TLS only after repeated OOM kills; internode/inter-broker TLS was re-enabled and verified on 2026-09-06 once the topology had shrunk to 4+4 nodes. Kubernetes manifests still use client-only TLS for Kafka.
- Heap-ceiling `OutOfMemoryError`s hid behind a `healthy` container and `OOMKilled=false`; `KAFKA_HEAP_OPTS` was raised to 320M.
- Serializing test packages with `-p 1` came out of this slice.

### Confirmation

Run for real: handshake succeeds, `/healthz` stays unauthenticated, missing key gets 401, wrong site gets 403, a revoked key is rejected, and an edge event flowed to `PUBLISHED`; the log lines show `encryption = encrypted(...TLSv1.3...)` on internode links.

## Pros and Cons of the Options

### Per-site API key (SHA-256 hashed at rest) over TLS (chosen)

- Good, because a single opaque secret configured once; rotate by issuing a new key and revoking the old; key secures identity, TLS secures the channel
- Bad, because a static key rather than short-lived credentials

### mTLS for edge to ingestion

- Good, because strong
- Bad, because requires per-site certificate issuance, distribution, rotation and revocation, a real PKI lifecycle

### JWT session tokens

- Good, because short-lived, self-healing leaks
- Bad, because advantage matters for interactive sessions; a rotatable static key with an explicit revoke path is simpler for a long-running service link

### A real ACME/public CA

- Good, because publicly trusted
- Bad, because nothing here has a public domain and it adds renewal automation for purely internal traffic

### Per-node Cassandra/Kafka identity certificates

- Good, because per-node identity
- Bad, because about 5x bookkeeping for a private trusted cluster whose threat model is external access

## More Information

Evidence: [proposal "Slice 15: Auth & TLS"](../history/ARCHITECTURE_PROPOSALS.md) with its two addenda; PLAN.md Slice 15 (../../PLAN.md); worklog ["Slice 15 — Auth & TLS"](../history/WORKLOG.md); threat model [`docs/security-threat-model.md`](../security-threat-model.md).
