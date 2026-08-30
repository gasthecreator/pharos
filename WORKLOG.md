# Worklog

A running, dated log of every substantive piece of work done on Pharos — by
Gemini (Antigravity), by Claude Code, or by Gideon directly. Treat this the way
you'd treat engineering documentation at an actual job: if it's not logged here,
it didn't happen. This is a record for a future technical walkthrough as much as
it is a build log — write entries so someone with no context on the session can
understand what changed and why.

Newest entries at the top.

---

## How to write an entry

```
## [YYYY-MM-DD] Short title

**Author:** Gemini / Claude Code / Gideon

**What:** (what was actually built/changed — be concrete: files, components,
behavior)

**Why:** (the reasoning — which of PLAN.md's four core challenges this serves,
or what problem it solves; link to an ARCHITECTURE_PROPOSALS.md entry if this
came out of an approved proposal)

**How:** (the technical approach — enough detail that someone could reconstruct
the decision without re-reading the diff)

**Files/modules touched:** (paths)

**Tests added/updated:** (what, and what failure mode it guards against —
especially for anything touching partition handling, dedup, or ordering)

**Follow-ups / left open:** (anything deliberately deferred, and why)
```

---

## Log

## [2026-08-29] Claude Code review: 4 proposals approved (1 with a required fix), Slice 1 plan cleared to build

**Author:** Claude Code

**What:** Reviewed Gemini's four architecture proposals in `ARCHITECTURE_PROPOSALS.md`
and the Slice 1 implementation plan Gemini produced. Approved three proposals
as-is (ingress topology via Central Ingestion HTTPS API; SQLite-WAL edge
durability as a per-site binary; the scoped FHIR R4 profile) and folded all
three into `PLAN.md` §2.1/§2.3, resolving former Open Questions 2, 4, and 5.
Approved the fourth (Cassandra LWT as the dedup store) with one required
modification, also folded into `PLAN.md` §2.2, resolving former Open Question 3.

**Why:** The dedup proposal as written implied a two-step "insert idempotency
key, then publish to Kafka" sequence with no atomicity between the steps. If the
LWT insert succeeds and the process crashes or partitions before the Kafka
publish completes, every future retry of that event would be seen as an
already-processed duplicate and silently dropped forever — exactly the failure
mode PLAN.md §2.2 flags as the easiest thing to get subtly wrong here. This is
a correctness gap, not a style preference, so it couldn't be waved through.

**How:** Required a transactional-outbox pattern instead: write the event with
its idempotency key and a `published: false` flag into Cassandra in one
statement (the LWT `IF NOT EXISTS` on that insert is the dedup check), then a
separate retriable step publishes to Kafka and flips `published: true`. An
interrupted publish becomes resumable instead of lost, and the shape matches
the store-then-forward pattern already used at the edge (§2.1). Also reviewed
Gemini's Slice 1 implementation plan (repo scaffolding, domain models,
client-side idempotency keys, SQLite-WAL edge queue) — it correctly defers both
the dedup/outbox logic and the Central Ingestion Kafka-publish path to a later
slice, so nothing in Slice 1 needed to change; cleared it to build as written.

**Files/modules touched:** `PLAN.md` (§2.1, §2.2, §2.3, §3, §4, §5),
`ARCHITECTURE_PROPOSALS.md` (all four entries marked resolved).

**Tests added/updated:** none — review only, no code exists yet.

**Follow-ups / left open:**
- Gemini: proceed with Slice 1 as planned (repo scaffolding, domain models,
  idempotency keys, SQLite-WAL edge store) — no changes needed there.
- Whoever builds Central Ingestion's dedup path (later slice) must implement it
  against the outbox pattern in `PLAN.md` §2.2, not against the original
  proposal text in `ARCHITECTURE_PROPOSALS.md`.
- Rate limiter backing store (Redis, tentative) is still unvalidated — not
  addressed by this round of proposals.
- Cassandra schema fit against real query patterns (PLAN.md §5) is still open.
- Claude Code's fault-injection test suite (partition/duplicate/out-of-order)
  can start once Slice 1's SQLite store lands — the concurrent-writer and
  crash-recovery tests Gemini already scoped in `sqlite_store_test.go` are a
  reasonable foundation to build on rather than duplicate.

## [2026-08-29] Architecture Review, Requirements Re-Derivation, and Ingress/Storage Proposals

**Author:** Gemini (Antigravity)

**What:** Conducted a comprehensive review of `PLAN.md`, `ARCHITECTURE_PROPOSALS.md`, and `WORKLOG.md`. Re-derived the four core distributed engineering challenges and resolved ambiguities by drafting four formal architecture proposals in `ARCHITECTURE_PROPOSALS.md`:
1. Ingress Topology Clarification: Edge forwards to Central Ingestion HTTPS API, not direct Kafka over WAN.
2. Edge Collector Local Durability: Embedded SQLite in WAL mode for crash-resilient store-and-forward.
3. Scoped FHIR R4 AdverseEvent Schema Profile: Formal contract for validation and dead-letter routing.
4. Central Dedup Store: Cassandra LWT table with a pluggable Go interface for optional Redis caching.
Produced a concrete implementation plan for the first development slice (scaffolding, domain models, client-side idempotency keys, and edge store-and-forward queue).

**Why:** Directly addresses all four core engineering challenges in `PLAN.md` (§2.1 Network partition tolerance, §2.2 Exactly-once processing semantics, §2.3 Rate limiting and DLQ, §2.4 Multi-timezone event ordering) and resolves open questions in §5. Adheres strictly to the non-negotiable process agreement: never edit `PLAN.md` directly; capture architectural ambiguities/proposals for Claude Code and Gideon review before implementing deviations.

**How:** 
- Analyzed the distributed systems boundaries: Kafka's transactional guarantees only protect internal pipeline hops; client-to-sink exactly-once requires client-assigned idempotency keys (`site_id:local_seq`) and atomic persistence before network attempts.
- Identified the network topology mismatch between edge sites and Kafka brokers across international WAN; formalized the DMZ role of the Central Ingestion HTTP service.
- Evaluated embedded storage options for the edge binary, selecting SQLite with WAL mode (`PRAGMA synchronous = NORMAL; PRAGMA journal_mode = WAL;`) for atomic sequence assignment, transactional status updates, and single-binary zero-external-daemon deployment.
- Structured the initial implementation slice for local execution with Docker Compose (Kafka KRaft mode + Apache Cassandra) with zero cloud spend.

**Files/modules touched:** `ARCHITECTURE_PROPOSALS.md`, `WORKLOG.md`, `implementation_plan.md` (artifact).

**Tests added/updated:** None yet — planning and architecture review phase.

**Follow-ups / left open:**
- Claude Code and Gideon review of the four proposals in `ARCHITECTURE_PROPOSALS.md`.
- User approval of Implementation Plan for Slice 1 to begin scaffolding and code implementation.

## [2026-08-28] Repo created, PLAN.md written, storage cost/licensing resolved

**Author:** Claude Code

**What:** Initialized the `pharos` git repo at `~/pharos`. Wrote the initial
`PLAN.md` covering project goal, the four core engineering challenges as explicit
design decisions, planned stack, a not-yet-started build checklist, and open
questions. Created `ARCHITECTURE_PROPOSALS.md` and this file (`WORKLOG.md`) to
formalize the working process between Gideon, Claude Code, and Gemini/Antigravity.

**Why:** Nothing existed yet for this project — confirmed with Gideon that no
scaffold or Notion notes exist beyond the original brief. Before Antigravity
starts building, the project needed a single source of truth for architecture
(`PLAN.md`) and a process that keeps Antigravity from silently rewriting that
source of truth mid-build (`ARCHITECTURE_PROPOSALS.md`), plus a documentation
habit that treats this like a real engineering role rather than a series of
disconnected sessions (`WORKLOG.md`, this file).

Separately, Gideon flagged a hard constraint: the project must be completable
using only AI subscriptions, with zero cloud infrastructure spend. This put
Cassandra's cost under question. Verified via web search: Apache Cassandra is
Apache License 2.0 — fully free, self-hosted, no usage cap, ever. The cost people
associate with Cassandra is exclusively for *managed* hosting (DataStax Astra,
AWS Keyspaces), which this project isn't using. Considered ScyllaDB as a
lighter-footprint alternative (same CQL wire protocol, lower resource use via its
shard-per-core design), but its license recently moved from open-source (AGPL) to
source-available with a 10TB free-usage cap — still free at this project's scale,
but Cassandra's unconditional Apache 2.0 license is the cleaner story and the more
recognizable name for the target audience (Eli Lilly Bio-IT recruiting). Decision:
stay with Cassandra, self-hosted via Docker.

**How:** No code yet — this was planning/process work. `PLAN.md` §5.1 documents
the resolved storage-cost decision in full, including the ScyllaDB comparison and
the rationale for keeping the query-pattern-fit question open separately from the
now-closed cost question.

**Files/modules touched:** `PLAN.md`, `ARCHITECTURE_PROPOSALS.md`, `WORKLOG.md`
(all new).

**Tests added/updated:** none — no code exists yet.

**Follow-ups / left open:**
- PLAN.md §5: Cassandra's fit against real query patterns is still unverified —
  revisit once schema + top queries are drafted.
- PLAN.md §5: edge collector local durability mechanism (SQLite-as-WAL vs.
  embedded log) still undecided.
- PLAN.md §5: dedup store choice (Redis vs. Cassandra LWT) still undecided.
- No commits made yet in the `pharos` repo — pending Gideon's go-ahead before
  pushing to GitHub.
