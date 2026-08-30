# Pharos — Living Plan

**Status:** pre-code. This file is the single source of truth for architecture and
progress. Both Antigravity and Claude Code read this before touching code. Keep it
updated as decisions change — do not let it go stale.

Last updated: 2026-08-29

---

## 1. Goal

Pharos is a distributed adverse-event ingestion pipeline: a system that reliably
collects, orders, deduplicates, and processes reports of adverse drug reactions
arriving from clinical trial sites across many countries and time zones, and gets
them into a central store without losing or duplicating data — even when a site
loses connectivity for hours or days.

**This is a portfolio piece for competitive SWE recruiting, targeted specifically
at Eli Lilly Bio-IT / Clinical engineering.** It is grounded in a verified real
interview question from that team: *"design a system to track adverse drug events
reported from clinical trials across multiple countries and time zones."*

**What this project deliberately is NOT:**
- Not a domain-science project. Earlier concepts for this portfolio slot (an ML
  model on tabular pharma data, a distributed molecular docking pipeline, a
  lab-automation protocol compiler) were killed after verification showed mature,
  funded incumbents already own those spaces (VirtualFlow/VinaLC for docking at
  HPC scale; Synthace's Antha + Tecan for lab automation).
- Not an attempt to replace real pharmacovigilance platforms (Oracle Argus,
  ArisGlobal, Veeva Vault Safety). We are not competing with them.
- Not a commercial product.

**What it IS:** a demonstration of distributed-systems engineering depth, using a
pharma-relevant payload shape (FHIR-ish adverse event records) as the vehicle.
Correctness under partition and failure is the point — not feature breadth.

### On the name

"Pharos" (the lighthouse of Alexandria) fits well: a lighthouse is a distributed
beacon — a fixed signal source that many distant, disconnected ships depend on and
that must keep signaling correctly regardless of what's happening at any one ship.
That maps cleanly onto edge sites reporting into a central signal that must stay
correct under partition. Keeping the name — it's specific to what the system does,
not generic. Reopen this if it stops feeling right once the architecture solidifies.

---

## 2. Core engineering challenges (design decisions)

These four are the actual point of the project. Everything else is scaffolding
around them.

### 2.1 Network partition tolerance

**Decision:** Store-and-forward at the edge. Each trial site runs a local edge
collector (not a thin HTTP client) that durably persists incoming adverse event
reports to local disk *before* attempting to forward anything upstream. Forwarding
to the central Kafka backbone happens asynchronously, with retry/backoff, and is
allowed to lag indefinitely without data loss.

**Reasoning:** A trial site (e.g. Nigeria) losing connectivity to a US data center
must not lose or corrupt data. The only way to guarantee that against an
unbounded-duration partition is to never depend on the network being up at the
moment the event is captured. The edge collector's local durability is the actual
reliability boundary — Kafka's replication guarantees only start once bytes leave
the site.

**Resolved 2026-08-29 — edge collector shape and storage:** the edge collector is
a standalone Go binary per trial site (not a shared multi-tenant service — this
keeps the partition-tolerance story simple: one process, one local disk, one
site's worth of blast radius), using embedded SQLite in WAL mode
(`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`) as its local durable
queue. Chosen over a custom file WAL (reinvents crash recovery/indexing for no
benefit) or an embedded Kafka-compatible log like Redpanda (too heavy for an
edge agent, complicates deployment on site workstations). Resolves former Open
Questions 2 and 5 — full comparison in
[ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md).

**Resolved 2026-08-29 — topology:** the edge collector never connects directly
to Kafka brokers. Trial-site network policies routinely block raw broker ports,
and a direct connection would also bypass per-site rate limiting and payload
validation, which have to happen centrally (§2.3). Instead the edge collector
forwards queued batches to the Central Ingestion service over an HTTPS API
(`POST /api/v1/events`) with retry/backoff; Central Ingestion is the only thing
that writes to Kafka.

**Resolved 2026-08-29 — retry/backoff formula:** Exponential Backoff with Full
Jitter: `backoff = random_between(0, min(MaxBackoff, BaseBackoff * 2^attempts))`,
with `BaseBackoff=500ms`, `MaxBackoff=30s`, `BatchSize=50`, `PollInterval=1s`
(when the queue is empty), `RequestTimeout=5s`. On HTTP 429 the forwarder
respects a `Retry-After` header if present (clamped 1s–60s); on timeouts,
connection refused, or 5xx it backs off per the formula above. Full Jitter
(not plain exponential backoff) specifically to avoid every site's retries
synchronizing into a thundering herd when Central Ingestion recovers from an
outage. Full detail in [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md).

### 2.2 Exactly-once processing semantics

**Decision:** Exactly-once is *not* delivered by Kafka transactions alone — those
only cover producer→topic→consumer hops internal to the Kafka cluster. The actual
end-to-end exactly-once boundary starts at the client (trial site) and has to be
enforced with an application-level idempotency key.

- Each adverse event report carries a client-generated idempotency key
  (`site_id + local_sequence_number`, assigned at the moment of capture, before
  any network attempt).
- The edge collector and the central ingestion service both dedup on that key.
  Central Ingestion's pipeline order is: intake → rate-limit → FHIR validation →
  dedup check → publish to Kafka (§2.3).
- Internal stream processing (ingestion → validation → storage) uses Kafka's
  idempotent producer + transactional writes to get effectively-once *within* the
  pipeline.

**Resolved 2026-08-29 — dedup store:** Cassandra, via a lightweight-transaction
insert (`INSERT ... IF NOT EXISTS`) into an `event_outbox` table. Chosen over
Redis+TTL because a TTL risks re-processing a duplicate that arrives after a
very long site outage — exactly the partition scenario §2.1 exists to
tolerate — where Cassandra gives a permanent, non-expiring dedup record
instead. Resolves former Open Question 3.

**Resolved 2026-08-30 — transactional outbox with a claim lock, not a bare
boolean:** the dedup-key insert and the Kafka publish must not be two
independent steps with a crash window between them, *and* concurrent
duplicate requests must not be able to both decide "the original crashed,
I'll publish too." A boolean `published` flag closes the first problem but
not the second — it can't distinguish "nobody is publishing this" from
"someone is publishing this right now." The actual design: `event_outbox`
carries a three-state `status` (`PUBLISHING` → `PUBLISHED`, set directly by
the winning `INSERT ... IF NOT EXISTS` — the insert winning *is* the claim)
plus a `claimed_at` lease timestamp. Any request that loses the insert reads
the row: if `PUBLISHED`, no-op; if `PUBLISHING` with a fresh lease, another
worker is actively handling it, do nothing; if `PUBLISHING` with an expired
lease (default 30s), steal it via a compare-and-swap `UPDATE ... IF
status='PUBLISHING' AND claimed_at=?` — only the winner of that CAS may call
Kafka. A background sweeper (every 10s, indexed via a `pending_outbox`
time-bucketed table to avoid scanning a low-cardinality column) applies the
identical claim/CAS logic as the ultimate backstop, independent of whether
the edge ever retries. The dead-letter path (§2.3) uses the exact same
three-state pattern, symmetrically — a rejected event has the identical
crash-window risk as an accepted one, and treating it as a special case was
the gap that got caught in review. Both `event_outbox.payload` and
`dead_letter_events.payload` store the *raw JSON bytes* Central Ingestion
received, never a re-serialized Go struct, per the Slice 2 fix. Full schemas,
CQL, and the lifecycle walkthrough are in
[ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). An in-process
per-key mutex may be layered on top purely to avoid redundant same-process
Cassandra round-trips — it is never the actual correctness mechanism, since
it wouldn't hold across a process restart or multiple instances; the
Cassandra claim/CAS is what's authoritative. Any dedup implementation that
uses a plain boolean instead of this claim/lease pattern does not satisfy
this section and should be sent back for revision.

**Reasoning:** A severe adverse event must never be silently dropped *or*
duplicated, even under retries. Retries are guaranteed to happen (that's the whole
point of 2.1), so dedup has to be a first-class part of the write path, not an
afterthought bolted onto Kafka.

**This is the easiest place for Antigravity-generated code to get subtly wrong** —
watch for: idempotency keys generated server-side instead of client-side (breaks
the guarantee across a retry after a dropped response), or dedup checks that race
under concurrent retries from the same site.

### 2.3 Rate limiting and dead-letter queues

**Decision:** Ingestion validates every payload against a FHIR-based adverse-event
schema at the edge of the central system (not at the edge collector — sites should
be able to buffer even malformed-looking data rather than lose it locally).
Malformed/out-of-spec payloads are routed to a Kafka dead-letter topic with
structured failure metadata (which validation rule failed, raw payload, site,
timestamp) rather than being dropped or blocking the pipeline.

**Resolved 2026-08-29 — rate limiter:** per-site token bucket behind a Go
`RateLimiter` interface, starting with an in-memory implementation (default
100-token capacity/burst, 10 tokens/sec refill) since Central Ingestion runs as
a single instance for now. Exhaustion returns HTTP 429 with `Retry-After`,
`X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` headers —
the edge forwarder's backoff (§2.1) is designed to respect `Retry-After`
directly. The interface is kept pluggable so a Redis-backed distributed token
bucket can be swapped in without changing callers once Central Ingestion
actually runs multiple replicas behind a load balancer — no reason to pay that
resource/complexity cost before it's needed.

**Resolved 2026-08-29 — what a token meters:** each token is consumed per
inbound HTTP batch request (`POST /api/v1/events`), not per individual event.
Since the edge forwarder batches up to `BatchSize` (50) events per request
(§2.1), the effective burst allowance is `capacity × BatchSize` — e.g. the
100-token default permits a burst of up to 5,000 events, with sustained
throughput up to `refill_rate × BatchSize` (500 events/sec at the default 10
tokens/sec). This is a request-level throttle on the HTTP intake layer, not a
precise per-event budget — full detail and the alternatives considered in
[ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md).

**Reasoning:** A single misbehaving site (bad clock, buggy client, malformed FHIR)
must not be able to degrade ingestion for every other site, and must not silently
lose data — DLQ entries need to be inspectable and replayable once fixed.

**Resolved 2026-08-30 — DLQ durability uses the same claim/lease outbox
pattern as §2.2, not a simpler one-shot write.** A rejected event has the
identical crash-window risk as an accepted one: a naive "write to Cassandra,
then publish to the Kafka DLQ topic" as two independent steps could crash
between them and leave the rejection unrecoverable, silently defeating the
"never lose data" guarantee this section exists to provide — it would just be
the same bug relocated to the rejection path. `dead_letter_events` mirrors
`event_outbox`'s exact schema shape (three-state `status`, `claimed_at`
lease, raw JSON `payload`): the insert on rejection writes `status='PUBLISHING'`
directly (the insert winning is the claim), a Kafka publish to
`pharos.events.dlq` (partitioned by `site_id`) follows, and only after that
succeeds does `status` flip to `PUBLISHED` — with the same background sweeper
and lease-expiry CAS as the accept path as the resumption backstop. Central
Ingestion only responds 422/207 to the edge after the Cassandra write
durably succeeds. Full schema and lifecycle in
[ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md).

**Resolved 2026-08-30 — batch response status codes are per-event-aware, not
all-or-nothing.** A batch can have a genuine mix of outcomes: some events
accepted, some rejected by FHIR validation, and some hit a transient
infrastructure failure (Cassandra or Kafka unavailable) unrelated to the
event's own validity. Collapsing all of that into a single status code loses
information the edge needs to retry correctly. The contract: **207
Multi-Status** whenever the batch has any successfully-processed events
(accepted or rejected) alongside infrastructure failures — the edge forwarder
is required to parse the body and act per-event (`MarkAcknowledged` for
accepted, `MarkRejected` for rejected, `MarkFailed` with backoff for the
`FAILED` status specifically introduced for this case). A bare **503** is
reserved for the narrow case where *every* event in the batch hit an infra
failure with nothing else to report. This was caught during review as an
incomplete fix: an earlier version returned 503 for any infra failure
regardless of what else succeeded, which the forwarder's response parser
(scoped to 200/201/207/422) silently ignored — safe by accident, because
retrying the whole batch is idempotent, but it meant the granular per-event
information was produced and never consumed, and any status code the
forwarder doesn't explicitly parse must never be assumed safe to fall back to
"acknowledge everything." The forwarder's fallback for an unmapped record now
depends on the response's own status code: 200/201 (unambiguous full success)
defaults an unmapped record to acknowledged, but 207 and anything else
defaults an unmapped record to `MarkFailed` with backoff — silence is never
treated as success.

**Resolved 2026-08-29 — payload schema:** rather than implementing the full FHIR
R4 `AdverseEvent` resource (large, mostly irrelevant regulatory sub-fields for
this project's purpose), Pharos targets a deliberately scoped, named subset:
`resourceType`, `identifier` (carrying the idempotency key), `actuality`,
`subject`, `event` (MedDRA-coded), `date` (event time, zone-aware) /
`recordedDate` (capture time), `seriousness`, `severity`, `study`, `location`,
`suspectEntity`. Being an explicit, documented subset — not an ad hoc shape —
means it reads as a deliberate scoping decision rather than an incomplete FHIR
implementation in a technical walkthrough. Resolves former Open Question 4; full
field list in [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md).

### 2.4 Multi-timezone event ordering and correctness

**Decision:** Do not attempt a single global total order — that's not achievable
correctly in a partition-tolerant distributed system, and pretending otherwise is
the kind of thing that doesn't survive a technical walkthrough. Instead:

- Every event carries both **event time** (when the adverse reaction was
  observed/reported at the site, in the site's local time, normalized to UTC at
  capture) and **ingestion time** (when the central system received it).
- Kafka partition key = `site_id`, which guarantees per-site ordering is preserved
  end-to-end (a given site's events arrive at the central log in the order that
  site produced them).
- Cross-site ordering is handled at the query/processing layer via event-time
  semantics (watermarking), not by forcing artificial global sequence numbers at
  ingestion.

**Reasoning:** This mirrors how real distributed systems (and real
pharmacovigilance timelines) actually have to reason about "when did this happen"
across time zones — you need event time for clinical/regulatory correctness and
ingestion time for operational monitoring, and they are not interchangeable.

---

## 3. Planned stack

| Layer | Choice | Status |
|---|---|---|
| Edge collector | Go | not sanity-checked against Rust — Go's default choice here for simpler concurrency model + easy single-binary edge deployment |
| Central ingestion service | Go | same reasoning |
| Event backbone | Apache Kafka | fairly confident fit |
| Primary storage | Apache Cassandra, self-hosted via Docker | **resolved 2026-08-28 — see §5.1** |
| Dedup store | Cassandra (LWT insert + transactional outbox to Kafka) | resolved 2026-08-29 — see §2.2 |
| Rate limiter backing store | In-memory token bucket (Go interface, Redis-pluggable later) | resolved 2026-08-29 — see §2.3 |

Go vs Rust: no strong reason yet to reach for Rust's stricter guarantees over Go's
simplicity for this scope. Revisit only if a specific component turns out to need
Rust-level control (unlikely for this project's scope).

---

## 4. Built vs. not yet

Nothing is built yet. This section starts empty and gets checked off as real code
lands — do not mark anything done based on a plan or a stub.

- [x] Repo scaffolding (Go module layout, linting, CI) — Slice 1
- [x] Edge collector: local durable buffering — Slice 1 (SQLite WAL)
- [x] Edge collector: forwarding to Central Ingestion API with retry/backoff — Slice 2
- [x] Idempotency key generation (client-side, at capture time) — Slice 1
- [x] Central ingestion service: HTTP intake (`POST /api/v1/events`) — Slice 2
- [x] Central ingestion service: FHIR schema validation (scoped profile, §2.3) — Slice 2
- [ ] Dead-letter topic + DLQ inspection tooling — persistence half done (Slice 3:
      Cassandra `dead_letter_events` + Kafka `pharos.events.dlq`, both durable
      before responding to the edge); no dedicated inspection tooling built yet
- [x] Per-site rate limiting — Slice 2
- [x] Dedup store: Cassandra LWT + transactional outbox to Kafka (§2.2) — Slice 3,
      verified against a real Cassandra cluster and a real Kafka broker, not just mocks
- [ ] Kafka topic design (partitioning strategy, retention) — partitioning by
      `site_id` done and verified (Slice 3); retention policy not yet configured
      (running on Kafka's defaults)
- [x] Cassandra schema design — Slice 3 (`event_outbox`, `dead_letter_events`,
      `pending_outbox`), verified against a real cluster
- [ ] Stream processing layer (event-time ordering / watermarking) — not started,
      next slice
- [ ] Fault-injection test suite (network partition simulation) — not yet a
      dedicated test; this is explicitly Claude's responsibility per §6, still owed
- [x] Fault-injection test suite (duplicate delivery) — covered by Slice 3's
      `TestConcurrentDuplicateRaces`, `TestSequentialDuplicateIdempotency`,
      `TestCassandraOutboxStore_RealIntegration`'s concurrent-race case
- [ ] Fault-injection test suite (out-of-order delivery) — not yet a dedicated
      test; still owed
- [x] Fault-injection test suite (malformed FHIR payloads → DLQ) — covered by
      Slice 3's `TestDeadLetterPipeline_DurabilityAndRouting` and related handler tests
- [ ] Observability (metrics on lag, dedup hit rate, DLQ volume) — in-memory
      counters exist (`Handler.ExtendedStats`) but nothing exported/scraped yet

---

## 5. Open questions

1. ~~**Cassandra vs. alternatives.**~~ **RESOLVED 2026-08-28.** Hard constraint
   surfaced: this project must be buildable on AI subscriptions alone, with no
   cloud infra spend. Verified: Apache Cassandra is Apache License 2.0 — fully
   free, no fee, no usage cap, ever, at any scale, self-hosted. There is no
   licensing cost to worry about; the only cost anyone pays for Cassandra is
   *managed* hosting (DataStax Astra, AWS Keyspaces), which this project doesn't
   need. Plan: self-host via Docker Compose on Gideon's own machine — a single
   node for daily dev (recommended minimum ~4GB RAM, reducible via JVM heap
   flags for a small dev dataset) and a 2-3 node local cluster (each container
   heap-capped) when specifically exercising multi-DC/replication behavior for
   the fault-injection tests. Considered ScyllaDB as a lighter-footprint,
   CQL-wire-compatible alternative, but its license changed from open-source
   (AGPL) to source-available with a 10TB free-usage cap — Cassandra's Apache 2.0
   license is unconditionally free and more universally recognized in an
   interview context, so staying with Cassandra. Sanity-check against real
   access patterns (§ below) is still open — the *cost* question is closed, the
   *fit* question is not.
   - **Still open:** actual query pattern fit (e.g., "all events for trial X in
     date range Y" may want a different partition key than "all events from site
     Z"). Revisit once schema + top 3-5 real queries are drafted.
2. ~~Edge collector local durability mechanism.~~ **RESOLVED 2026-08-29** —
   SQLite-as-WAL. See §2.1.
3. ~~Dedup store choice.~~ **RESOLVED 2026-08-29** — Cassandra LWT, with a
   mandatory transactional-outbox pattern to the Kafka publish step (not a bare
   check-then-publish). See §2.2. Approved with this one required modification
   to Gemini's original proposal — see
   [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md) for the full review.
4. ~~Exact FHIR resource profile to target.~~ **RESOLVED 2026-08-29** — scoped
   named subset of FHIR R4 `AdverseEvent`. See §2.3.
5. ~~One Go binary per site vs. shared multi-tenant edge service.~~
   **RESOLVED 2026-08-29** — one binary per site. See §2.1.

---

## 6. Working agreement (for whoever reads this — Antigravity, Claude, or future Gideon)

- Gideon builds features in Google Antigravity (Gemini), usually in sessions
  separate from Claude Code.
- Claude Code's role: plan/architecture review, test suite (including
  fault-injection tests for partition/duplicate/out-of-order scenarios — this is
  Claude's primary hands-on-code responsibility), and reviewing Antigravity's
  output against this file and the four challenges in §2.
- **All work happens on feature branches (`feat/`, `fix/`), never committed
  straight to `main`.** Gemini/Gideon push a branch; Claude Code reviews the
  diff (same as it reviews proposals and does spot-check code review); Gideon
  merges via PR. Resolved 2026-08-29 — the first three commits on this repo
  went straight to `main` before this was decided; that's not being rewritten,
  but everything from here forward uses branches.
- **This file (`PLAN.md`) is not edited directly by Antigravity/Gemini.** If
  building surfaces a reason to deviate from or extend what's recorded here,
  Gemini writes the proposal into [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md)
  instead — never edits this file to match new code. Gideon then tells Claude Code
  to review that file. Claude either approves (and folds the accepted decision
  into this file itself, marking the proposal resolved) or rejects with reasoning
  written back into the proposals file. This happens *before* Gemini implements
  the change, not after.
- **Every implementation session gets logged in [WORKLOG.md](WORKLOG.md)** —
  what was built, why, how, files/tests touched — regardless of whether Gemini or
  Claude did the work. Treat this like an actual engineering log at a job: if it's
  not in the worklog, it didn't happen.
- If a change conflicts with a decision already recorded here, **stop and flag it
  to Gideon** rather than silently reconciling it or rewriting this file to match
  new code.
- Update this file's checklist and open questions as work actually lands — not
  speculatively.
