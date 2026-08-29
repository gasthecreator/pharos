# Pharos — Living Plan

**Status:** pre-code. This file is the single source of truth for architecture and
progress. Both Antigravity and Claude Code read this before touching code. Keep it
updated as decisions change — do not let it go stale.

Last updated: 2026-08-28

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

**Open question:** what does the edge collector's local durable queue look like —
embedded WAL, SQLite, or an embedded Kafka-compatible log (e.g. Redpanda in
single-node mode)? Leaning SQLite-as-WAL for simplicity of a single deployable Go
binary; revisit once the edge collector's exact write pattern is designed.

### 2.2 Exactly-once processing semantics

**Decision:** Exactly-once is *not* delivered by Kafka transactions alone — those
only cover producer→topic→consumer hops internal to the Kafka cluster. The actual
end-to-end exactly-once boundary starts at the client (trial site) and has to be
enforced with an application-level idempotency key.

- Each adverse event report carries a client-generated idempotency key
  (`site_id + local_sequence_number`, assigned at the moment of capture, before
  any network attempt).
- The edge collector and the central ingestion service both dedup on that key —
  ingestion does an insert-if-not-exists against the dedup store before
  publishing to Kafka, so a retried/duplicate send from a reconnecting site is a
  no-op, not a duplicate record.
- Internal stream processing (ingestion → validation → storage) uses Kafka's
  idempotent producer + transactional writes to get effectively-once *within* the
  pipeline.

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

Rate limiting is per-site (token bucket), enforced at the central ingestion
service, backed by a shared store (Redis, most likely) since ingestion runs as
multiple instances behind a load balancer.

**Reasoning:** A single misbehaving site (bad clock, buggy client, malformed FHIR)
must not be able to degrade ingestion for every other site, and must not silently
lose data — DLQ entries need to be inspectable and replayable once fixed.

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
| Dedup store | TBD (Redis vs Cassandra LWT) | open question, tied to 2.2 |
| Rate limiter backing store | Redis (leading candidate) | not yet validated |

Go vs Rust: no strong reason yet to reach for Rust's stricter guarantees over Go's
simplicity for this scope. Revisit only if a specific component turns out to need
Rust-level control (unlikely for this project's scope).

---

## 4. Built vs. not yet

Nothing is built yet. This section starts empty and gets checked off as real code
lands — do not mark anything done based on a plan or a stub.

- [ ] Repo scaffolding (Go module layout, linting, CI)
- [ ] Edge collector: local durable buffering
- [ ] Edge collector: forwarding to Kafka with retry/backoff
- [ ] Idempotency key generation (client-side, at capture time)
- [ ] Central ingestion service: HTTP/gRPC intake
- [ ] Central ingestion service: FHIR schema validation
- [ ] Dead-letter topic + DLQ inspection tooling
- [ ] Per-site rate limiting
- [ ] Dedup store + insert-if-not-exists path
- [ ] Kafka topic design (partitioning strategy, retention)
- [ ] Cassandra schema design
- [ ] Stream processing layer (event-time ordering / watermarking)
- [ ] Fault-injection test suite (network partition simulation)
- [ ] Fault-injection test suite (duplicate delivery)
- [ ] Fault-injection test suite (out-of-order delivery)
- [ ] Fault-injection test suite (malformed FHIR payloads → DLQ)
- [ ] Observability (metrics on lag, dedup hit rate, DLQ volume)

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
2. Edge collector local durability mechanism — SQLite-as-WAL vs. embedded
   log-structured store (see 2.1).
3. Dedup store choice — Redis (fast, needs TTL tuning to not evict a key before a
   slow retry arrives) vs. Cassandra lightweight transactions (consistent with
   primary storage, but LWTs are expensive at Cassandra's usual write volume).
4. Exact FHIR resource profile to target (full FHIR AdverseEvent resource is
   large — likely want a deliberately scoped subset for this project, documented
   explicitly so it doesn't read as an incomplete FHIR implementation).
5. Whether the edge collector should be one Go binary per site or a shared
   multi-tenant service the site talks to over a local network — affects the
   partition-tolerance story materially and should be decided before any code
   exists for 2.1.

---

## 6. Working agreement (for whoever reads this — Antigravity, Claude, or future Gideon)

- Gideon builds features in Google Antigravity (Gemini), usually in sessions
  separate from Claude Code.
- Claude Code's role: plan/architecture review, test suite (including
  fault-injection tests for partition/duplicate/out-of-order scenarios — this is
  Claude's primary hands-on-code responsibility), and reviewing Antigravity's
  output against this file and the four challenges in §2.
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
