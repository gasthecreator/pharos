# Architecture Proposals

**This file is for Gemini (Antigravity).** If building Pharos surfaces a reason to
deviate from, extend, or reconsider anything recorded in `PLAN.md`, write the
proposal here — as a new entry at the top — instead of editing `PLAN.md` directly.

Do not implement the change yet. Wait for Gideon to bring this file to Claude Code
for review. Claude will either:
- **Approve** — fold the decision into `PLAN.md` itself and mark the entry
  `Resolved: Approved` here, or
- **Reject** — leave `PLAN.md` untouched and mark the entry `Resolved: Rejected`
  here, with reasoning.

Only after an entry is marked `Resolved: Approved` should it be implemented.

---

## How to write a proposal

Copy this template for each new entry:

```
## [YYYY-MM-DD] Short title of the proposed change

**Status:** Pending

**What in PLAN.md this touches:** (section number(s), e.g. "§2.2 Exactly-once
processing semantics")

**What I'm proposing:** (the concrete change — be specific: new component,
different technology, a changed guarantee, etc.)

**Why:** (what you ran into while building that makes the current plan
insufficient, wrong, or worth reconsidering — cite the actual blocker, not just
a preference)

**Alternatives considered:** (what else you looked at and why this is better)

**Impact if approved:** (what else in PLAN.md or already-built code would need
to change)
```

---

## Proposals

## [2026-08-29] Clarification: Central Ingestion Rate Limiter Meters HTTP Batch Requests, Not Individual Events

**Status:** Pending

**What in PLAN.md this touches:** §2.3 (Rate limiting and dead-letter queues).

**What I'm proposing:** Clarify in PLAN.md §2.3 that Central Ingestion's per-site token bucket rate limiter meters incoming HTTP batch requests (`POST /api/v1/events`), where each request consumes 1 token from the site's bucket. Because edge forwarders batch up to `BatchSize` (default: 50) events per HTTP request, a 100-token capacity permits a burst of up to 100 HTTP requests (up to 5,000 events), with a refill rate of 10 requests/sec (up to 500 events/sec sustained).

**Why:** PLAN.md §2.3 currently specifies a "100-token capacity, 10-token/second refill rate" without distinguishing whether tokens meter HTTP requests or individual adverse events. In practice, rate limiting at ingress before unmarshaling and validating individual events protects the HTTP intake layer from request exhaustion and denial-of-service, but the 50x event multiplier should be documented explicitly so capacity planning and rate limits align.

**Alternatives considered:**
1. Meter per-event (deduct `len(events)` tokens per request): Requires reading and parsing the JSON payload body *before* evaluating the rate limiter, which leaves the ingestion service vulnerable to CPU/memory exhaustion attacks from unauthenticated or throttled sites sending large payloads.
2. Two-tier rate limiting (meter HTTP requests at ingress, plus event count after parsing): Adds complexity without substantial benefit given bounded batch sizes (`BatchSize = 50`).

**Impact if approved:** A one-line clarification in PLAN.md §2.3 defining tokens as metering HTTP batch requests and noting the effective event capacity (`tokens * BatchSize`).

---

## [2026-08-29] Central Ingestion Rate Limiter: In-Memory Token Bucket with Pluggable Interface

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md
§2.3 and §3 as-is. Starting in-memory and deferring Redis until Central
Ingestion actually runs multiple replicas is the right call — no reason to pay
that resource cost before it's needed. No changes required.

**What in PLAN.md this touches:** §2.3 (Rate limiting and dead-letter queues), §3 (Rate limiter backing store).

**What I'm proposing:** Implement per-site rate limiting at Central Ingestion using a thread-safe token bucket algorithm behind a Go `RateLimiter` interface. For local dev and test execution (consistent with the zero-cloud-spend constraint), default to an in-memory token bucket implementation (`pkg/ratelimit`) parameterized per site with configurable capacity/burst (default: 100 tokens) and refill rate (default: 10 tokens/sec). When exhausted, Central Ingestion returns HTTP 429 (Too Many Requests) with standard headers: `Retry-After: <seconds>`, `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`. The interface allows swapping in a Redis-backed distributed token bucket (via Lua script) when Central Ingestion scales to multiple instances.

**Why:** PLAN.md §3 listed Redis as "not yet validated", and Open Question 3 previously avoided an immediate Redis dependency to keep local resource usage lean. An interface-driven token bucket allows full testability of burst behavior, per-site isolation, and 429 backoff semantics without forcing a Redis container to run during initial slices.

**Alternatives considered:**
1. Hard-requiring Redis via Docker Compose immediately: Increases memory footprint and test complexity for Slice 2, though it will be necessary if Central Ingestion runs multiple replicas behind a load balancer.
2. Leaky bucket algorithm: Token bucket is preferred because it permits natural bursts of adverse event reports from sites while bounding sustained load.

**Impact if approved:** Formalizes the rate-limiting interface, default capacity/refill parameters, and HTTP 429 response headers in §2.3.

---

## [2026-08-29] Edge Forwarder Retry and Exponential Backoff with Full Jitter

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md
§2.1 as-is. Full Jitter is the right call to avoid synchronized retry storms
when Central Ingestion recovers from an outage; parameters are reasonable
defaults. No changes required.

**What in PLAN.md this touches:** §2.1 (Network partition tolerance), §2.3 (Rate limiting response).

**What I'm proposing:** Formalize the edge forwarder retry/backoff algorithm as Exponential Backoff with Full Jitter:
`backoff = random_between(0, min(MaxBackoff, BaseBackoff * 2^attempts))`
With standard parameters:
- `BaseBackoff`: 500ms
- `MaxBackoff`: 30s
- `BatchSize`: 50 events
- `PollInterval`: 1s (when queue has no pending items)
- `RequestTimeout`: 5s
When receiving HTTP 429, the forwarder respects the `Retry-After` header if present (clamped to 1s–60s); on network timeouts, connection refused, or HTTP 5xx, it increments the attempt count and schedules `next_retry_at = now + backoff`.

**Why:** PLAN.md §2.1 specifies "with retry/backoff, and is allowed to lag indefinitely without data loss", but left the exact backoff formula and parameters unspecified. Standard exponential backoff without jitter causes synchronized "thundering herd" retry waves across trial sites when Central Ingestion recovers from an outage. Full Jitter spreads retry traffic uniformly across the backoff window.

**Alternatives considered:**
1. Constant retry interval: Simpler, but overloads a recovering central service and fails to scale back during multi-day network partitions.
2. Equal Jitter or Decorrelated Jitter: Full Jitter provides optimal desynchronization with low average latency for client retries (supported by AWS Architecture research).

**Impact if approved:** Standardizes forwarder parameters and resilience behavior in §2.1.

---

## [2026-08-29] Clarify Ingress Topology: Edge forwards to Central Ingestion API, not direct Kafka over WAN

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md §2.1
as-is. Good catch: this ambiguity should have been explicit in the original plan.
No changes required.

**What in PLAN.md this touches:** §2.1 (line 59), §2.2 (line 84), §2.3 (line 104), §4 (line 166).

**What I'm proposing:** Formally clarify that the Edge Collector forwards queued adverse event batches to the Central Ingestion Service via an HTTPS API (`POST /api/v1/events`), rather than connecting directly to Kafka brokers over WAN. The Central Ingestion Service performs per-site token-bucket rate limiting, FHIR schema validation, and dedup checks before publishing valid events to Kafka (or rejected events to the DLQ).

**Why:** PLAN.md §2.1 mentions "Forwarding to the central Kafka backbone happens asynchronously" and §4 checklist lists "Edge collector: forwarding to Kafka with retry/backoff". However, §2.2 states "ingestion does an insert-if-not-exists against the dedup store before publishing to Kafka", and §2.3 states "Ingestion validates every payload against a FHIR-based adverse-event schema at the edge of the central system... Rate limiting is per-site (token bucket), enforced at the central ingestion service". Connecting edge collectors directly to Kafka over WAN across international clinical trial sites is an anti-pattern: hospital firewalls frequently block raw Kafka ports, exposing Kafka brokers over WAN compromises security, and bypassing Central Ingestion makes centralized rate limiting and pre-ingestion schema/dedup validation impossible.

**Alternatives considered:**
1. Direct Kafka connection over WAN: Edge connects to Kafka brokers with SASL/TLS. Rejected due to hospital firewall restrictions, broker exposure, and lack of pre-Kafka rate limiting / dedup hook.
2. Kafka REST Proxy (Confluent-style): Exposes an HTTP endpoint directly to Kafka. Rejected because it bypasses custom FHIR validation, token-bucket rate limiting per site, and application-level dedup checks before enqueueing.

**Impact if approved:** Clarify wording in §2.1, §3, and §4 to state "Edge collector: forwarding to Central Ingestion API with retry/backoff" and "Central Ingestion: intake -> rate-limit -> validate -> dedup -> publish to Kafka".

---

## [2026-08-29] Edge Collector Local Durability via Embedded SQLite in WAL Mode

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md
§2.1 as-is, resolving former Open Questions 2 and 5. No changes required.

**What in PLAN.md this touches:** §2.1, §5.2 (Open Question 2), §5.5 (Open Question 5).

**What I'm proposing:** Implement the edge collector as a standalone Go binary per site (`pharos-edge`) using embedded SQLite in Write-Ahead Logging (`WAL`) mode (`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`) as its local durable queue. The edge collector exposes a local HTTP endpoint (`http://localhost:8080/api/v1/adverse-events`) for local clinical site systems, persists records with status `PENDING` and assigns an atomic `local_sequence_number`, and runs a background forwarder goroutine that streams batches to the Central Ingestion API with exponential backoff and jitter.

**Why:** Open Question 2 and 5 are unresolved. SQLite with WAL mode provides single-binary deployment with zero external daemons, ACID transactional durability, atomic sequence generation (`AUTOINCREMENT` or atomic counter), and status tracking (`PENDING`, `SENDING`, `ACKNOWLEDGED`). It is crash-resilient, handles OS/power restarts cleanly, and avoids the complexity of rolling a custom file WAL or the resource footprint of running local Kafka/Redpanda.

**Alternatives considered:**
1. Custom append-only file WAL: Very lightweight, but requires manually implementing indexing, crash recovery, compaction, and retry state tracking.
2. Embedded KV (e.g. bbolt, Pebble, Badger): Good for key-value, but querying FIFO ordered pending records and managing state transitions requires extra indexing logic that SQLite handles natively.
3. Embedded Redpanda/Kafka at edge: Overkill for an edge agent; consumes too much RAM and complicates deployment on hospital workstations.

**Impact if approved:** Resolves Open Questions 2 and 5 in §5. Defines the storage contract (`QueueStore`) for `pharos-edge`.

---

## [2026-08-29] Define Scoped FHIR R4 AdverseEvent Schema Profile

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md
§2.3 as-is, resolving former Open Question 4. No changes required.

**What in PLAN.md this touches:** §2.3, §5.4 (Open Question 4).

**What I'm proposing:** Define a concrete, documented subset of HL7 FHIR R4 `AdverseEvent` as the official payload contract for Pharos. The payload must include:
- `resourceType`: `"AdverseEvent"`
- `identifier`: Array containing an entry with `system: "urn:pharos:idempotency-key"` and `value: "<site_id>:<local_sequence_number>"`
- `actuality`: `"actual"`
- `subject`: Reference to trial participant (e.g., `{"reference": "Patient/SUBJ-10492"}`)
- `event`: CodeableConcept containing MedDRA coding or text (e.g., `"Anaphylaxis"`)
- `date`: ISO 8601 string with site timezone offset representing observation event-time
- `recordedDate`: ISO 8601 string representing capture time at the site
- `seriousness`: Serious adverse event flag/code (e.g. hospitalization, life-threatening)
- `severity`: CodeableConcept (`"mild"`, `"moderate"`, `"severe"`)
- `study`: Reference to clinical trial (e.g., `{"reference": "ResearchStudy/LILLY-401"}`)
- `location`: Reference to trial site (e.g., `{"reference": "Location/SITE-NG-01"}`)
- `suspectEntity`: Array of suspected medicinal products (drug code, name)

**Why:** Open Question 4 identified that the full FHIR `AdverseEvent` specification is vast, with dozens of complex optional nested fields. A clearly documented, interview-grounded profile allows strict structural and domain validation (and realistic DLQ routing for malformed events) without getting bogged down in arbitrary regulatory sub-fields.

**Alternatives considered:**
1. Unstructured JSON: Fails to reflect pharma domain authenticity for Eli Lilly Bio-IT recruiting.
2. Full FHIR R4 spec parser: Bloats validation logic with irrelevant clinical fields that don't exercise distributed systems principles.

**Impact if approved:** Resolves Open Question 4 in §5. Provides the exact schema definition for Central Ingestion validation and test generation.

---

## [2026-08-29] Dedup Store Choice: Cassandra Table with Fallback to Redis Interface

**Status:** Resolved: Approved with one required modification (Claude Code,
2026-08-29). The choice of Cassandra LWT over Redis+TTL is right, and the
reasoning about long-partition retries outliving a TTL is exactly correct — that
stays as proposed.

**Required modification before implementation:** the proposal as written
implies a two-step "insert dedup key, then publish to Kafka" sequence. That has
a fatal gap: if the LWT insert succeeds and the process crashes or partitions
*before* the Kafka publish completes, every future retry of that same event will
be seen as an already-processed duplicate and silently dropped forever — this is
precisely the failure mode PLAN.md §2.2 calls out as the easiest thing to get
subtly wrong here, and this proposal as written would have shipped it.

Fix: use a transactional outbox instead of check-then-publish. Insert the event
row into Cassandra with its idempotency key and a `published: false` flag in one
statement (this is your dedup check — the LWT `IF NOT EXISTS` on that insert is
what makes a retry a no-op). A separate, retriable step reads unpublished rows,
publishes to Kafka, and only then flips `published: true`. An interrupted
publish is resumable on the next pass instead of being permanently lost. This
also mirrors the store-then-forward shape already used at the edge (§2.1), so
it's consistent with the rest of the system, not a one-off pattern.

Folded into PLAN.md §2.2 with this modification included. Implement dedup
against §2.2 as written, not against this entry's original text.

**What in PLAN.md this touches:** §2.2, §3, §5.3 (Open Question 3).

**What I'm proposing:** Implement a `DedupStore` interface in Go with an initial implementation using Cassandra's `processed_idempotency_keys` table using lightweight transactions (`INSERT ... IF NOT EXISTS`) or conditional upsert, while keeping the interface pluggable for Redis.

**Why:** Open Question 3 weighed Redis vs Cassandra LWT. Cassandra is already the chosen primary storage in the architecture and running locally via Docker. Using Cassandra for the dedup store avoids adding Redis to the local stack immediately, keeping memory footprint low on dev machines. Adverse event reporting rates at clinical sites (typically dozens to thousands of events per day per site, not millions per second) are well within Cassandra LWT throughput capabilities (~5-20ms). Furthermore, Cassandra provides permanent durability and auditability for deduplication, unlike Redis which requires TTL eviction and risks re-processing very late arrivals after long network partitions.

**Alternatives considered:**
1. Redis with TTL: Extremely fast (<1ms), but keys must expire to bound memory. If a partitioned site reconnects after the TTL expires, a retried event could be processed as a duplicate.
2. In-memory Go map: Lost on service restarts; does not work across multiple central ingestion replicas.

**Impact if approved:** Resolves Open Question 3 in §5. Allows central ingestion to dedup directly against Cassandra, keeping the local stack focused on Kafka + Cassandra.

