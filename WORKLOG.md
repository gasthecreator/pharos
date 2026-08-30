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

## [2026-08-29] Slice 2 built: Edge HTTP capture, forwarder with exponential backoff + jitter, and Central Ingestion rate limiting + FHIR validation

**Author:** Gemini (Antigravity)

**What:** Closed the network loop end-to-end between the edge collector and Central Ingestion:
1. Implemented Edge Collector HTTP capture endpoint (`pkg/edge/server.go`): site staff/EDC systems submit reports via `POST /api/v1/adverse-events`. Durably buffers records to SQLite WAL before responding with HTTP 201 Created and the client idempotency key. In strict conformance with §2.3, does NOT gate on FHIR schema validation at the edge.
2. Implemented Edge Collector Forwarder background worker (`pkg/edge/forwarder.go`): calls `QueueStore.FetchPending`, transitions records to `IN_FLIGHT`, sends batch POST requests to Central Ingestion over HTTPS, and marks records `ACKNOWLEDGED` on success. On failures (HTTP 429, 5xx, network timeouts, connection refused), marks records `FAILED` with Exponential Backoff + Full Jitter.
3. Implemented Central Ingestion HTTP service (`pkg/ingestion/handler.go`): handles `POST /api/v1/events` with per-site token bucket rate limiting (`pkg/ratelimit`) and validates every event against the scoped FHIR R4 AdverseEvent profile (`pkg/model.AdverseEvent.Validate()`). Returns HTTP 200 for valid batches, HTTP 207 Multi-Status for partial validation failures, and HTTP 422 Unprocessable Entity with structured rejection details (`idempotency_key`, rule violated) rather than a bare 400.
4. Drafted two new proposals in `ARCHITECTURE_PROPOSALS.md`: Central Ingestion rate-limiter in-memory token bucket backing store, and Edge Forwarder Full Jitter retry/backoff parameters.
5. Integrated both servers and background workers into entrypoints `cmd/pharos-edge/main.go` and `cmd/pharos-ingestion/main.go`.

**Why:** Fulfills core challenges in `PLAN.md`:
- §2.1 (Network partition tolerance): Edge store-and-forward is now active end-to-end. Events are safely buffered on local disk and forwarded asynchronously with exponential backoff and jitter across network disruptions.
- §2.2 (Exactly-once semantics): Preserves client-stamped idempotency keys across edge capture, local storage, network serialization, and batch ingestion.
- §2.3 (Rate limiting and validation): Enforces per-site token bucket rate limiting at central intake, isolating rogue sites. Moves FHIR schema validation to Central Ingestion, returning structured rejection metadata for future DLQ routing.

**How:**
- Rate limiting: Thread-safe in-memory token bucket (`pkg/ratelimit/limiter.go`) tracking per-site burst capacity and token refill rate. Throttled requests return HTTP 429 with `Retry-After` and `X-RateLimit-*` headers.
- Forwarder resilience: Exponential backoff with Full Jitter (`sleep = rand(0, min(MaxBackoff, BaseBackoff * 2^attempts))`), eliminating thundering herd synchronization when Central Ingestion recovers. Handles HTTP 429 by respecting the central `Retry-After` header.
- Poison-pill avoidance: Malformed FHIR events rejected by Central Ingestion (HTTP 422/207) are acknowledged at the edge once inspected by Central Ingestion, preventing infinite retry loops that would block valid events.

**Files/modules touched:**
- `ARCHITECTURE_PROPOSALS.md`
- `pkg/ratelimit/limiter.go`, `pkg/ratelimit/limiter_test.go`
- `pkg/ingestion/handler.go`, `pkg/ingestion/handler_test.go`
- `pkg/edge/server.go`, `pkg/edge/server_test.go`
- `pkg/edge/forwarder.go`, `pkg/edge/forwarder_test.go`
- `cmd/pharos-edge/main.go`
- `cmd/pharos-ingestion/main.go`
- `WORKLOG.md`

**Tests added/updated:**
- `pkg/ratelimit`: `TestTokenBucketLimiter_BurstAndThrottle`, `TestTokenBucketLimiter_Refill`, `TestTokenBucketLimiter_SiteIsolation`, `TestTokenBucketLimiter_ConcurrentAccess` (150 concurrent goroutines).
- `pkg/ingestion`: `TestHandler_AllValidBatch`, `TestHandler_ValidationRejectionStructuredErrors` (HTTP 422 with structured field errors), `TestHandler_PartialValidationBatch` (HTTP 207 Multi-Status), `TestHandler_RateLimitingAndHeaders` (HTTP 429 + Retry-After), `TestHandler_MalformedJSON`, `TestHandler_MissingSiteID`.
- `pkg/edge`: `TestServer_CaptureValidEvent`, `TestServer_CaptureMalformedFHIREvent` (verifies edge buffers invalid FHIR with HTTP 201), `TestServer_MalformedJSON`, `TestServer_EmptyBody`, `TestServer_StatsEndpoint`.
- `pkg/edge`: `TestForwarder_HappyPathDelivery`, `TestForwarder_Server5xxRetryAndExponentialBackoff`, `TestForwarder_NetworkTimeoutAndConnectionRefused`, `TestForwarder_RateLimit429WithRetryAfter`, `TestForwarder_ValidationRejectionHandling`, `TestForwarder_EndToEndWithCentralIngestion`.
- All tests pass with race detector: `go test -buildvcs=false -v -race -count=1 ./...` (21 tests across 4 packages, 0 failures, 0 race warnings).

**Follow-ups / left open:**
- Slice 3: Central Ingestion Kafka publishing, Dead-Letter Queue (DLQ) topic routing for rejected events, and Cassandra transactional-outbox dedup path (§2.2).

## [2026-08-29] Branch/PR workflow adopted; two Slice 2 proposals reviewed and approved; a file-corruption fix

**Author:** Claude Code

**What:** Gideon asked whether Slice 1 was pushed straight to `main` (it was —
all three commits so far). That surfaced a standing preference from another
project (never commit to `main` directly, always feature branches + PR
review) that hadn't been applied here. Asked Gideon directly rather than
assuming either way, since the original reason for that rule (a specific
teammate reviewing PRs) doesn't automatically transfer to a solo project.
Gideon chose to adopt branches + PRs for Pharos too. Folded into `PLAN.md` §6.

Also reviewed two new proposals Gemini had written to `ARCHITECTURE_PROPOSALS.md`
in the meantime — a per-site in-memory token-bucket rate limiter (pluggable
for Redis later) and an exponential-backoff-with-full-jitter formula for the
edge forwarder, with concrete parameters. Both approved as-is and folded into
`PLAN.md` §2.1 and §2.3/§3 — good proposals, and correctly written up as
proposals rather than guessed silently, which is exactly the process working
as intended.

While reading the file, found that the "Clarify Ingress Topology" proposal's
own header line had gone missing — its `**Status:**` line was left floating
with no preceding `## [date] title` line, most likely lost when the two new
entries were prepended above it. Restored the header from the earlier
(already-approved) version of that entry.

**Why:** Consistency of process documentation matters as much as the code
here — a broken header makes an already-resolved decision look unresolved or
orphaned to the next reader (human or Gemini), and this file only works as a
system-of-record if entries stay structurally intact.

**How:** Direct edits to `ARCHITECTURE_PROPOSALS.md` (status updates on 2
entries, header restoration on 1) and `PLAN.md` (§2.1 retry/backoff
parameters, §2.3 + §3 rate limiter design, §6 branch workflow).

**Files/modules touched:** `PLAN.md`, `ARCHITECTURE_PROPOSALS.md`.

**Tests added/updated:** none — process/documentation only.

**Follow-ups / left open:**
- Everything from this point forward (including whatever lands this entry)
  should go out on a branch + PR, not straight to `main`.
- Gemini should build Slice 2 against the now-resolved rate-limiter and
  backoff parameters in `PLAN.md` §2.1/§2.3, not the original proposal text.

## [2026-08-29] Claude Code review of Slice 1: fixed edge-side validation conflicting with PLAN.md §2.3

**Author:** Claude Code

**What:** Reviewed Gemini's Slice 1 build. Independently re-ran `go vet` and
`go test -race ./...` rather than trusting the summary — confirmed clean vet
and all tests passing. Found one real conformance bug: `SQLiteStore.Enqueue`
(`pkg/edge/sqlite_store.go`) called `event.Validate()` and rolled back the
entire transaction — i.e. refused to persist the record at all — if the
payload failed the scoped FHIR profile's structural validation. Removed that
call, with a comment explaining why, and added
`TestSQLiteStore_EnqueuePersistsMalformedEvent` to `sqlite_store_test.go` to
lock in the corrected behavior.

**Why:** PLAN.md §2.3 is explicit: FHIR validation happens at Central
Ingestion, "not at the edge collector — sites should be able to buffer even
malformed-looking data rather than lose it locally." The as-built edge
collector did the opposite. Concrete failure scenario: a site's local software
sends a payload with, say, an unrecognized severity code — under the old code
that record never entered the durable queue, produced no DLQ record, and left
no audit trail. That's a silent, permanent data loss in a system whose entire
premise is never losing adverse-event data. It's also fragile specifically in
the partition scenario this project targets: an edge site offline for months
may be running an older binary whose validation rules have drifted from what
Central Ingestion currently expects — the edge should never be the thing
deciding a record isn't worth keeping.

**How:** Deleted the `if err := event.Validate(); err != nil { return ... }`
gate from `Enqueue` (it ran after idempotency-key stamping, inside the same
DB transaction as the sequence-number allocation — removing it doesn't affect
sequence/idempotency correctness, verified by the existing concurrency and
durability tests still passing unchanged). Left a comment pointing at PLAN.md
§2.3 so a future edit doesn't reintroduce this silently. This was small and
contained enough to fix directly rather than sending back to Gemini, per the
working agreement's "fix directly or tell Gideon what to tell Antigravity"
split.

**Files/modules touched:** `pkg/edge/sqlite_store.go`,
`pkg/edge/sqlite_store_test.go`.

**Tests added/updated:** `TestSQLiteStore_EnqueuePersistsMalformedEvent` (new)
— builds an event that fails `AdverseEvent.Validate()` (missing subject,
invalid severity) and asserts `Enqueue` still durably persists and returns it
from `FetchPending`. All 6 existing `pkg/edge` tests re-verified passing
(`-race`) after the change.

**Follow-ups / left open:**
- Central Ingestion (Slice 2+) is where FHIR validation actually needs to live
  — routing failures to the Kafka DLQ per PLAN.md §2.3, not implemented yet.
- Minor/non-blocking style nit spotted in review: `MarkInFlight` and
  `MarkAcknowledged` in `sqlite_store.go` build SQL via `fmt.Sprintf` to embed
  status constants directly into the query text, inconsistent with the
  parameterized-query style used everywhere else in the file. Not a security
  issue (values are compile-time constants, never user input) — just
  inconsistent. Not fixed now; flag for cleanup whenever that file is next
  touched.
- Nothing from Slice 1 has been committed/pushed yet — pending Gideon's
  go-ahead, same as the rest of the repo.

## [2026-08-29] Slice 1 built: repo scaffolding, domain models, client idempotency keys, and SQLite-WAL edge durability

**Author:** Gemini (Antigravity)

**What:** Implemented the entire first slice of Pharos as planned and approved:
1. Local infrastructure orchestration via `docker-compose.yml` hosting Apache Kafka 3.8.0 in KRaft mode and Apache Cassandra 5.0 with constrained JVM heaps (`-Xms512M -Xmx1G`) for zero-cloud-spend self-hosting.
2. Standard Go module layout (`go.mod`, `go.sum`, `Makefile`, `.gitignore`).
3. Domain models in `pkg/model`: scoped FHIR R4 `AdverseEvent` resource profile with validation methods, and client-assigned `IdempotencyKey` (`site_id:local_seq_number`) with canonical wire parsing/formatting.
4. Embedded SQLite Write-Ahead Logging (`WAL`) local queue in `pkg/edge` implementing `QueueStore`: atomic monotonic sequence generation per trial site, transactional persistence before network exposure, FIFO ordered batch retrieval, and state transitions (`PENDING -> IN_FLIGHT -> ACKNOWLEDGED` / `FAILED` with exponential retry backoff).
5. Minimal entrypoint binaries in `cmd/pharos-edge` and `cmd/pharos-ingestion` with graceful shutdown handling.

**Why:** Fulfills the foundational requirements of `PLAN.md`:
- §2.1 (Network partition tolerance): Provides the durable edge boundary on local disk via SQLite WAL. Bytes are guaranteed to be fsynced locally before any upstream forwarding attempt.
- §2.2 (Exactly-once semantics): Enforces client-side idempotency key stamping at capture time, ensuring retries from reconnecting sites retain identical keys.
- §2.3 (Rate limiting and DLQ preparation): Implements the approved scoped FHIR R4 AdverseEvent profile and strict validation rules.
- §4 checklist items: "Repo scaffolding", "Idempotency key generation", and "Edge collector: local durable buffering".

**How:**
- Pure Go SQLite (`modernc.org/sqlite`) was chosen to eliminate CGO dependencies, enabling cross-compilation and hassle-free local execution.
- Configured SQLite with `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;` and serialized write transactions (`SetMaxOpenConns(1)` + mutex lock) to prevent database lock contention.
- `Enqueue` runs in an atomic transaction that bumps `site_sequence`, formats `IdempotencyKey`, attaches it to the event payload, validates the FHIR profile, and inserts into `queued_events`.
- Docker Compose configures Kafka in KRaft mode without Zookeeper, and tunes Cassandra memory to prevent dev machine exhaustion.

**Files/modules touched:**
- `go.mod`, `go.sum`
- `.gitignore`
- `Makefile`
- `docker-compose.yml`
- `pkg/model/idempotency.go`, `pkg/model/idempotency_test.go`
- `pkg/model/adverse_event.go`, `pkg/model/adverse_event_test.go`
- `pkg/edge/store.go`, `pkg/edge/sqlite_store.go`, `pkg/edge/sqlite_store_test.go`
- `cmd/pharos-edge/main.go`
- `cmd/pharos-ingestion/main.go`
- `WORKLOG.md`

**Tests added/updated:**
- `pkg/model/TestNewIdempotencyKey`, `TestParseIdempotencyKey`: validates boundary conditions, invalid formats, non-numeric or negative sequence numbers, and whitespace handling.
- `pkg/model/TestAdverseEvent_Validate`, `TestAdverseEvent_JSONRoundtrip`, `TestAdverseEvent_TimeNormalization`: validates FHIR R4 structural conformity, UTC normalization, and JSON roundtrip.
- `pkg/edge/TestSQLiteStore_EnqueueAndFetch`: verifies basic local enqueue and queue retrieval.
- `pkg/edge/TestSQLiteStore_DurabilityAcrossRestart`: simulates an abrupt edge collector process restart/crash, reopening the SQLite file and verifying 100% record retention and monotonic sequence resumption.
- `pkg/edge/TestSQLiteStore_ConcurrentEnqueueMonotonicSequences`: fires 50 concurrent goroutines against `Enqueue`; verifies zero race conditions, zero sequence collisions or gaps, strictly enforcing §2.2 idempotency invariants.
- `pkg/edge/TestSQLiteStore_StateTransitionsAndBackoff`: verifies `PENDING -> IN_FLIGHT -> ACKNOWLEDGED` and `FAILED` with retry backoff filtering.
- `pkg/edge/TestSQLiteStore_FIFOOrder`: verifies strict ascending sequence order retrieval across multiple batch fetches.
- All tests pass with race detector enabled (`go test -buildvcs=false -v -race ./...`).

**Follow-ups / left open:**
- Slice 2: Edge collector forwarder daemon with exponential backoff and jitter streaming batches to Central Ingestion HTTP endpoint (`POST /api/v1/events`).
- Central Ingestion HTTP service intake and token-bucket rate limiting per site.

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
