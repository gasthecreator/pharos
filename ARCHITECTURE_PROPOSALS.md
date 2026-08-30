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

#### [2026-08-30] Slice 4 Architecture: Kafka Consumer Topology, Canonical Cassandra Query Tables, Event-Time Watermarking, and Idempotent Downstream Sinks

**Status:** Requires revision (Claude Code, 2026-08-30) — do not implement
yet. The consumer topology, the three-table canonical schema (genuinely
resolves the long-open §5 query-pattern question, and the idempotent-upsert
reasoning for redelivered messages correctly holds across all three tables
since `idempotency_key` is part of every primary key), and the decision to
skip Flink for a pure-Go watermark tracker are all approved as-is. One
required fix on the watermark formula itself, and one recommended change to
the write strategy.

**Required fix — the watermark formula has no idle-partition exclusion, and
that directly undermines the feature's own stated purpose.**
`W = min_p(T_p) - L` takes the minimum over *all* active partitions with no
way to exclude one that's gone silent. Walk through the exact scenario this
project exists to handle: a site (say, Nigeria) loses connectivity for three
days — PLAN.md §2.1's own example. Under this formula, that partition's `T_p`
stops advancing the moment the site goes offline, and since a frozen
partition is still counted in the `min()`, the *global* watermark `W` freezes
at that moment too — even though every other site's partition keeps
advancing normally. The proposal's own motivating question — "is the
clinical safety data for yesterday complete across all global trial sites?"
— would answer "no, indefinitely" for the *entire dataset* the instant any
single site has the exact outage this whole project is built to tolerate.
This isn't an edge case; it's the central scenario, so a watermark design
that hangs on it can't ship as-is.

Fix: track wall-clock time since each partition's last-consumed message,
separately from that partition's event-time progress. If a partition has
been idle (no new messages, not merely no *recent* event-time progress)
beyond a configurable threshold, exclude it from the `min()` computation
until it produces again — the same idle-source-detection pattern Flink and
Kafka Streams use for exactly this problem. Re-include a partition the
moment it resumes producing, recomputing based on its new `T_p`. Write this
down explicitly as part of the `WatermarkTracker` design, including what
happens to the completeness signal for windows that were computed while the
partition was excluded (do they get revisited once the site reconnects and
its backlog of "late" events arrive, or stay as originally reported? PLAN.md
already requires late events to be durably flagged rather than dropped,
which covers the data — but the *completeness signal* itself needs the same
scrutiny, since it's the visible artifact clinical safety staff would
actually act on).

**Recommended change — reconsider the logged batch across 3 tables.**
`event_outbox` uses lightweight transactions because two different actors
raced on the *same* partition key and needed a distributed compare-and-swap
to resolve who gets to publish — a genuine correctness necessity. Writing
`canonical_events`/`events_by_study`/`events_by_site` is a different
situation: there's exactly one writer (the consumer processing this
message), all three writes are individually idempotent upserts, and the
consumer already withholds the Kafka offset commit until all three succeed
— which already gives "retry the whole set until every table reflects it"
without needing Cassandra's own atomicity machinery. A logged batch across
three *different* partition keys is a well-known Cassandra throughput
anti-pattern (the coordinator has to durably write the batch log to two
replicas before executing statements against three different partitions'
owning nodes) for a guarantee this design doesn't actually need — the
retry-via-uncommitted-offset already provides it. Recommend independent
writes (parallel, gated by an errgroup or WaitGroup before the offset
commit) instead. Not blocking if there's a concrete reason to keep the
logged batch — but the stated rationale ("atomic... ensures multi-table
consistency") doesn't hold up against what the offset-commit gate already
provides, so make the actual tradeoff explicit if you disagree rather than
defaulting to the heavier mechanism.

Minor, non-blocking: confirm how `study_id` gets extracted for
`events_by_study`'s partition key, since `model.AdverseEvent.Study` is
modeled as a list (`[]Reference`) — presumably `Study[0]`, matching how
`SiteID()` already extracts from a single reference field, but say so
explicitly rather than leaving it implicit in the consumer code.

Revise the watermark design with the fix above and re-submit before writing
implementation code.

**What in PLAN.md this touches:**
- §2.4 Multi-timezone event ordering and correctness
- §3 Planned stack (Consumer layer)
- §4 Built vs. not yet (Stream processing layer, Kafka consumer, query tables)
- §5 Open questions (Question 1: Cassandra schema fit against real query patterns)

**What I'm proposing:**
Four interconnected architectural choices closing out the pipeline from Kafka to durable queryable storage:

1. **Topology & Packaging: Dedicated `pharos-consumer` Binary (`cmd/pharos-consumer` and `pkg/consumer`)**
   - **Structure**: Separate binary `cmd/pharos-consumer` containing the Kafka consumer group worker.
   - **Reasoning**: Decouples edge intake scaling (HTTP request bound) from downstream consumer scaling (Kafka partition bound). While Central Ingestion scales with incoming edge network connections, the consumer scales up to the topic's partition count (3 partitions currently). Isolating failures ensures ingestion never slows down during downstream database maintenance or backfills.
   - **Programmatic Engine**: The core consumption logic is implemented as `pkg/consumer.Engine` so integration tests and local test harnesses can run the consumer in-process without spawning subprocesses.
   - **Consumer Group**: Uses `segmentio/kafka-go` `Reader` configured with `GroupID: "pharos-canonical-sink"`, deterministic rebalance handling, and explicit manual commit after Cassandra durable write.

2. **Canonical Cassandra Schema: Two Query-Specific Tables + Canonical Entity Table**
   - Resolves PLAN.md §5 Question 1 ("actual query pattern fit: all events for trial X in date range Y vs all events from site Z").
   - Cassandra requires partition-key-first modeling. We propose 3 tables in keyspace `pharos`:
     - **`pharos.canonical_events`** (Entity Table / Point Lookup):
       - Primary Key: `(idempotency_key)`
       - Serves: exact point lookups by client idempotency key for deduplication audit, payload inspection, and Kafka lineage metadata (`kafka_topic`, `kafka_partition`, `kafka_offset`, `consumed_at`).
     - **`pharos.events_by_study`** (Regulatory / Clinical Safety Query):
       - Primary Key: `((study_id), event_time, idempotency_key)`
       - Clustering Order: `(event_time DESC, idempotency_key ASC)`
       - Serves: `SELECT * FROM events_by_study WHERE study_id = ? AND event_time >= ? AND event_time <= ?;`
       - Fast range scans ordered by clinical event time for DSMB and FDA safety reviews.
     - **`pharos.events_by_site`** (Site Operations & Auditing Query):
       - Primary Key: `((site_id), local_seq, idempotency_key)`
       - Clustering Order: `(local_seq DESC, idempotency_key ASC)`
       - Serves: `SELECT * FROM events_by_site WHERE site_id = ? AND local_seq >= ?;`
       - Verifies continuous per-site monotonic sequence numbering and site data delivery.
   - **Write Strategy**: The consumer executes an atomic CQL logged batch (`BEGIN BATCH ... APPLY BATCH;`) writing to all 3 tables simultaneously, or concurrent idempotent upserts. In Cassandra, writes across multiple tables within the same keyspace and datacenter are atomic in a single logged batch.

3. **Event-Time Ordering and Watermarking Semantics (§2.4)**
   - **Problem**: Global sites in diverse time zones (e.g. Tokyo UTC+9, London UTC+0, Indianapolis UTC-5) produce events stamped with their local event time (normalized to UTC). If a site is disconnected for hours, its events arrive late at Central Ingestion.
   - **Solution**: Implement `pkg/consumer.WatermarkTracker`:
     - Tracks the maximum observed `event_time` per active partition $p$: $T_p = \max(\text{event\_time}_p)$.
     - Defines the global watermark $W$ with configurable bounded lateness tolerance $L$ (default 15m, configurable per study):
       $$W = \min_{p \in \text{active\_partitions}}(T_p) - L$$
     - A time window $[t_{start}, t_{end})$ is deemed **Complete** once $W \ge t_{end}$.
     - Events arriving with `event_time < W` are processed into Cassandra (never dropped per PLAN.md's core premise), but flagged with `is_late = true` and tracked via metric counters to signal late-arriving pharmacovigilance reports requiring retroactive safety evaluation.
   - This delivers mathematically rigorous multi-timezone correctness without heavy external dependencies like Flink or Spark.

4. **Consumer-Side Idempotency**
   - On consumer crash or partition rebalance, Kafka may redeliver uncommitted messages.
   - Because all 3 Cassandra tables are keyed by `idempotency_key` (either directly or as the tie-breaking clustering column), CQL `INSERT` statements are natural idempotent upserts.
   - Redelivering an identical event writes the exact same columns and payload into Cassandra without creating duplicate rows.
   - Kafka offsets are committed only after the Cassandra write succeeds.

**Why:**
- `event_outbox` was designed for transactional publishing, not analytical querying; trying to query it by study or site requires full-table scans.
- Watermarking provides an honest answer to "is the clinical safety data for yesterday complete across all global trial sites?", which is a core Lilly Bio-IT challenge.
- Separating the consumer binary reflects realistic production deployment boundaries.

**Alternatives considered:**
- *Folding consumer into ingestion binary*: Easier to run locally, but couples HTTP API scaling to Kafka partition constraints. Rejected for production architecture, but mitigated by providing an in-process library mode for unit/integration tests.
- *Cassandra Materialized Views instead of dual/triple writes*: Cassandra MVs are known for operational fragility, tombstone issues, and silent lag during node failures. Application-layer multi-table batch writes are standard in production Cassandra.
- *Full Apache Flink stream processor*: Heavy JVM framework requiring ZooKeeper/Kubernetes orchestration, violating the project constraint of running cleanly on developer hardware. A pure-Go watermark tracker delivers identical semantics with zero operational bloat.

**Impact if approved:**
- New CQL migration `migrations/002_canonical_schema.cql` adding `canonical_events`, `events_by_study`, `events_by_site`.
- New package `pkg/consumer` containing consumer engine, watermark tracker, and Cassandra canonical store.
- New binary `cmd/pharos-consumer`.
- Full unit and integration test suite asserting single-row idempotency on message redelivery, per-site ordering preservation, and watermark lag detection.

---

#### [2026-08-30] Slice 3 Architecture: Cassandra Transactional Outbox, Schema, Consistency Levels, and Kafka Ingestion & DLQ Pipelines

**Status:** Resolved: Approved (Claude Code, 2026-08-30). Traced the
concurrent-race scenario against the revised design and it holds: two
requests racing on the same key both attempt
`INSERT ... status='PUBLISHING' IF NOT EXISTS`, Cassandra's LWT linearizes it
so exactly one wins and publishes, the loser sees a fresh `PUBLISHING` claim
and correctly does nothing (safe — durability already happened at the
winning insert; the sweeper is the real backstop, not the loser's response).
The lease-expiry CAS (`IF status='PUBLISHING' AND claimed_at=?`) correctly
serializes even multiple simultaneous lease-stealers, and the same mechanism
uniformly closes the sweeper-vs-live-request race too. DLQ path is genuinely
symmetric now. Raw payload storage is explicit. Cleared to implement.

One harmless, non-blocking note for implementation: `status='PENDING'` is
now vestigial — since the accept-path and DLQ-path inserts both write
`status='PUBLISHING'` directly (correctly — the insert winning *is* the
claim), no code path ever actually produces a `PENDING` row. Sub-case 2d and
the sweeper's `PENDING` branch are dead code left over from the pre-revision
two-step design. Doesn't cause a bug, just delete it for clarity when writing
the actual Go code rather than carrying forward an unreachable branch.

**What in PLAN.md this touches:** §2.2 (Exactly-once processing semantics & transactional outbox), §2.3 (Dead-letter queues), §2.4 (Kafka partitioning by site_id), §3 (Planned stack).

**What I'm proposing:**
Define the revised architectural decisions, Cassandra schemas, consistency levels, outbox execution lifecycle, and Kafka pipeline for Slice 3:

### 1. Cassandra Consistency Levels
- **Serial Consistency (LWT Paxos phase):** `LOCAL_SERIAL` for `INSERT ... IF NOT EXISTS` and conditional `UPDATE ... IF` statements. Confines Paxos consensus within the local datacenter, avoiding cross-DC WAN roundtrips while providing linearizable ordering on the partition key (`idempotency_key`).
- **Write Consistency (Commit phase):** `LOCAL_QUORUM` for multi-node deployments; falls back to `ONE` in the single-node local Docker development environment where replication factor is 1.
- **Read Consistency:** `LOCAL_QUORUM` (or `ONE` for single-node dev).

### 2. Cassandra Schemas & Bootstrapping
- **Keyspace:** `pharos` (`SimpleStrategy`, replication_factor: 1 for local dev; `NetworkTopologyStrategy` in production).

- **Table 1: `pharos.event_outbox` (Dedup & Transactional Outbox)**
  ```cql
  CREATE TABLE IF NOT EXISTS pharos.event_outbox (
      idempotency_key text,
      site_id text,
      local_seq bigint,
      payload text,
      status text,
      claimed_at timestamp,
      created_at timestamp,
      published_at timestamp,
      kafka_topic text,
      kafka_partition int,
      kafka_offset bigint,
      PRIMARY KEY (idempotency_key)
  );
  ```
  - `status`: Three-state lifecycle string (`PENDING`, `PUBLISHING`, `PUBLISHED`).
  - `claimed_at`: Timestamp recording when a worker acquired the `PUBLISHING` lease.
  - `payload`: **Stores raw JSON bytes** (`json.RawMessage` from Slice 2's fix) as captured over the wire, never re-serialized from Go structs, guaranteeing zero data loss or field dropping.

- **Table 2: `pharos.dead_letter_events` (Dead-Letter Audit Store & Outbox)**
  ```cql
  CREATE TABLE IF NOT EXISTS pharos.dead_letter_events (
      idempotency_key text,
      site_id text,
      payload text,
      rejection_reason text,
      validation_errors text,
      rejected_at timestamp,
      status text,
      claimed_at timestamp,
      published_at timestamp,
      kafka_topic text,
      kafka_partition int,
      kafka_offset bigint,
      PRIMARY KEY (idempotency_key)
  );
  ```
  - Mirrors the exact same three-state lifecycle (`PENDING`, `PUBLISHING`, `PUBLISHED`) and `claimed_at` lease mechanism as `event_outbox` for symmetric crash resilience on the rejection path.
  - `payload`: **Stores raw JSON bytes** as received, preserving invalid or unrecognized fields for clinical/regulatory audit.

- **Table 3: `pharos.pending_outbox` (Time-Bucketed Sweeper Index)**
  ```cql
  CREATE TABLE IF NOT EXISTS pharos.pending_outbox (
      bucket text,
      idempotency_key text,
      created_at timestamp,
      PRIMARY KEY (bucket, idempotency_key)
  );
  ```
  Provides efficient index queries for the background sweeper without scanning low-cardinality status columns across the main tables. Entries are removed upon reaching `status = 'PUBLISHED'`.

- **Bootstrapping Strategy:** Schema-on-connect via Go initialization helper (`EnsureSchema(session)`), with a declarative CQL script at `migrations/001_init_schema.cql` for `cqlsh` and CI automation.

---

### 3. Transactional Outbox Lifecycle & Concurrency-Safe Claim Lock

To close the race condition where concurrent requests or premature retries see `published == false` and both attempt to publish to Kafka, we use a three-state claim lock (`PENDING` -> `PUBLISHING` -> `PUBLISHED`) enforced via Cassandra Lightweight Transactions:

#### A. Accept Path (Synchronous Fast-Path with Atomic Claim)
1. Intake receives event batch, evaluates rate limiter, and validates each event against scoped FHIR profile.
2. For valid events, Central Ingestion executes an atomic insert with initial `status = 'PUBLISHING'` and `claimed_at = now`:
   ```cql
   INSERT INTO pharos.event_outbox (
       idempotency_key, site_id, local_seq, payload, status, claimed_at, created_at
   ) VALUES (?, ?, ?, ?, 'PUBLISHING', ?, ?)
   IF NOT EXISTS;
   ```
3. **Branching on `applied`:**
   - **Case 1: `applied == true` (New Event):**
     The request won the LWT insert AND acquired the exclusive `PUBLISHING` claim. It proceeds directly to Kafka publishing (Step 4).
   - **Case 2: `applied == false` (Key Exists — Duplicate or Concurrent In-Flight):**
     Read the existing row (`SELECT status, claimed_at FROM pharos.event_outbox WHERE idempotency_key = ?`):
     - **Sub-case 2a: `status == 'PUBLISHED'`:** The event was already successfully published to Kafka in an earlier pass. Return HTTP 200 immediately (no-op, zero duplicate Kafka messages).
     - **Sub-case 2b: `status == 'PUBLISHING'` and `now - claimed_at < LeaseTimeout` (default 30s):**
       Another worker is actively in flight publishing this event right now. Do **NOT** call Kafka producer. Return HTTP 200 to the caller (or wait for in-flight completion).
     - **Sub-case 2c: `status == 'PUBLISHING'` and `now - claimed_at >= LeaseTimeout` (Lease Expired / Claimant Crashed):**
       The previous claimant crashed or hung before completing publication. Attempt to steal the expired lease via conditional CAS update:
       ```cql
       UPDATE pharos.event_outbox
       SET status = 'PUBLISHING', claimed_at = ?
       WHERE idempotency_key = ?
       IF status = 'PUBLISHING' AND claimed_at = ?;
       ```
       If this LWT CAS update returns `applied == true`, this worker won the lease and proceeds to Kafka publishing (Step 4). If `applied == false`, another worker won the lease; abort publishing and return HTTP 200.
     - **Sub-case 2d: `status == 'PENDING'`:**
       Attempt to acquire the claim:
       ```cql
       UPDATE pharos.event_outbox
       SET status = 'PUBLISHING', claimed_at = ?
       WHERE idempotency_key = ?
       IF status = 'PENDING';
       ```
       If `applied == true`, proceed to Kafka publishing (Step 4). Otherwise abort.
4. **Kafka Publishing Step:**
   Call Kafka producer to publish to topic `pharos.events.adverse` using partition key `site_id` (idempotent producer enabled).
5. **Mark Published:**
   On successful Kafka acknowledgement, finalize status:
   ```cql
   UPDATE pharos.event_outbox
   SET status = 'PUBLISHED', published_at = ?, kafka_topic = ?, kafka_partition = ?, kafka_offset = ?
   WHERE idempotency_key = ?
   IF status = 'PUBLISHING';
   ```
6. **Acknowledge Edge:** Return HTTP 200 OK.

#### B. In-Process Defense-in-Depth (Keyed Mutex)
In addition to the Cassandra LWT claim lock (which is the authoritative distributed correctness mechanism across all instances), Central Ingestion maintains an in-memory per-key lock map (`sync.Map` of per-idempotency-key mutexes) to short-circuit same-process goroutines from performing redundant Cassandra Paxos roundtrips for identical keys arriving simultaneously.

#### C. Background Outbox Sweeper & Lease Reclamation
To guarantee eventual publication if an edge node crashes and never retries an interrupted publish:
- A background worker runs periodically (every 10s) and scans `pharos.pending_outbox` for uncompleted events.
- For each entry, reads `status` and `claimed_at` from `event_outbox`.
- If `status == 'PUBLISHED'`: deletes the entry from `pending_outbox`.
- If `status == 'PENDING'` or (`status == 'PUBLISHING'` and `now - claimed_at >= LeaseTimeout`):
  Attempts to claim via LWT:
  ```cql
  UPDATE pharos.event_outbox SET status = 'PUBLISHING', claimed_at = now
  WHERE idempotency_key = ?
  IF status = ? AND claimed_at = ?;
  ```
  If won, publishes to Kafka, updates `status = 'PUBLISHED'`, and removes from `pending_outbox`.

---

### 4. Dead-Letter Pipeline (DLQ) — Symmetric Outbox Treatment
For events failing FHIR validation, Central Ingestion mirrors the exact same durable outbox pattern *before* responding with HTTP 422/207:
1. Write to Cassandra `pharos.dead_letter_events` with `status = 'PUBLISHING'`, `claimed_at = now`, structured failure metadata, and the raw payload bytes:
   ```cql
   INSERT INTO pharos.dead_letter_events (
       idempotency_key, site_id, payload, rejection_reason, validation_errors,
       rejected_at, status, claimed_at
   ) VALUES (?, ?, ?, ?, ?, ?, 'PUBLISHING', ?)
   IF NOT EXISTS;
   ```
2. If `applied == true` (or if claiming a stale lease), publish structured DLQ event to Kafka topic `pharos.events.dlq` partitioned by `site_id`.
3. On Kafka ack, update Cassandra:
   ```cql
   UPDATE pharos.dead_letter_events
   SET status = 'PUBLISHED', published_at = ?, kafka_topic = ?, kafka_partition = ?, kafka_offset = ?
   WHERE idempotency_key = ?
   IF status = 'PUBLISHING';
   ```
4. Return HTTP 422 (or 207 Multi-Status) to the edge.
5. If a crash occurs before the DLQ Kafka publish, subsequent edge retries or the sweeper follow the identical lease-reclamation logic, guaranteeing that rejected events are never lost from the Kafka DLQ topic.

---

### 5. Kafka Client & Configuration
- **Library:** `github.com/segmentio/kafka-go` (pure Go, zero cgo/librdkafka runtime dependencies).
- **Topics:**
  - Main Topic: `pharos.events.adverse`, partitioned by `site_id` (preserves per-site FIFO ordering per §2.4).
  - DLQ Topic: `pharos.events.dlq`, partitioned by `site_id`.
- **Producer Configuration:**
  - `RequiredAcks: -1` (`all` ISR replicas).
  - Partitioning: Deterministic hash on `site_id`.
  - Compression: Snappy.

---

### 6. Verification Plan & Test Assertions
1. **Crash Window Resumption:** Invalidate Kafka connection or inject a crash after Cassandra LWT insert -> verify record remains in Cassandra with `status = 'PUBLISHING'` -> restore Kafka and retry -> verify event transitions to `status = 'PUBLISHED'` and is published to Kafka exactly once.
2. **Sequential Duplicate Idempotency:** Submit the same idempotency key twice -> verify the second request is recognized as a duplicate (`applied: false`, `status = 'PUBLISHED'`), returns HTTP 200, and does not trigger a second Kafka message.
3. **Concurrent Duplicate Race:** Fire concurrent goroutines with identical idempotency keys -> **assert that exactly one Kafka publish call actually occurred** (not merely that one insert returned `applied: true`), and that both callers receive successful responses.
4. **Stale Lease Reclamation:** Inject a crashed worker with `status = 'PUBLISHING'` and expired `claimed_at` -> run the sweeper / retry -> verify lease is reclaimed via LWT and published to Kafka exactly once.
5. **Dead-Letter Pipeline Durability:** Submit malformed FHIR events -> verify structured rejection metadata and raw payload are durably stored in Cassandra `pharos.dead_letter_events` and published to Kafka DLQ topic `pharos.events.dlq` before HTTP 422 is returned.
6. **Raw Payload Preservation:** Verify that payloads with unmodeled fields or raw JSON bytes are stored verbatim in both Cassandra and Kafka messages without struct re-serialization.

**Why:**
- Eliminates the concurrent publish race by requiring any worker to hold an active `PUBLISHING` claim won via Cassandra LWT before calling the Kafka producer.
- Closes the crash window on both the accept path and the DLQ path symmetrically.
- Ensures all data (valid and invalid) is stored and forwarded in raw wire format without data loss.

**Alternatives considered:**
- In-process mutex only: Unsafe across multiple Central Ingestion instances. The proposal uses Cassandra LWT as the true distributed lock, adding in-process mutexes purely as an optimization to reduce Cassandra round-trips.
- Leaky outbox without lease timeout: If a worker crashes during Kafka publish, rows would remain stuck in `PUBLISHING` forever. The 30s `claimed_at` lease timeout ensures deterministic recovery.

**Impact if approved:** Governs Slice 3 implementation across `pkg/dedup`, `pkg/kafka`, and `pkg/ingestion`.

---

## [2026-08-29] Clarification: Central Ingestion Rate Limiter Meters HTTP Batch Requests, Not Individual Events

**Status:** Resolved: Approved (Claude Code, 2026-08-29) — folded into PLAN.md
§2.3 as-is. One correction to the reasoning, not the conclusion: alternative
#1's justification says per-event metering "requires reading and parsing the
JSON payload body before evaluating the rate limiter" — but the current
handler already reads and parses the body before checking the rate limiter
regardless of metering unit (see `pkg/ingestion/handler.go` — body parse is
step 1, rate-limit check is step 3). So per-request metering doesn't actually
avoid that parse-before-limit cost today; the real DoS-hardening move would be
checking the limiter off the `X-Site-ID` header before reading the body at
all, which isn't implemented either way. Not blocking — out of scope for this
project's purposes — but noting so the proposal's stated reasoning doesn't
get confused with an actual DoS-protection guarantee, which this isn't.

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

