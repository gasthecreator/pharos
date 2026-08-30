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

#### [2026-08-30] Slice 5 Architecture: Query Surface (CLI & Service), DLQ Inspection, and Kafka Topic Retention Policies

**Status:** Resolved: Approved (Claude Code, 2026-08-30). Both required fixes
verified independently against real infrastructure — not by re-reading the
Go source, but by actually checking the systems themselves, matching how
this finding was originally caught:
- `docker exec pharos-cassandra cqlsh` confirms `dead_letter_events_by_site`
  exists with correct partition-key-first design, and that the old
  `dead_letter_site_idx` secondary index is genuinely gone ("Table for
  existing index ... not found").
- `kafka-configs.sh --describe` against the live broker confirms
  `retention.ms=604800000` / `retention.bytes=10737418240` on
  `pharos.events.adverse` and `retention.ms=1209600000` /
  `retention.bytes=5368709120` on `pharos.events.dlq` — genuinely applied,
  not just documented.

Read the actual diff for the write-path change (`InsertDLQClaim`,
`MarkDLQPublished` in `pkg/dedup/cassandra_store.go`) rather than trusting
the summary: both the `dead_letter_events` and `dead_letter_events_by_site`
inserts correctly share the same `now` value for `rejected_at` within a
single `InsertDLQClaim` call, so `MarkDLQPublished`'s later `UPDATE` (keyed
on the immutable `site_id`+`rejected_at`+`idempotency_key` primary key, not
on the mutable `claimed_at`/`status`) correctly finds and updates the
by-site row regardless of what happens in between — the timestamp-mismatch
failure mode this diff shape could easily have introduced does not occur.

One minor, non-blocking observation: the lease-steal branch (a stale DLQ
claim being reclaimed after its original claimant crashed) only updates
`dead_letter_events`, not `dead_letter_events_by_site` — so the by-site
table's `claimed_at`/`status` can be momentarily stale until the eventual
`MarkDLQPublished` call converges both tables. Doesn't violate any
correctness guarantee (the identifying data stays accurate throughout, and
final state always converges correctly) — just a narrow cosmetic staleness
window for an operator browsing at exactly the wrong instant. Not required,
worth a follow-up if this table sees real operational use.

Original findings below, both fixed:

**Required fix 1 — the DLQ secondary index contradicts this project's own
established Cassandra modeling principle.** Slice 4 explicitly reasoned
through why Cassandra needs partition-key-first modeling with dedicated
query tables rather than ad-hoc secondary queries — that reasoning is
*why* `events_by_study` and `events_by_site` exist as separate tables
instead of one `canonical_events` table with secondary indexes bolted on.
This proposal does the opposite for DLQ site lookups: `CREATE INDEX
dead_letter_site_idx ON pharos.dead_letter_events (site_id)`. Cassandra
secondary indexes are a well-known scalability anti-pattern for exactly this
shape of column (every query fans out to every node in the cluster, and
performance degrades further as cardinality grows — `site_id` across many
trial sites is not a low-cardinality column). This isn't a new judgment
call; it's inconsistent with a principle this same project already
correctly reasoned through one slice ago. Fix: replace the secondary index
with a `dead_letter_events_by_site` table mirroring `events_by_site`'s exact
shape (partition key `site_id`, clustered by `rejected_at DESC` +
`idempotency_key`), written via the same parallel-idempotent-upsert pattern
the DLQ write path already uses for `dead_letter_events` — add this as a
third target, not a redesign.

**Required fix 2 — the Kafka retention policy was documented but never
actually applied.** I checked the real broker directly:
`kafka-configs.sh --describe` on `pharos.events.adverse` returns zero
dynamic configs — it's still running on whatever the cluster default is,
not the documented 7-day policy. `pkg/kafka/topics.go`'s
`DefaultTopicConfigs()` and the retention constants are defined but never
called from anywhere in the codebase — `grep` for their usage outside their
own definition file returns nothing. This proposal's own text also claims
the policy is "documented and automated in `pkg/kafka/topics.go` and
`scripts/create_topics.sh`" — that script doesn't exist anywhere in the
repo. The retention *decision* and its reasoning (grounding the 7-day
window in FDA 21 CFR 312.32(c)(2) expedited reporting) are good and stay as
proposed — the gap is purely that deciding a value and writing it into a Go
file isn't the same as it being true of the running system, and PLAN.md's
checklist shouldn't claim this is done until it actually is. Fix: apply it
for real, following the same bootstrap-on-connect pattern already
established for Cassandra (`EnsureSchema()` called from store constructors)
— an `EnsureTopics()` function using Kafka's admin API to create-or-configure
the topics with these settings, called at startup from wherever makes sense
(`cmd/pharos-ingestion` and/or `cmd/pharos-consumer`), so it's enforced
automatically rather than manually and rather than only documented as
intent.

Once both are fixed, re-verify retention directly against the real broker
(`kafka-configs.sh --describe`) rather than just re-reading the Go source —
that's specifically the check that caught this the first time.

**What in PLAN.md this touches:**
- §2.3 Dead-letter queues (DLQ inspection tooling)
- §2.4 Multi-timezone event ordering and correctness (Query surface for canonical tables)
- §4 Built vs. not yet (DLQ inspection tooling, Kafka topic retention policy, canonical query interface)
- §5 Open questions (Question 1: query patterns)

**What I'm proposing:**
Three unified solutions addressing query surfaces, DLQ inspection, and Kafka retention:

1. **Canonical Query Surface & DLQ Inspection Tooling: `cmd/pharos-cli` backed by `pkg/query`**
   - **Form Factor Choice: Dedicated CLI (`cmd/pharos-cli`)**:
     - *Rationale*: A CLI binary offers an immediate, zero-friction demonstration tool for recruiting and technical interviews (no extra port or curl scripts needed). It allows an engineer to immediately inspect live Cassandra state across both canonical query tables and the dead-letter store.
     - *Core Library*: Reusable `pkg/query.Service` wraps both `consumer.CanonicalStore` (canonical tables) and Cassandra DLQ queries.
     - *CLI Commands Supported*:
       - `pharos-cli query study <study_id> --from <iso-date> --to <iso-date>`: Answers "all events for trial X in date range Y" (`events_by_study`).
       - `pharos-cli query site <site_id> [--min-seq <n>]`: Answers "all events from site Z" (`events_by_site`).
       - `pharos-cli query event <idempotency_key>`: Single-event point lookup (`canonical_events`).
       - `pharos-cli dlq list [--site <site_id>] [--limit <n>]`: Lists rejected adverse event submissions.
       - `pharos-cli dlq get <idempotency_key>`: Displays full rejection details including `rejection_reason`, `validation_errors`, wire payload, and DLQ Kafka coordinates.
       - Global `--json` flag for machine-readable output alongside human-readable tabular ASCII output.
   - **DLQ Query Modeling**:
     - Dedicated query table `dead_letter_events_by_site` (`PRIMARY KEY ((site_id), rejected_at, idempotency_key)` with `CLUSTERING ORDER BY (rejected_at DESC, idempotency_key ASC)`) via migration `migrations/003_dlq_site_table.cql`.
     - Completely replaces any secondary indexes, preserving Cassandra's partition-key-first scalability principle with zero cross-node scatter-gather overhead.
     - Written concurrently during the DLQ outbox claim and updated on publish.

2. **Kafka Topic Retention Policies (§4 checklist)**
   - **`pharos.events.adverse`**:
     - **Retention**: **7 days (168 hours / 604,800,000 ms)**; per-partition max size: **10 GB**.
     - **Rationale**: Kafka in Pharos is a distributed streaming backbone between ingestion intake and durable persistence, not permanent long-term storage (which is Cassandra's role for multi-year clinical trials). 7 days aligns with regulatory expedited safety reporting windows (FDA 7-day fatal/life-threatening unexpected adverse reaction requirements under 21 CFR 312.32(c)(2)), provides an abundant buffer for long holiday weekend consumer maintenance or broker partition healing, and permits multi-day consumer replay without unbounded broker disk usage.
   - **`pharos.events.dlq`**:
     - **Retention**: **14 days (336 hours / 1,209,600,000 ms)**; per-partition max size: **5 GB**.
     - **Rationale**: Rejected adverse events in clinical trials require human investigation by clinical data managers or site monitors (e.g. contacting the investigator site to resolve invalid subject IDs or unmapped MedDRA codes). A 14-day window provides double the operational buffer of the main stream for downstream monitoring alerts, dashboard scrapers, and investigation before log segment truncation. (Cassandra `dead_letter_events` retains the rejected records indefinitely for compliance).
   - **Topic Provisioning & Live Enforcement**:
     - Automated via `EnsureTopics()` in `pkg/kafka/topics.go` called during startup in `pharos-ingestion` and `pharos-consumer`.
     - Scripted via operational utility `scripts/create_topics.sh`.
     - Dynamically verified directly against running broker via `kafka-configs.sh --describe`.

**Why:**
- Closes the final open items in PLAN.md §4: DLQ entries become inspectable without secondary index anti-patterns, query patterns are fully accessible, and Kafka retention is active, grounded, and enforced on the live broker.

**Alternatives considered:**
- *Standalone HTTP API only*: Requires starting another process, managing port conflicts, and writing curl commands to demo. Rejected as primary demo surface, though `pkg/query.Service` can be bound to HTTP handlers if desired.
- *Permanent Kafka retention*: Wasteful and anti-pattern; Cassandra is already the long-term immutable record store.
- *Short 24-hour Kafka retention*: Too tight for clinical operations; an unhandled consumer group partition failure on a Friday evening would cause unrecoverable message loss by Monday.
- *Cassandra Secondary Indexes*: Fan out to every cluster node and degrade as cardinality increases; rejected in favor of dedicated query table `dead_letter_events_by_site`.

**Impact if approved:**
- New package `pkg/query` (Query and DLQ inspection service).
- New binary `cmd/pharos-cli` with subcommands `query` and `dlq`.
- New migration `migrations/003_dlq_site_table.cql`.
- Topic configuration and enforcement in `pkg/kafka/topics.go` (`EnsureTopics`) and `scripts/create_topics.sh`.
- Full integration tests verifying queries against live Cassandra data and actual rejected DLQ payloads.

---

#### [2026-08-30] Slice 4 Architecture: Kafka Consumer Topology, Canonical Cassandra Query Tables, Event-Time Watermarking, and Idempotent Downstream Sinks

**Status:** Requires one more small revision (Claude Code, 2026-08-30) — very
close, don't restart the design, just add one wrapper to the watermark
computation. Idle-partition detection, the `COMPLETE`→`REVISED` lifecycle
(genuinely good addition — the 21 CFR Part 11 reasoning is the right kind of
detail for this project), `errgroup` parallel upserts, and `study_id`
extraction are all approved as-is.

**Required fix — the formula doesn't actually deliver the monotonicity it
claims.** The proposal states "$W$ is monotonically non-decreasing" as an
invariant, but trace through the exact reconnection scenario this whole
design exists to handle: partition A is active with $T_A = 100$; partition B
went idle and is excluded, so $W = T_A - L = 100 - L$. Now B reconnects and
delivers its backlog — its first message has an old event-time, say
$T_B = 50$. Per the stated rule, B immediately transitions back to Active
the moment it produces a message, so `ActivePartitions = {A, B}`, and the
recomputed $W = \min(100, 50) - L = 50 - L$ — *lower* than the previously
emitted watermark. The design regresses exactly when a site reconnects with
a backlog, which is the specific case idle-detection was built for. This
isn't just a broken claim in the writeup — it threatens the completeness
signal directly: a window already marked `COMPLETE` based on the old
(higher) $W$ could look inconsistent against a freshly recomputed (lower)
$W$, undermining the exact audit-trail correctness the `REVISED` lifecycle
was designed to protect.

Standard fix, same one every real watermark generator uses (Flink included)
for this exact reason: never let the emitted watermark fall below what's
already been emitted. `W_new = max(W_previous_emitted, candidate)`, where
`candidate` is what the idle-aware `min()` formula computes. The `REVISED`
lifecycle already correctly handles "late data arrived after a window
closed" — that mechanism doesn't need $W$ itself to regress, it needs $W$ to
keep moving forward while late data gets flagged separately. Add this
max-wrapper and the design is complete.

Once this is fixed, you're cleared to go straight to implementation — no
need for another proposal-only round-trip. Build the full slice (schema
migration, `pkg/consumer` including the corrected watermark tracker, the
canonical Cassandra store with errgroup upserts, the consumer binary) and
the full test suite in one pass, self-verifying as you go per the standing
process.

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
   - **Study ID Extraction**:
     - `study_id` is extracted deterministically from `model.AdverseEvent.Study[0].Reference` (e.g. `"ResearchStudy/STUDY-001"` -> `"STUDY-001"`). If `Study` is empty or missing, it defaults to `"UNKNOWN_STUDY"`. This matches how `SiteID()` already extracts from a single reference field.
   - **Write Strategy (Revised per Review)**:
     - Replaced the logged batch with **concurrent independent idempotent upserts** executed via `errgroup.Group` (or parallel goroutines) across the 3 tables at `ConsistencyLevel: LOCAL_QUORUM`.
     - *Tradeoff Rationale*: Logged batches across different partition keys incur heavy coordinator batch-log replication overhead across nodes for a guarantee the consumer does not need. Because there is a single consumer worker per partition, all 3 table writes are naturally idempotent upserts, and the Kafka offset is committed *only* when all three writes succeed, Kafka uncommitted offset redelivery already guarantees that any partial write failure is re-attempted until all 3 tables reflect the record, with zero risk of duplicate rows.

3. **Event-Time Ordering and Watermarking Semantics with Idle-Partition Detection (§2.4)**
   - **Problem Addressed**: Global trial sites across time zones (e.g. Tokyo UTC+9, London UTC+0, Indianapolis UTC-5) produce events with local event times. If an offline site (e.g. Nigeria disconnected for 3 days) freezes its partition, a naive $W = \min(T_p) - L$ would freeze the global watermark indefinitely across all sites.
   - **Idle Source Detection**:
     - For every partition $p$, `pkg/consumer.WatermarkTracker` tracks:
       1. $T_p$: the highest event-time observed in consumed events on partition $p$.
       2. $U_p$: the local wall-clock timestamp (`time.Now().UTC()`) when a message was last consumed on partition $p$.
     - Partition State:
       - **Active**: `now - U_p <= IdleTimeout` (default `10m` production, `30s` test).
       - **Idle**: `now - U_p > IdleTimeout`.
     - **Watermark Formula with Monotonic Guard**:
       $$W_{candidate} = \begin{cases} \min_{p \in \text{ActivePartitions}}(T_p) - L & \text{if ActivePartitions} \neq \emptyset \\ \max_{p}(T_p) - L & \text{if all partitions are idle} \end{cases}$$
       $$W = \max(W_{previous\_emitted}, W_{candidate})$$
       where $L$ is the bounded lateness tolerance (e.g. 15 minutes). The outer $\max$ wrapper guarantees that the emitted watermark never regresses, even when an idle partition reawakens with an older backlogged event time.
     - **Re-inclusion on Awakening**: The instant partition $p$ consumes a new message, $U_p$ is updated to `now`, transitioning $p$ immediately back to **Active**. Watermark $W$ is recomputed, and because $W = \max(W_{previous\_emitted}, W_{candidate})$, $W$ is strictly monotonically non-decreasing ($W_t \ge W_{t-1}$).
   - **Completeness Signal Lifecycle & Revision Policy**:
     - *Clinical Safety Decision*: When an analytical or DSMB window $[t_1, t_2)$ satisfies $W \ge t_2$, it transitions from `OPEN` to `COMPLETE`.
     - *Late Backlog Handling*: When a disconnected site reconnects and delivers a backlog with event times $t_{event} < t_2$, the records are durably written to Cassandra and marked `is_late = true`.
     - Crucially, the window status itself is transitioned from `COMPLETE` to `REVISED` (accompanied by an emitted `LateArrivalAudit` log detailing newly ingested late records).
     - *Rationale*: In pharmacovigilance and 21 CFR Part 11 electronic records, once safety staff or DSMB members act on a "complete" window report, retroactively mutating past results without an audit trail is a regulatory violation, while ignoring late adverse events is a patient safety violation. Transitioning the window status to `REVISED` preserves an immutable audit trail of what was originally reported vs what arrived late.

4. **Consumer-Side Idempotency**
   - On consumer crash or partition rebalance, Kafka may redeliver uncommitted messages.
   - Because all 3 Cassandra tables are keyed by `idempotency_key` (either directly or as the tie-breaking clustering column), CQL `INSERT` statements are natural idempotent upserts.
   - Redelivering an identical event writes the exact same columns and payload into Cassandra without creating duplicate rows.
   - Kafka offsets are committed only after the Cassandra write succeeds.

**Why:**
- `event_outbox` was designed for transactional publishing, not analytical querying; trying to query it by study or site requires full-table scans.
- Watermarking with idle partition detection provides an honest answer to "is the clinical safety data for yesterday complete across all global trial sites?", tolerating network partitions without stalling the global watermark.
- Parallel upserts with offset commit gating maximize Cassandra write throughput while preserving exact-once processing semantics downstream.

**Alternatives considered:**
- *Static watermarks without idle detection*: Fails the project's central scenario (a site going offline freezes global watermarks indefinitely). Rejected.
- *Discarding late data on reconnected partitions*: Violates core clinical safety premise (never drop adverse events). Rejected.
- *Silently modifying closed windows without status revision*: Violates 21 CFR Part 11 regulatory auditability. Rejected in favor of the `COMPLETE` -> `REVISED` lifecycle.
- *CQL Logged Batches*: Unnecessary coordination bottleneck across distinct partition keys; offset-commit gating delivers identical durability with much higher throughput.

**Impact if approved:**
- New CQL migration `migrations/002_canonical_schema.cql` adding `canonical_events`, `events_by_study`, `events_by_site`.
- New package `pkg/consumer` containing consumer engine, watermark tracker (with idle detection and window revision tracking), and Cassandra canonical store.
- New binary `cmd/pharos-consumer`.
- Full unit and integration test suite asserting single-row idempotency on message redelivery, per-site ordering preservation, idle-partition watermark progression, and late-arrival window revision.

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

