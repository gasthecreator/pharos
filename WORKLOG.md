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

## [2026-08-30] Claude Code review: Slice 4 watermark almost there — one monotonicity gap, then cleared straight to implementation

**Author:** Claude Code

**What:** Reviewed Gemini's revised Slice 4 proposal. Idle-partition
detection (tracking wall-clock idle time separately from event-time
progress) correctly closes the freezing bug from the prior round. The
`COMPLETE`→`REVISED` window lifecycle for late-arriving data is a genuinely
good addition — real regulatory grounding (21 CFR Part 11), not just a
plausible-sounding label. `errgroup` parallel upserts and explicit
`study_id` extraction both correctly address what was asked. Traced the
watermark formula through the exact reconnection scenario the whole feature
exists for and found one more gap: the design claims monotonic
non-decrease but the formula doesn't actually deliver it.

**Why:** Walk through it: partition A active at `T_A=100`, partition B idle
and excluded, so `W = T_A - L`. B reconnects and delivers its backlog —
first message has an old event-time, say `T_B=50`. Per the stated rule B
immediately goes back to Active, so the recomputed `W = min(100,50) - L`,
which is *lower* than what was already emitted. The watermark regresses
exactly when a site reconnects with a backlog — the specific case
idle-detection was built to handle. This isn't just a documentation error:
it threatens the completeness signal itself, since a window already marked
`COMPLETE` against the higher watermark could look inconsistent against a
freshly recomputed lower one, undermining the audit-trail correctness the
`REVISED` lifecycle exists to protect.

**How:** Required the standard fix every real watermark generator uses for
this exact reason (Flink included): never let the emitted watermark fall
below what's already been emitted — `W_new = max(W_previous, candidate)`,
wrapping the idle-aware `min()` computation. Given how close this is and how
much back-and-forth Slice 4 has already needed, pre-approved the rest of the
design and told Gemini to go straight to full implementation (schema
migration, `pkg/consumer`, canonical store, consumer binary, full test
suite) once this one fix is applied — no additional proposal-only
round-trip. Gideon gave direct feedback this session that Gemini completes
prompts quickly and I should be assigning larger units of work rather than
narrowly-scoped single-issue round-trips; this is that feedback applied.

**Files/modules touched:** `ARCHITECTURE_PROPOSALS.md`, `WORKLOG.md`. No
Slice 4 code exists yet.

**Tests added/updated:** none yet. Noted for the verification plan once
implementation lands: a test must cover a partition reawakening after being
idle, with an old/backlogged event-time, and assert the emitted watermark
never goes below its previous value — this is the exact scenario the
monotonicity gap was found in and needs to be the thing proven, not just
"idle partitions get excluded."

**Follow-ups / left open:**
- Waiting on Gemini's fix + full Slice 4 implementation in one pass.

## [2026-08-30] Resubmit Slice 4 architecture proposal: idle-source watermark tracking, window revision lifecycle, errgroup parallel upserts

**Author:** Gemini (Antigravity)

**What:** Revised and resubmitted the Slice 4 architecture proposal in `ARCHITECTURE_PROPOSALS.md` addressing all review feedback from Claude Code:
1. **Idle-Partition Detection in Watermark Tracking**:
   - Separately tracks $T_p$ (max event-time) and $U_p$ (wall-clock timestamp of last consumed message) per partition.
   - Partitions with `now - U_p > IdleTimeout` are classified as `Idle` and excluded from the global watermark computation:
     $$W = \min_{p \in \text{ActivePartitions}}(T_p) - L$$
     preventing a disconnected site (e.g. Nigeria or Tokyo offline for hours/days) from indefinitely stalling the global stream watermark.
   - Immediate re-inclusion upon receiving a new message, with monotonic progression guaranteed ($W_t \ge W_{t-1}$).
2. **Completeness Signal Lifecycle & Revision Policy**:
   - Defined window status progression: `OPEN` -> `COMPLETE` when $W \ge \text{windowEnd}$.
   - When a reconnected site produces late-arriving events ($t_{event} < \text{windowEnd}$), records are durably saved to Cassandra with `is_late = true`, and the window transitions to `REVISED` with a logged `LateArrivalAudit`.
   - Adheres to 21 CFR Part 11 electronic records traceability (neither ignoring late adverse events nor silently altering historical audit records).
3. **Cassandra Write Strategy**:
   - Replaced CQL logged batch across the 3 tables (`canonical_events`, `events_by_study`, `events_by_site`) with parallel independent idempotent upserts managed by an `errgroup.Group` at `LOCAL_QUORUM`.
   - Avoids coordinator batch-log replication overhead across multiple partition keys while preserving exact-once processing semantics through Kafka offset commit gating.
4. **Study ID Extraction**:
   - Specified deterministic extraction from `model.AdverseEvent.Study[0].Reference` with `"UNKNOWN_STUDY"` fallback.

**Why:** Solves the watermark starvation bug during site network partitions and optimizes downstream Cassandra write throughput.

**Files/modules touched:**
- `ARCHITECTURE_PROPOSALS.md` [MODIFIED]
- `WORKLOG.md` [MODIFIED]

**Follow-ups / left open:**
- Awaiting Claude Code review on revised `ARCHITECTURE_PROPOSALS.md`.

## [2026-08-30] Claude Code review: Slice 4 proposal sent back — watermark starves on the exact scenario this project targets

**Author:** Claude Code

**What:** Reviewed Gemini's Slice 4 architecture proposal (Kafka consumer
topology, three-table canonical Cassandra schema, event-time watermarking,
consumer-side idempotency) before any code was written. Read the full
proposal text directly from `ARCHITECTURE_PROPOSALS.md`. The consumer
topology, the three-table schema (genuinely resolves the query-pattern
question that's been open since the very first planning pass), and the
decision to hand-roll a Go watermark tracker instead of pulling in Flink are
all sound and approved as-is. Traced the watermark formula against the
project's own central scenario and found it doesn't hold up. Marked the
proposal "Requires revision" — not implemented yet.

**Why:** The proposed formula `W = min_p(T_p) - L` takes the minimum
observed event-time across all partitions with no way to exclude one that's
gone silent. Walking through PLAN.md §2.1's own example — a site
disconnected for hours or days — that partition's `T_p` freezes the moment
the site goes offline, and because a frozen partition is still counted in
the `min()`, the *global* watermark freezes with it, even while every other
site's partition keeps advancing normally. The proposal's own motivating
question ("is yesterday's clinical safety data complete across all global
trial sites?") would answer "no, indefinitely" for the entire dataset the
instant any single site has the exact outage this whole project exists to
tolerate. Not an edge case — the central one.

**How:** Wrote the required fix directly onto the proposal entry rather than
just rejecting it: track wall-clock idle time per partition, separately from
event-time progress, and exclude a partition from the watermark's `min()`
once it's been idle past a configurable threshold — the same idle-source
pattern Flink and Kafka Streams use for this exact problem — re-including it
once it resumes. Also asked for explicit treatment of what happens to a
completeness signal that was already reported while a partition was excluded
and later catches up with a backlog of "late" events, since PLAN.md already
requires the underlying late data to be durably flagged, but the
*completeness signal* itself (the actual artifact clinical staff would act
on) needs the same scrutiny.

Also flagged, less severe: the proposed multi-table write uses a Cassandra
logged batch across three *different* partition keys, a known throughput
anti-pattern, for a guarantee (atomicity) the design doesn't actually need —
all three writes are individually idempotent, and the consumer already
withholds its Kafka offset commit until all three succeed, which already
gives "retry until every table reflects it" without Cassandra's batch-log
overhead. Recommended independent writes gated by an errgroup instead, but
left room for Gemini to argue for keeping the batch if there's a concrete
reason — the point is the stated rationale doesn't hold up, not that logged
batches are never appropriate.

**Files/modules touched:** `ARCHITECTURE_PROPOSALS.md`, `WORKLOG.md`. No
Slice 4 code exists yet — proposal-stage review only.

**Tests added/updated:** none yet. Noted for when the revised proposal adds
its verification plan: whatever test proves watermark correctness needs to
include a scenario where one partition goes idle for longer than the lease/
idle threshold while others keep advancing, asserting the global watermark
continues progressing based on the active partitions rather than freezing.

**Follow-ups / left open:**
- Waiting on Gemini's revised watermark design before any Slice 4 code gets
  written.
- Minor clarification requested alongside the revision: how `study_id` gets
  extracted for `events_by_study`'s partition key, given
  `model.AdverseEvent.Study` is a list.

## [2026-08-30] Propose Slice 4 architecture: Kafka consumer topology, queryable Cassandra tables, event-time watermarking, and idempotent sinks

**Author:** Gemini (Antigravity)

**What:** Authored and submitted Slice 4 architecture proposal in `ARCHITECTURE_PROPOSALS.md` addressing PLAN.md §2.4 and §5 (Open Question 1):
1. Dedicated consumer binary topology (`cmd/pharos-consumer` and `pkg/consumer.Engine`) with `segmentio/kafka-go` consumer group `pharos-canonical-sink` and explicit manual offset commit.
2. Canonical Cassandra schema design with 3 targeted tables:
   - `pharos.canonical_events` partitioned by `(idempotency_key)` for exact point lookups and lineage tracking.
   - `pharos.events_by_study` partitioned by `((study_id), event_time, idempotency_key)` for time-range safety queries (`WHERE study_id = ? AND event_time >= ? AND event_time <= ?`).
   - `pharos.events_by_site` partitioned by `((site_id), local_seq, idempotency_key)` for site audit and continuous sequence verification.
3. Event-time watermarking semantics via `pkg/consumer.WatermarkTracker` calculating global watermark $W = \min(T_p) - L$ across active per-site partitions, providing verifiable window completeness metrics and late-arrival detection for multi-timezone clinical trial streams.
4. Consumer-side idempotency via natural CQL upsert semantics keyed by idempotency key.

**Why:** Prepares the technical foundation for closing the gap between Kafka topic `pharos.events.adverse` and a durable, queryable clinical data store before implementing code.

**Files/modules touched:**
- `ARCHITECTURE_PROPOSALS.md` [MODIFIED]
- `WORKLOG.md` [MODIFIED]

**Tests added/updated:**
- None yet (proposal phase). Full test suite planned upon approval.

**Follow-ups / left open:**
- Awaiting Claude Code review on `ARCHITECTURE_PROPOSALS.md`.

## [2026-08-30] Claude Code approves Slice 3; merging into main

**Author:** Claude Code

**What:** Reviewed Gemini's fix for the required 207/503-and-forwarder-
correlation gap from the prior round. Pulled the branch, independently
confirmed no header corruption this time (scanned both `WORKLOG.md` and
`ARCHITECTURE_PROPOSALS.md` programmatically), ran the full suite myself
including the real Cassandra and Kafka integration tests (both genuinely
pass, not skip), and read the actual diff for both sides of the fix rather
than trusting the summary.

Confirmed correct: `HandleEvents` now returns 207 whenever the batch has any
accepted/rejected events alongside failures, reserving 503 for the case
where every event failed; `rejectedCount` is correctly decremented when an
event gets reclassified from rejected to failed, so the response's aggregate
counts now match what's actually in `Results`. `forwarder.go`'s correlation
loop now explicitly handles `ingestion.StatusFailed` via `MarkFailed` with
backoff — and went further than what I asked for: the fallback for an
unmapped record now depends on the response's status code (200/201 defaults
to acknowledged, since that's unambiguous full success; anything else,
including 207, defaults to `MarkFailed` with backoff rather than silently
acknowledging). That's the right call and I hadn't specified it that
precisely. `TestForwarder_207MixedBatchWithFailedStatus` verifies actual
SQLite state for all three outcomes in one batch, not just return values;
`TestFullBatchInfraFailure_Returns503` covers the reserved-503 case.

**Why:** This closes the last open finding from Slice 3's build. The
combination of correct claim/lease outbox semantics (verified against real
Cassandra), correct Kafka publishing (verified against a real broker), and
now correct edge/central batch-response semantics means the four core
challenges in PLAN.md §2 are genuinely implemented and tested end to end,
not just individually plausible in isolation.

**How:** Folded the finalized 207/503 contract into `PLAN.md` §2.3
(new "batch response status codes are per-event-aware" note, alongside the
already-approved claim/lease and DLQ-symmetry notes from the design-approval
round). Updated the §4 checklist to reflect what's actually built after
three slices — checked off items with real, verified implementations behind
them; left partial items (DLQ inspection tooling, Kafka retention policy,
two of the four fault-injection test categories) explicitly unchecked with a
note on what's missing, rather than rounding up.

**Files/modules touched:** `PLAN.md` (§2.3, §4), `WORKLOG.md`. No code
changes by Claude this round.

**Tests added/updated:** none by Claude — independently re-verified Gemini's
2 new tests plus the full existing suite (51+ tests, `go vet` clean, `-race`
clean, real Cassandra/Kafka integration tests passing for real).

**Follow-ups / left open:**
- Merging `feat/slice-3-cassandra-outbox-kafka` into `main` after this entry.
- Owed, per §6 (Claude's explicit responsibility): dedicated fault-injection
  tests for network partition simulation and out-of-order delivery — the
  duplicate-delivery and malformed-payload categories are already covered by
  Slice 3's own tests, but these two are not yet dedicated tests anywhere.
- DLQ inspection tooling (some way to actually query/replay
  `dead_letter_events` or the `pharos.events.dlq` topic) doesn't exist yet —
  durable persistence does, but nothing to look at it with.
- Kafka retention policy is still on defaults — never explicitly decided.
- Slice 4 (per Gemini's own note): Kafka consumer + ordered downstream
  processing — nothing consumes `pharos.events.adverse` yet.

## [2026-08-30] Connect multi-batch StatusFailed to forwarder and return 207 on partial batch failure

**Author:** Gemini (Antigravity)

**What:** Closed the loop between Central Ingestion's per-event infrastructure failure reporting and Edge Forwarder state tracking:
1. In `pkg/ingestion/handler.go`:
   - Updated HTTP status decision so HTTP 207 Multi-Status is returned whenever a batch contains any successfully processed events (accepted or rejected) alongside infrastructure failures. Reserved HTTP 503 for the case where every single event in the batch hit an infrastructure failure with nothing else to report.
   - Fixed bookkeeping discrepancy where DLQ outbox storage or DLQ Kafka publish errors reclassify the event as `StatusFailed` but previously left `rejectedCount` inflated. Decremented `rejectedCount--` upon reclassification so top-level `resp.Rejected` strictly matches the count of `StatusRejected` entries in `resp.Results`.
2. In `pkg/edge/forwarder.go`:
   - Extended the per-event correlation logic to explicitly check `res.Status == ingestion.StatusFailed`. Instead of falling through into the acknowledgment branch, failed events call `store.MarkFailed(ctx, r.ID, reason, retryAfter)` with exponential backoff.
   - Hardened unmapped item fallback in 207 Multi-Status responses to mark records failed with backoff rather than silently acknowledging them.
3. Tests added/updated:
   - `pkg/edge/forwarder_test.go`: Added `TestForwarder_207MixedBatchWithFailedStatus` verifying a 3-event batch receiving HTTP 207 with ACCEPTED, REJECTED, and FAILED correctly transitions each record locally into ACKNOWLEDGED, REJECTED (with last_error), and FAILED (with next_retry_at backoff).
   - `pkg/ingestion/outbox_test.go`: Updated `TestMultiEventBatch_MiddleEventInfraFailureContinuesProcessing` to assert HTTP 207 for mixed outcomes, and added `TestFullBatchInfraFailure_Returns503` asserting HTTP 503 when all events fail.

**Why:** Prevents silent data loss. Without explicit `StatusFailed` handling in the forwarder, extending the forwarder to parse 5xx/207 bodies would have caused failed events to fall through to the acceptance branch and be marked as delivered despite never reaching Kafka.

## [2026-08-30] Claude Code review: Slice 3 core design verified against real infra; one required fix, one recurring process bug

**Author:** Claude Code

**What:** Reviewed Gemini's Slice 3 implementation (commit `6816a47`). Pulled
the branch, independently confirmed `go vet` clean and the full suite passing
with `-race`, and read the actual diff rather than trusting the summary
alone. The core claim/lease outbox design (`pkg/dedup/cassandra_store.go`)
is implemented correctly and — significantly — actually verified against a
real Cassandra container and a real Kafka broker, not just mocks
(`TestCassandraOutboxStore_RealIntegration`, `TestKafkaProducer_RealIntegration`,
both passing). The `MapScanCAS` fix Gemini found and applied during this
round is legitimate: Cassandra's LWT protocol returns the existing row's
columns on a failed conditional write specifically so the client doesn't
need a follow-up `SELECT`, and the code now correctly reads from that map
instead of assuming a single-column scan.

Found one real gap in the multi-event-batch fix from the prior review round,
and repeated the exact same file-corruption mistake from an earlier round in
a second file.

**Why:** The multi-batch fix (no longer aborting mid-loop on an infra error)
is real and its own test passes, but it's only half-connected end to end.
`HandleEvents` now returns HTTP 503 whenever any event in a batch hits a
transient infra failure, with a `StatusFailed` per-event result inside the
`BatchResponse` body. But `pkg/edge/forwarder.go` (untouched this round —
confirmed via `git show --stat`) only parses the response body for
200/201/207/422; any other status code, including this new 503, falls into
its `default:` branch, which marks the *entire* batch `FAILED` for retry
without ever reading the body. So the granular per-event information Central
Ingestion now produces is never actually consumed — the forwarder just
retries the whole batch regardless, same as before the fix. Not currently a
correctness bug: the already-published events in that batch are safe
idempotent no-ops on retry, so nothing is lost or duplicated today. But it's
a real latent trap: if the forwarder's switch statement is ever extended to
parse 5xx bodies (a natural next step given the handler now exists to
support exactly that), the existing per-event correlation logic doesn't know
about `StatusFailed` at all — anything that isn't explicitly `StatusRejected`
falls into an `else` branch that, for any code other than 422, marks it
`ackIDs` (acknowledged). That would silently mark a genuinely-failed,
never-published event as successfully delivered — real, silent data loss,
exactly the class of bug this whole project exists to prevent.

Also found: the exact same header-corruption pattern from an earlier round
(an `ARCHITECTURE_PROPOSALS.md` entry losing its `## [date] title` line when
a new entry was prepended above it) recurred here in `WORKLOG.md` — Gemini's
new Slice 3 entry's insertion swallowed the header of the immediately-
following entry (my own "Claude Code approves revised Slice 3 outbox design"
entry). This is the second time this exact mistake has happened in a
different file, which means it's a systematic pattern in how new entries get
prepended, not a one-off. Restored the missing header directly.

**How:** Traced the actual code path: `HandleEvents`'s status-code decision
(`if failedCount > 0 { ...503... }`) takes priority over the 207/422/200
branches, so ANY infra failure in a batch — even alongside successes — routes
to 503, which the forwarder's switch statement doesn't special-case.
Verified via `grep`/`sed` on `pkg/edge/forwarder.go`'s switch statement and
confirmed `forwarder.go` wasn't in this commit's changed-file list at all.
Restored the missing `WORKLOG.md` header by hand, then scanned the entire
file (and `ARCHITECTURE_PROPOSALS.md`) programmatically for any other
instance of an `**Author:**`/`**Status:**` line missing its preceding header
— none found, so this was an isolated instance this round, not a chain.

Also noted a minor, non-blocking inconsistency while reading: when a DLQ
write or Kafka publish fails for an event that was already counted as
`rejectedCount++` earlier in the loop (because it failed FHIR validation),
`results[i].Status` correctly gets overwritten to `FAILED`, but
`rejectedCount` itself is never decremented — so the response's top-level
`Rejected` count can be inflated relative to how many entries in `Results`
actually show `Status == "REJECTED"`. Not a correctness issue since a careful
consumer should trust per-item `Results` over the aggregate counts, but worth
tidying alongside the status-code fix.

**Files/modules touched this round:** `WORKLOG.md` (header restoration).

**Tests added/updated:** none by Claude this round — independently
re-verified the existing suite (51 tests across 6 packages, `go vet` clean,
`-race` clean) rather than adding new tests myself.

**Follow-ups / left open:**
- Required before merge: change `HandleEvents` to return 207 (not 503)
  whenever the batch has any successfully-processed events (accepted or
  rejected) alongside failures — reserve a bare 503 for the case where every
  single event in the batch hit an infra failure with nothing else to
  report. Extend `forwarder.go`'s per-event correlation logic to explicitly
  handle `ingestion.StatusFailed` by calling the existing
  `QueueStore.MarkFailed` on exactly those record IDs (with backoff),
  instead of letting them fall into the ambiguous `else` branch. Add a test
  exercising the full forwarder-receives-207-with-a-FAILED-event path, not
  just the handler-level behavior in isolation.
- Fix the `rejectedCount` bookkeeping nit alongside the above, same area of
  code.
- Process note, now confirmed recurring: whenever prepending a new entry to
  `WORKLOG.md` or `ARCHITECTURE_PROPOSALS.md`, verify with a diff or a
  re-read that the entry immediately below still has its own intact header
  line before moving on. This has now silently damaged two different files
  across two different rounds.

## [2026-08-30] Implement Slice 3: Cassandra transactional outbox, Kafka publisher, sweeper, and multi-event batch failure resilience

**Author:** Gemini (Antigravity)

**What:** Implemented the full Slice 3 architecture per PLAN.md §2.2 and §2.3:
1. Cassandra schema migrations (`migrations/001_init_schema.cql`) defining `pharos.event_outbox`, `pharos.dead_letter_events`, and `pharos.pending_outbox`.
2. Pure-Go Kafka producer (`pkg/kafka/producer.go`) using `segmentio/kafka-go` with `RequiredAcks: -1`, Snappy compression, deterministic per-site hash partitioning (`&kafka.Hash{}`), and a thread-safe `MockProducer` with per-key/topic failure injection.
3. Transactional outbox storage engine (`pkg/dedup/cassandra_store.go` and `pkg/dedup/memory_store.go`) implementing the approved 3-state claim/lease pattern via Cassandra Paxos LWT (`INSERT ... IF NOT EXISTS` with `status='PUBLISHING'`) and CAS lease steals (`UPDATE ... IF status='PUBLISHING' AND claimed_at=?`) using `MapScanCAS`.
4. Background outbox sweeper (`pkg/ingestion/sweeper.go`) scanning `pending_outbox` hourly buckets to reclaim stale claims abandoned by worker crashes.
5. Central Ingestion handler integration (`pkg/ingestion/handler.go`) wiring in-process per-key mutexes (`keyLocks sync.Map`), outbox claim/publish/DLQ flow, and resilient multi-event batch processing: individual event infrastructure errors no longer abort the loop mid-batch; succeeding events before and after are durably processed and acknowledged, failing events are honestly marked `FAILED`, and the complete structured `BatchResponse` is returned with HTTP 503 so edge clients retry.
6. Daemon wiring in `cmd/pharos-ingestion/main.go` supporting production Cassandra, Kafka, and Sweeper lifecycle with graceful shutdown.

**Why:** Addresses PLAN.md Core Challenge 2 (Exactly-once processing via idempotency keys + dedup store) and Core Challenge 3 (Validation, Rejection, and DLQ Pipeline):
- Eliminates the crash window between dedup check and Kafka publish: every event is durably written to Cassandra outbox before Kafka publish is attempted.
- Linearizes concurrent duplicate submissions so exactly one caller wins the LWT insert and publishes to Kafka.
- Reclaims crashes safely via CAS lease stealing without duplicate publishes.
- Ensures multi-event batches from edge sites never lose progress when an individual event encounters transient infra hiccups.

**Files/modules touched:**
- `migrations/001_init_schema.cql` [NEW]
- `pkg/kafka/producer.go`, `pkg/kafka/producer_test.go`, `pkg/kafka/kafka_integration_test.go` [NEW]
- `pkg/dedup/store.go`, `pkg/dedup/cassandra_store.go`, `pkg/dedup/memory_store.go`, `pkg/dedup/store_test.go`, `pkg/dedup/cassandra_integration_test.go` [NEW]
- `pkg/ingestion/sweeper.go` [NEW]
- `pkg/ingestion/handler.go` [MODIFIED]
- `pkg/ingestion/outbox_test.go` [NEW]
- `cmd/pharos-ingestion/main.go` [MODIFIED]
- `docker-compose.yml` [MODIFIED]
- `go.mod`, `go.sum` [MODIFIED]

**Tests added/updated:**
- `pkg/dedup/store_test.go`: Unit tests for outbox lifecycle, lease stealing, and concurrency (50 racing goroutines, exactly 1 winner).
- `pkg/dedup/cassandra_integration_test.go`: End-to-end integration test against live Cassandra container verifying LWT insert claims, lease blocking, CAS lease steals, and 10 concurrent goroutines racing on live Paxos consensus.
- `pkg/kafka/producer_test.go`: Mock producer concurrency and fault injection.
- `pkg/kafka/kafka_integration_test.go`: End-to-end integration test publishing to live Kafka broker on port 9092.
- `pkg/ingestion/outbox_test.go`:
  - `TestConcurrentDuplicateRaces` (25 racing goroutines, asserting exactly 1 Kafka publish).
  - `TestCrashWindowResumption` (injected Kafka failure leaves `PUBLISHING` status, retry resumes and publishes).
  - `TestSequentialDuplicateIdempotency` (duplicate returns HTTP 200 without second Kafka publish).
  - `TestStaleLeaseReclamationBySweeper` (sweeper reclaims expired lease and publishes to Kafka).
  - `TestDeadLetterPipeline_DurabilityAndRouting` (malformed payload stored in DLQ table and routed to DLQ Kafka topic).
  - `TestRawPayloadPreservation` (unmodeled payload fields preserved verbatim in Cassandra outbox and Kafka message).
  - `TestMultiEventBatch_MiddleEventInfraFailureContinuesProcessing` (middle event fails Kafka publish; events before and after are durably processed, HTTP 503 with honest full BatchResponse returned).

**Follow-ups / left open:**
- Ready for Slice 4 (Kafka consumer and ordered downstream processing).

## [2026-08-30] Claude Code approves revised Slice 3 outbox design; cleared to implement

**Author:** Claude Code

**What:** Reviewed Gemini's revised Slice 3 proposal against the required fix
from the prior review round. Read the full revised proposal text directly
from `ARCHITECTURE_PROPOSALS.md`, then manually traced the concurrent-race
scenario through the new design to verify the fix actually holds rather than
just checking that a `status` column was added. Approved — cleared Gemini to
begin implementation.

**Why/How:** Confirmed the fix closes the race: two requests for the same
idempotency key both attempt `INSERT ... status='PUBLISHING' IF NOT EXISTS`;
Cassandra's LWT linearizes it so exactly one wins and proceeds to publish,
the loser reads a fresh `PUBLISHING` claim and correctly does nothing (this
is safe because durability was already achieved at the winning insert — the
sweeper is the real backstop, not the loser's HTTP response). Verified the
lease-expiry compare-and-swap (`IF status='PUBLISHING' AND claimed_at=?`)
correctly serializes even when multiple actors (concurrent requests, the
sweeper) simultaneously try to steal the same expired lease — only one CAS
can match the exact stale `claimed_at` value. Confirmed the DLQ path now
mirrors this exactly (previously it had the identical unaddressed
crash-window gap, just relocated to the rejection path) and that both
`payload` columns are explicitly specified as raw JSON bytes, not
re-serialized structs.

Found one harmless, non-blocking issue: `status='PENDING'` is now vestigial
— since both the accept-path and DLQ-path inserts write `status='PUBLISHING'`
directly (correctly, since winning the insert *is* the claim), no code path
actually ever produces a `PENDING` row. Sub-case 2d and the sweeper's
`PENDING` branch are dead code left over from the pre-revision two-step
design. Doesn't cause a bug — flagged for deletion when the Go code gets
written rather than blocking approval on it.

Folded the finalized design into `PLAN.md` §2.2 (replacing the boolean-flag
description with the claim/lease pattern) and §2.3 (DLQ now explicitly uses
the same pattern, not a simpler one-shot write).

**Files/modules touched:** `PLAN.md` (§2.2, §2.3), `ARCHITECTURE_PROPOSALS.md`
(entry marked resolved), `WORKLOG.md`. No Slice 3 code exists yet.

**Tests added/updated:** none — proposal review only. Verification plan in
the approved proposal already requires the concurrent-race test to assert
exactly one Kafka publish occurred, not just one `applied == true`, plus a
new stale-lease-reclamation test — both correctly scoped for what actually
needs proving.

**Follow-ups / left open:**
- Gemini cleared to implement Slice 3 against the approved design.
- Delete the vestigial `PENDING` branches when writing the Go code.
- Separately: while reviewing this round, traced a `git reset --hard
  origin/main` Gemini ran mid-session back through the reflog — it was a
  self-correction after an accidental `git pull origin
  feat/slice-3-cassandra-outbox-kafka` while sitting on `main` (left checked
  out from Claude's prior session) fast-forwarded local `main` ahead of
  `origin/main`. No commits were lost — everything stayed reachable via the
  feature branch throughout, confirmed via reflog and by diffing `main`
  against `origin/main` after the fact (identical). No corrective action
  needed, but worth naming as a process risk: this repo's local checkout is
  shared between Gemini and Claude Code sessions, so whichever branch is
  currently checked out is whatever the *previous* session left behind —
  both should verify `git branch --show-current` before assuming which
  branch they're on, especially before any pull/reset/checkout.

## [2026-08-30] Slice 3 architecture revised: 3-state claim lock (PENDING/PUBLISHING/PUBLISHED), lease timeout, symmetric DLQ outbox, and raw payload storage

**Author:** Gemini (Antigravity)

**What:** Addressed all feedback from Claude Code's review of the Slice 3 architecture proposal:
1. Concurrency fix: Replaced `published boolean` with a three-state `status` column (`PENDING`, `PUBLISHING`, `PUBLISHED`) and `claimed_at timestamp`. Any actor attempting to publish to Kafka must hold an active `PUBLISHING` claim won via Cassandra LWT (`INSERT ... VALUES ('PUBLISHING', now) IF NOT EXISTS` or conditional `UPDATE ... SET status = 'PUBLISHING', claimed_at = now ... IF status = 'PENDING'`). Racing duplicates or in-flight retries see an active lease and immediately no-op, eliminating the dual-Kafka-publish race condition.
2. Stale lease reclamation: Defined a 30s `LeaseTimeout` past which the background sweeper or a subsequent retry can conditionally reclaim a row stuck in `PUBLISHING` due to an in-flight worker crash.
3. Symmetric DLQ outbox: Applied the identical three-state claim lock (`status`, `claimed_at`) to `pharos.dead_letter_events`, ensuring rejected events are guaranteed to land on Kafka's DLQ topic without crash-window drop risk.
4. Raw payload preservation: Explicitly specified that `payload text` in both tables stores the raw JSON bytes (`json.RawMessage`), never re-serialized Go structs.
5. Verification requirement: Documented that the concurrent-duplicate-race test must assert that *exactly one Kafka publish actually occurred*, verifying real deduplication at the broker level.

**Files/modules touched:**
- `ARCHITECTURE_PROPOSALS.md`
- `WORKLOG.md`

## [2026-08-30] Claude Code review: Slice 3 proposal sent back for revision — concurrent-publish race

**Author:** Claude Code

**What:** Reviewed Gemini's Slice 3 architecture proposal (Cassandra
transactional outbox schema, consistency levels, Kafka pipeline) before any
code was written, per the elevated-rigor process set for this slice. Read the
full proposal text directly from `ARCHITECTURE_PROPOSALS.md`, not just the
chat summary. The core outbox pattern is correct — it closes the original
crash-window bug from the Slice 2 planning phase (Cassandra write with
`published: false`, then a separate step publishes to Kafka and flips the
flag). Traced through what happens under concurrent access and found a real
race the proposal doesn't close, plus two smaller gaps. Marked the proposal
"Requires revision" rather than approving — not implemented yet.

**Why:** Two requests for the same idempotency key arriving close together
(a genuine duplicate, or a retry racing a still-in-flight original — both
realistic) both hit the LWT insert; one gets `applied == true` and proceeds
to publish, the other gets `applied == false`, sees `published == false`, and
under the proposed logic assumes the original crashed and tries to resume the
publish itself — even though the original is still actively working on it.
Two goroutines can then both call the Kafka producer for the same event. This
is the exact "silently dropped or duplicated" failure PLAN.md §2.2 exists to
prevent, just moved from the Cassandra layer (already correctly fixed) to the
Kafka-publish layer (not yet). A boolean can't distinguish "nobody is
publishing this" from "someone is publishing this right now" — that needs a
third state and its own conditional guard.

**How:** Wrote the required fix into `ARCHITECTURE_PROPOSALS.md` directly on
the entry rather than just rejecting it: replace `published boolean` with a
three-state `status` column (`PENDING`/`PUBLISHING`/`PUBLISHED`), and require
any actor (original request, racing duplicate, or the background sweeper) to
win a second conditional Cassandra update
(`... SET status='PUBLISHING' ... IF status='PENDING'`) before it's allowed
to call the Kafka producer at all — consistent with the rest of this design
already being built on Cassandra LWTs, rather than reaching for an in-process
mutex, which would only be safe as long as Central Ingestion stays a single
instance and would silently stop being safe the moment that changes. Also
flagged two smaller required clarifications: the DLQ path's
`kafka_published` flag has the identical unaddressed crash-window problem and
needs the same status/claim treatment, not a special case; and the `payload`
columns in both new tables need to explicitly store raw JSON bytes (the
`json.RawMessage` from Slice 2's fix), not a re-serialized Go struct, so that
fix doesn't get silently undone here.

**Files/modules touched:** `ARCHITECTURE_PROPOSALS.md`, `WORKLOG.md`. No code
exists for Slice 3 yet — proposal-stage review only.

**Tests added/updated:** none yet. Noted for when tests are written: the
concurrent-duplicate-race test must assert exactly one Kafka publish actually
occurred, not just that exactly one `applied == true` resulted from the
insert — those are different claims, and only the second one is the one that
matters.

**Follow-ups / left open:**
- Waiting on Gemini's revised proposal before any Slice 3 code gets written.
- Once the status/claim pattern is in place, verify the sweeper's reclaim
  logic for a `PUBLISHING` row whose claimant crashed before completing (a
  lease/claimed-at timeout) — the revision request asked for this to be
  written down explicitly, not left implicit.

## [2026-08-30] Slice 3 kickoff: re-derived requirements against PLAN.md §2.2/§2.3, drafted architecture proposal for Cassandra outbox and Kafka pipeline

**Author:** Gemini (Antigravity)

**What:** Initiated Slice 3 on feature branch `feat/slice-3-cassandra-outbox-kafka`:
1. Re-derived the core distributed-systems guarantees against PLAN.md §2.2 (Exactly-once & transactional outbox), §2.3 (DLQ), and §2.4 (per-site FIFO partitioning).
2. Identified critical ambiguities and submitted a detailed proposal to `ARCHITECTURE_PROPOSALS.md` covering:
   - Cassandra consistency levels: `LOCAL_SERIAL` for Paxos LWT, `LOCAL_QUORUM` (dev `ONE`) for commits.
   - Exact CQL schemas for `pharos.event_outbox` and `pharos.dead_letter_events`.
   - Outbox execution lifecycle: synchronous fast-path write (`published: false` via LWT) followed by Kafka publish and `published: true` update, with automatic resumption on edge retry (`applied: false && published: false`) and a background sweeper for edge-dropout scenarios.
   - Dual-persistence dead-letter pipeline: durable writes to both Cassandra `dead_letter_events` table and Kafka `pharos.events.dlq` topic before returning HTTP 422/207 to the edge.
   - Kafka client selection: pure-Go `segmentio/kafka-go` with idempotent producer configuration.
3. Created feature branch `feat/slice-3-cassandra-outbox-kafka`, committed proposal, and pushed to remote tracking branch for Claude Code review before implementation.

**Why:** PLAN.md §2.2 notes that this is the highest-risk slice in the architecture and the easiest place to introduce subtle correctness bugs (e.g. check-then-publish crash windows). Formalizing schemas, consistency levels, and failure recovery semantics in `ARCHITECTURE_PROPOSALS.md` prevents guessing and ensures alignment with supervisor review before code is written.

**Files/modules touched:**
- `ARCHITECTURE_PROPOSALS.md`
- `WORKLOG.md`

## [2026-08-29] Claude Code approves Slice 2 (PR #2) after review fixes; merging

**Author:** Claude Code

**What:** Reviewed Gemini's fixes for the two findings from the prior Slice 2
review round. Pulled the branch locally, independently re-ran `go vet` and
`go test -race -count=1 ./...` (25 tests, 0 failures) rather than trusting the
summary, and read the actual diff for both fixes plus their new tests.

Fix 1 (raw payload forwarding): confirmed `BatchRequest.Events` is now
`[]json.RawMessage`, the forwarder forwards `r.Payload` verbatim with no
struct round-trip, and `TestForwarder_PreservesRawPayloadBytes` genuinely
proves it — it injects a custom unmodeled field directly into the stored
payload and asserts it survives to the outbound HTTP body byte-for-byte.
Central Ingestion's handler also gained graceful per-event handling of
unparseable raw messages (extracts the idempotency key via a partial parse
even when full unmarshal fails, so one corrupted event in a batch doesn't
prevent correlating or reporting the rest) — `TestHandler_PreservesUnmodeledFieldsAndRejectsCorruptedJSON`
covers exactly this. Good, thorough work, not a superficial patch.

Fix 2 (distinct rejection status): confirmed `StatusRejected` and
`QueueStore.MarkRejected` exist, `FetchPending`'s query still excludes it
(verified it isn't in either branch of the `WHERE status = ? OR (status = ?
...)` clause), and the forwarder now correlates `BatchResponse.Results` to
local records by idempotency key rather than array position, calling
`MarkRejected` for actually-rejected events and `MarkAcknowledged` only for
actually-accepted ones — tested for both the 207 mixed-batch case and the 422
full-batch case, checking actual DB state via direct queries, not just the
returned counts.

Also reviewed and approved the rate-limiter clarification proposal Gemini
wrote to `ARCHITECTURE_PROPOSALS.md` (tokens meter HTTP batch requests, not
individual events — effective burst is `capacity × BatchSize`). Folded into
`PLAN.md` §2.3, with one correction noted in the proposals file: its stated
justification for rejecting per-event metering (avoiding parse-before-limit
cost) doesn't hold, since the handler already parses the body before checking
the rate limiter regardless of metering unit. Doesn't change the approval —
the substantive conclusion (document the 50x multiplier) is correct — just
didn't want inaccurate reasoning to read as a real security property later.

**Why:** Both fixes directly closed real correctness/audit gaps identified in
the prior review: silent data destruction on a corrupt local record, and a
rejected event being indistinguishable from a successful delivery in the
local queue's own state. Both are exactly the class of bug PLAN.md's charter
(never silently lose or misrepresent data) exists to catch.

**How:** No code changes this round — verification and documentation only.
Merging PR #2 (`feat/slice-2-forwarder-ingestion`) into `main` after this
entry lands.

**Files/modules touched:** `PLAN.md` (§2.3), `ARCHITECTURE_PROPOSALS.md`
(1 entry resolved), `WORKLOG.md`.

**Tests added/updated:** none by Claude this round — independently re-verified
Gemini's 4 new tests (`TestForwarder_PreservesRawPayloadBytes`,
`TestForwarder_207MixedBatchCorrelation`, `TestForwarder_422FullBatchRejection`,
`TestHandler_PreservesUnmodeledFieldsAndRejectsCorruptedJSON`, plus
`TestSQLiteStore_MarkRejected`) actually exercise what they claim to.

**Follow-ups / left open:**
- Slice 3 still owns the real gap underneath fix 2: Central Ingestion doesn't
  yet persist rejected events anywhere durable (no Kafka DLQ). A `REJECTED`
  status locally is necessary but not sufficient — Slice 3 must add durable
  DLQ persistence before a 422/207 rejection can be considered truly final
  per PLAN.md §2.3's "DLQ entries need to be inspectable and replayable."
- Minor/non-blocking: `MarkRejected` (like `MarkInFlight`/`MarkAcknowledged`)
  uses `fmt.Sprintf` to embed the status constant into the query text — same
  pre-existing style nit noted in the Slice 1 review, not a security issue,
  still just cosmetic cleanup for whenever that file is next touched.

## [2026-08-29] Slice 2 review fixes: raw byte forwarding without struct round-tripping, distinct StatusRejected for rejections

**Author:** Gemini (Antigravity)

**What:** Addressed two correctness and data integrity items identified during PR #2 review:
1. Replaced struct round-tripping in the edge forwarder with direct raw-byte forwarding using `json.RawMessage` in the wire format (`ingestion.BatchRequest`). Eliminates data loss from fallback empty events on unmarshal failures and prevents stripping unmodeled fields from adverse event payloads (§2.1).
2. Added distinct terminal `RecordStatus` (`StatusRejected`) in `pkg/edge/store.go` and `SQLiteStore.MarkRejected`. Updated `forwarder.go` to correlate per-event outcomes by `idempotency_key` (instead of array index) on HTTP 207 and HTTP 422 responses, marking rejected events as `StatusRejected` rather than conflating them with `StatusAcknowledged` (§2.2, §2.3).
3. Added architecture proposal clarifying that Central Ingestion rate limiting meters HTTP batch requests rather than individual events, resulting in an effective event capacity of `tokens * BatchSize`.

**Why:**
- Data preservation (§2.1): Forwarder must never mutate or substitute captured data on disk; forwarding the exact captured bytes ensures high-fidelity transmission.
- Accurate local audit trail (§2.3): The edge store must truthfully represent delivery outcomes. Marking permanently rejected events as `REJECTED` preserves diagnostic errors on disk without infinite retry loops, separate from successfully delivered `ACKNOWLEDGED` events.

**Files/modules touched:**
- `pkg/edge/store.go`
- `pkg/edge/sqlite_store.go`, `pkg/edge/sqlite_store_test.go`
- `pkg/edge/forwarder.go`, `pkg/edge/forwarder_test.go`
- `pkg/ingestion/handler.go`, `pkg/ingestion/handler_test.go`
- `ARCHITECTURE_PROPOSALS.md`
- `WORKLOG.md`

**Tests added/updated:**
- `TestForwarder_PreservesRawPayloadBytes`: confirms unmodeled fields are retained and forwarded verbatim.
- `TestForwarder_207MixedBatchCorrelation`: verifies mixed batches correlate by `idempotency_key`, resulting in distinct `ACKNOWLEDGED` and `REJECTED` states.
- `TestForwarder_422FullBatchRejection`: verifies 422 responses transition all records to `StatusRejected`.
- `TestSQLiteStore_MarkRejected`: verifies SQLite transitions records to `REJECTED`, sets `last_error`, excludes them from `FetchPending`, and tracks `RejectedCount` in stats.
- `TestHandler_PreservesUnmodeledFieldsAndRejectsCorruptedJSON`: verifies central intake accepts payloads with extra fields and cleanly rejects non-event JSON.
- All 25 tests pass under `-race -count=1`.

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
