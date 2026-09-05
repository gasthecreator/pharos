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

#### [2026-09-05] Slice 14: Multi-Region Cassandra + Kafka (Simulated)

**Status:** Resolved: Approved (Claude Code, 2026-09-05).

**What in PLAN.md this touches:** §2.1, §2.2, §2.4, §5, Phase 2 Slice 14.

**A constraint PLAN.md didn't anticipate, found before writing any config:
this host has 8GB RAM total, Docker Desktop's VM is capped at ~6.28GB, and
the *existing* single-DC 3-node Cassandra + 3-broker Kafka topology was
already using ~5GB of that (`docker stats`: Cassandra nodes 870MB-1.05GB
each with no Kafka heap cap set at all, broker RSS 656-900MB each purely
from JVM defaults). Naively *adding* a second 3-node Cassandra DC, a second
3-broker Kafka cluster, and MirrorMaker 2 on top of that would need well
over 10GB and reliably OOM the Docker VM. This doesn't change the decision
to build the real thing — it changes how. Two mitigations, both applied,
neither a scope cut:**

1. **Aggressive, explicit JVM heap tuning on every node, tighter than Slice
   7's original defaults.** Cassandra's existing 256M heap is lowered further
   where the 2-DC topology needs the headroom; Kafka brokers get an explicit
   `KAFKA_HEAP_OPTS` for the first time (Slice 7 never set one, which is the
   entire reason broker RSS was 650-900MB against no configured ceiling).
   Six small, disciplined JVMs fit where three undisciplined ones barely did.
2. **The topology replaces the existing containers, it doesn't run alongside
   them.** `docker-compose.yml`'s single-DC Cassandra/Kafka service
   definitions are edited in place into the 2-DC/2-cluster definitions --
   there is no window where both the old 3+3 and the new 6+6+MM2 topology
   need to be up at once. The realistic total (six tuned Cassandra nodes, six
   tuned Kafka brokers, one MirrorMaker 2 process, existing
   Prometheus/Grafana) comes out lower than the old topology's *untuned*
   footprint, not higher.

**Fresh bootstrap, not a live single-DC-to-two-DC migration.** Converting a
running keyspace's replication strategy from `SimpleStrategy` to
`NetworkTopologyStrategy` against an actually-expanding cluster is a real,
delicate operational procedure (`ALTER KEYSPACE`, then `nodetool rebuild`/
`repair` per new-DC node, done carefully to avoid inconsistent reads
mid-stream) -- appropriate to test as its own concern, but not what this
slice asks for, and not something to smuggle in unrehearsed. Nothing in this
project's Cassandra volumes is production data; it's this session's own
accumulated test fixtures. Tearing down the old volumes and bootstrapping
the 2-DC topology fresh proves exactly what this slice is actually about --
that the *configuration and application logic* are multi-region-correct --
without conflating it with a live-migration procedure PLAN.md never scoped.

**Cassandra: `NetworkTopologyStrategy` across `dc-us`/`dc-eu`, 3 nodes each.**
The official Cassandra image already honors `CASSANDRA_DC`/`CASSANDRA_RACK`
env vars to populate `cassandra-rackdc.properties` under
`GossipingPropertyFileSnitch` (already this project's snitch since Slice 7)
-- no hand-written config files needed, just distinct values per container.
Keyspace replication becomes `{'class': 'NetworkTopologyStrategy', 'dc-us':
3, 'dc-eu': 3}`. `LOCAL_QUORUM` was already this project's consistency level
specifically because it's the correct choice once real datacenters exist
(Slice 7's own reasoning) -- this is what that decision was made for.

**A real correctness gap found while tracing this, not just infrastructure:**
none of the three places that construct a `gocql.ClusterConfig` (`internal/
dedup/cassandra_store.go`, `internal/consumer/canonical_store.go`, `internal/
query/service.go`) set a DC-aware host selection policy. In a single-DC
cluster this was invisible -- there's only one DC, so "local" is unambiguous
regardless of which host gocql's default round-robin policy happens to pick
as coordinator. Across two real DCs, `LOCAL_QUORUM`'s meaning is decided by
whichever node ends up coordinating the request, so an undirected policy
could coordinate a `dc-us` service's write through a `dc-eu` node, silently
satisfying quorum against the *wrong* DC's replicas. Fixed by adding
`cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.
DCAwareRoundRobinPolicy(cfg.LocalDC))` in all three places, with a new
`LocalDC` config field (default `dc-us`, since Central Ingestion/consumer/
query are all simulated as deployed in the primary region for this exercise).
This is exactly the kind of gap the "convert to NetworkTopologyStrategy" line
in PLAN.md's original Slice 14 scoping was implicitly asking to be found.

**Kafka: existing cluster stays cluster A (`dc-us`), a genuinely independent
second cluster becomes cluster B (`dc-eu`), MirrorMaker 2 replicates both
topics A→B.** Cluster A's three existing brokers get `broker.rack` values
identifying them as `dc-us`; cluster B is three *new* broker containers with
their own KRaft controller quorum and a distinct `CLUSTER_ID` (a shared
`CLUSTER_ID` would make them the same cluster, not two independent ones) and
their own `broker.rack` values for `dc-eu`. MirrorMaker 2 (via
`connect-mirror-maker.sh`, already bundled in the `apache/kafka` image this
project already depends on -- no new image) replicates `pharos.events.
adverse` and `pharos.events.dlq` one-directionally A→B, matching the
standard "primary region replicates to a DR region" shape rather than a
bidirectional setup, which would need MM2's loop-prevention/topic-renaming
machinery for a property this project doesn't need.

**Simulated WAN: `tc netem` for steady-state latency, the same mechanism for
the full-partition fault-injection test.** `cap_add: [NET_ADMIN]` on every
Cassandra and Kafka container makes `tc qdisc add dev eth0 root netem delay
<80-150ms>` runnable via `docker exec` against the containers on the "other"
side of a simulated link. The full-partition scenario uses the same `tc`
mechanism (`netem loss 100%`) rather than a second, different mechanism
(e.g. Docker network disconnect) -- one tool for both the steady-state and
fault-injection cases is simpler to reason about and keeps the test close to
what a real operator would reach for. This is standard, recognized practice
for testing multi-region behavior without real geographic infrastructure,
exactly as PLAN.md's own framing says -- not a shortcut.

**Deliverable, unchanged from PLAN.md:** the full existing test suite passes
against the 2-region topology without modification to the tests themselves,
plus a new fault-injection test proving no data loss or duplication across a
`tc`-induced regional partition -- reusing this project's existing outbox/
idempotency guarantees (§2.1, §2.2), which don't care *why* a publish or a
Cassandra write failed, only that it failed and must be safely retryable.

**Impact if approved:**
- `docker-compose.yml`: Cassandra section rewritten for 6 nodes/2 DCs with
  tuned heaps and `cap_add: [NET_ADMIN]`; Kafka section rewritten for 2
  independent 3-broker clusters with `broker.rack`, tuned heaps, distinct
  `CLUSTER_ID`s, `cap_add: [NET_ADMIN]`; new MirrorMaker 2 service; existing
  data volumes torn down and recreated fresh under the new topology.
- `internal/dedup/cassandra_store.go`, `internal/consumer/canonical_store.go`:
  keyspace bootstrap switches to `NetworkTopologyStrategy`; new `LocalDC`
  config field; DC-aware gocql host selection policy.
- `internal/query/service.go`: same DC-aware host selection policy fix
  (doesn't bootstrap the keyspace, but still needs correct LOCAL_QUORUM
  routing for reads).
- New MirrorMaker 2 properties file.
- New fault-injection test for the regional-partition scenario.
- No wire-format changes -- this slice is topology and driver
  configuration, not data model.

**Addendum [2026-09-05], written after actually bringing the topology up:**
the 3+3 Cassandra plan above genuinely OOM-killed on this host --
`docker inspect` confirmed `OOMKilled: true` on `pharos-cassandra-1` with all
6 Cassandra nodes + 6 Kafka brokers + MirrorMaker 2 running together, even
after the heap tuning already described. Heap turned out not to be the
dominant lever: dropping Cassandra's heap further, from 192M to 128M, barely
moved real RSS (~700-950MB per node at *both* settings) -- the floor is
JVM+off-heap baseline overhead on this image, not the configured heap.
Adaptation actually shipped: **dc-eu runs 2 nodes (RF=2) instead of 3**, for
both Cassandra and Kafka cluster B. dc-us (and Kafka cluster A) keep their
full 3 nodes/RF=3 unchanged -- that's the one fault-tolerance property this
project's application logic and tests actually exercise (`LocalDC` defaults
to `dc-us` everywhere; nothing ever coordinates `LOCAL_QUORUM`/ISR against
dc-eu directly). Verified real footprint with all 13 containers up and the
full suite run twice: ~4.8-5.1GB, comfortably under the ~6.28GB Docker VM
ceiling.

Two more things found only by actually running this, not by planning it:
1. **A real `tc` priomap bug**, caught by hand before it reached the
   automated test: a `prio` qdisc's `priomap` decides which band *unmatched*
   traffic defaults to, and an all-`2`s priomap accidentally routed default
   (same-DC) traffic into the *same* band as the explicit u32-filtered
   (cross-DC) traffic -- silently breaking intra-DC gossip, not just the
   intended inter-DC link. Fixed with an all-`0`s priomap so default traffic
   stays in an untouched band, reserving a separate band exclusively for the
   u32-matched peer IPs.
2. **Paxos LWTs under heavy concurrent contention got measurably more
   sensitive, independent of the partition test.** `TestCassandraOutboxStore_RealIntegration`'s
   10-goroutine race sub-test occasionally saw zero clean winners (never
   more than one -- no correctness violation) -- once during the partition
   test specifically, and again on a later plain full-suite run against the
   healthy, unpartitioned cluster, ruling out the partition itself as the
   cause. Root cause found by comparing repeated full-suite runs: 128M heap
   was lean enough to pass CI once but not reliably -- a *different*
   operation (a plain, non-Paxos `MarkPublished` update) also hit an
   operation timeout on a later run, pointing at GC pauses under `-race`'s
   own overhead plus concurrent test-suite load, not anything specific to
   Paxos contention or the regional partition. Fixed two ways: (a) bumped
   Cassandra's heap from 128M to 176M -- still comfortably under budget,
   but enough headroom that two full-suite runs in a row came back clean;
   (b) made the race sub-test itself retry (fresh key each time, up to 3
   attempts) on exactly 0 winners specifically, since that outcome means
   "every attempt errored out," not "two attempts both won" -- the actual
   correctness property this test exists to catch (`>1` winners) still
   fails immediately, on the first occurrence, no retry. Belt-and-suspenders
   deliberately: the heap fix addresses the actual cause, the test-retry
   fix keeps the suite from being sensitive to whatever *next* transient
   slowdown this environment produces.
3. **A third real bug, and a real host-level lesson, both found while
   re-verifying after the 176M heap bump.** Bringing everything up in one
   `docker compose up -d` starts MirrorMaker 2 concurrently with Cassandra
   and Kafka's own startup -- before any topic exists for it to replicate,
   MM2 busy-loops discovery/retry, piling CPU contention on top of 10 other
   JVMs' own startup work. This produced two more genuine OOM kills (`docker
   inspect`: `OOMKilled: true`) even at 176M heap, in a configuration that
   had looked stable moments earlier in an idle snapshot -- CPU starvation
   delays GC, letting RSS balloon before it can be reclaimed. Fixed by
   sequencing MirrorMaker 2 strictly last: Cassandra + Kafka +
   Prometheus/Grafana come up and settle first, then migrations and topics
   are provisioned, and only then does `mirrormaker` start (applied to both
   the local verification process and `.github/workflows/ci.yml`, which had
   the identical single-shot `docker compose up -d` ordering issue). Separately,
   repeated bring-up/tear-down cycles over several hours of this same
   verification eventually degraded the local Docker Desktop VM itself into
   a genuinely unresponsive state (containers reported "zombie and can not
   be killed," host load average briefly hit 25 on an 8-core machine) --
   unrelated to anything in this project's own config, but real enough to
   need a full Docker Desktop restart (done with the user's explicit
   go-ahead) before verification could complete. Included here because it's
   exactly the kind of thing a future session repeating this bring-up cycle
   many times in one sitting should recognize rather than mistake for a
   fourth application-level bug.

---

#### [2026-09-05] Slice 13: Consumer Crash/Restart Watermark Continuity

**Status:** Resolved: Approved (Claude Code, 2026-09-05).

**What in PLAN.md this touches:** §2.4 (event-time watermarking), Phase 2
Slice 13.

**What PLAN.md's own wording assumes, and why that assumption doesn't hold
yet.** Slice 13 is phrased as "prove the watermark cannot regress... the
same monotonic-guard principle from §2.4, now tested against process death
specifically" — as if the guarantee already exists and this slice is purely
a testing exercise. Tracing `WatermarkTracker` (`internal/consumer/watermark.go`)
before writing that test found this isn't actually true: `previousEmitted`
(the field the strict monotonic guard in `advanceWatermarkLocked` actually
compares against) along with `partitionHighWatermark` and
`partitionLastActivity` are plain in-process fields on a struct that
`cmd/pharos-consumer/main.go` constructs fresh via `consumer.NewWatermarkTracker`
on every process start. A real crash and restart — not just a partition
reawakening within a *live* process, which is what the existing tests
actually cover — throws all of that away. `previousEmitted` comes back
zero-valued, so the guard's own escape hatch (`wt.previousEmitted.IsZero()
|| candidate.After(wt.previousEmitted)`) is trivially satisfied by whatever
the newly-replayed messages produce, even if that's *earlier* than what was
already reported pre-crash — which is entirely plausible, since Kafka
resumes each partition from its last *committed* offset, not from
"wherever the in-memory watermark had gotten to." The externally observable
symptom is real, not theoretical: `pharos_consumer_watermark_seconds`
(`cmd/pharos-consumer/main.go`'s status ticker) would visibly regress on
Prometheus/Grafana across a restart. Writing a test that merely confirms
today's actual behavior would either have to assert the regression (turning
a bug into a "spec") or be quietly satisfied by a test that never exercises
enough real replay to surface it — neither is honest. This gets fixed as
part of this slice, not deferred, per the standing instruction to build the
correct thing rather than the one that's easiest to make green.

**Decision: persist a watermark checkpoint to Cassandra, keyed by consumer
group, restored explicitly at startup before the engine consumes anything.**
A new `consumer_watermark_checkpoints` table (single row per `group_id`,
upserted — this is operational recovery state, not the clinical
data-of-record `PLAN.md`'s retention framing is about, same distinction
Slice 11 already drew for `event_outbox`/`pending_outbox`) stores
`previous_emitted timestamp`, `partition_high_watermark map<int, timestamp>`,
and `partition_last_activity map<int, timestamp>` — Cassandra's native
collection column types are a direct fit for the tracker's own in-memory
shape, no serialization format to invent. `WatermarkTracker` gains
`Snapshot()` (returns a copy of this state for persisting) and `Restore(cp)`
(seeds the three fields directly, bypassing the guarded `advanceWatermarkLocked`
path entirely — this is initialization, not a live event arriving).
`cmd/pharos-consumer/main.go` loads the checkpoint for its configured
`--kafka-group` before starting the engine and calls `Restore` if one
exists; a new periodic goroutine (10s interval, tighter than the existing
30s status-log ticker since this is now load-bearing correctness state, not
just an operational log line) calls `Snapshot()` and saves it. Restoring
all three fields (not just `previous_emitted`) matters for more than just
the monotonic floor: `computeCandidateWatermarkLocked`'s active/idle
partition classification depends on `partitionLastActivity`, and a restart
that dropped only the floor while forgetting per-partition state could
still compute a *wrong* (if not literally regressed) forward watermark
immediately after restart.

**Why periodic persistence, not persistence on every message.** Mirrors the
same reasoning as Slice 12's backup interval: writing the checkpoint on
every single processed message would mean an extra Cassandra write in the
hot path of every event, for state that only matters at the (rare) moment
of a crash. A periodic snapshot bounds the "what could still regress"
window to whatever advanced in the interval between the last checkpoint and
the crash — an explicit, bounded exposure, not an eliminated one, exactly
like Slice 12's disk-backup interval. 10s is deliberately tighter than
Slice 12's 5-minute default because this state guards a correctness
property that's externally visible on every scrape, not just a
disaster-recovery snapshot consulted rarely.

**Why not derive the watermark from Cassandra's own canonical tables
instead of a dedicated checkpoint table** (e.g. `MAX(event_time)` per
partition from `canonical_events`): rejected. There's no efficient query
for that — `canonical_events` is keyed by `idempotency_key`, and the
partition-clustered tables are keyed by `study_id`/`site_id`, not by Kafka
partition, so answering "what was the high-watermark per *Kafka partition*"
would mean a full-table scan or `ALLOW FILTERING`, both already established
anti-patterns in this project (Slice 11 rejected the same shape of idea for
its own archive index). A dedicated small checkpoint table is the same
choice this project already made for `known_studies`/`known_sites` (Slice
11) and `pending_outbox` before that: purpose-built tracking state instead
of querying the data-of-record tables for something they're not shaped to
answer efficiently.

**The residual exposure window, stated plainly:** a crash within the 10s
window after the last successful checkpoint can still lose up to that
window's worth of watermark progress — the restored floor will be whatever
was last checkpointed, not the true pre-crash instant. This is bounded, not
eliminated, exactly like Slice 12's disk-backup window; tightening the
interval trades more Cassandra write volume for a smaller window.

**Impact if approved:**
- `internal/consumer/canonical_store.go`: `CanonicalStore` interface gains
  `SaveWatermarkCheckpoint`/`LoadWatermarkCheckpoint`; new
  `consumer_watermark_checkpoints` table in `EnsureSchema`; implementations
  on both `CassandraCanonicalStore` and `MemoryCanonicalStore`.
- `internal/consumer/watermark.go`: new `WatermarkCheckpoint` type,
  `Snapshot()`/`Restore()` methods on `WatermarkTracker`.
- `cmd/pharos-consumer/main.go`: load-and-restore at startup, periodic
  checkpoint-save goroutine, save-on-graceful-shutdown too (minimizes the
  exposure window on the one path where it's actually avoidable).
- No wire-format or Kafka changes — this is purely consumer-side recovery
  state.

---

#### [2026-09-05] Slice 12: Edge Collector Durability Hardening

**Status:** Resolved: Approved (Claude Code, 2026-09-05).

**What in PLAN.md this touches:** §2.1 (edge durability), Phase 2 Slice 12.

**The three options PLAN.md named, weighed against each other:**

1. **Periodic backup to a second local path or removable media.** Chosen.
2. **A lightweight local replica process.** Rejected: this means running a
   *second daemon per site*, needing its own failure handling, its own
   monitoring, and (if it's ever meant to actually take over on primary
   failure) a real leader-election/failover story — a lot of new
   operational surface for a single-machine resiliency problem, at a trial
   site that this project's own §2.1 reasoning already assumes is
   resource-constrained and likely unstaffed by anyone who'd notice a
   second process misbehaving.
3. **Document and accept the residual exposure window.** Rejected as the
   *sole* answer, but its instinct is right and shows up in what's actually
   built: periodic backup doesn't eliminate the exposure window either, it
   only bounds it to "whatever changed since the last backup interval" —
   that residual window is stated explicitly below, not hidden.

**Decision: periodic `VACUUM INTO`, not raw file copying.** SQLite has a
built-in mechanism for exactly this — `VACUUM INTO '<path>'` produces a
complete, transactionally-consistent snapshot of the database even while
it's actively being written to, which a naive `cp` of the `.db` file next
to a live WAL cannot safely guarantee (a raw copy mid-write can capture an
inconsistent state). `modernc.org/sqlite` (already this project's only
SQLite dependency) implements the full SQLite engine, so this is a
standard SQL statement, not a new dependency. A background goroutine in
`pharos-edge` runs it on a configurable interval (default 5 minutes) to a
configurable second path — same mechanism whether that path is another
local directory or removable media; it's just a file path either way.

**The other half, which makes the backup actually useful: restore-on-missing-primary.**
A backup nobody ever reads back is not a durability story, just a second
place to lose. `SQLiteStore`'s constructor now checks: if the primary
database file doesn't exist *and* a backup file exists at the configured
path, copy the backup into place before opening it, restoring transparently.
If neither exists, this is genuinely a fresh instance and Slice 8's
epoch-based key resilience already handles that safely. If the primary
*does* exist, the backup is irrelevant and ignored — there's nothing to
restore.

**The residual exposure window, stated plainly:** anything captured after
the last successful backup and lost before the next one runs is still at
risk if the primary disk fails in that window. Bounded by the backup
interval (default 5 minutes means at most ~5 minutes of exposure), not
eliminated. This is an explicit, accepted tradeoff, not an oversight — a
tighter interval trades more disk I/O for a smaller window, and is
configurable per deployment.

**Alternatives considered:** continuous (not periodic) replication via
tailing the WAL file directly. Rejected: `VACUUM INTO` already gives a
correct, complete snapshot with a single SQL statement and no new failure
modes of its own; hand-rolling WAL-tailing would need to reimplement
SQLite's own crash-consistency guarantees to be safe, for a marginal
reduction in the exposure window over just shortening the backup interval.

**Impact if approved:**
- `internal/edge/sqlite_store.go`: restore-on-missing-primary in the
  constructor, a new `Backup(ctx, path string) error` method wrapping
  `VACUUM INTO`.
- `cmd/pharos-edge/main.go`: new `--backup-path`/`--backup-interval` flags
  and a background goroutine calling `Backup` on that interval.
- No changes to the wire format, Central Ingestion, or any Cassandra
  schema — this is purely an edge-local durability improvement.

---

#### [2026-09-05] Slice 11: Data Retention & Lifecycle (Tiered Archival)

**Status:** Resolved: Approved (Claude Code, 2026-09-05).

**What in PLAN.md this touches:** §2.4 (canonical query tables), §2.3
(DLQ), Phase 2 Slice 11.

**What's actually being tiered, and what isn't:** the canonical tables
(`canonical_events`, `events_by_study`, `events_by_site`) and the DLQ
tables (`dead_letter_events`, `dead_letter_events_by_site`) — this is the
clinical safety data this project's whole 21 CFR Part 11 framing is about,
and it's what "retention" actually means here: never delete it, move it to
cheaper storage once it's no longer active. **`event_outbox` and
`pending_outbox` are deliberately excluded.** They're operational
claim/lease bookkeeping for exactly-once delivery (§2.2), not the
data-of-record — once an event is durably `PUBLISHED` and has flowed
through to the canonical tables, the outbox row's job is done. Conflating
"archive the clinical record" with "prune bookkeeping metadata" would
muddy two genuinely different concerns with different lifetimes and
different consequences if handled wrong (losing a canonical record is a
compliance problem; losing a stale outbox claim row after enough time has
passed is not). Outbox pruning is real future work, but it's a retention
*policy* question (how long is "long enough past the lease timeout to be
safe"), not this slice's tiering problem — noted as a follow-up, not
silently dropped.

**Decision: file-based cold tier, partitioned the same way the hot tier
already is.** Archived rows are exported as gzip-compressed JSON Lines,
one file per (partition key, month) — `archive/by_study/<study_id>/<YYYY-MM>.jsonl.gz`,
`archive/by_site/<site_id>/<YYYY-MM>.jsonl.gz`,
`archive/dlq_by_site/<site_id>/<YYYY-MM>.jsonl.gz` — deliberately mirroring
this project's own established partition-key-first modeling principle
(§2.4, §5.1) instead of inventing a different physical layout for cold
storage than the one already proven correct for hot storage. Local disk
satisfies the zero-cloud-spend constraint the same way it already does
for everything else in this project.

**Why no secondary index database for the archive tier:** the obvious
alternative — a small SQLite (or Cassandra) index mapping
idempotency_key → archive file/offset for O(1) point lookups — was
considered and rejected. It's a second source of truth that has to stay
perfectly consistent with the archive files themselves, for a lookup path
(a point query against *already-cold, already-inactive* data) that isn't
performance-critical by definition — if it were still being looked up
frequently, it wouldn't be a candidate for archival in the first place.
Point lookups (`GetEvent`, `GetDLQEvent`) against archived data fall back
to scanning that key's site's archive files across all months only *after*
a hot-tier miss — slower than a hot lookup, which is an honest and
acceptable tradeoff for cold data, not a design gap.

**Query-layer fallback, by query shape:**
- `GetEventsByStudy`/`GetEventsBySite`/DLQ site listing (all already
  time/range-aware or naturally bounded): always also consult the
  relevant archive files for the requested range, merge with hot results,
  sort consistently. These are exactly the query shapes where "some of
  what you're asking for might have aged into cold storage" is the normal
  case, not an edge case.
- `GetEvent`/`GetDLQEvent` (point lookups): archive fallback only fires on
  a hot-tier miss, since the overwhelmingly common case (recent data)
  should never pay the cost of touching the cold tier at all.

**Retention threshold:** 90 days of "active" data stays in Cassandra by
default, configurable via a flag on the archival job — chosen as a
reasonable default for what's operationally "recent," not a regulatory
requirement (the regulatory requirement this project cares about is the
opposite: don't ever delete, which the design already guarantees by
tiering instead of deleting).

**The archival job itself:** `pharos-cli archive run [--older-than 90d]
[--archive-dir <path>] [--dry-run]` — a CLI subcommand, not a new binary,
consistent with how Slice 10 added `dlq replay` to the existing CLI rather
than standing up a new deployment artifact for one more operational
concern. Scans the hot tables for rows older than the threshold, exports
them to the partitioned archive files, and only deletes the exported rows
from Cassandra after the export is confirmed written and flushed to disk —
never delete-then-write.

**Alternatives considered:**
- **Cassandra TTL** (native per-row expiration): rejected outright — TTL
  *deletes* data, it doesn't tier it. Using it here would violate the
  actual retention requirement this slice exists to satisfy.
- **A second Cassandra table/keyspace as the "cold" tier** instead of
  local files: rejected as unnecessary complexity — it's still hot,
  always-on infrastructure either way, and doesn't get anything the file
  approach doesn't already provide for this project's actual scale and
  zero-cloud-spend constraint.

**Impact if approved:**
- New `internal/archive` package: writer (export + verified delete),
  reader (partition-aware file scan for a given key/range).
- `internal/query.CassandraService` gains an optional archive reader,
  falling back per the rules above.
- `cmd/pharos-cli`: new `archive run` subcommand.
- No Cassandra schema changes — this slice moves rows between tiers, it
  doesn't change what a row looks like.

---

#### [2026-09-04] Slice 10: DLQ Replay & Reprocessing

**Status:** Resolved: Approved (Claude Code, 2026-09-04).

**What in PLAN.md this touches:** §2.3 (DLQ), Phase 2 Slice 10.

**What I'm proposing:** a new Central Ingestion endpoint,
`POST /api/v1/dlq/{key}/replay`, that fetches a DLQ record's stored raw
payload and resubmits it through `processOneEvent` — the exact same
validate → claim → publish function `HandleEvents` already uses for a
fresh submission, extracted into its own method specifically so both
callers share one implementation rather than two copies of
fault-injection-tested logic drifting apart over time. `pharos-cli dlq
replay <key>` (and `--all --site X`) is a thin HTTP client against this
endpoint — it does not read or write Cassandra directly for the mutation
itself, only for `--all`'s listing step (via the already-read-only
`query.Service`).

**Why server-side, not CLI-driven Cassandra writes:** every outbox/DLQ
mutation in this project has been Central Ingestion's job since Slice 2 —
`pharos-cli` has only ever been a read path. Having the CLI flip a DLQ
record's status directly would be the first exception to that boundary.
Routing replay through an HTTP endpoint keeps it intact: Central Ingestion
still owns every write, the CLI is still purely a client of it.

**On success:** `MarkDLQReplayed` (`StatusReplayed`, DLQ-only — never a
valid `event_outbox` status) transitions the *original* record from
PUBLISHED to REPLAYED via the same CAS-guarded UPDATE pattern
`MarkDLQPublished` already established, so a still-in-flight or
already-replayed record can't be replayed a second time. The row is never
deleted or overwritten beyond status/`replayed_at` — `rejection_reason`
and the original stored payload* stay exactly as they were, keeping the
rejection part of the audit trail rather than erasing it.

*Caveat found during live verification, not by design: replay always
resubmits *whatever is currently stored* in `payload`. If a payload is
corrected out-of-band before replay (the only way replay can ever
actually succeed, short of a validation rule itself changing), the stored
`payload` column reflects the corrected version afterward, while
`rejection_reason` still describes the *original* failure — this is
correct and intentional (the reason a payload was rejected is a fact
about that submission, not something replay should rewrite), but worth
being explicit about for anyone reading a replayed record later.

**On failure:** the DLQ record is left completely untouched.
`processOneEvent`'s `InsertDLQClaim` call against an already-existing
PUBLISHED key is a no-op by construction (`IF NOT EXISTS` fails, and the
existing-status branch returns `Acquired: false` for anything that isn't
a fresh, unclaimed key) — so a failed replay attempt structurally cannot
corrupt or duplicate the original entry, without needing special-case
logic for "this is a replay, don't touch the DLQ." Verified live, not
just in unit tests: same-payload replay of a genuinely-still-invalid
event returned the identical rejection reason and left the record
untouched.

**A real migration gap found and fixed while implementing this:**
`CassandraOutboxStore.EnsureSchema()` uses `CREATE TABLE IF NOT EXISTS`,
which is a no-op against the already-existing `dead_letter_events` /
`dead_letter_events_by_site` tables — meaning the new `replayed_at`
column would never have been added to an already-bootstrapped keyspace
automatically, breaking this project's "bootstrapped automatically, no
manual migration step" guarantee. Fixed with the same idempotent
column-check pattern already used for SQLite in Slice 8
(`internal/edge/sqlite_store.go`'s `hasColumn`), now mirrored on the
Cassandra side via `system_schema.columns` — checked and added on every
`EnsureSchema()` call, safe to run on every startup.

**A second, unrelated bug found while wiring up `pharos-cli`'s new
`--central-url` flag:** `globalFlags.Parse()` was never actually called
anywhere in `cmd/pharos-cli/main.go` — `--hosts`/`--port`/`--keyspace`
were registered on a `flag.FlagSet` but silently never took effect
regardless of position, since a separate manual loop (handling only
`--json`/`--memory`) was the actual parsing mechanism in use. Fixed by
generalizing that manual loop to handle all global flags in any
position — preserving the existing (already-working) behavior that
`--json`/`--memory` can appear anywhere in argv, rather than narrowing to
stdlib `flag.Parse()`'s stricter "flags must precede positional args"
rule, which would have been a behavior regression.

**Alternatives considered:** having `pharos-cli` itself fetch the raw
payload via `query.Service` and POST it to `/api/v1/events` directly (the
existing batch endpoint), rather than adding a dedicated replay endpoint.
Rejected: that path has no way to also mark the *original* DLQ record
REPLAYED without a second, separate write the CLI would have to perform
directly against Cassandra — reintroducing exactly the write-boundary
violation the chosen design avoids.

**Impact if approved:**
- `internal/dedup`: `StatusReplayed`, `DLQRecord.ReplayedAt`,
  `OutboxStore.MarkDLQReplayed` (Cassandra + in-memory), `EnsureSchema`
  migration fix, `GetDLQRecord`/DLQ SELECT queries extended.
- `internal/query`: `DLQRecord.ReplayedAt` plumbed through all three DLQ
  query paths.
- `internal/ingestion`: `HandleEvents`'s per-event loop extracted into
  `processOneEvent` (verified behavior-identical against the full existing
  test suite before anything new was added on top), new
  `POST /api/v1/dlq/{key}/replay` route and handler.
- `cmd/pharos-cli`: new `dlq replay` subcommand, new `--central-url` flag,
  and the `--hosts`/`--port`/`--keyspace` parsing fix above.
- New migration `migrations/004_dlq_replay.cql` (documentation of the
  schema state; the actual bootstrap is `EnsureSchema`'s migration check).

---

#### [2026-09-04] Slice 9: Wire-Format Schema Versioning

**Status:** Resolved: Approved (Claude Code, 2026-09-04).

**What in PLAN.md this touches:** §2.3 (FHIR validation), Phase 2 Slice 9.

**What I'm proposing:** an explicit `schemaVersion int` field on
`model.AdverseEvent` (`json:"schemaVersion,omitempty"`), stamped by the
edge collector at capture time (mirroring exactly how it already stamps
the idempotency key) rather than left to whatever a submitting client
happens to send. `Validate()` becomes a dispatch: look up a validator
function by version in a `map[int]func(*AdverseEvent) error`, defaulting
an absent/zero field to `SchemaVersionV1` for full backward compatibility
with every event ever captured before this ships (nothing gets
retroactively invalidated), and returning a specific, typed
`ErrUnsupportedSchemaVersion` for anything not in the map. That error
flows through the *existing* validation-rejection path into the DLQ
unchanged — an unrecognized version was already going to be a rejection
via `ev.Validate()`'s error return; this doesn't add a new code path, it
adds a new reason a rejection can happen.

**Why a dispatch map now, when there's only one version:** the point of
this slice is to not need a second, more invasive change the day a real
v2 shows up. Adding `SchemaVersionV2` and its validator later is then a
pure addition to the map — v1's validator, tests, and behavior stay
completely untouched. Building the dispatch shape today, even with one
entry, is the actual hardening; a bare `if version != 1 { reject }` would
still work today but would force a larger refactor later instead of an
addition.

**Why the edge stamps it, not left implicit:** the whole reason this
matters is edge binaries at different sites getting updated at different
times over years. If the field were only set by whichever validator
happens to run centrally, every site's events would silently claim
whatever the *current* Central Ingestion binary's default is, which
defeats the purpose — the version has to reflect what the *submitting*
binary actually understood at capture time.

**Explicitly out of scope:** a second Cassandra/DLQ column for
`schema_version`. The raw wire payload is already preserved byte-for-byte
in both the canonical tables and the DLQ (`payload` column, confirmed in
`internal/dedup/store.go`), so the version is always recoverable from
there for anyone inspecting a record — a dedicated column would duplicate
data already durably stored elsewhere for no operational gain, which is
the kind of complexity that doesn't buy anything and shouldn't be added
just because it's available to add.

**Alternatives considered:** a bare version check without a dispatch
table (`if ev.SchemaVersion > 1 { reject }`) — rejected per the reasoning
above, since it would need to become a dispatch table the moment a real
v2 exists anyway, at which point the "simple" version was never actually
simpler, just deferred.

**Impact if approved:**
- `internal/model/adverse_event.go`: new field, `Validate()` restructured
  into a version dispatch, `ErrUnsupportedSchemaVersion` added.
- `internal/edge/sqlite_store.go`: `Enqueue` stamps
  `model.CurrentSchemaVersion` alongside the existing idempotency-key
  stamp, only if not already set (matching the existing
  location-reference-defaulting pattern in the same function).
- No changes to Cassandra schema, the DLQ mechanism, or any downstream
  consumer — an unsupported-version rejection is just a new *reason* for
  a code path that already exists.

---

#### [2026-08-31] Slice 8: Idempotency Key Resilience Across Edge Instance Loss

**Status:** Resolved: Approved (Claude Code, 2026-08-31). Authored directly
by Claude rather than proposed by Gemini first, since the design fell out
of investigating the bug itself — presented here for the same review
rigor as any other proposal, not implemented unreviewed.

**What in PLAN.md this touches:** §2.2 (Exactly-once processing semantics),
§2.1 (edge collector durability), Phase 2 Slice 8.

**The bug, precisely:** the idempotency key is `site_id:local_seq`
(`internal/model/idempotency.go`), and `local_seq` is allocated purely by
`internal/edge/sqlite_store.go`'s `site_sequence` table, starting at 1 for
any site_id that table has never seen. A trial site's disk failing and
being replaced with the same `--site-id` flag produces a brand-new, empty
`site_sequence` table — the next event that site submits gets
`local_seq=1` again, identical to the very first event that site *ever*
submitted, months or years earlier. Central Ingestion's outbox
(`internal/dedup`) sees that idempotency key already has a `PUBLISHED`
claim from the original submission and — correctly, by its own logic —
treats the new event as an already-processed duplicate. It is never
published to Kafka. **A genuinely new adverse event is silently dropped**,
not from a bug in the claim/lease mechanism, but because that mechanism
rests on an assumption (`local_seq` is monotonic per site, forever) that a
hardware replacement quietly violates. This was hit empirically during
Slice 6 verification and initially mistaken for a test artifact.

**What I'm proposing:** encode a per-instance epoch into the numeric value
of `local_seq` itself, computed entirely inside `SQLiteStore.Enqueue`, with
**zero changes to `IdempotencyKey`, `ParseIdempotencyKey`, the wire format,
any Cassandra schema, or any downstream consumer** (`internal/ingestion`,
`internal/consumer`, `internal/query`, `pharos-cli` all already only ever
read `LocalSeq` off a parsed key as an opaque `uint64` — none of them
interpret its internal structure).

Concretely:
- Add one column to the edge's local `site_sequence` table:
  `instance_epoch INTEGER NOT NULL DEFAULT 0`.
- The existing allocation query gains one bound parameter, and is
  otherwise unchanged:
  ```sql
  INSERT INTO site_sequence (site_id, last_seq, instance_epoch) VALUES (?, 1, ?)
  ON CONFLICT(site_id) DO UPDATE SET last_seq = last_seq + 1;
  ```
  The `instance_epoch` value passed is only ever *used* on the `INSERT`
  branch — the `ON CONFLICT` branch's `SET` clause doesn't mention it, so
  it's left untouched on every subsequent call. This means the exact
  moment a site_id gets a row in `site_sequence` for the *first time this
  local database file has ever seen it* is the exact moment a fresh epoch
  gets minted — which is precisely the signal needed, with no separate
  "is this a fresh file" bookkeeping required.
- That stamped `instance_epoch` is minutes since a fixed project epoch
  (`2026-01-01T00:00:00Z`), not raw Unix seconds — computed once, at
  `INSERT` time, as `(time.Now().UTC().Unix() - projectEpochUnix) / 60`.
- The `local_seq` value actually stamped onto the outgoing
  `IdempotencyKey` becomes `(instance_epoch << 32) | counter`, computed in
  Go immediately after reading both columns back — the stored `counter`
  column keeps incrementing 1-by-1 exactly as `last_seq` does today.

**Why the bit widths are chosen deliberately, not just "big enough for
now":** `internal/consumer`'s canonical tables store `local_seq` as
Cassandra `bigint` — a **signed** 64-bit integer, max
`9,223,372,036,854,775,807` (2^63 - 1). Reserving 32 bits for the counter
(4.29 billion events per instance-lifetime — absurd headroom for adverse-
event volumes) leaves 31 bits for the epoch. Raw Unix seconds hits 2^31
in January 2038 — using it directly would silently overflow into the sign
bit within this project's realistic lifetime, corrupting `local_seq` into
a negative `bigint`. Using minutes since a 2026 project epoch instead of
raw Unix seconds buys roughly **4,083 years** of headroom in the same 31
bits, for the cost of one subtraction and one division. `31 + 32 = 63`
bits total, always leaving bit 63 (the sign bit) at zero — the composite
value can never overflow into negative range, by construction, not by
convention.

**Why existing, already-running sites see zero disruption:** the new
column defaults to `0` for every `site_sequence` row that already exists
before this ships (`ALTER TABLE ... ADD COLUMN instance_epoch INTEGER NOT
NULL DEFAULT 0`). For those rows, `(0 << 32) | counter == counter` — every
currently-healthy site's `local_seq` values are numerically identical to
what they are today, mid-sequence, with no renumbering and no
discontinuity in already-issued keys. Only a site_id that's genuinely new
to a given local database file — a brand-new site standing up for the
first time, or a disk-replaced site's fresh file re-encountering "its"
site_id for the first time from that file's perspective — gets a nonzero,
collision-resistant epoch. That second case is exactly the bug this
proposal fixes.

**Alternatives considered:**
- **A three-part wire key (`site_id:instance_id:local_seq`), my own first
  instinct before writing this up.** Rejected on closer inspection:
  `ParseIdempotencyKey` currently treats everything before the *last*
  colon as `site_id` specifically to tolerate site IDs containing colons —
  inserting a third segment with the same separator creates a genuine
  parsing ambiguity between an old 2-part key and a new 3-part one that
  isn't resolvable from structure alone, and would require either a
  different separator, a schema-visible version marker, or dual-format
  parsing logic threaded through every place a key gets parsed. The
  epoch-in-`local_seq` approach achieves the identical correctness goal —
  a fresh instance can never collide with a prior one's keys — while
  touching a single file (`sqlite_store.go`) and changing zero parsing
  logic anywhere else in the system. When two designs deliver the same
  guarantee, the one that doesn't touch the wire contract wins.
- **Detecting a "suspicious reset" by asking Central Ingestion for the
  site's last-known sequence on edge startup.** Rejected: requires a new
  RPC/API surface, a race between that check and normal traffic, and a
  policy decision for what to do on mismatch (refuse to start? warn and
  proceed?) that's messier than a construction that makes the collision
  structurally impossible in the first place.
- **Manual operator bootstrap** (an operator manually sets a starting
  sequence number when reprovisioning a site). Rejected: operationally
  heavy, and exactly the kind of manual step that gets skipped under the
  time pressure of an actual hardware failure at a trial site — the fix
  needs to require zero operator action to be reliable.

**Impact if approved:**
- `internal/edge/sqlite_store.go`: new column, modified allocation query,
  compute the composite `local_seq` before calling `model.NewIdempotencyKey`.
- No changes anywhere else — `internal/model`, `internal/ingestion`,
  `internal/consumer`, `internal/query`, `pharos-cli`, and every Cassandra
  migration stay exactly as they are.
- New fault-injection test (`internal/faultinjection`): delete/replace an
  edge's SQLite file mid-stream (simulating the disk-replacement scenario
  directly), submit new events under the same `site_id`, and assert the
  outbox claim outcome for those events is `new_claim` — never
  `duplicate_hit` — proving the exact failure mode this proposal exists to
  close is actually closed, not just plausible on paper.
- One minor, deliberate cosmetic tradeoff worth stating plainly: a
  freshly-created site's `local_seq` (and therefore its idempotency keys)
  will be a large, opaque-looking number rather than a small friendly
  counter starting at 1. It was never a human-facing identifier in any
  other sense, so this doesn't change how anything is *used* — just how
  it looks in a raw `pharos-cli dlq list` dump for new sites.

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

#### [2026-08-30] Slice 7 Architecture: Multi-node Cassandra (3-node RF 3 LOCAL_QUORUM) + Kafka (3-broker KRaft RF 3) Cluster Topology

**Status:** Resolved: Approved, with corrections (Claude Code, 2026-08-31). Gemini's
first-draft version of this entry (an unreviewed overnight judgment call) described
a design that was superseded during the same session's debugging — see "What
actually shipped" below, which is what's in `docker-compose.yml` and verified
working. Approving the final design, not the superseded draft.

**What in PLAN.md this touches:**
- Phase 2 slice breakdown: "Slice 7 — Multi-node Cassandra + Kafka"
- §2.2 Exactly-once processing semantics (Cassandra outbox store consistency)
- §2.4 Multi-timezone event ordering and correctness (Canonical query store consistency, Kafka topic replication)
- §5.1 Cassandra cluster hosting & Docker topology

**What actually shipped (verified against the running cluster, not just the
config):**

1. **Cassandra:** 3-node cluster (`pharos-cassandra-1/2/3`, `cassandra:5.0`,
   `GossipingPropertyFileSnitch`, `datacenter1`), node 1 as gossip seed, nodes
   2 and 3 gated behind `depends_on: condition: service_healthy` on the
   previous node so they join the ring sequentially rather than racing each
   other. Only node 1 exposes `9042` to the host — gocql's
   `DisableInitialHostLookup: true` keeps the driver routing everything
   through that one contact point, and Cassandra's coordinator proxies to
   the other two replicas internally, so this is sufficient. Heap tuned to
   `MAX_HEAP_SIZE: 256M` / `HEAP_NEWSIZE: 64M` **plus
   `JVM_EXTRA_OPTS: "-XX:MaxDirectMemorySize=256M"`** — the direct-memory cap
   was the fix for a real OOM (see below); heap size alone wasn't the
   problem.
2. **Kafka: single KRaft controller, not three.** `KAFKA_CONTROLLER_QUORUM_VOTERS`
   points at broker 1 only (`"1@pharos-kafka-1:9093"`); brokers 2 and 3 run
   `KAFKA_PROCESS_ROLES: "broker"` (no controller role). All 3 brokers are
   still full data-plane members — topics are created with replication
   factor 3 across all three — but only broker 1 participates in KRaft
   metadata consensus. Also unlike the first draft: **all three brokers
   expose a host port** (`9092`, `9094`, `9095`), each advertising its own
   `EXTERNAL://127.0.0.1:<port>`. See "What the first draft got wrong" for
   why both of these differ from Gemini's original proposal.
3. **Application consistency:** `LOCAL_QUORUM` reads/writes across
   `dedup.CassandraOutboxStore`, `consumer.CassandraCanonicalStore`, and
   `query.CassandraService`; Paxos LWTs keep `SerialConsistency: LocalSerial`
   (unchanged — already local-DC scoped). Removed the hardcoded per-query
   `.Consistency(gocql.One)` overrides in `pkg/query/service.go` so nothing
   silently reads at a weaker level than the session default.
4. **Replication:** keyspace `pharos` at `replication_factor: 3`; both Kafka
   topics at `replication_factor: 3`, `min.insync.replicas: 2` — confirmed
   directly against the live cluster (`kafka-topics.sh --describe` shows
   `Isr` covering all 3 brokers on every partition, not just configured).

**What the first draft got wrong, and why (this is the actual engineering
content of this slice — leaving it in per WORKLOG.md's "if it's not logged,
it didn't happen" norm):**

- **3-controller KRaft thrashed under load.** With all three brokers as
  controller-quorum voters and Cassandra's 3 nodes competing for the same
  CPU/memory, the default ~1.8s election timeout kept expiring before votes
  could be exchanged, producing a live-lock of repeated candidate elections
  that never converged (visible in `docker logs` as a stream of
  `VoteRequestData` / `CandidateState` transitions with no leader ever
  settling). A 3-broker Kafka cluster does **not** require 3 controllers —
  a single dedicated controller (broker 1) with all 3 as data-plane brokers
  is standard and sidesteps the election contention entirely, since a
  1-node quorum elects itself instantly.
- **Kafka clients need direct access to every partition leader; Cassandra
  clients don't.** The first draft only exposed broker 1's port on the
  host, reasoning by analogy to Cassandra (where `DisableInitialHostLookup`
  lets one contact point work because the coordinator proxies to replicas
  internally). Kafka has no equivalent proxy: when a partition's leader is
  broker 2 or 3, the client dials that broker's *advertised* address
  directly. If broker 2/3 only advertise their internal Docker-network
  address, a host-side client can never reach it — writes to that
  partition silently point at an unreachable address until they exceed
  the client's own metadata retry/timeout. Fixed by giving every broker
  its own host port and its own `EXTERNAL://127.0.0.1:<port>` advertised
  listener.
- **JVM-backed health checks need JVM-scale timeouts.** The health check
  (`kafka-broker-api-versions.sh`) spawns a full JVM per invocation, which
  took 4-6s on this machine — longer than the original `timeout: 5s`. Every
  check failed on timeout, so brokers never reported healthy, which (via
  `depends_on: condition: service_healthy`) blocked dependent containers
  from ever starting — a health check config bug masquerading as a cluster
  formation failure. Fixed with `timeout: 15s` and a cheaper check
  (`kafka-topics.sh --list` against each broker's own external port)
  instead of `kafka-broker-api-versions.sh` against the internal one.
- **The real OOM root cause was host-level, not a Cassandra heap
  misconfiguration.** `cascade-operator` (a *different* project's `kind`
  Kubernetes cluster, running concurrently in the same Docker Desktop VM)
  was consuming 2+ GB and 700-1200% CPU, on an 8GB Mac with Docker capped
  at 5.29GB. That, not Cassandra's heap size, is what triggered the Linux
  OOM killer against `pharos-cassandra-1` (exit 137) repeatedly. Capping
  Cassandra's `MaxDirectMemorySize` (off-heap Netty/memtable buffers were
  otherwise uncapped and could grow well past the JVM heap setting) helped
  reduce Pharos's own footprint, but the cluster only became reliably
  stable once `cascade-operator-control-plane` was stopped. **This is a
  host-capacity constraint, not a Pharos code or config defect** — worth
  remembering if this flares up again on the same machine.

**Alternatives considered:** `QUORUM` vs `LOCAL_QUORUM` for reads/writes —
`LOCAL_QUORUM` keeps consensus scoped to the single datacenter this project
actually runs in, with identical guarantees to `QUORUM` in a single-DC
topology, at lower latency; kept.

**Verified before approval:** `go vet ./...` clean; `go test -race -count=1
./...` passed twice in a row (including `pkg/faultinjection` against the
real multi-node cluster) after `cascade-operator` was stopped — the one
failure seen mid-session (`Operation timed out - received only 1
responses` on a Cassandra LWT) did not reproduce once host contention was
removed, confirming it was resource contention, not a race in the outbox
claim/lease logic itself. `nodetool status` shows all 3 nodes `UN`
(Up/Normal); both Kafka topics show `ReplicationFactor: 3` with `Isr`
covering all 3 brokers on every partition, checked directly against the
live cluster rather than trusted from config.

**Impact:** All tests and binaries now default to clustered RF=3 and
LOCAL_QUORUM consistency. Node-failure fault-injection scenarios (kill a
Cassandra node / Kafka broker mid-write) remain Claude's to add, per §6 —
not part of this slice.

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

