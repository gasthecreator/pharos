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

### Roadmap: two phases, decided 2026-08-30

The four core challenges in §2 are implemented and verified against real
infrastructure (Slices 1-5), but that is **not** the same thing as this being
production-ready — those are different bars, and conflating them would be
dishonest about what's actually been built. Gideon wants this to genuinely
reach production-hardening eventually, but explicitly chose to sequence it:

1. **Phase 1 (now): make it portfolio-ready.** A README and a working demo
   that let a reader/interviewer actually understand and see the system run,
   accurately representing what exists today — including being explicit about
   what's deliberately out of scope so far, rather than implying more
   maturity than is real.
2. **Phase 2 (next): real production hardening.** Concretely, what's missing
   for that bar and not yet started: every service has only ever run as a
   single Docker container (Cassandra at replication factor 1, one Kafka
   broker — no multi-node cluster has ever been exercised); there is no
   authentication or TLS anywhere (anything that can reach the HTTP ports can
   submit or read adverse event data); nothing has been load-tested at
   realistic throughput; there's no deployment automation (Kubernetes
   manifests, IaC); no observability/alerting; no backup or disaster-recovery
   plan for Cassandra; no multi-instance scaling ever tested for Central
   Ingestion or the consumer group; and no access-audit logging or compliance
   tooling for the clinical data itself beyond what the DLQ/outbox design
   already provides. This is genuinely weeks of work, not a gap in rigor —
   the four core challenges were hard distributed-systems problems solved
   correctly; this list is a different, broader kind of engineering.

Don't let Phase 2 quietly bleed into Phase 1's scope, and don't let Phase 1's
speed be read as evidence Phase 2 will be equally fast — it won't be, and
that's expected, not a problem to solve.

### Phase 2 slice breakdown, scoped 2026-08-30

Sequenced so each slice is reviewable on its own and later slices can lean on
earlier ones (observability before load testing, multi-node infra before
scaling/backup-restore drills). Each slice follows the exact same discipline
as Slices 1-5: feature branch, PR, WORKLOG.md entry, and anything not already
resolved here goes through ARCHITECTURE_PROPOSALS.md before implementation —
Phase 2 does not get a lower bar than Phase 1 just because it's bigger.

- **Slice 6 — Observability & metrics.** Closes the one item Phase 1 left
  open. Instrument `pharos-ingestion`, `pharos-consumer`, and `pharos-edge`
  with Prometheus (`prometheus/client_golang`), expose `/metrics` on each
  (ingestion/edge already run an HTTP server; consumer needs a small one
  added for this). Minimum metric set, matching what §4's checklist already
  named: request count/latency by status code, rate-limit rejections,
  validation failures, dedup claim-hit rate (existing vs. new key), outbox/DLQ
  publish latency, consumer lag per partition, watermark value, late-arrival
  count, Cassandra write latency/errors, edge queue depth (pending SQLite
  rows) and forwarder attempt/success/failure counts. Add Prometheus +
  Grafana to `docker-compose.yml` (self-hosted, Apache 2.0 / AGPL-with-free-
  tier respectively — confirm no licensing surprise before wiring in, same
  diligence as the original Cassandra-vs-cost check) with a scrape config and
  a starter dashboard. Update this file's §4 checklist and the README once
  real metrics are flowing — verified against the live `/metrics` endpoints
  and a real Grafana panel, not just "the code compiles."

  **Resolved 2026-08-30 — implemented.** All three services expose
  `/metrics` (ingestion/edge share their existing HTTP server; consumer got
  a dedicated one on `--metrics-port`, default 9091, alongside `/healthz`)
  via a new `internal/metrics` package of package-level Prometheus collectors —
  safe as globals specifically because each binary runs as exactly one
  process per site/service, never multiple independent instances sharing a
  process. Every metric named above is wired to a real call site (not a
  stub): request-latency/status-code via a small `statusRecordingWriter`
  wrapping `http.ResponseWriter` in `HandleEvents` rather than threading a
  status variable through every existing response branch; rate-limit/
  validation/dedup/DLQ counters sit directly next to the atomic counters
  those events already incremented; Cassandra write latency wraps
  `Engine.Step`'s existing `SaveEvent` call; consumer lag/watermark/
  partition-activity are polled every 30s from the tracker's already-public
  methods (`CurrentWatermark`, `PartitionStats`) and the kafka-go reader's
  own `Stats().Lag`, rather than reaching into `WatermarkTracker` internals —
  deliberately, since that logic took two review rounds to get right and
  didn't need touching for this. Edge queue depth/oldest-pending-age poll
  `SQLiteStore.GetStats` every 10s the same way. Confirmed against the real
  stack, not just compilation: ran the demo flow, hit all three raw
  `/metrics` endpoints and saw real non-zero counters, and confirmed
  Grafana renders live data on the provisioned dashboard.

  **Known limitation, caught during that same verification, not fully
  fixed tonight:** `internal/edge` transitively imports `internal/ingestion` (for the
  wire-format types `BatchRequest`/`BatchResponse`), and all three services
  import the single shared `internal/metrics` package directly — so every
  binary's `/metrics` actually exposes *all* services' metric names, zero-
  valued for whatever that binary doesn't itself touch (confirmed:
  `pharos-edge`'s raw `/metrics` output includes `pharos_consumer_kafka_lag`
  and `pharos_ingestion_dlq_writes_total`, both always 0). The real,
  non-zero values are correct on their owning service's endpoint — this
  doesn't corrupt any actual metric — but it means Prometheus stores extra
  flat-zero time series per metric name across every job. Mitigated tonight
  by scoping every dashboard query with an explicit `job="pharos-..."`
  label filter (see `pharos-overview.json`) rather than doing a deeper
  refactor at this hour on `handler.go`/`forwarder.go`, which are both
  fault-injection-tested critical paths not worth touching for a cosmetic
  fix under time pressure. The correct real fix, for whoever picks this up:
  extract the wire types edge actually needs out of `internal/ingestion` into
  their own dependency-free package (e.g. `internal/wire`), so `internal/edge` no
  longer transitively pulls in `internal/ingestion` (and by extension anything it
  imports) at all.

  Grafana OSS is
  AGPLv3 — free to self-host with no usage cap, same free-forever footing as
  Cassandra's Apache 2.0, so no cost-constraint conflict. Full stack in
  `docker-compose.yml` (`prometheus`, `grafana` services) plus
  `observability/` (Prometheus scrape config, Grafana datasource/dashboard
  provisioning, and the starter dashboard JSON itself).

- **Slice 7 — Multi-node Cassandra + Kafka.** Every service has only ever run
  against a single-container Cassandra (RF=1) and a single Kafka broker —
  this slice is what actually proves the "distributed" in this project's
  premise. **Resolved 2026-08-30 — target topology:** 3-node Cassandra
  cluster via `docker-compose`, keyspace replication factor 3, application
  consistency level `LOCAL_QUORUM` for both reads and writes on every table
  (the existing LWT claim/lease CAS operations already use Cassandra's serial
  consistency internally and don't need to change — this only affects the
  plain reads/writes around them). 3-broker Kafka cluster, still KRaft mode
  (no ZooKeeper), topic replication factor 3, `min.insync.replicas=2`.
  `gocql`/`kafka-go` client configs need multiple contact points/broker
  addresses. Full existing test suite (including `internal/faultinjection`) must
  pass unchanged against the multi-node setup before this slice is done —
  that's the actual proof, not a new feature. **New node-failure
  fault-injection scenarios (kill one Cassandra node or one Kafka broker
  mid-write, verify no data loss/duplication) are Claude's to add afterward,
  per §6 — this slice delivers the multi-node infra and confirms the
  existing suite survives it, not new fault-injection tests.**

  **Done 2026-08-31.** Shipped with two real deviations from the plan above,
  both because the literal spec didn't survive contact with a real cluster:
  Kafka runs a single KRaft controller (broker 1) rather than all three
  brokers as controller-quorum voters (3-way election contention thrashed
  under load), and all three Kafka brokers — not just broker 1 — expose a
  host port, since unlike Cassandra, Kafka clients dial partition leaders
  directly rather than going through a proxying coordinator. Full detail,
  including a real OOM root-caused to a *different* project's Kubernetes
  cluster competing for the same Docker Desktop memory, in
  [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). `go vet` clean;
  `go test -race -count=1 ./...` passed twice against the real 3-node
  Cassandra / 3-broker Kafka cluster, including `internal/faultinjection`.
  `nodetool status` confirms all 3 nodes `UN`; both Kafka topics confirmed
  at `ReplicationFactor: 3` with `Isr` covering all 3 brokers on every
  partition, checked directly against the live cluster.

### Re-scoped 2026-08-31 — core pipeline hardening inserted ahead of Slices 15-21

A design-review pass asked directly: setting the not-yet-started slices
aside, does the *already-built* core pipeline (§2.1-§2.4) actually have
gaps, or is production-hardening the only thing left? It found real ones —
one of them (Slice 8 below) isn't speculative, it's a failure mode this
project's own testing accidentally reproduced. Gideon's call: fix these
before Auth/TLS through the dashboard, and don't skip multi-region just
because there's no physical multi-region hardware available — simulate it
as rigorously as a single machine allows instead of skipping the problem.
That reprioritized the numbering: the seven slices below are new and come
first; everything previously numbered Slice 8-14 is renumbered Slice 15-21
with no change to their own content or intent.

- **Slice 8 — Idempotency key resilience (queue-instance identity).** A
  real, empirically-observed gap, not a hypothetical: the idempotency key
  is purely `site_id:local_seq` (`internal/model/idempotency.go`), and
  `local_seq` lives only in the edge's local SQLite file. Replace a site's
  hardware (a disk failure at a trial site is not a theoretical event) with
  the same `--site-id` and a fresh empty database, and `local_seq` restarts
  at 1 — regenerating keys that collide with genuinely different events
  submitted months earlier from the original hardware. Central Ingestion's
  dedup layer, working exactly as designed, treats the new events as
  already-published duplicates and never republishes them: a real adverse
  event silently dropped, not because the claim/lease logic is wrong, but
  because it's built on an assumption (`local_seq` is monotonic per site,
  forever) that a hardware swap quietly breaks. This was hit firsthand
  during Slice 6 verification and initially mistaken for a test artifact —
  it isn't.

  **Resolved 2026-08-31 — design: encode a per-instance epoch into
  `local_seq` itself, not the wire key.** The original sketch above
  (extend the wire key to `site_id:instance_id:local_seq`) didn't survive
  contact with the actual parsing code — `ParseIdempotencyKey` already
  treats everything before the *last* colon as `site_id` specifically to
  tolerate site IDs containing colons, and a third segment creates a real
  ambiguity between old 2-part and new 3-part keys that isn't resolvable
  from structure alone. The approved design instead computes
  `local_seq = (instance_epoch << 32) | counter` entirely inside
  `internal/edge/sqlite_store.go` — `IdempotencyKey`,
  `ParseIdempotencyKey`, the wire format, every Cassandra schema, and
  every downstream consumer are untouched, since none of them interpret
  `local_seq`'s internal structure, only its value. `instance_epoch` is
  stamped once, in minutes since a fixed 2026-01-01 project epoch (not raw
  Unix seconds — that would hit 2^31 and overflow into Cassandra
  `bigint`'s sign bit in January 2038; minutes-since-project-epoch buys
  ~4,083 years in the same 31 bits), at the exact moment a `site_id` gets
  a row in the edge's local `site_sequence` table for the first time that
  local database file has ever seen it — which is precisely the signal
  for "this is either a brand-new site or a disk-replaced one," with no
  separate detection needed. Existing sites see zero disruption: their
  already-existing rows default `instance_epoch` to 0, making their
  `local_seq` numerically identical to today. Full reasoning, the bit-width
  derivation, and rejected alternatives in
  [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). New
  fault-injection test (still needed, unchanged from the original plan):
  wipe/replace an edge's SQLite file mid-stream, confirm subsequent
  genuinely-new submissions land as `new_claim`, never `duplicate_hit`.

  **Done 2026-09-04.** Implemented entirely inside
  `internal/edge/sqlite_store.go` as designed — zero changes anywhere else.
  Added `NewSQLiteStoreWithEpochSource` alongside the existing
  `NewSQLiteStore`, matching the same dependency-injection pattern
  `Forwarder` already uses for its `HTTPClient`, so tests can control
  `instance_epoch` deterministically instead of sleeping past a real
  wall-clock minute boundary to force two different epochs. Updated every
  existing `internal/edge` unit test that had hardcoded small sequence
  numbers (`1`, `2`, `1..N`) to assert the actual invariant instead
  (nonzero, monotonic, contiguous, matches the key's own local_seq) — a
  fresh database file now always mints a real, non-trivial epoch, not just
  a legacy-migrated one. New fault-injection test
  (`TestEdgeInstanceLoss_DiskReplacementDoesNotDropNewEvents`) simulates
  exactly the bug scenario: two edge "instances" for the same site_id,
  different local database files, different injected epochs standing in
  for real elapsed time — both submit real events through the same real
  Central Ingestion/Cassandra/Kafka, and neither's idempotency keys
  collide with the other's. `go vet`, `go build`, `gofmt`, and
  `golangci-lint` (0 issues) all clean; full test suite passed twice in a
  row against the real multi-node cluster, including the new test and
  every existing fault-injection scenario.

- **Slice 9 — Wire-format schema versioning.** Forward-looking hardening,
  not a fix for an existing bug: the AdverseEvent wire schema hasn't changed
  since Slice 1, but a system meant to run for years with independently
  updated edge binaries across many sites needs a version story *before*
  the first real schema change, not after. Add an explicit integer
  `schemaVersion` field; Central Ingestion's handler dispatches validation
  by version, and an unrecognized version is a clean, specific DLQ
  rejection — never a crash or a silent misparse. Sequenced right after
  Slice 8 since both are wire-format changes worth reviewing together.

  **Done 2026-09-04.** `AdverseEvent.Validate()` dispatches by
  `SchemaVersion` through a `map[int]func(*AdverseEvent) error`, currently
  holding one entry (`SchemaVersionV1`); absent/zero defaults to v1 for
  full backward compatibility, anything else returns typed
  `ErrUnsupportedSchemaVersion` and flows through the existing DLQ
  rejection path unchanged. The edge stamps `CurrentSchemaVersion` at
  capture time (mirroring the idempotency-key stamp), never overriding a
  caller-set value. Full reasoning in
  [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). `go vet`,
  `go build`, `gofmt`, `golangci-lint` (0 issues) all clean; full suite
  passed twice against the real multi-node cluster.

- **Slice 10 — DLQ replay & reprocessing.** The DLQ is fully inspectable
  (`pharos-cli dlq list/get`) but has no path back into the pipeline — once
  whatever caused a rejection is actually fixed, a rejected event just stays
  rejected forever. For adverse-event data specifically, "we can see the
  serious event we rejected but can never get it back into the system" is a
  real operational hole, not a nice-to-have. **Decision:**
  `pharos-cli dlq replay <idempotency-key>` resubmits the record's stored
  raw payload through the exact same Central Ingestion validation path a
  fresh submission takes — not a trust-it-now backdoor. Replay reuses the
  *same* idempotency key (it's a resubmission, not a new event), so it goes
  through the identical claim/lease path any retry already uses — no new
  outbox mechanism needed. On success, the original DLQ record is marked
  `REPLAYED`, never deleted — the original rejection stays part of the
  audit trail, matching the "never silently mutate a reported result"
  principle §2.4 already established for late-arriving data.

  **Done 2026-09-04.** `HandleEvents`'s per-event logic extracted into
  `processOneEvent` (verified behavior-identical against the full existing
  suite before building on top of it) so `HandleDLQReplay` reuses the exact
  same validate/claim/publish path, not a second copy. Two real gaps found
  and fixed along the way, unrelated to the feature itself but blocking it
  correctly: `EnsureSchema()`'s `CREATE TABLE IF NOT EXISTS` never adds
  columns to an already-existing table, so `replayed_at` needed the same
  idempotent-migration pattern already used for SQLite in Slice 8, now
  mirrored via `system_schema.columns`; and `pharos-cli`'s
  `--hosts`/`--port`/`--keyspace` flags were silently non-functional since
  nothing ever called `Parse()` on the `flag.FlagSet` registering them —
  fixed while wiring up the new `--central-url` flag. Verified live against
  the real running binaries, not just unit tests: submitted a genuinely
  invalid event, confirmed replay of the unchanged payload still fails
  identically, corrected the stored payload directly in Cassandra to
  simulate "the issue was fixed," and confirmed replay then accepts and
  publishes to the main topic while the original DLQ record shows
  `REPLAYED` with its original rejection reason preserved. Full reasoning
  in [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). `go vet`,
  `go build`, `gofmt`, `golangci-lint` (0 issues) all clean; full suite
  passed twice against the real multi-node cluster.

- **Slice 11 — Data retention & lifecycle.** `event_outbox`, the canonical
  tables, and the DLQ all grow forever today, with no tiering or archival
  plan — a real gap made a little ironic by this project's own 21 CFR Part
  11 framing, which implies long *retention* of clinical safety data, not
  deletion. Needs a real proposal (genuinely open): the interesting
  engineering problem isn't deletion, it's tiering — hot storage (the
  existing Cassandra cluster) for recent/active data, a cold/archival tier
  (periodic export to compressed storage — local disk satisfies the
  zero-cloud-spend constraint) for data past an age threshold, with the
  query layer able to fall back to archival lookup for old records rather
  than just losing access to them.

  **Done 2026-09-05.** `internal/archive` package: gzip-JSONL cold tier
  partitioned by study_id/site_id + month, mirroring this project's own
  partition-key-first modeling instead of inventing a new physical layout.
  `known_studies`/`known_sites` tracking tables (upserted alongside existing
  writes) let the archival job discover what to scan without a secondary
  index or `ALLOW FILTERING`, reusing `events_by_study`'s and
  `dead_letter_events_by_site`'s already-efficient time-based clustering
  for the actual scan. `pharos-cli archive run [--older-than 90d]
  [--dry-run]` exports then deletes (never delete-then-write, confirmed via
  `f.Sync()` before any Cassandra delete runs). `query.CassandraService`
  falls back to the archive on a hot-tier miss (point lookups) or always
  merges it in (range/site-scoped queries, since "some of this aged into
  cold storage" is the normal case for those shapes, not an edge case).
  `event_outbox`/`pending_outbox` deliberately excluded — operational
  claim/lease bookkeeping, not the data-of-record; noted as a real,
  separate follow-up, not silently dropped. Full reasoning in
  [ARCHITECTURE_PROPOSALS.md](ARCHITECTURE_PROPOSALS.md). `go vet`,
  `go build`, `gofmt`, `golangci-lint` (0 issues) all clean; full suite
  passed twice against the real multi-node cluster, including a flagship
  integration test proving the whole lifecycle (seed old data → confirm
  hot-tier queryable → archive for real → confirm deleted from Cassandra →
  confirm still queryable via fallback). Verified live via the actual
  `pharos-cli` binary too, not just Go tests: archived 172 real records
  accumulated from this session's own testing, confirmed one directly gone
  from Cassandra via `cqlsh`, and confirmed `pharos-cli query event` still
  returns it correctly through the archive fallback.

- **Slice 12 — Edge collector durability hardening.** A site's entire local
  durability guarantee today is one un-replicated SQLite file (this is what
  makes Slice 8's failure mode possible in the first place) — a disk
  failure before forwarding completes loses whatever hadn't been sent yet.
  Needs a proposal weighing real options against the operational reality of
  a resource-constrained trial site (periodic WAL backup to a second local
  path or removable media; a lightweight local replica process; or
  explicitly documenting and accepting the residual data-loss exposure
  window) rather than defaulting to the most complex option unexamined.

  **Done 2026-09-05.** Periodic `VACUUM INTO` backup (not raw file copy —
  produces a transactionally-consistent snapshot even while the primary is
  actively being written to), plus restore-on-missing-primary at startup:
  `RestoreFromBackupIfMissing(dbPath, backupPath)` copies the backup into
  place before `NewSQLiteStore` ever opens the path, since opening a
  nonexistent path in WAL mode immediately creates an empty file — too late
  to tell "genuinely fresh instance" from "primary lost, should restore."
  `pharos-edge` gained `--backup-path`/`--backup-interval` (default 5m) flags
  and a background goroutine calling the new `Backup(ctx, path)` method on
  that interval. Verified with 6 new tests in `internal/edge/backup_test.go`
  (backup produces a genuinely restorable snapshot; repeated backups
  overwrite cleanly via temp-file-then-rename since `VACUUM INTO` refuses an
  existing target; restore fires only when the primary is truly absent;
  restore is a strict no-op whenever the primary exists, even if a backup is
  also present, so it can never clobber live data; both-absent stays a no-op
  for Slice 8's epoch resilience to handle) and live end-to-end verification
  against the actual built `pharos-edge` binary: submitted a real adverse
  event, waited for a real backup cycle to fire, killed the process, deleted
  the primary `.db`/`-wal`/`-shm` files outright (simulating full disk loss),
  restarted with identical flags, and confirmed via `sqlite3` that the exact
  row — correct `local_seq` included — was genuinely restored from the
  backup file, not just present because deletion silently failed.

- **Slice 13 — Consumer crash/restart fault-injection (watermark
  continuity).** Claude's own hands-on test work per §6, parallel to the
  node-failure scenarios already planned. Kill `pharos-consumer` mid-stream
  and restart it; prove the watermark cannot regress below what was already
  reported before the crash, even while some partitions are still catching
  up on replay — the same monotonic-guard principle from §2.4 (already
  tested against partition reawakening), now tested against process death
  specifically.

  **Done 2026-09-05.** Investigating before writing the test found the
  guarantee didn't actually exist yet across a real restart:
  `WatermarkTracker`'s `previousEmitted`/per-partition state was plain
  in-process memory, reset to zero on every `cmd/pharos-consumer` start, so
  the strict monotonic guard's own zero-value escape hatch would let a
  restart's first replayed message (Kafka resumes from the last *committed*
  offset, not "wherever the in-memory watermark had gotten to") silently
  regress the externally observed `pharos_consumer_watermark_seconds` gauge.
  Fixed rather than merely tested around, per ARCHITECTURE_PROPOSALS.md's
  Slice 13 entry: a `consumer_watermark_checkpoints` Cassandra table (one row
  per consumer group, `map<int, timestamp>` columns matching the tracker's
  own shape) persists `previousEmitted` + per-partition high-watermark/
  activity every 10s and on graceful shutdown; `cmd/pharos-consumer`
  restores it via `WatermarkTracker.Restore` before the engine consumes
  anything. Also found and fixed a real precision bug while writing the
  fault-injection test: Cassandra's `timestamp` column is millisecond-only,
  so a nanosecond-precision Go time round-tripped through it came back
  slightly *earlier*, tripping the guard as a false regression — fixed by
  truncating to millisecond precision inside `Snapshot()` itself, so its
  return value already equals whatever persistence will produce. Verified
  with unit tests (`internal/consumer/watermark_test.go`, including a
  dedicated millisecond-truncation regression test) and the flagship
  fault-injection test `internal/faultinjection/consumer_restart_watermark_test.go`:
  real Kafka + real Cassandra, a dedicated single-partition test topic
  (avoids wading through this session's entire accumulated topic backlog
  under a fresh consumer group), one real event processed and checkpointed,
  the engine discarded with no graceful save (a true crash, not a clean
  shutdown), a second real event published with an *earlier* event_time than
  the first (exactly what post-restart replay can hand you), a brand-new
  engine restored from the Cassandra-persisted checkpoint and proven to
  never report a watermark below the pre-crash floor. Also verified against
  the actual built `pharos-consumer` binary: a fresh consumer group
  processing this session's real Kafka backlog, `kill -9`'d mid-stream (no
  graceful shutdown), restarted with the same `--kafka-group` — log
  confirmed `Watermark tracker restored from checkpoint (...previous_emitted:
  2026-09-05T02:03:28Z)`, and the next status tick reported the identical
  watermark, not a regression, while correctly flagging the newly-processed
  event as late against the restored floor.

- **Slice 14 — Multi-region Cassandra + Kafka (simulated).** Explicit
  constraint: no real geographically-distributed hardware exists for this
  project. The response is to simulate the topology and its failure modes
  as rigorously as one machine allows, not to skip the problem — proving
  the *configuration and application logic* are multi-region-correct, while
  being honest that real WAN latency/partition characteristics are
  approximated, not reproduced.
  - **Cassandra:** convert from `SimpleStrategy` (Slice 7) to
    `NetworkTopologyStrategy` across two simulated datacenters (`dc-us`,
    `dc-eu`), 3 nodes each, `GossipingPropertyFileSnitch` configured with
    distinct `dc`/`rack` per container, replication
    `{'dc-us': 3, 'dc-eu': 3}`. `LOCAL_QUORUM` is already the app's
    consistency level from Slice 7 specifically because it's the correct
    choice once real datacenters exist — this slice is what that decision
    was made for.
  - **Kafka:** `broker.rack` set per broker across the two simulated
    regions so partition replicas are rack/region-aware, not all landing in
    one simulated region — and, further, a second independent Kafka
    cluster in the second region with MirrorMaker 2 (free, open-source, no
    cloud spend) replicating both topics, to exercise real cross-region DR
    rather than just in-cluster rack placement.
  - **Simulated WAN conditions:** Linux traffic-control (`tc netem`) on the
    relevant Docker network interfaces to inject realistic cross-region
    latency (~80-150ms) and, for a fault-injection test, a full simulated
    partition between the two regions — proving the existing
    partition-tolerance guarantees (§2.1, §2.2) actually hold across a
    region split, not just a single-node failure. This is standard,
    recognized practice for testing multi-region behavior without real
    geographic infrastructure, not a shortcut.
  - **Deliverable:** the full existing test suite passes unchanged against
    the 2-region topology, plus a new fault-injection scenario (a
    `tc`-induced regional partition) proving no data loss or duplication
    across it.

  **Done 2026-09-05.** Cassandra converted to `NetworkTopologyStrategy`
  across `dc-us` (3 nodes, RF=3, unchanged) and `dc-eu` (2 nodes, RF=2, not
  the originally-planned 3 — see ARCHITECTURE_PROPOSALS.md's Slice 14
  addendum: 6+6+MirrorMaker2 genuinely OOM-killed on this 8GB host even
  after heap tuning, and dc-eu is the DC nothing in this project's own
  application logic ever coordinates `LOCAL_QUORUM` against, so it's the
  one that could be trimmed without cutting corners on what's actually
  tested). `GossipingPropertyFileSnitch` with distinct `CASSANDRA_DC`/
  `CASSANDRA_RACK` per container. A real, previously-invisible correctness
  gap was found and fixed while wiring this up: none of the three
  `gocql.ClusterConfig` construction sites had a DC-aware host selection
  policy, meaning `LOCAL_QUORUM`'s meaning depended on whichever DC the
  coordinator happened to land in — harmless with one DC, silently wrong
  across two. Fixed with `TokenAwareHostPolicy(DCAwareRoundRobinPolicy(cfg.LocalDC))`
  in all three (`internal/dedup`, `internal/consumer`, `internal/query`),
  `LocalDC` defaulting to `dc-us`.

  Kafka: existing cluster stays cluster A (`dc-us`, `broker.rack` added,
  unchanged 3 brokers), a genuinely independent cluster B (`dc-eu`, 2
  brokers, own KRaft controller quorum, own `CLUSTER_ID`) with MirrorMaker 2
  replicating `pharos.events.adverse`/`pharos.events.dlq` one-directionally
  A→B — verified genuinely working by watching the mirrored
  `dc-us.pharos.events.adverse`/`dc-us.pharos.events.dlq` topics actually
  appear on cluster B, not just configured.

  Simulated WAN: `tc netem` via `cap_add: [NET_ADMIN]`, using `tc filter`
  u32-matching on peer IPs (not a blanket interface-wide `netem`) so only
  genuine cross-DC traffic is affected, same-DC traffic passes normally —
  caught and fixed a real bug in this mechanism before it reached the
  automated test: an all-`2`s `priomap` accidentally routed default
  (same-DC) traffic into the same lossy band as the explicitly-filtered
  cross-DC traffic, breaking intra-DC gossip along with the intended link.

  Full existing suite (`go test -race -count=1 ./...`) run twice against
  the real 5-node Cassandra / 5-broker Kafka / MirrorMaker2 topology, both
  clean, plus the new flagship fault-injection test
  (`internal/faultinjection/regional_partition_test.go`): a real `tc`-induced
  partition between dc-us and dc-eu, gossip genuinely marking dc-eu `DN`
  (confirmed via `nodetool status`, not assumed), a real write+read via the
  actual application code path (`consumer.CassandraCanonicalStore`,
  `LOCAL_QUORUM`) succeeding throughout with dc-eu completely unreachable,
  then healing and confirming the during-partition write reached dc-eu via
  hinted handoff. Also verified the steady-state simulated-latency half of
  "Simulated WAN conditions" separately from the full-partition case: real
  `tc netem delay 100ms 20ms` applied surgically between dc-us and dc-eu,
  `LOCAL_QUORUM` operations against dc-us confirmed unaffected.

  One thing found, investigated, and fixed rather than just narrated: initial
  heap tuning (128M/node) let the full suite pass, but not reliably —
  `TestCassandraOutboxStore_RealIntegration`'s 10-way LWT race sub-test hit
  occasional zero-winner runs (never more than one — no correctness
  violation) both during the partition test and, on a later run, against the
  healthy unpartitioned cluster; a *different*, non-Paxos operation
  (`MarkPublished`) also hit a timeout on another run, pointing at GC
  pressure under `-race`'s overhead plus concurrent test load at 128M heap,
  not anything specific to Paxos contention or partitioning. Fixed at the
  root (bumped heap to 176M — still well under the memory budget) and,
  belt-and-suspenders, made the race sub-test itself retry on exactly 0
  winners with a fresh key (still failing immediately, no retry, on the one
  outcome — `>1` winners — that would be an actual correctness violation).

  Re-verifying at 176M surfaced a *third* real bug, plus a real
  environmental one: a single `docker compose up -d` starts MirrorMaker 2
  concurrently with Cassandra/Kafka's own startup, and MM2 busy-looping
  against not-yet-existing topics piled enough CPU contention on top of 10
  other JVMs' startup work to produce two more genuine OOM kills even at
  176M heap. Fixed by sequencing MirrorMaker 2 strictly last — Cassandra +
  Kafka + observability come up and settle, migrations and topics get
  provisioned, only then does `mirrormaker` start — applied to both local
  verification and `.github/workflows/ci.yml`. Separately (and not a code
  bug at all): several hours of repeated bring-up/tear-down cycles during
  this same verification eventually degraded the local Docker Desktop VM
  into a genuinely unresponsive state (unkillable "zombie" containers, host
  load average briefly at 25 on 8 cores), needing a full Docker Desktop
  restart — done with the user's explicit go-ahead — before the final clean
  verification below. With that sequencing fix and a healthy Docker VM, the
  full suite passed cleanly twice in a row.

This is genuinely months, not weeks, of work sequenced across many
sessions — nobody should expect this list to close quickly, and no single
slice above should be treated as "finished" until it's reviewed and
verified against real infrastructure the same way every earlier slice was.

- **Slice 15 — Auth & TLS** *(was Slice 8)*. Nothing in this system
  authenticates or encrypts anything today — any process that can reach the
  HTTP ports can submit or read adverse event data, which is disqualifying
  for anything claiming production-readiness with clinical data. Needs a
  proposal in ARCHITECTURE_PROPOSALS.md before implementation (genuinely
  open: per-site API keys vs. mTLS for edge→ingestion; TLS termination
  approach for Cassandra/Kafka inter-node and client traffic, now across
  two simulated regions per Slice 14) — this is exactly the kind of
  decision that shouldn't get made unattended overnight, so treat it as
  requiring review before building, not skippable.

  **Done 2026-09-05.** Per-site API keys (SHA-256 hashed, no salt — the key
  itself is a 256-bit random value, not a human password), verified via new
  `internal/auth` (`KeyStore`/`CassandraKeyStore`/`MemoryKeyStore`,
  `RequireAPIKey` HTTP middleware) rather than mTLS — see
  ARCHITECTURE_PROPOSALS.md's Slice 15 entry for the full reasoning (mTLS's
  per-site certificate lifecycle doesn't fit this project's "resource-
  constrained trial site" deployment model). `internal/tlsutil` provides one
  project-owned self-signed CA (`scripts/generate_certs.sh`) trusted by
  every Go service; `Default*Config()` functions across `internal/dedup`,
  `internal/consumer`, `internal/query`, `internal/auth`, and
  `internal/kafka` now default to this TLS the same way `LocalDC`/
  `RemoteDCs` became defaults in Slice 14. `cmd/pharos-cli` gained `site
  create-key`/`site revoke-key`; `cmd/pharos-ingestion` gained `--enable-auth`
  (default true, fails closed) and `--tls-cert`/`--tls-key`/`--ca-cert`;
  `cmd/pharos-edge` gained `--api-key`/`--ca-cert`.

  TLS scope was narrowed twice against real, repeated OOM kills (`docker
  inspect`: `OOMKilled: true`) that survived Slice 14's own heap tuning — see
  ARCHITECTURE_PROPOSALS.md's Slice 15 addendum for the full sequence.
  Summary of what shipped: Cassandra's `client_encryption_options` only
  (internode stays plaintext, since port 7000 never leaves the private
  Docker network); Kafka's `EXTERNAL` listener only (inter-broker
  `INTERNAL` stays plaintext); Kafka cluster B (`dc-eu`) reduced from 2
  brokers to 1; and — found necessary only after that still wasn't enough
  headroom — Cassandra's `dc-eu` reduced from 2 nodes/RF=2 to 1 node/RF=1
  (`cassandra-5` removed). Both `dc-eu` reductions use the same reasoning
  Slice 14 already established: nothing in this project's application logic
  ever coordinates `LOCAL_QUORUM`/ISR against `dc-eu` directly, so its own
  internal replication redundancy isn't a property this project's tests
  exercise. `dc-us` (and Kafka cluster A) keep their full node counts
  throughout, every time.

  Two real, previously-latent bugs found and fixed while verifying this
  against live infrastructure, not by planning it: (1) gocql's
  `TokenAwareHostPolicy` panics if reused across `CreateSession()` calls on
  the same cluster config — latent since Slice 14, surfaced only via the
  existing no-keyspace-fallback retry path; fixed with a fresh
  `*gocql.ClusterConfig` built per attempt in both `internal/dedup` and
  `internal/consumer`. (2) Two integration tests
  (`internal/consumer/consumer_integration_test.go`,
  `internal/faultinjection/out_of_order_test.go`) built a raw
  `kafkaGo.NewReader` directly, bypassing `consumer.NewKafkaReader` and its
  TLS `Dialer` — leftover from before this slice, causing a plaintext `EOF`
  against Kafka's now-SSL-only client listener; confirmed via real
  broker-side `SSL handshake failed` logs, not guessed, and fixed by routing
  both through `NewKafkaReader`.

  Also found only by actually running the full suite, not a code bug: `go
  test`'s default package parallelism (`-p` = `GOMAXPROCS`) genuinely
  OOM-killed a Cassandra node mid-run — every package's real Cassandra/Kafka
  connections opening simultaneously, on top of `-race`'s own overhead, is
  real load this topology has no headroom for beyond its idle steady state.
  Fixed by serializing package execution (`-p 1`), applied to both local
  verification and `.github/workflows/ci.yml`.

  Verified for real: the full suite (`go test -race -count=1 -p 1 ./...`)
  passed cleanly twice in a row against the final topology (4 Cassandra
  nodes, 4 Kafka brokers, MirrorMaker2, all TLS-enabled on client-facing
  listeners). Beyond the automated suite, the actual built binaries were
  run directly against live infrastructure: `pharos-cli site create-key`/
  `revoke-key`, `pharos-ingestion --enable-auth --tls-cert --tls-key
  --ca-cert`, and `pharos-edge --api-key --ca-cert` — confirming TLS
  handshake succeeds, `/healthz` stays unauthenticated, a request with no
  key gets 401, one claiming a different site than it authenticated as gets
  403, a revoked key is rejected, and a real edge-captured event genuinely
  flows edge → (TLS + API key) → Central Ingestion → Cassandra outbox →
  Kafka, ending `PUBLISHED` — confirmed by querying the live
  `pharos.event_outbox` table directly, not by trusting the HTTP response
  alone.

- **Slice 16 — Load testing** *(was Slice 9)*. Needs Slice 7 (multi-node)
  and benefits from Slice 14 (multi-region) to produce numbers worth
  trusting. `k6` or `vegeta` scripts simulating realistic multi-site
  submission volume; establish baseline throughput/latency numbers under
  normal operation and under one site producing a burst; identify the
  actual bottleneck rather than assuming one.

  **Done 2026-09-05.** `k6` (over `vegeta`: richer scenario scripting for
  "9 sites steady + 1 site bursting concurrently," native percentile
  reporting) driving the real built `pharos-ingestion` (TLS + per-site auth
  enabled, per Slice 15) and `pharos-consumer` binaries against the real
  4-node Cassandra / 4-broker Kafka / MirrorMaker2 topology from Slice 15 —
  new `loadtest/pharos_load_test.js`, `loadtest/README.md`,
  `scripts/loadtest_setup.sh` (provisions fresh per-site API keys).

  Two scenarios run concurrently in one test, not as separate runs, because
  the actual question this slice asks ("what happens under one site's
  burst") is only answered by comparing the two against each other:
  `steady_state` (9 sites, ~1 submission/2s each, ~4.5 req/s sustained
  combined) and `burst_site` (1 separate site, idle 30s then a genuine
  30 req/s arrival rate for 15s via k6's `constant-arrival-rate` executor).

  **Baseline numbers** (steady_state, real end-to-end HTTP round trip
  through TLS + auth + Cassandra outbox + Kafka publish): p50 ~50ms, p90
  ~85ms, p95 ~101ms, max 232ms, across 3 consecutive real runs.

  **Burst absorption**: the bursting site's own 450 requests (30/s × 15s)
  against its 100-token bucket + 10/s refill correctly got ~200 rejected
  with 429 (token-bucket math: 100 + 15×10 = 250 acceptable, 450−250=200 —
  matches the observed count almost exactly) across all three runs, while
  **zero** rejections ever leaked to any of the 9 steady-state sites and
  their own p95/max latency stayed indistinguishable from the no-burst
  baseline — real, repeated confirmation that `internal/ratelimit`'s
  per-site token buckets genuinely isolate sites from each other under load,
  not just in the unit tests.

  **The actual bottleneck** (not assumed): comparing
  `pharos_ingestion_request_duration_seconds` (~122ms avg end-to-end) against
  `pharos_ingestion_outbox_publish_duration_seconds` (~53ms avg, the Kafka
  publish step specifically) leaves ~69ms unaccounted for elsewhere in the
  request path — and `pharos_consumer_cassandra_write_duration_seconds` (the
  *downstream* canonical write) is only ~10ms avg, ruling that out too. The
  remaining, dominant cost is the **Cassandra outbox LWT insert** (the
  Paxos-based idempotency check itself) — Lightweight Transactions are
  well-known to cost more than a normal write due to the Paxos round trip,
  and this is the first place in the whole pipeline that number was ever
  actually measured rather than assumed. If this system needed to go
  materially faster, this is the specific place worth optimizing first, not
  Kafka or the downstream consumer.

  **A real methodology bug found and fixed along the way, worth recording
  because it's a general lesson about load-testing rate limiters, not just
  this one script**: the first burst attempt used a single sequential VU
  looping as fast as it could, and got **zero** 429s -- not because the
  limiter didn't work, but because real per-request backend latency
  (~120ms) naturally paced that one VU to ~8 req/s, *below* the 10
  tokens/sec sustained refill rate, so the bucket never actually emptied.
  A token-bucket limiter caps *arrival rate*, not "how many requests one
  slow client can queue up" -- proving it needs genuine concurrent arrival
  rate (k6's `constant-arrival-rate` executor, which adds VUs as needed to
  hit a target rate regardless of per-request latency), not a bigger loop.
  Fixed by switching `burst_site` to that executor. A second, narrower bug
  surfaced immediately after: the burst's own idempotency-key sequence
  (`${__VU}-${__ITER}`) produced a non-numeric `local_seq` segment,
  which `internal/model.ParseIdempotencyKey` correctly rejects -- every
  non-rate-limited burst request was actually failing FHIR validation
  (422), not succeeding, until caught by checking
  `pharos_ingestion_validation_failures_total` rather than trusting k6's
  own check output (which only asserted "got a response," not "got a
  200"). Fixed with a plain numeric sequence.

- **Slice 17 — Deployment automation** *(was Slice 10)*. Kubernetes
  manifests or an equivalent IaC approach for every service, health/
  readiness probes wired to the metrics from Slice 6, basic CI image build.

  **Done 2026-09-05.** Kubernetes chosen over a vaguer "equivalent IaC" --
  more recognizable and directly demonstrable for this project's stated
  recruiting audience. `deploy/docker/Dockerfile.{ingestion,consumer,edge,cli}`
  (multi-stage, `CGO_ENABLED=0` scratch images -- every dependency in this
  project, including `modernc.org/sqlite`, is pure Go, so scratch is
  genuinely sufficient, not just minimal for its own sake); `deploy/k8s/`
  manifests for every service (two Cassandra StatefulSets mirroring
  Slice 14/15's exact 2-DC topology, two independent Kafka KRaft
  StatefulSets, MirrorMaker 2, migrations/topic-provisioning Jobs, the
  three Go services with liveness/readiness probes against their real
  `/healthz` endpoints, Prometheus); `deploy/k8s/deploy.sh` orchestrating
  the whole bring-up against a local `kind` cluster (no cloud spend,
  matching §5.1).

  Verified for real against `kind`, not just written and assumed correct --
  see `deploy/k8s/README.md` for full detail. Three real bugs found and
  fixed: (1) ConfigMap volumes are always read-only in K8s, and the stock
  Cassandra entrypoint's `chown` (the same behavior Slice 15 already
  worked around for TLS materials) fails outright on a read-only
  `cassandra.yaml` -- fixed with an initContainer copying it into a
  writable `emptyDir` first. (2) A compound, two-layer KRaft
  controller-quorum deadlock: `OrderedReady`'s default sequential pod
  creation can't succeed (pod 0 needs voters that don't exist yet) --
  fixed with `podManagementPolicy: Parallel` -- and even then, K8s
  headless Services don't publish a pod's DNS record until it's already
  Ready, a second deadlock beneath the first -- fixed with
  `publishNotReadyAddresses: true`. (3) `deploy.sh` originally regenerated
  a brand-new CA on every run, silently rotating the Secret's cert
  material out from under already-running pods whose JVMs kept the old
  keystore loaded -- fixed by generating once and reusing on subsequent
  runs.

  Live end-to-end verification hit the same class of resource constraint
  every earlier slice already found with Docker Compose, now compounded by
  `kind`'s own control-plane overhead sharing the same ~6.3GB host budget
  as the application pods -- confirmed via `docker stats` (5.1GB/82% and
  climbing, pods cycling through real OOM/eviction restarts). Applying the
  same reasoning Slice 14/15 already established, verification proceeded
  at reduced scale (`dc-eu`/cluster B trimmed first, then `cassandra-us`/
  `kafka-a` scaled to 1 replica each for the live pass specifically,
  settling at 2.3GB/36%) while the committed manifests keep `replicas: 3`
  as the intended production shape -- this slice's job is proving
  deployment *mechanics* work, not re-proving full 2-DC distributed
  correctness a second time in a different runtime, which Slice 14 already
  did once against Docker Compose. At that scale: a real event was
  submitted through `pharos-edge` (K8s pod) over TLS + API key to
  `pharos-ingestion` (K8s pod), landed in the Cassandra outbox, published
  to Kafka, consumed by `pharos-consumer` (K8s pod), and confirmed
  `PUBLISHED`/queryable in the Cassandra canonical store via direct
  `cqlsh` -- not by trusting the HTTP response alone. Prometheus confirmed
  scraping all three app services successfully.

- **Slice 18 — Backup & disaster recovery** *(was Slice 11)*. Cassandra
  snapshot/restore procedure for the multi-node cluster from Slice 7, an
  actual tested restore drill (not just a documented procedure that's never
  been run), a stated RPO/RTO, and — now that Slice 14 exists — a tested
  cross-region failover, not just a single-region restore.

  **Done 2026-09-05.** `scripts/backup_cassandra.sh` (`nodetool snapshot`
  on every node, copied out via `docker cp` into a timestamped host
  directory, then `nodetool clearsnapshot` so live nodes don't accumulate
  pinned SSTables) and `scripts/restore_cassandra.sh` (copies a backup's
  SSTables into each node's *current* live table directory, looked up via
  `system_schema.tables` -- not a filesystem glob, see below -- then
  `nodetool refresh`, no restart needed).

  Deliberately scoped to "the keyspace's data was lost or corrupted, node
  identity/tokens intact" (an accidental `DROP KEYSPACE`, a bad migration,
  on-disk corruption limited to this keyspace) -- restoring a *fully
  destroyed node* from snapshot alone is a different problem this doesn't
  solve: Cassandra 5.0's default vnodes assign random tokens to a genuinely
  fresh node, so it generally won't own the same ranges the original did,
  and blindly copying old SSTables in would misplace data. That case is
  what RF>1 and this project's multi-node topology (Slice 7/14) already
  solve via streaming from surviving replicas, not something a backup
  restores you from.

  **A real, live drill, not a documented-but-never-run procedure**: with
  the user's explicit approval for the destructive step, real pipeline
  data (3 events through the actual edge→ingestion→Kafka→consumer
  pipeline) was backed up, `canonical_events` was genuinely `DROP`ped,
  recreated via migration, and restored -- verified by querying the
  actual rows back, not by trusting script output. Two real bugs found
  along the way: (1) the backup script stored each table's *full*
  `table-uuid` directory name rather than the plain table name, so
  restore's `<table>-*` glob could never match after a drop+recreate
  regenerates a new UUID -- fixed by stripping the UUID suffix at backup
  time. (2) `DROP TABLE` doesn't delete the old on-disk directory
  immediately (Cassandra leaves it as an orphan pending background
  cleanup), so after a drop+recreate there are *two* `<table>-<uuid>`
  directories on disk and a "first match" glob has no way to know which
  is current -- restore silently wrote into the stale, orphaned one.
  Fixed by querying `system_schema.tables` for the table's actual current
  `id` instead of globbing the filesystem at all.

  **Measured, not guessed, RTO/RPO**: backup takes ~60s across all 4
  nodes/11 tables; a full keyspace-wide restore takes ~125s. A real,
  timed drill (`DROP TABLE` → recreate → restore → verify) completed in
  well under 3 minutes end to end. RPO for `event_outbox`,
  `dead_letter_events`, `site_api_keys`, `known_sites`, `known_studies`,
  `consumer_watermark_checkpoints`, and `pending_outbox` is bounded by
  however often `backup_cassandra.sh` runs (a periodic schedule --
  every 15-30 minutes is a reasonable operational default -- is the
  actual RPO commitment for these tables, since they have no other
  reconstruction path). `canonical_events`, `events_by_study`, and
  `events_by_site`, by contrast, have a much better *effective* RPO
  bounded by Kafka's own 7-day topic retention: since the downstream
  consumer can always replay from Kafka to fully rebuild these
  derived/query-layer tables, their real recovery point is however far
  back Kafka itself still has the data, independent of snapshot
  frequency -- a genuine architectural property of this project's
  event-sourced design, not something that needed building for this
  slice, just recognized and stated.

  **A tested cross-region failover, not just a single-region restore**:
  new `internal/faultinjection/cross_region_failover_test.go` proves the
  property Slice 14's own `regional_partition_test.go` doesn't -- that
  `dc-eu` is a genuine failover target, not just a passive replication
  sink. A store configured with `LocalDC: "dc-eu"` (exactly what an
  operator would reconfigure Central Ingestion/consumer to during a real
  `dc-us` outage), connected directly through `cassandra-4`'s own CQL
  port (newly published as `9043:9042` in `docker-compose.yml` -- it
  wasn't exposed to the host before, since nothing needed to reach `dc-eu`
  directly until this test), genuinely coordinates `LOCAL_QUORUM` reads
  and writes against `dc-eu` alone while a real `tc`-induced partition
  makes `dc-us` completely unreachable -- confirmed via `dc-eu`'s *own*
  gossip view (not `dc-us`'s, which is all the existing test checks).

  A real, subtle bug found only by running this against the full test
  suite (not in isolation, where it passed cleanly): the test hung for
  the full 10-minute Go test timeout on `store.Close()`. Root cause:
  even with `DisableInitialHostLookup` already set, gocql still applies
  its per-connection `HostFilter` to hosts discovered via topology
  events -- meaning a `LocalDC`-scoped session still learns about (and
  keeps trying to background-reconnect to) the *other* region's nodes at
  their container-internal addresses, which the host can never reach
  (Docker Desktop for Mac doesn't route host→container-IP without a
  published port) -- and during a real regional outage, those addresses
  are genuinely dead too, so this isn't just a host-networking quirk: an
  actual production failover would hit the identical hang. Fixed with a
  new opt-in `RestrictToLocalDC` field on `CassandraStoreConfig`
  (default `false`, no behavior change for any existing caller) that
  scopes gocql's host awareness to `LocalDC` only. First attempt used
  `gocql.DataCentreHostFilter`, which rejects even the explicitly-given
  initial contact host (its DC isn't known yet at the point the filter
  first runs, before any connection exists) -- switched to
  `gocql.WhiteListHostFilter` (matches by address, sidestepping the
  DC-population ordering problem entirely).

  Verified for real: the full suite (`go test -race -count=1 -p 1 ./...`)
  passed cleanly twice in a row against the live 4-node/2-DC Cassandra +
  4-broker/2-cluster Kafka + MirrorMaker2 topology, including both the
  existing regional-partition test and the new cross-region-failover
  test.

- **Slice 19 — Multi-instance scaling** *(was Slice 12)*. Run 2+
  `pharos-ingestion` instances behind a load balancer (this is what the
  rate limiter's already-pluggable interface from §2.3 exists for — swap in
  the Redis-backed implementation here, don't build a new one), and 2+
  `pharos-consumer` instances, verifying Kafka consumer-group rebalancing
  actually works correctly with this project's watermark tracking.

  **Done 2026-09-05/06.** New `internal/ratelimit.RedisLimiter` implements
  the existing `RateLimiter` interface from Slice 2 — no new interface,
  exactly PLAN.md's own "swap in ... don't build a new one" instruction —
  as a distributed token bucket, the identical algorithm
  `TokenBucketLimiter` already used, moved into one atomic Redis Lua
  script (`EVAL`) so concurrent instances' check-and-decrement can't race
  at the network level the way separate GET/SET calls would. New `redis`
  service in `docker-compose.yml` (no auth/TLS -- never leaves the private
  Docker network, same reasoning Cassandra/Kafka's *internal* listeners
  already use). New `nginx` service (`nginx/nginx.conf`) load-balancing
  2+ `pharos-ingestion` instances via TCP-level passthrough (the `stream`
  module), not TLS-terminating HTTP proxying — ingestion already
  terminates its own TLS per Slice 15, and passthrough keeps the
  handshake genuinely end-to-end without nginx needing a copy of the
  project's private key. New `--redis-addr` flag on `pharos-ingestion`
  (opt-in; empty still uses the in-process limiter, correct only for
  exactly one instance, with an explicit startup warning saying so).

  **Verified live, not assumed**: 2 real `pharos-ingestion` instances
  behind real nginx (confirmed alternating via nginx's own stream access
  log, not just "it returned 200"), 10 rapid requests for one site
  round-robined across both — exactly 5 accepted and 5 throttled with 429,
  matching the configured burst capacity precisely. This is the
  determinative proof: a per-process bucket (the pre-Slice-19 default)
  would have allowed roughly 10 (5 per instance's own independent state),
  not 5 shared across both.

  **A real, genuine bug found and fixed on the consumer side, before it
  was ever run** -- reasoned through by considering what 2 consumer
  instances sharing one Kafka group actually implies, not discovered by
  chasing a failure: `SaveWatermarkCheckpoint` did a plain overwriting
  `INSERT` keyed by `group_id` alone. Each instance's own
  `WatermarkTracker` only ever observes the partitions Kafka assigned to
  *it* -- with 2+ instances in the same group (required for rebalancing
  to split partitions between them at all), whichever instance saved last
  would silently replace the *other* instance's partitions in
  `partition_high_watermark`/`partition_last_activity` with nothing, and
  could regress `previous_emitted` below a value another, further-ahead
  instance had already reported externally -- undoing Slice 13's own
  monotonic-regression guarantee the moment a second instance joined the
  group. Fixed with CQL's native map addition (`col + {...}`, merging
  only the specific partition keys an instance actually has data for,
  leaving every other instance's keys untouched) and a read-then-max-
  then-write advance for `previous_emitted` (a Paxos/LWT conditional
  update was tried first and genuinely timed out under this cluster's
  real load after hours of testing -- "Operation timed out - received
  only 1 responses" -- simplified to a plain compare-and-set, which is
  the right amount of rigor for what's already a periodic, eventually-
  consistent crash-recovery snapshot, not a per-message consistency
  mechanism). New tests prove the merge against both `MemoryCanonicalStore`
  and real Cassandra: two simulated instances checkpointing disjoint
  partitions end up with *all* partitions in the merged result, and an
  instance with a smaller local view can never regress a larger value
  another instance already persisted.

  **Verified live end to end**: 2 real `pharos-consumer` instances in one
  Kafka group, real traffic across 6 sites/3 partitions — Kafka's own
  rebalance protocol split ownership between them (confirmed via each
  instance's own distinct, stabilized consumed-message count, not
  assumed), and the shared checkpoint row in
  `pharos.consumer_watermark_checkpoints` genuinely contained all 3
  partitions' data merged from both instances. Then one instance was
  killed outright: `kafka-consumer-groups.sh --describe` confirmed the
  survivor took over *all three* partitions with zero lag
  (CURRENT-OFFSET == LOG-END-OFFSET on every one), and its own watermark
  advanced forward (not reset or regressed) after inheriting the dead
  instance's further-progressed partitions.

  Full suite (`go test -race -count=1 -p 1 ./...`) passed cleanly twice
  in a row against the live multi-instance topology.

- **Slice 20 — Compliance / access-audit logging** *(was Slice 13)*. Who
  queried what, building on the existing `LateArrivalAudit` pattern from
  §2.4 rather than inventing a second audit mechanism — and, once Slice 10
  (DLQ replay) exists, who replayed what belongs in the same audit trail.

  **Done 2026-09-06.** New `internal/audit` package: `Store` interface
  (`RecordAccess`, `ListByOperator`, `Close`) with a `CassandraStore`
  (`pharos.access_audit_log`, partitioned by `operator`, clustered by a
  `timeuuid` `audit_id` DESC for natural newest-first ordering plus
  per-entry uniqueness with no separate counter) and a `MemoryStore` for
  `--memory` CLI mode/tests. Named and shaped after `LateArrivalAudit`
  (§2.4, Slice 13) per PLAN.md's own instruction, but genuinely
  persisted — `LateArrivalAudit` itself was confirmed, by inspection, to
  still only ever live in a `WatermarkTracker`'s in-memory state with no
  Cassandra table and no INSERT anywhere; a real pre-existing gap for its
  own "21 CFR Part 11" purpose, out of this slice's explicit scope to fix
  (the instruction was to follow its *pattern*, not extend a mechanism
  that isn't durable to begin with).

  Wired into `pharos-cli`: a new `--operator` flag, required (and
  enforced, not just documented) for `query`/`dlq list`/`dlq get` against
  real Cassandra — `query.Service` has no identity concept of its own
  (confirmed via `grep -n "^func "` across `internal/query/service.go`),
  so without this, real access to patient adverse-event data would carry
  no attributable identity at all. Every one of those commands now
  records a `RecordAccess` call (outcome derived from the underlying call's
  own error, so success/failure is never hand-typed twice) after it runs.
  A new `pharos-cli audit list --operator <name> [--limit <n>]` command
  reads the trail back — without it, every entry this slice writes would
  be unread and pointless; --operator here deliberately means the trail
  being *inspected*, not the inspector, so it isn't defaulted the way
  query/dlq's self-identity is.

  **Two real, previously-undiscovered bugs found by reading the DLQ replay
  code, not by hitting a failure first**: (1) `replayDLQRecord` built its
  HTTP request with zero headers at all — no `X-Site-ID`/`X-API-Key` —
  meaning `pharos-cli dlq replay` has been silently broken (401) against
  every real deployment since Slice 15 turned auth on by default, never
  caught because Slice 15's own testing used curl with explicit headers or
  `--memory`, never this actual CLI path. (2) the same function's
  `http.Client{}` carried no TLS trust config at all, so against the
  HTTPS-with-this-project's-own-CA every real deployment actually runs
  (per README), replay would *also* fail with "certificate signed by
  unknown authority" even with the headers fixed — a second gap in the
  identical function. Fixed both: new `--site-id`/`--api-key` flags sent
  as headers, and `--ca-cert` (already an existing global flag) now also
  builds the replay client's `tls.Config` via `tlsutil.ClientConfig.StdTLSConfig()`,
  the same helper every other TLS client in this project already uses.

  Server-side: `internal/ingestion/handler.go`'s `Handler` gained an
  `auditStore audit.Store` field and a `SetAuditStore` setter mirroring
  `SetKeyStore` exactly (opt-in, no breaking change). `HandleDLQReplay`
  now records one `DLQ_REPLAY` entry per attempt — not just successes —
  keyed by the *authenticated* site from `auth.SiteIDFromContext` (the
  real credential the middleware already checked, not a self-declared
  one), covering not-found/conflict/forbidden/failed/rejected/accepted
  outcomes. `cmd/pharos-ingestion/main.go` connects a Cassandra-backed
  audit store using the same `--cassandra-hosts`/`--cassandra-port`/
  `--cassandra-keyspace`/`--ca-cert` flags every other store already uses,
  falling back to a process-local `MemoryStore` (with a clear warning) on
  connection failure rather than failing closed like the API key store:
  this is a compliance record for an action the auth middleware's
  site-ownership check already guards, not the security control itself,
  so an unreachable audit backend blocking all ingestion traffic would be
  a disproportionate outage for what it protects.

  New tests: `internal/audit`'s own unit tests (3, `MemoryStore`) plus a
  real-Cassandra integration test (`TestCassandraStore_RealIntegration`,
  random per-run operator name, proving persistence/ordering/outcome/
  resource fidelity). `internal/ingestion/auth_test.go` gained two new
  tests exercising audit recording through the *genuine* `RequireAPIKey`
  middleware path (not a direct `HandleDLQReplay` call, since the
  authenticated-site context key is deliberately unexported outside
  `internal/auth`): one proving a successful self-replay records a
  `SUCCESS` entry, one proving a cross-site *forbidden* replay attempt is
  still recorded (with an outcome mentioning why) — "who tried what"
  matters for compliance as much as "who succeeded."

  **Verified live, not assumed**, against the real running stack (not the
  test suite): built real `pharos-cli`/`pharos-ingestion` binaries, ran
  ingestion with real TLS + auth enabled (the actual default), created a
  real site API key, submitted a real malformed event that landed in the
  real DLQ with a real idempotency key, then round-tripped `dlq list`/
  `dlq get` and confirmed both showed up seconds later via
  `pharos-cli audit list --operator compliance-officer@test` reading real
  rows back from Cassandra. Replayed that still-invalid record via the
  now-fixed CLI path and got a real structured HTTP 422 (proving the auth
  headers and TLS trust fix actually work — this would previously have
  been a bare 401 or a TLS handshake error) whose `DLQ_REPLAY` audit entry
  landed under the *site's own* operator identity, not the CLI's. Then
  seeded a second, validly-payloaded DLQ record directly through the real
  `CassandraOutboxStore` (mirroring the existing unit test's own approach,
  just against real Cassandra) and replayed it for a genuine HTTP 200 —
  confirmed a `SUCCESS` audit entry landed for that one too, proving both
  outcome branches for real. Full suite (`go test -race -count=1 -p 1
  ./...`) passed cleanly twice in a row.

### Slice 21 — Web dashboard (portfolio accessibility — not production hardening) *(was Slice 14)*

Scoped 2026-08-31, sequenced separately from every numbered slice above.
Everything in this project is currently operated via `curl` + `pharos-cli`
+ Grafana — deliberately, since the four core distributed-systems
challenges are the actual point, and a web frontend proves nothing about
partition tolerance or exactly-once semantics that the CLI + fault-injection
suite doesn't already prove better. This slice exists for a narrower
reason: a non-technical viewer (a recruiter, an interviewer without a
terminal handy) can't run commands, and a browser page they can just look
at closes that gap. **This is not a production-hardening item — don't let
it get counted as progress on Slices 8-20, and don't let it block or get
blocked by them.**

**Decision: server-rendered Go, not a JS frontend.** A new `pharos-dashboard`
binary using stdlib `html/template` (or a minimal Go templating helper —
not a new one unless there's a real reason), reusing `internal/query.Service`
— the exact same interface `pharos-cli` already uses, so this is a new
presentation layer over already-built, already-tested query logic, not new
business logic. Explicitly rejected a React/Next.js frontend: this project's
whole stated pitch is "Go, Kafka, Cassandra — nothing else," and pulling in
an npm/Node toolchain for a demo-only accessibility layer would dilute that
story and add a second build/dependency system for something that isn't the
point of the project. Revisit only if a real reason surfaces (e.g. genuinely
needing client-side interactivity this can't deliver).

**Scope:**
- Recent-events feed across all sites (reuse `events_by_study`/`events_by_site`
  query patterns already in `internal/query.Service`, latest N).
- DLQ view: rejected events with their structured validation reasons, and
  (once Slice 10 exists) a replay action, reusing `internal/query.Service`'s
  existing DLQ methods rather than a second read path.
- Query by site/study, mirroring `pharos-cli query`'s existing patterns —
  don't reinvent the query logic, just render it.
- A simple form to submit a test adverse event, POSTing directly to Central
  Ingestion's existing `/api/v1/events` — this is the only write path, and
  it exercises the real pipeline live rather than faking it.
- Link out to the existing Grafana dashboard (Slice 6) for system health
  rather than re-building charts this project already has.

**Explicitly out of scope:** authentication (this project has none anywhere
yet — Slice 15 owns that decision when it happens; the dashboard doesn't
change the project's risk posture, since it's a UI over data that's already
reachable unauthenticated). Real-time push/websockets — a page refresh or a
short polling interval is enough for a demo.

**Who builds it:** feature work, so Gemini per the established split in §6 —
Claude scopes/reviews as usual. Sequence whenever convenient; not a
dependency of Slices 8-20 or vice versa.

**Done 2026-09-06.** *(Built by Claude Code directly, not Gemini — superseded
by the session-standing instruction to build every remaining slice without
handing off feature work.)* New `pharos-dashboard` binary (`cmd/pharos-dashboard/main.go`)
and `internal/dashboard` package, stdlib `html/template` + `net/http`
exactly as decided, reusing `internal/query.Service` unchanged — no new
business logic, only a presentation layer over it, matching the same
interface `pharos-cli` already uses.

Every scope bullet: a recent-events feed across all sites (new
`ListRecentEvents` capability, below), a DLQ view with a replay action, a
site/study/event query page mirroring `pharos-cli query`'s shapes, a test
event submission form, and a Grafana link-out (`--grafana-url`, default
`http://localhost:3000`).

**A real gap surfaced while scoping "recent-events feed across all
sites":** no existing query answered it. `canonical_events` is keyed only by
idempotency_key (no ordering); `events_by_study`/`events_by_site` are each
scoped to one partition key. None of the three can answer "what's come in
recently, across every site" without `ALLOW FILTERING` or a full-table
scan — both already avoided everywhere else in this schema. Added a new
`pharos.events_recent` table, bucketed by the UTC hour of `ConsumedAt` (not
clinical `EventTime`, which can be backfilled/late and would scatter a
"what just happened" feed across arbitrary past buckets) — bounds partition
size the way every other write-time query table in this schema already
does, rather than one partition growing forever. `CassandraCanonicalStore.SaveEvent`
now writes it as a fifth parallel upsert; `ListRecentEvents` fans out over
the last 48 hour-bucket partitions (newest first) rather than scanning
unboundedly. Returned records are intentionally partial (no
payload/timing/Kafka fields — a feed row links to `GetEvent` for full
detail instead of duplicating the whole payload into a fourth table for
data a summary view doesn't need); documented clearly since every other
`CanonicalRecord`-returning method in this codebase returns a fully
populated record. `MemoryCanonicalStore` got the equivalent (sort-at-read
rather than bucket simulation, since it already holds whole records).
Wired through `query.Service`'s existing interface (`CassandraService`/`MemoryService`),
not a new one.

**Auth boundary, decided explicitly, not defaulted into:** the dashboard
has no identity of its own (PLAN.md's own "authentication is out of scope"
still holds), but DLQ replay and event submission both hit
auth-enforcing real endpoints (§2.1, §2.2, Slice 15). Rather than the
dashboard holding a standing service-account credential (which couldn't
replay *any* site's record anyway, since Slice 15's own check requires the
authenticated site to match the record's owner) or silently degrading
those two actions, both forms ask the human at the browser for that
specific site's Site ID/API Key per submission — forwarded to Central
Ingestion for that one request only, never stored, exactly the same
`X-Site-ID`/`X-API-Key` + `--ca-cert`-trusted-TLS proxy shape `pharos-cli`'s
own (Slice 20-fixed) replay path uses. This is also why the dashboard
needed no changes to get "who replayed what" audited: Slice 20's
server-side `HandleDLQReplay` recording already covers every caller,
dashboard included, keyed by whichever site the human authenticated as.

**Verified live, not assumed**: built the real binary, ran it against the
live Cassandra cluster's genuine accumulated data from this session's
earlier slices (the recent-events feed rendered real fault-injection/load-test
records already in the cluster, not seeded fixtures). Created a real site
key, submitted a real malformed event through the dashboard's own submit
form (proxied to real Central Ingestion, got a real HTTP 422 shown
verbatim), then replayed that real DLQ record through the dashboard's
replay form with real credentials — got a real "still rejected" result
back (the stored payload's own `actuality` was still invalid, exactly as
expected for its own unmodified stored bytes) — and confirmed via
`pharos-cli audit list --operator SITE-DASH-LIVE` that Slice 20's
server-side audit trail recorded the attempt without any dashboard-side
change, proving the two slices genuinely integrate rather than just
coexisting.

New tests: `internal/consumer` gained `ListRecentEvents` coverage (a real
Cassandra integration test proving a saved event lands in and is
returned by the real `events_recent` table; three `MemoryCanonicalStore`
unit tests for ordering, limit, and upsert-updates-position semantics).
`internal/dashboard` gained handler-level tests for every route (index,
query by all three types, DLQ list/detail, and — using an `httptest.Server`
standing in for Central Ingestion — replay and submit, including proving
credentials are validated locally *before* ever calling out, that
X-Site-ID/X-API-Key are forwarded correctly, and that a non-200 response is
shown as an error rather than mistaken for success).

Full suite (`go test -race -count=1 -p 1 ./...`) passed cleanly twice in a
row.

### Slice 22 — Property-based & deterministic simulation testing

Scoped 2026-08-31. Every fault-injection test in this project (existing and
planned — Slice 13's crash/restart, Slice 14's region partition) is a
hand-picked scenario. That's necessary but not sufficient: a handful of
scenarios a human thought to write down is a much smaller slice of the
actual state space than what a real deployment will eventually hit. This
slice generalizes fault injection into automated exploration, in two
stages of increasing ambition — deliberately staged so the cheap, high-value
part ships even if the ambitious part turns out to be too much.

**Stage A — property-based testing.** Using `pgregory.net/rapid` (or
equivalent) against the claim/lease outbox (§2.2) and the watermark tracker
(§2.4): generate random sequences of operations (concurrent claims,
retries, a crash simulated mid-publish, events arriving late or
out-of-order) and assert the core invariants hold after *every* generated
sequence, not just the specific interleavings existing tests happen to
construct by hand. Invariants worth checking this way: exactly one publish
per idempotency key ever reaches Kafka; the watermark never decreases;
`LateArrivalAudit` never has a duplicate entry for the same
(window, idempotency_key) pair. Cheap relative to Stage B — reuses the
existing in-memory test doubles, no new abstractions needed.

**Stage B — deterministic simulation, FoundationDB-style.** The more
ambitious piece: introduce `Clock` and `Network` abstractions the core
pipeline runs against (the codebase is already reasonably decoupled for
this — `HTTPClient`, `QueueStore`, and `Producer` are already interfaces,
not concrete types), so the *entire* edge→forward→ingest→publish→consume→
canonical-write path can run against simulated, seed-controlled time and
message delivery instead of real HTTP/Kafka/Cassandra. A single integer
seed deterministically reproduces one exact interleaving of message delays,
reordering, crash timing, and inter-site clock skew — which means running
100,000 random seeds is a CI job measured in seconds, not hours, and any
failing seed is trivially reproducible (rerun that exact seed) rather than
a flaky one-off. This is deliberately not a rewrite of the pipeline — the
simulation swaps out the I/O boundaries, not the logic being tested.

**Why this, not just more hand-written fault-injection tests:** a handful
of scenarios proves the specific things a human thought to check. A
simulation run across thousands of seeds finds the interleaving nobody
thought to write down — which is exactly the category every real bug
caught during this project's review process (the concurrency race, the
monotonicity violation, the audit duplication) fell into before it was
found.

**Sequencing:** doesn't block or get blocked by Slices 8-21. Best built
once Slice 8/9's wire-format changes land, so the simulation's invariant
checks target the final wire contract rather than a moving target.

**Who builds it:** test/verification infrastructure, so Claude's own work
per §6 — Stage A is well-scoped enough that Gemini could reasonably take
it instead, if bandwidth suggests splitting the two stages across builders.

**Done 2026-09-06.** *(Built by Claude Code directly, superseding the
Stage-A-could-go-to-Gemini note — this session's standing instruction to
complete every remaining scoped slice itself.)*

**New `internal/clock` package**: a `Clock` interface (`Now() time.Time`)
with `Real` (production, wraps `time.Now().UTC()`) and `Simulated`
(manually-advanced, safe for concurrent use) implementations. Wired into
`dedup.MemoryOutboxStore` (new `SetClock`, all 6 internal `time.Now()`
call sites replaced) and `consumer.Engine` (new `SetClock`), both defaulting
to `Real` — zero behavior change for every existing caller. Also fixed
`WatermarkTracker.RegisterWindow`'s one remaining direct `time.Now()` call
by adding an explicit `now` parameter (no production callers existed; only
2 test call sites needed updating).

**Stage A — `pgregory.net/rapid`** (chosen over stdlib `testing/quick`/native
fuzzing: neither had any precedent in this codebase, and rapid's `t.Repeat`
state-machine-style API is a direct fit for "generate random sequences of
operations"). Two property test files:
`internal/dedup/outbox_property_test.go` (claim/steal/publish/stale-finalize
against a `Simulated`-clocked `MemoryOutboxStore`, mirrored for the DLQ
path including `MarkDLQReplayed`'s precondition) and
`internal/consumer/watermark_property_test.go` (window registration, event
processing with event times spanning late/on-time/future, watermark
queries). CI raises rapid's default 100 generated sequences per test to
5000 via `RAPID_CHECKS` (pure in-memory Go, genuinely free relative to this
workflow's real Cassandra/Kafka integration tests).

**Two real, previously-undiscovered bugs found — on the very first
generated sequence, in both cases:**

1. **No fencing on `MarkPublished`/`MarkDLQPublished`.** Neither method
   checked that the claim being finalized was still the *current* one for
   its key: `MemoryOutboxStore`'s had no check beyond "does this key
   exist," and `CassandraOutboxStore`'s CAS conditioned only on
   `status = 'PUBLISHING'`, discarding whether the CAS actually applied at
   all. A claimant whose lease genuinely expired and was legitimately
   stolen by another claimant (the exact scenario `FetchStaleClaims`/the
   sweeper exist to detect) could still finalize late with its own stale
   data, silently overwriting the real claimant's Kafka lineage bookkeeping
   — and, if the stale claimant's own publish attempt was *also* still
   genuinely in flight (a real Kafka write taking longer than the 30s
   lease under real degraded conditions, which this exact multi-hour
   session hit more than once), a genuine duplicate publish. Fixed: both
   methods now take an `expectedClaimedAt time.Time` (the value `InsertClaim`
   returned for this specific claim) and only finalize if the record's
   current `claimed_at` still matches — Cassandra's CAS now conditions on
   `IF status = 'PUBLISHING' AND claimed_at = ?`, and checks whether it
   actually applied. Non-matching finalizes return a new
   `dedup.ErrClaimSuperseded`, not a hard failure (the underlying event was
   still genuinely published by *someone*). Threaded through all 3
   production call sites (`internal/ingestion/handler.go`) and the
   sweeper's 2. New tests prove it against both stores, including a real
   Cassandra LWT integration test.
2. **A terminal `REPLAYED` DLQ record was still steal-able.**
   `InsertClaim`/`InsertDLQClaim`'s steal branch checked only
   `existing.Status == StatusPublished` before treating an existing record
   as an abandoned in-flight claim — meaning a `REPLAYED` record (reached
   via `MarkDLQReplayed`, e.g. a client retrying a stale cached submission
   long after the original rejection was already fixed and replayed) fell
   through to the steal branch too, silently mutating its `claimed_at` and
   incorrectly reporting `Acquired: true` for a row that's supposed to be
   immutable once terminal. Real Cassandra's own CAS already correctly
   conditioned on `IF status = 'PUBLISHING'` and was unaffected; only the
   in-memory store (used by every fast test and, now, every property test)
   had the gap, despite its own doc comment claiming to "accurately
   simulate" Cassandra's CAS semantics. Fixed by checking
   `existing.Status != StatusPublishing` instead of `== StatusPublished`.
   Pinned with a dedicated hand-written regression test in addition to the
   property test that found it.

The watermark property test found no bugs after 5000 generated sequences —
a genuine, useful negative result: `ProcessEvent`/`CurrentWatermark`'s
monotonic guard and `appendLateAuditIfNotExistsLocked`'s dedup both held up
robustly across far more interleavings than any hand-written test
constructs.

**Stage B — deterministic simulation.** New `internal/simulation` package:
a `Broker` (in-memory, partitioned, FIFO-per-partition with a
randomized-but-order-preserving delivery delay) implementing
`kafka.Producer` directly, plus a `SimulatedReader` implementing
`consumer.MessageReader` whose `FetchMessage` never advances past an
uncommitted offset — exactly mirroring real kafka-go's manual-commit
semantics, so "redelivery after a simulated crash" emerges for free from
correctly modeling that invariant rather than needing a separate mechanism.
A `Harness` wires the REAL `ingestion.Handler` (submissions driven through
the actual `HandleEvents` HTTP entry point via `httptest`, not a shortcut
call into unexported internals), the REAL `ingestion.Sweeper`, and the REAL
`consumer.Engine` together against this broker plus the already-existing
`MemoryOutboxStore`/`MemoryCanonicalStore`, all sharing one seeded
`*rand.Rand` and one `clock.Simulated` — no pipeline logic reimplemented,
only I/O boundaries swapped, exactly as scoped. Scoped to the
ingest→outbox→publish→consume→canonical-write path specifically (not
edge's HTTP/SQLite leg, which already has its own real-infra fault-injection
coverage, Slice 12/14, and whose local-durability correctness is a
different problem than this slice's target invariants) — a deliberate,
documented scope decision, not a silent shortfall against "the entire
path" language in this slice's original framing.

`TestSimulation_ExactlyOnceAndMonotonicWatermarkAcrossManySeeds` drives 500
seeds, each a random interleaving of event submission (including
deliberate resubmission of already-accepted keys — the "retries" scenario),
broker delivery delay, consumer fetch/commit steps, and sweeper reclamation
passes, checking after each seed: every idempotency key that ever received
HTTP 200 was published to the (simulated) broker's own committed log
*exactly* once (verified from the broker's own record, independent of the
outbox's own bookkeeping — the specific property Stage A's two bug fixes
above exist to guarantee), the watermark never regressed at any point
observed live during the run, and every accepted key eventually reached the
canonical store. `TestSimulation_SameSeedIsDeterministic` proves the actual
FoundationDB-style claim this stage exists to make: running the identical
seed twice, independently, produces a bit-for-bit identical fingerprint
(accepted order, publish counts, final watermark, canonical store save
count) — if this ever failed, it would mean some part of the harness was
silently reaching for real wall-clock time or another non-deterministic
source instead of the shared seeded `Rng`/`Clock`, undermining every other
test's own reproducibility.

500 seeds × 150 ticks run in under 10 seconds with `-race` -- nowhere near
PLAN.md's aspirational 100,000, but the actual bottleneck at that count
would be re-running this same harness more times, not a fundamental
scaling limit; raising `numSeeds` is a one-line change if a future
investigation wants deeper exploration.

Full suite (`go test -race -count=1 -p 1 ./...`, `RAPID_CHECKS=5000`)
passed cleanly twice in a row.

### Slice 23 — Chaos Control Panel (extends Slice 21's dashboard)

Scoped 2026-08-31. Today, "this system survives a network partition" is a
claim a reader has to take on faith from `WORKLOG.md`. This slice makes it
something they can watch happen: real chaos actions, triggered from
`pharos-dashboard` (Slice 21), acting on the actual live multi-node
cluster, next to a live panel proving the system is still correct while
it's being hurt.

**Scope:**
- Chaos actions, each wired to a capability that already exists or is
  about to (nothing here invents a new failure mode, it just exposes ones
  this project already knows how to cause): partition a site (the same
  transport-blocking pattern `internal/faultinjection` already uses against
  a real edge instance instead of a test harness), kill/restart a Cassandra
  node, inject a duplicate delivery, skew a site's clock, and — once Slice
  14 exists — partition an entire simulated region.
- A live "Correctness Ledger" panel: events submitted, events delivered
  exactly-once, duplicates correctly suppressed, late arrivals correctly
  audited, current watermark and how long it's held its monotonicity
  streak — all read from the existing `/metrics` endpoints (Slice 6), not
  a new data source invented for this panel.

**Explicit safety guard, not an assumption:** chaos actions mutate real
running infrastructure. This only ever makes sense on a local/demo
instance, and since this project has no authentication yet (Slice 15),
the dashboard must not silently trust its deployment context to keep
random visitors from killing a Cassandra node — chaos actions stay
disabled unless the operator explicitly passes `--enable-chaos` at
startup, off by default, not something that ships live just because
`pharos-dashboard` happens to be reachable.

**Sequencing:** needs Slice 21 (dashboard) to exist as its host. Each
chaos action becomes available as its underlying capability lands — the
site-partition action could ship immediately (the capability already
exists in `internal/faultinjection`), while the region-partition action
naturally waits for Slice 14.

**Who builds it:** feature work, so Gemini per the established split in
§6 — Claude scopes/reviews as usual.

**Done 2026-09-06.** *(Built by Claude Code directly, superseding the
Gemini split — this session's standing instruction to complete every
remaining scoped slice itself.)* This is the final slice in this
project's numbered plan.

New `internal/chaos` package (production code, not test-only): `StopContainer`/
`StartContainer`/`RestartContainer` (`docker stop`/`start`/`restart` against
the real `pharos-cassandra-N` containers) and `PartitionRegions`/
`HealRegionPartition`, reusing the exact `tc netem`-via-`docker-exec`
mechanism `internal/faultinjection/regional_partition_test.go` already
proved correct, adapted to return errors instead of calling `t.Fatalf` so
it's callable from a running process, not just a test binary.

**Site partition needed genuinely new capability, not reuse**, contrary to
this slice's own original phrasing ("the same transport-blocking pattern
`internal/faultinjection` already uses... against a real edge instance
instead of a test harness"): that pattern
(`network_partition_test.go`'s `partitionableTransport`) is a pure
in-process test double with no hook a running `pharos-edge` process could
ever expose. Built a real equivalent instead: new `edge.ChaosClient`
(wraps the real `HTTPClient`, defaults to pass-through, `SetPartitioned`
toggles a simulated connection-refused failure) and
`edge.RegisterChaosAdminRoutes` (`/admin/chaos/partition`|`heal`|`status`),
wired into `cmd/pharos-edge/main.go` behind a new `--enable-chaos` flag on
`pharos-edge` itself — the dashboard has no standing knowledge of which
edges exist or their credentials, so the operator supplies the target
edge's own admin URL per action, matching the same "human supplies the
specific target per request" pattern replay/submit already established
in Slice 21.

**Duplicate delivery and clock skew both reuse the existing submission
proxy** (Slice 21's `submitRawEvent`, factored out of `handleSubmitPost`
for this reuse) rather than inventing a second write path: "inject a
duplicate" is the identical payload POSTed twice in a row (the real
mechanism `internal/ingestion/outbox_test.go`'s own
`TestSequentialDuplicateIdempotency` already proves at the unit level,
exercised here against the real running Central Ingestion); "skew a
site's clock" submits one real event with `date`/`recordedDate` offset
from now by an operator-supplied Go duration — data-level skew, not
literal OS clock manipulation (edge has no `Clock` abstraction in
production, and this project's actual resilience claim is about
handling skewed *event timestamps*, which is what the watermark/
late-arrival machinery already exists to prove, not about the host
clock itself).

**Correctness Ledger** reads Central Ingestion's and `pharos-consumer`'s
real `/metrics` endpoints (Slice 6) via a small (~60 line) Prometheus
exposition-format line scanner (`internal/dashboard/ledger.go`) — no new
data source, no new metrics client dependency. Surfaces requests total,
new-claim/duplicate-hit counts, DLQ writes, consumed/late-arrival/error
counts, Kafka lag, and the current watermark (with computed age) —
everything this slice's scope asked for that the existing metrics
already expose.

**Safety guard implemented exactly as specified, not assumed**: a new
`ChaosOptions{Enabled bool}` on `pharos-dashboard` (`--enable-chaos`, off
by default) gates every action handler *individually* (`requireChaosEnabled`,
checked first in every one) — not just route registration, so there's no
way to reach a live action even via a direct request while disabled. The
region-partition action additionally self-heals after 60s
(`time.AfterFunc`, cancelable if healed manually first) so a forgotten
browser tab can't leave the real cluster partitioned indefinitely — an
explicit safety net beyond what was asked, since the operator flag alone
doesn't protect against simply forgetting to heal.

**A real, previously-undiscovered CI/test-infrastructure flake was found
and fixed while live-verifying this slice**: `go test ./...` runs packages
in alphabetical order, which put `internal/chaos`'s new disruptive tests
(a real `docker stop`/`start` cycle on a live Cassandra node, a real
`tc`-based dc-us/dc-eu partition + heal) immediately before
`internal/consumer`'s own real end-to-end test. Even after this project's
already-tight ~6.3GB shared Docker VM budget settled by every readiness
signal tried (gossip UN status, `nodetool describecluster` schema
agreement, and finally a genuine TLS `kafka-go` metadata probe — deliberately
*not* `kafka-topics.sh`, which was directly observed to fail with
`OutOfMemoryError: Java heap space` spawning its own JVM inside an
already-loaded broker container, itself a real illustration of how tight
this budget is), `internal/consumer`'s test still intermittently hit
spurious Cassandra/Kafka timeouts immediately afterward. Rather than
continuing to chase an ever more precise readiness probe for what is
fundamentally shared-host resource contention, `.github/workflows/ci.yml`
now runs `internal/chaos` as its own final step, with nothing sensitive
scheduled immediately after it — removing the ordering hazard entirely
instead of narrowing its probability. The three readiness checks stay in
`internal/chaos`'s own tests regardless (still correct and valuable for
that package's own internal reliability, e.g. its own second
partition-after-heal call depends on the first heal having genuinely
completed).

**Verified live, not assumed**, against the real running stack: built real
`pharos-ingestion`/`pharos-edge`/`pharos-dashboard`/`pharos-cli` binaries.
Confirmed the Correctness Ledger renders real, live data (ingestion
`/metrics` reachable, consumer's correctly reported unreachable since none
was running). Ran a real site-partition/heal cycle against a real
`pharos-edge --enable-chaos` instance: confirmed via the edge's own
`/admin/chaos/status` and `pharos_edge_forwarder_outcomes_total` metric
that forwarding genuinely failed (`network_error`) while partitioned and
genuinely resumed (`success`) after healing — the real store-and-forward
recovery cycle (§2.1), not simulated. Ran the real duplicate-delivery
action and confirmed via Central Ingestion's own metrics exactly one
`new_claim` and one `duplicate_hit` resulted. Ran the real clock-skew
action and confirmed the event was accepted with the deliberately skewed
timestamp. Ran the real node-kill/restart and region-partition/heal
actions directly via `internal/chaos`'s own integration tests against the
live shared cluster (confirmed via `nodetool status` before/after that the
cluster returned to full health).

Full suite passed cleanly twice in a row with `internal/chaos` correctly
isolated as CI now runs it (`go test -race -count=1 -p 1` over every other
package, then `internal/chaos` on its own).

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

**Resolved 2026-08-30 — DLQ inspection & query tooling:** Surfaced through
`internal/query.Service` and `cmd/pharos-cli dlq list/get`. Migration
`migrations/003_dlq_site_index.cql` adds a secondary index on
`dead_letter_events (site_id)`, allowing site-scoped queries without table scans.
Inspection displays the exact structured FHIR validation errors, wire payload,
and Kafka DLQ lineage.

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

**Resolved 2026-08-30 — downstream consumer and watermarking (§5 Question
1 resolved too):** a dedicated `pharos-consumer` binary reads
`pharos.events.adverse` (decoupling its scaling from Central Ingestion's HTTP-
bound scaling) and writes to three purpose-built Cassandra tables:
`canonical_events` (point lookup by `idempotency_key`), `events_by_study`
(partitioned by `study_id`, clustered by `event_time DESC` — serves "all events
for trial X in date range Y"), and `events_by_site` (partitioned by `site_id`,
clustered by `local_seq DESC` — serves "all events from site Z" and verifies
continuous per-site sequence delivery). All three writes are independent,
idempotent upserts (every table's primary key includes `idempotency_key`), run
in parallel, gated behind the Kafka offset commit — no Cassandra-side atomicity
machinery needed, since a partial failure just means Kafka redelivers the whole
write set until every table reflects it.

Watermarking answers "is yesterday's clinical safety data complete across all
global trial sites?" honestly, without pulling in Flink: `W_candidate =
min(T_p)` over partitions active in the last `IdleTimeout` (default 10m),
falling back to `max(T_p)` if every partition has gone idle — a partition
excluded from the `min()` the moment it stops producing prevents one offline
site from freezing the global watermark for everyone else, the exact
partition-tolerance scenario §2.1 exists to handle. The emitted watermark is
`W = max(W_previous, W_candidate)`, which — this took two review rounds to get
right — is required precisely because a reawakened partition delivering old
backlogged data would otherwise pull the candidate (and therefore the
watermark) backward below what was already reported. Analytical windows
transition `OPEN → COMPLETE` once `W` passes their end, and `COMPLETE →
REVISED` (with an immutable, deduplicated `LateArrivalAudit` trail) if data
arrives late for an already-closed window — matching 21 CFR Part 11's
requirement to never silently mutate a reported result, while still never
dropping the late data itself (`is_late: true`, always persisted).

**Resolved 2026-08-30 — Canonical query surface:** Answered via `internal/query.Service`
and `cmd/pharos-cli query study|site|event`. Answers both named queries against
real Cassandra tables ("all events for trial X in date range Y" via
`events_by_study` and "all events from site Z" via `events_by_site`), plus single-event
point lookups (`canonical_events`). Supports both human-readable tabwriter tables
and `--json` formatting.

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
- [x] Dead-letter topic + DLQ inspection tooling — persistence in Cassandra
      `dead_letter_events` + Kafka `pharos.events.dlq` (Slice 3) and CLI
      inspection tooling (Slice 5: `cmd/pharos-cli dlq list/get` and
      `internal/query.Service`), verified against real Cassandra with actual
      validation failure details. Site-scoped lookups go through a dedicated
      `dead_letter_events_by_site` table (partition-key-first, matching
      `events_by_site`'s pattern) rather than a secondary index — the first
      draft used an index, which contradicted this project's own established
      Cassandra modeling principle; caught in review and replaced. Confirmed
      directly against the live cluster that the index is gone and the table
      is correctly structured, not just by re-reading the migration file.
- [x] Per-site rate limiting — Slice 2
- [x] Dedup store: Cassandra LWT + transactional outbox to Kafka (§2.2) — Slice 3,
      verified against a real Cassandra cluster and a real Kafka broker, not just mocks
- [x] Kafka topic design (partitioning strategy, retention) — partitioning by
      `site_id` done and verified (Slice 3). Retention (7 days / 10GB for
      `pharos.events.adverse`, grounded in FDA 21 CFR 312.32(c)(2) expedited
      safety reporting; 14 days / 5GB for `pharos.events.dlq`) is enforced by
      an `EnsureTopics()` bootstrap in `internal/kafka/topics.go`, mirroring how
      Cassandra schema already gets ensured on connect, called from
      `cmd/pharos-ingestion` and `cmd/pharos-consumer` startup. The first
      draft only defined the values as unused Go constants without ever
      applying them — caught by checking the real broker directly
      (`kafka-configs.sh --describe` showed zero dynamic configs), not by
      re-reading the source. Confirmed independently after the fix that both
      topics now genuinely carry the configured `retention.ms`/
      `retention.bytes` on the live cluster.
- [x] Cassandra schema design — Slice 3 outbox tables (`event_outbox`,
      `dead_letter_events`, `pending_outbox`) + Slice 4 canonical query tables
      (`canonical_events`, `events_by_study`, `events_by_site`), all verified
      against a real cluster
- [x] Stream processing layer (event-time ordering / watermarking) — Slice 4:
      `internal/consumer` reads `pharos.events.adverse`, tracks per-partition
      watermarks with idle-source exclusion and a monotonic max guard,
      manages `OPEN`/`COMPLETE`/`REVISED` window lifecycle with a
      deduplicated late-arrival audit trail. Verified against real
      Cassandra/Kafka, including the exact partition-reawakening-with-
      backlog scenario that broke the first two design attempts.
- [x] Fault-injection test suite (network partition simulation) — `internal/faultinjection`,
      Claude's own work per §6. Real edge + real Central Ingestion + real
      Cassandra + real Kafka: a site loses connectivity entirely (buffers
      locally, zero loss), then the partition heals *asymmetrically* (Central
      Ingestion fully processes a request but the response never reaches the
      edge, forcing a retry of an already-completed write) — proves the
      claim/lease outbox makes that retry a safe no-op, not just that data
      survives an outage
- [x] Fault-injection test suite (duplicate delivery) — covered by Slice 3's
      `TestConcurrentDuplicateRaces`, `TestSequentialDuplicateIdempotency`,
      `TestCassandraOutboxStore_RealIntegration`'s concurrent-race case
- [x] Fault-injection test suite (out-of-order delivery) — `internal/faultinjection`,
      Claude's own work per §6. Submits one site's events to Central Ingestion
      in scrambled local_seq order, drains through the real consumer, and
      verifies `events_by_site` returns them correctly ordered by the schema's
      clustering key regardless of arrival order
- [x] Fault-injection test suite (malformed FHIR payloads → DLQ) — covered by
      Slice 3's `TestDeadLetterPipeline_DurabilityAndRouting` and related handler tests
- [x] Observability (metrics on lag, dedup hit rate, DLQ volume) — Phase 2
      Slice 6: Prometheus instrumentation (`internal/metrics`) across all three
      services, self-hosted Grafana with a provisioned starter dashboard.
      Verified against live `/metrics` endpoints and real Grafana panels
      with real traffic flowing, not just that the code compiles.

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
   interview context, so staying with Cassandra.
   - **RESOLVED 2026-08-30 — query pattern fit.** The two named queries ("all
     events for trial X in date range Y" vs. "all events from site Z") did
     need different partition keys, exactly as flagged — resolved with two
     separate purpose-built tables (`events_by_study`, `events_by_site`)
     rather than forcing one schema to serve both. See §2.4.
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
