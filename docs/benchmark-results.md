# Load test results

**Generated:** 2026-09-05, via `loadtest/pharos_load_test.js` (see
[`loadtest/README.md`](../loadtest/README.md) for exact reproduction
steps) against the real 4-node Cassandra / 4-broker Kafka / MirrorMaker2
topology (PLAN.md Slice 15), with TLS + per-site auth enabled — not a
simulated or mocked backend.

Two scenarios ran concurrently in one test (not as separate runs), because
the actual question this benchmark answers — "what happens to every other
site when one site bursts far past its rate limit?" — is only answered by
comparing them against each other: `steady_state` (9 sites, ~4.5 req/s
sustained combined) and `burst_site` (1 separate site, idle 30s then a
genuine 30 req/s arrival rate for 15s via k6's `constant-arrival-rate`
executor).

## Baseline latency (steady_state, real end-to-end HTTP round trip)

| Percentile | Latency |
|---|---|
| p50 | ~50ms |
| p90 | ~85ms |
| p95 | ~101ms |
| max | 232ms |

Consistent across 3 consecutive real runs.

## Burst absorption

The bursting site's own 450 requests (30/s × 15s) against its 100-token
bucket + 10/s refill correctly got ~200 rejected with HTTP 429
(token-bucket math: 100 + 15×10 = 250 acceptable, 450−250=200 — matches
the observed count almost exactly), across all three runs. **Zero**
rejections ever leaked to any of the 9 steady-state sites, and their own
p95/max latency stayed indistinguishable from the no-burst baseline — real,
repeated confirmation that per-site token buckets genuinely isolate sites
from each other under load, not just in unit tests.

## The actual bottleneck (measured, not assumed)

Comparing `pharos_ingestion_request_duration_seconds` (~122ms avg
end-to-end) against `pharos_ingestion_outbox_publish_duration_seconds`
(~53ms avg, the Kafka publish step specifically) leaves ~69ms unaccounted
for elsewhere in the request path — and
`pharos_consumer_cassandra_write_duration_seconds` (the *downstream*
canonical write) is only ~10ms avg, ruling that out too. The remaining,
dominant cost is the **Cassandra outbox LWT insert** (the Paxos-based
idempotency check itself). Lightweight Transactions are well-known to cost
more than a normal write due to the Paxos round trip, and this is the
first place in the pipeline that number was ever actually measured rather
than assumed. If this system needed to go materially faster, this is the
specific place worth optimizing first — not Kafka, not the downstream
consumer.

## A methodology bug found along the way (a general lesson, not just this script)

The first burst attempt used a single sequential VU looping as fast as it
could, and got **zero** 429s — not because the limiter didn't work, but
because real per-request backend latency (~120ms) naturally paced that one
VU to ~8 req/s, *below* the 10 tokens/sec sustained refill rate, so the
bucket never actually emptied. A token-bucket limiter caps *arrival rate*,
not "how many requests one slow client can queue up" — proving it needs
genuine concurrent arrival rate (k6's `constant-arrival-rate` executor,
which adds VUs as needed to hit a target rate regardless of per-request
latency), not a bigger loop. A second, narrower bug surfaced immediately
after: the burst's own idempotency-key sequence produced a non-numeric
segment that failed FHIR validation (422, not the intended 429) until
caught by checking `pharos_ingestion_validation_failures_total` directly
rather than trusting the load-test tool's own "got a response" check.

## Micro-benchmarks (application code only)

These measure the application's own CPU and allocation cost with the in-memory
stores and a mock Kafka producer. They contain **no** Cassandra or Kafka, so
they do not replace the end-to-end load test above and say nothing about
infrastructure latency; the benchmarked end-to-end cost is dominated by the
Cassandra LWT outbox insert, which none of these include.

Single run, 2026-10-11, Apple M2, `go1.27.0 darwin/arm64`, `GOMAXPROCS=2`
(the `-2` suffix), on a machine that was also doing other work, so treat
the numbers as order-of-magnitude. Reproduce one package at a time with
`go test -run '^$' -bench . -benchmem ./internal/<pkg>/`.

| Benchmark | Package | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| `BenchmarkValidate` (profile validation of a decoded event) | `internal/model` | 84 | 32 | 1 |
| `BenchmarkDecodeAndValidate` (JSON decode + validate, about 208 MB/s) | `internal/model` | 2,324 | 616 | 10 |
| `BenchmarkNewIdempotencyKey` | `internal/model` | 3.7 | 0 | 0 |
| `BenchmarkIdempotencyKeyStringAndParse` | `internal/model` | 133 | 80 | 4 |
| `BenchmarkTokenBucketAllow_SingleSite` | `internal/ratelimit` | 74 | 0 | 0 |
| `BenchmarkTokenBucketAllow_ManySites` (1,000 sites) | `internal/ratelimit` | 69 | 0 | 0 |
| `BenchmarkTokenBucketAllow_ParallelSameSite` | `internal/ratelimit` | 106 | 0 | 0 |
| `BenchmarkMemoryOutbox_ClaimAndPublish` (insert claim + finalize) | `internal/dedup` | 460 | 295 | 3 |
| `BenchmarkMemoryOutbox_DuplicateHit` (retry of a published key) | `internal/dedup` | 88 | 194 | 2 |
| `BenchmarkHandleEvents_Batch50` (whole intake path for 50 events) | `internal/ingestion` | 298,983 per batch (about 6 us/event) | 216,911 | 1,274 |

Takeaway: the application's own per-event cost is on the order of microseconds,
so the roughly 50-100 ms end-to-end latency in the load test is infrastructure
(Cassandra Paxos and Kafka publish), not Go code. A regression here would show as
a change in these figures on the same machine.

## Caveats (read before citing these numbers)

This ran on one machine, against one topology, on one day — not averaged
over multiple hardware profiles or repeated over a longer window. The
*relative* result (steady-state sites fully isolated from the bursting
site's 429s, and the Cassandra LWT insert as the dominant cost) is the
defensible takeaway; the absolute millisecond figures will vary with
different hardware, network conditions, or cluster sizing. Full detail and
the raw investigation are in `docs/history/WORKLOG.md`'s Slice 16 entry and
`PLAN.md`'s Slice 16 writeup.
