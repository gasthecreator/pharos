# Contributing

This is a solo-maintained product, but it's built with the
same discipline as a real engineering team — this file documents that
discipline so it's legible to anyone reading the repo, not just followed
implicitly.

## Before touching code

Read [`PLAN.md`](PLAN.md) first. It's the living architecture doc and the
single source of truth for every design decision, with the reasoning behind
each one — not just what was decided, but why, and what alternatives were
rejected. If something in the codebase seems to contradict `PLAN.md`, that's
a bug in the code or the doc — flag it, don't silently reconcile it.

## Branching and PRs

- All work happens on a feature branch (`feat/`, `fix/`, `docs/`, `chore/`),
  never committed straight to `main`.
- Every change goes through a PR, reviewed against `PLAN.md` and the four
  core engineering challenges it describes, before merging.
- PRs don't merge themselves — merging is a deliberate, explicit step, not
  something that happens automatically once checks pass.

## Proposing an architecture change

`PLAN.md` isn't edited directly to match new code. If building something
surfaces a reason to deviate from or extend what's already decided, write
the proposal into [`docs/history/ARCHITECTURE_PROPOSALS.md`](docs/history/ARCHITECTURE_PROPOSALS.md)
instead, with the actual reasoning (what you ran into, what alternatives you
considered, why this one wins) — not just the conclusion. That gets reviewed
and either folded into `PLAN.md` as `Resolved: Approved`, or left as
`Resolved: Rejected` with reasoning. Implementation happens after that
review, not before it.

## Recording decisions (ADRs)

Write an ADR in [`docs/adr/`](docs/adr/) for any significant or hard-to-reverse
decision: copy [`0000-template.md`](docs/adr/0000-template.md), take the next
number, and add it to the index in [`docs/adr/README.md`](docs/adr/README.md).
The proposal-review protocol above stays the mechanism for deciding;
the ADR is the short, permanent record of what was decided and why.

## Logging the work

Every implementation session — what was built, why, how, what was tested —
gets an entry in [`docs/history/WORKLOG.md`](docs/history/WORKLOG.md), regardless of who or what did
the work. Treat it like an engineering log at an actual job: if it's not
logged there, it didn't happen. Entries are dated and left in permanently,
including the ones that documented a design going back to the drawing board
— that's the record of real engineering judgment, not something to clean up
after the fact.

## Before opening a PR

```bash
make fmt-check   # gofmt
make vet         # go vet
make lint        # golangci-lint, installed into ./bin at the version CI uses
make build       # confirms everything compiles
make coverage    # unit tests that need no infrastructure, with -race and a per-package table
make up          # brings up Cassandra/Kafka/Redis in the safe order CI uses -- see its own comment
make test        # full suite against real Cassandra + Kafka (-p 1, chaos run last, RAPID_CHECKS=5000)
```

`make up` matters here, not just `docker compose up -d`: starting every
container (including MirrorMaker 2) at once has caused real OOM kills on
this host, so `up` brings services up in the same safe order
`.github/workflows/ci.yml` uses. CI runs these same checks (lint, build,
unit tests, then the real-infrastructure suite) plus `go mod tidy` drift,
`govulncheck`, CodeQL and the container/manifest hardening checks; see
`.github/workflows/ci.yml`. A green run there is the bar, not "it worked on
my machine once."

## Testing strategy

Cheap checks run first and everything expensive is real, not mocked:

| Layer | What it proves | How to run | Needs infra |
|---|---|---|---|
| Unit | Pure logic, error paths, in-memory stores | `make coverage` (race on; excludes the real-infra tests by name, see `UNIT_SKIP` in the Makefile) | no |
| Real-infrastructure integration | The same code against real Cassandra/Kafka (Paxos LWTs, TLS, consumer groups) | `make up` then `make test` | yes |
| Property-based | Invariants (exactly one publish per key, watermark never regresses, no duplicate audit entries) over generated operation sequences, including the seeded simulation in `internal/simulation` | runs inside `make test`; CI sets `RAPID_CHECKS=5000` (default 100) | no |
| Fault-injection / chaos | Partitions, crashes, disk replacement, regional failover against the real stack | `internal/faultinjection` in `make test`; `internal/chaos` runs as its own final step (it disrupts the cluster) | yes |
| Fuzz | Input parsers and validators never panic, are deterministic, and the encoder and validator agree | `go test -fuzz=FuzzParseIdempotencyKey -fuzztime=30s ./internal/model/` (one target at a time; targets: `FuzzParseIdempotencyKey`, `FuzzNewIdempotencyKey`, `FuzzAdverseEventValidate`, `FuzzSchemaVersionDispatch` in `internal/model`, `FuzzBatchRequestJSON` in `internal/wire`, `FuzzHandleEvents` in `internal/ingestion`, `FuzzWatermarkTracker` in `internal/consumer`). Seed corpora under `testdata/fuzz/` run as plain tests on every `go test` | no |
| Benchmarks | The application's own per-operation cost (in-memory stores only) | `go test -run '^$' -bench . -benchmem ./internal/<pkg>/`; results in `docs/benchmark-results.md` | no |
| Load | End-to-end throughput, latency and per-site isolation | k6, see `loadtest/README.md` | yes |

Two practical notes. Four packages (`internal/consumer`, `internal/dedup`,
`internal/kafka`, `internal/query`) mix unit tests with real-infrastructure
tests that fail rather than skip when Cassandra/Kafka are unreachable; new
real-infrastructure tests there must be added to `UNIT_SKIP`. And a failing
fuzz target writes its input to `testdata/fuzz/<Target>/`; commit that file
with the fix so the regression stays covered.

## What "done" means for a slice

A slice isn't done when the code compiles or a design looks right on paper —
it's done when it's verified against real infrastructure (real Cassandra,
real Kafka, not mocks) and the result of that verification is what's
described in `PLAN.md` and `docs/history/WORKLOG.md`. Several entries in `docs/history/WORKLOG.md`
exist specifically because something that looked correct in review turned
out to have a real bug once checked against live infrastructure — that
pattern is the point, not an embarrassment to avoid repeating.
