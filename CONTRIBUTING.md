# Contributing

This is a portfolio project with a single maintainer, but it's built with the
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
the proposal into [`ARCHITECTURE_PROPOSALS.md`](ARCHITECTURE_PROPOSALS.md)
instead, with the actual reasoning (what you ran into, what alternatives you
considered, why this one wins) — not just the conclusion. That gets reviewed
and either folded into `PLAN.md` as `Resolved: Approved`, or left as
`Resolved: Rejected` with reasoning. Implementation happens after that
review, not before it.

## Logging the work

Every implementation session — what was built, why, how, what was tested —
gets an entry in [`WORKLOG.md`](WORKLOG.md), regardless of who or what did
the work. Treat it like an engineering log at an actual job: if it's not
logged there, it didn't happen. Entries are dated and left in permanently,
including the ones that documented a design going back to the drawing board
— that's the record of real engineering judgment, not something to clean up
after the fact.

## Before opening a PR

```bash
make fmt-check   # gofmt
make lint        # go vet
make build       # confirms everything compiles
docker compose up -d && make test   # full suite against real Cassandra + Kafka
```

CI runs the same checks — see `.github/workflows/ci.yml`. A green run there
is the bar, not "it worked on my machine once."

## What "done" means for a slice

A slice isn't done when the code compiles or a design looks right on paper —
it's done when it's verified against real infrastructure (real Cassandra,
real Kafka, not mocks) and the result of that verification is what's
described in `PLAN.md` and `WORKLOG.md`. Several entries in `WORKLOG.md`
exist specifically because something that looked correct in review turned
out to have a real bug once checked against live infrastructure — that
pattern is the point, not an embarrassment to avoid repeating.
