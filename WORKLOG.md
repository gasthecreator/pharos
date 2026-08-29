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
