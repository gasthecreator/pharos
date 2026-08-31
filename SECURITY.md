# Security Policy

Pharos is a portfolio project demonstrating distributed-systems engineering
patterns using a clinical-trial-adverse-event payload shape. **It is not a
production system, is not deployed anywhere with real data, and should not
be used to handle real patient or clinical trial information.** All example
data in this repo (patient references, adverse event codes, study
identifiers) is synthetic.

## Known, deliberate gaps

This is tracked explicitly, not hidden. As of this writing there is:
- No authentication or transport encryption on any HTTP endpoint or
  inter-service connection (tracked as Phase 2 Slice 8 in `PLAN.md`).
- No secrets management — there are no secrets in this system yet, by
  design, since nothing requires one.
- No hardened deployment story (Phase 2 Slices 10-12).

Don't run this outside a local/dev environment, and don't feed it real
personal or clinical data.

## Reporting a vulnerability

This is a solo-maintained portfolio repository. If you find a genuine
security issue in the code itself (not the already-documented gaps above —
those are known and tracked in `PLAN.md`), please open a GitHub issue or
reach out to the maintainer directly rather than a public disclosure, so
there's time to assess it before it's public. Given the project's current
scope (no production deployment, no real data), response time is
best-effort, not SLA-bound.
