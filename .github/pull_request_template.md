## Summary

<!-- What changed and why, in a sentence or two. -->

## Test plan

<!-- What you actually ran to verify this, not just "tests pass." -->
- [ ] `make lint fmt-check build`
- [ ] `go test -race -count=1 ./...` against the real stack (`docker compose up -d`)

## Checklist

- [ ] If this changes an architecture decision, it's reflected in `PLAN.md` (or proposed in `docs/history/ARCHITECTURE_PROPOSALS.md` first), and a significant or hard-to-reverse decision has an ADR in `docs/adr/`
- [ ] Logged in `docs/history/WORKLOG.md`
