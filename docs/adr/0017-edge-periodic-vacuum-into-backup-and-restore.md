---
status: accepted
date: 2026-09-05
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0017. Edge backup with periodic `VACUUM INTO` and restore-on-missing-primary

## Context and Problem Statement

A site's whole local durability was one un-replicated SQLite file; a disk failure before forwarding completes loses unsent events. Options had to be weighed against a resource-constrained, likely unstaffed trial site.

## Decision Drivers

- Correct snapshot of a live database
- Minimal operational surface at the site
- State the residual exposure window honestly

## Considered Options

- Periodic `VACUUM INTO` snapshot to a second path, restored automatically when the primary is missing (chosen)
- Local replica process
- Document and accept the exposure
- Tail the WAL file directly

## Decision Outcome

`pharos-edge` takes `--backup-path` and `--backup-interval` (default 5m). A goroutine calls `Backup(ctx, path)` (`VACUUM INTO` via temp-file-and-rename). `SQLiteStore`'s constructor restores the backup only when the primary does not exist; if neither exists it is a fresh instance and ADR 0013 applies.

### Consequences

- Residual exposure is bounded by the backup interval (about 5 minutes by default).

### Confirmation

Six tests in `internal/edge/backup_test.go` cover restorable snapshots, repeated overwrites and restore behavior.

## Pros and Cons of the Options

### Periodic `VACUUM INTO` snapshot to a second path, restored automatically when the primary is missing (chosen)

- Good, because transactionally consistent even during writes; one SQL statement; no new dependency
- Bad, because data written after the last backup and before failure is still at risk

### Local replica process

- Good, because continuous protection
- Bad, because a second daemon per site needing monitoring and failover logic

### Document and accept the exposure

- Good, because zero cost
- Bad, because rejected as the sole answer (the backup interval still states the window)

### Tail the WAL file directly

- Good, because tighter window
- Bad, because would reimplement SQLite crash-consistency; shortening the backup interval gets most of the benefit

## More Information

Evidence: [proposal "Slice 12"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md Slice 12 (../../PLAN.md); worklog ["Slice 12 — edge collector durability hardening"](../history/WORKLOG.md).
