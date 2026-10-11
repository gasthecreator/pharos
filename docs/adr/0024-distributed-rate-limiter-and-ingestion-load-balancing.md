---
status: accepted
date: 2026-09-06
decision-makers: Claude Code (scoping and build); Gideon Sanni (project owner)
---

# 0024. Redis-backed token bucket and TCP passthrough load balancing for multiple ingestion instances

## Context and Problem Statement

The in-process limiter from ADR 0006 is correct for exactly one instance. Running two or more `pharos-ingestion` instances requires a shared limiter and a load balancer that does not need the private TLS key.

## Decision Drivers

- Per-site limits must hold across instances
- Reuse the existing `RateLimiter` interface
- Keep end-to-end TLS between client and instance

## Considered Options

- `RedisLimiter` (atomic Lua `EVAL` token bucket) plus nginx `stream` TCP passthrough (chosen)
- Separate GET/SET calls in Redis
- HTTP reverse proxy terminating TLS

## Decision Outcome

`internal/ratelimit.RedisLimiter` implements the existing interface; `--redis-addr` is opt-in (empty uses the in-process limiter with an explicit startup warning). `nginx/nginx.conf` balances 2+ ingestion instances. Consumer scaling needed no new mechanism beyond the Kafka consumer group, but required merging watermark checkpoints (ADR 0018).

### Consequences

- Kubernetes runs one ingestion replica, so none of this is exercised there.

### Confirmation

Verified live: two ingestion instances behind nginx (alternation confirmed in nginx's access log) accepted exactly 5 and throttled 5 of 10 rapid requests, matching the shared burst capacity, where per-process buckets would have allowed about 10.

## Pros and Cons of the Options

### `RedisLimiter` (atomic Lua `EVAL` token bucket) plus nginx `stream` TCP passthrough (chosen)

- Good, because identical algorithm, race-free check-and-decrement, TLS handshake stays end-to-end so nginx needs no key material
- Bad, because adds Redis (no auth/TLS, private network only)

### Separate GET/SET calls in Redis

- Good, because simpler
- Bad, because races between instances at the network level

### HTTP reverse proxy terminating TLS

- Good, because L7 features
- Bad, because nginx would need a copy of the project's private key

## More Information

Evidence: PLAN.md Slice 19 (../../PLAN.md); worklog ["Slice 19 — Multi-instance scaling"](../history/WORKLOG.md).
