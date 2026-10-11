---
status: accepted
date: 2026-08-29
decision-makers: Claude Code (reviewer, recorded approval in the proposal); Gideon Sanni (project owner)
---

# 0006. Per-site token-bucket rate limiting that meters HTTP batch requests

## Context and Problem Statement

One misbehaving site must not degrade ingestion for everyone else. Two questions: what algorithm and backing store, and what does a token meter (HTTP requests or individual events)?

## Decision Drivers

- Per-site isolation
- Natural bursts of adverse-event reports allowed, sustained load bounded
- Testability without forcing Redis into every early slice
- Standard 429 semantics for clients

## Considered Options

- In-memory token bucket behind a `RateLimiter` interface (chosen)
- Hard-require Redis immediately
- Leaky bucket
- Meter per event instead of per request

## Decision Outcome

A thread-safe token bucket per site (default capacity 100, refill 10 tokens/second) behind the `RateLimiter` interface. Each HTTP batch request consumes one token; with the edge's default batch size of 50, a full bucket allows up to 5,000 events in a burst. Exhaustion returns HTTP 429 with `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`.

### Consequences

- Review noted this is not a DoS-protection guarantee: the handler already parses the body before checking the limiter, and limiting off the `X-Site-ID` header first is not implemented.
- ADR 0024 adds the distributed implementation.

### Confirmation

Unit tests for burst/throttle/refill and a k6 load test showing zero rejections leaking from a bursting site to the other nine (`docs/benchmark-results.md`).

## Pros and Cons of the Options

### In-memory token bucket behind a `RateLimiter` interface (chosen)

- Good, because testable burst behavior with no extra infrastructure; interface allows a distributed implementation later
- Bad, because correct only for a single ingestion instance

### Hard-require Redis immediately

- Good, because distributed from day one
- Bad, because extra memory/test complexity before multiple replicas exist

### Leaky bucket

- Good, because smooth output
- Bad, because does not permit the natural bursts token bucket allows

### Meter per event instead of per request

- Good, because finer accounting
- Bad, because needs parsing the body before limiting; adds complexity for bounded batches

## More Information

Evidence: proposals ["Central Ingestion Rate Limiter: In-Memory Token Bucket with Pluggable Interface" and "Clarification: ... Meters HTTP Batch Requests"](../history/ARCHITECTURE_PROPOSALS.md); PLAN.md §2.3 (../../PLAN.md).
