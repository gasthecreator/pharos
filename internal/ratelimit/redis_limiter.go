package ratelimit

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisTokenBucketScript is TokenBucketLimiter's exact algorithm (§2.3),
// moved into a single atomic Lua script rather than left as separate
// Redis commands (§2.4, PLAN.md Slice 19: Multi-instance scaling). This is
// the whole reason this type exists: TokenBucketLimiter's bucket lives in
// one process's memory, so with 2+ pharos-ingestion instances behind a
// load balancer, each instance enforces the configured limit
// *independently* -- a site's real, effective limit becomes N times the
// configured one, purely as a function of how the load balancer happens
// to distribute that site's requests. A plain "GET tokens, compute, SET
// tokens" from Go would reintroduce the identical race at the network
// level (two instances' GETs interleaving before either SETs), so the
// read-compute-write has to happen as one atomic Redis operation --
// exactly what EVAL of a Lua script guarantees (Redis executes the whole
// script single-threaded, with no other command interleaving partway
// through).
const redisTokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  last_refill = now
end

local elapsed = now - last_refill
if elapsed < 0 then
  elapsed = 0
end
tokens = math.min(capacity, tokens + elapsed * refill_rate)

local allowed = 0
if tokens >= 1.0 then
  tokens = tokens - 1.0
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens)}
`

// RedisLimiter implements RateLimiter as a distributed token bucket shared
// across every pharos-ingestion instance via Redis (§2.3, §2.4, PLAN.md
// Slice 19: Multi-instance scaling) -- the "swap in the Redis-backed
// implementation" PLAN.md's own Slice 19 wording asks for, using the
// already-pluggable RateLimiter interface Slice 2 built for exactly this.
type RedisLimiter struct {
	client            *redis.Client
	defaultCapacity   float64
	defaultRefillRate float64
	scriptSHA         string
}

// NewRedisLimiter creates a distributed token bucket limiter backed by the
// given Redis client. capacity/refillRate match TokenBucketLimiter's own
// constructor semantics (max burst size, tokens added per second).
func NewRedisLimiter(ctx context.Context, client *redis.Client, capacity, refillRate float64) (*RedisLimiter, error) {
	if capacity <= 0 {
		capacity = 100
	}
	if refillRate <= 0 {
		refillRate = 10
	}
	sha, err := client.ScriptLoad(ctx, redisTokenBucketScript).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load rate limit script into Redis: %w", err)
	}
	return &RedisLimiter{
		client:            client,
		defaultCapacity:   capacity,
		defaultRefillRate: refillRate,
		scriptSHA:         sha,
	}, nil
}

// Allow consumes 1 token if available for the given siteID, coordinated
// through Redis so the limit is enforced across every ingestion instance
// sharing this Redis, not just the one that happened to receive this
// request.
func (l *RedisLimiter) Allow(ctx context.Context, siteID string) (bool, RateLimitResult, error) {
	key := fmt.Sprintf("pharos:ratelimit:%s", siteID)
	now := float64(time.Now().UnixMicro()) / 1e6

	// TTL well past how long a full refill from empty would take, so an
	// inactive site's key expires on its own rather than accumulating
	// forever -- not a correctness requirement (a missing key just resets
	// to a full bucket, per the script's own "tokens == nil" branch), but
	// keeps Redis memory bounded for a fleet with many transient sites.
	ttlSeconds := int64(l.defaultCapacity/l.defaultRefillRate) + 3600

	res, err := l.client.EvalSha(ctx, l.scriptSHA, []string{key},
		l.defaultCapacity, l.defaultRefillRate, now, ttlSeconds).Result()
	if err != nil {
		// NOSCRIPT can happen after a Redis restart/flush wipes the
		// script cache -- reload once and retry rather than failing every
		// request until the process restarts.
		if redis.HasErrorPrefix(err, "NOSCRIPT") {
			sha, loadErr := l.client.ScriptLoad(ctx, redisTokenBucketScript).Result()
			if loadErr != nil {
				return false, RateLimitResult{}, fmt.Errorf("failed to reload rate limit script: %w", loadErr)
			}
			l.scriptSHA = sha
			res, err = l.client.EvalSha(ctx, l.scriptSHA, []string{key},
				l.defaultCapacity, l.defaultRefillRate, now, ttlSeconds).Result()
		}
		if err != nil {
			return false, RateLimitResult{}, fmt.Errorf("rate limit check failed: %w", err)
		}
	}

	fields, ok := res.([]interface{})
	if !ok || len(fields) != 2 {
		return false, RateLimitResult{}, fmt.Errorf("unexpected rate limit script result: %#v", res)
	}
	allowedInt, _ := fields[0].(int64)
	allowed := allowedInt == 1

	var tokensNow float64
	if s, ok := fields[1].(string); ok {
		_, _ = fmt.Sscanf(s, "%f", &tokensNow)
	}
	remaining := int(math.Floor(tokensNow))

	var resetAfter time.Duration
	if !allowed {
		// Mirrors TokenBucketLimiter's own ResetAfter calculation: time
		// until at least 1 token is available, given the current
		// (fractional, just-below-1) token count the script returned.
		missing := 1.0 - tokensNow
		if missing > 0 {
			resetAfter = time.Duration(missing/l.defaultRefillRate*1000) * time.Millisecond
		}
	}

	return allowed, RateLimitResult{
		Allowed:    allowed,
		Limit:      int(l.defaultCapacity),
		Remaining:  remaining,
		ResetAfter: resetAfter,
	}, nil
}
