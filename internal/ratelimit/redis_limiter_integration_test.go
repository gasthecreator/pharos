package ratelimit

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func isRedisPortOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	if !isRedisPortOpen("127.0.0.1:6379") {
		t.Skip("skipping: Redis is not open on 127.0.0.1:6379")
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestRedisLimiter_BurstAndThrottle mirrors TestTokenBucketLimiter_BurstAndThrottle
// exactly, proving RedisLimiter implements the identical token-bucket
// contract, just against real Redis instead of in-process state.
func TestRedisLimiter_BurstAndThrottle(t *testing.T) {
	ctx := context.Background()
	client := newTestRedisClient(t)
	siteID := t.Name()
	require := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	require(client.Del(ctx, "pharos:ratelimit:"+siteID).Err())

	limiter, err := NewRedisLimiter(ctx, client, 5.0, 1.0)
	require(err)

	for i := 0; i < 5; i++ {
		allowed, res, err := limiter.Allow(ctx, siteID)
		require(err)
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
		expectedRemaining := 4 - i
		if res.Remaining != expectedRemaining {
			t.Errorf("request %d: expected remaining %d, got %d", i+1, expectedRemaining, res.Remaining)
		}
	}

	allowed, res, err := limiter.Allow(ctx, siteID)
	require(err)
	if allowed {
		t.Fatalf("request 6 should be throttled")
	}
	if res.ResetAfter <= 0 {
		t.Errorf("expected a positive ResetAfter when throttled, got %v", res.ResetAfter)
	}
}

// TestRedisLimiter_SharedAcrossMultipleInstances is the actual property
// PLAN.md's Slice 19 (Multi-instance scaling) exists to prove: two
// separate RedisLimiter instances (standing in for two separate
// pharos-ingestion processes behind a load balancer, each with its own
// in-process client) pointed at the same Redis must enforce ONE shared
// limit for a site, not one independent limit per instance.
// TokenBucketLimiter (in-memory) fails this exact test -- each instance
// would allow the site's full burst capacity all over again.
func TestRedisLimiter_SharedAcrossMultipleInstances(t *testing.T) {
	ctx := context.Background()
	client := newTestRedisClient(t)
	siteID := t.Name()
	if err := client.Del(ctx, "pharos:ratelimit:"+siteID).Err(); err != nil {
		t.Fatalf("failed to reset key: %v", err)
	}

	capacity := 5.0
	// Two independent RedisLimiter instances, each with its own
	// connection, simulating two separate pharos-ingestion processes.
	instanceA, err := NewRedisLimiter(ctx, client, capacity, 1.0)
	if err != nil {
		t.Fatalf("failed to create instance A limiter: %v", err)
	}
	clientB := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer clientB.Close()
	instanceB, err := NewRedisLimiter(ctx, clientB, capacity, 1.0)
	if err != nil {
		t.Fatalf("failed to create instance B limiter: %v", err)
	}

	// Alternate requests between the two "instances" -- exactly what a
	// round-robin load balancer does across two ingestion processes.
	totalAllowed := 0
	for i := 0; i < 10; i++ {
		var allowed bool
		var err error
		if i%2 == 0 {
			allowed, _, err = instanceA.Allow(ctx, siteID)
		} else {
			allowed, _, err = instanceB.Allow(ctx, siteID)
		}
		if err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
		if allowed {
			totalAllowed++
		}
	}

	// The site's bucket capacity is 5 -- CRITICAL: no more than 5 of the
	// 10 requests (spread across two independent client instances) may
	// have been allowed. TokenBucketLimiter would allow 10 (5 per
	// instance's own independent in-memory bucket); this is the
	// regression this test exists to catch.
	if totalAllowed != int(capacity) {
		t.Fatalf("CRITICAL: expected exactly %d of 10 requests allowed (shared limit across instances), got %d -- the rate limit is not actually shared", int(capacity), totalAllowed)
	}
}

// TestRedisLimiter_RefillOverTime proves tokens genuinely refill based on
// real elapsed wall-clock time stored in Redis, not just per-process time.
func TestRedisLimiter_RefillOverTime(t *testing.T) {
	ctx := context.Background()
	client := newTestRedisClient(t)
	siteID := t.Name()
	if err := client.Del(ctx, "pharos:ratelimit:"+siteID).Err(); err != nil {
		t.Fatalf("failed to reset key: %v", err)
	}

	limiter, err := NewRedisLimiter(ctx, client, 2.0, 10.0) // capacity 2, refill 10/sec
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	// Exhaust the burst.
	for i := 0; i < 2; i++ {
		allowed, _, err := limiter.Allow(ctx, siteID)
		if err != nil || !allowed {
			t.Fatalf("request %d should be allowed (err=%v allowed=%v)", i+1, err, allowed)
		}
	}
	if allowed, _, err := limiter.Allow(ctx, siteID); err != nil || allowed {
		t.Fatalf("expected throttled immediately after exhausting burst (err=%v allowed=%v)", err, allowed)
	}

	// At 10 tokens/sec, waiting 200ms should refill ~2 tokens.
	time.Sleep(200 * time.Millisecond)
	allowed, _, err := limiter.Allow(ctx, siteID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Fatalf("expected a request to be allowed again after waiting for refill")
	}
}
