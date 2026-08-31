package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketLimiter_BurstAndThrottle(t *testing.T) {
	ctx := context.Background()
	capacity := 5.0
	refillRate := 1.0 // 1 token per sec
	limiter := NewTokenBucketLimiter(capacity, refillRate)
	siteID := "SITE-US-01"

	// First 5 requests should succeed (consuming burst)
	for i := 0; i < 5; i++ {
		allowed, res, err := limiter.Allow(ctx, siteID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
		expectedRemaining := 4 - i
		if res.Remaining != expectedRemaining {
			t.Errorf("request %d: expected remaining %d, got %d", i+1, expectedRemaining, res.Remaining)
		}
	}

	// 6th request should be throttled
	allowed, res, err := limiter.Allow(ctx, siteID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatalf("request 6 should be throttled")
	}
	if res.Remaining != 0 {
		t.Errorf("expected 0 remaining, got %d", res.Remaining)
	}
	if res.ResetAfter <= 0 {
		t.Errorf("expected positive ResetAfter, got %v", res.ResetAfter)
	}
}

func TestTokenBucketLimiter_Refill(t *testing.T) {
	ctx := context.Background()
	capacity := 2.0
	refillRate := 10.0 // 10 tokens per second -> 1 token every 100ms
	limiter := NewTokenBucketLimiter(capacity, refillRate)
	siteID := "SITE-NG-01"

	// Exhaust bucket
	_, _, _ = limiter.Allow(ctx, siteID)
	_, _, _ = limiter.Allow(ctx, siteID)
	allowed, _, _ := limiter.Allow(ctx, siteID)
	if allowed {
		t.Fatal("expected throttle immediately after exhaust")
	}

	// Wait 150ms for at least 1 token to refill
	time.Sleep(150 * time.Millisecond)

	allowed, res, err := limiter.Allow(ctx, siteID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Fatalf("expected request to succeed after refill, got ResetAfter: %v", res.ResetAfter)
	}
}

func TestTokenBucketLimiter_SiteIsolation(t *testing.T) {
	ctx := context.Background()
	capacity := 3.0
	refillRate := 1.0
	limiter := NewTokenBucketLimiter(capacity, refillRate)

	siteA := "SITE-A"
	siteB := "SITE-B"

	// Exhaust Site A
	for i := 0; i < 3; i++ {
		allowed, _, _ := limiter.Allow(ctx, siteA)
		if !allowed {
			t.Fatalf("siteA request %d should be allowed", i+1)
		}
	}
	allowedA, _, _ := limiter.Allow(ctx, siteA)
	if allowedA {
		t.Fatal("siteA should be throttled")
	}

	// Site B must be unaffected (§2.3 per-site isolation)
	for i := 0; i < 3; i++ {
		allowedB, _, err := limiter.Allow(ctx, siteB)
		if err != nil {
			t.Fatalf("siteB error: %v", err)
		}
		if !allowedB {
			t.Fatalf("siteB request %d should be allowed despite siteA exhaustion", i+1)
		}
	}
}

func TestTokenBucketLimiter_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	capacity := 100.0
	refillRate := 0.0001 // negligible refill during test
	limiter := NewTokenBucketLimiter(capacity, refillRate)
	siteID := "SITE-CONCURRENT"

	const numGoroutines = 150
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var allowedCount int32
	var throttledCount int32

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			allowed, _, err := limiter.Allow(ctx, siteID)
			if err != nil {
				t.Errorf("error in concurrent allow: %v", err)
				return
			}
			if allowed {
				atomic.AddInt32(&allowedCount, 1)
			} else {
				atomic.AddInt32(&throttledCount, 1)
			}
		}()
	}

	wg.Wait()

	if allowedCount != 100 {
		t.Errorf("expected exactly 100 allowed requests, got %d", allowedCount)
	}
	if throttledCount != 50 {
		t.Errorf("expected exactly 50 throttled requests, got %d", throttledCount)
	}
}
