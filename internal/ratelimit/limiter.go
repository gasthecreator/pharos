package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// RateLimitResult contains metadata about the rate limiting decision (§2.3).
type RateLimitResult struct {
	Allowed    bool          `json:"allowed"`
	Limit      int           `json:"limit"`
	Remaining  int           `json:"remaining"`
	ResetAfter time.Duration `json:"reset_after"`
}

// RateLimiter defines the contract for per-site rate limiting.
type RateLimiter interface {
	// Allow checks whether a request from the given siteID is permitted under the rate limit.
	Allow(ctx context.Context, siteID string) (bool, RateLimitResult, error)
}

type siteBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// TokenBucketLimiter implements per-site token bucket rate limiting in memory.
// Thread-safe and provides per-site isolation (§2.3).
type TokenBucketLimiter struct {
	defaultCapacity   float64
	defaultRefillRate float64
	buckets           sync.Map // map[string]*siteBucket
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter.
// capacity: max burst size. refillRate: tokens added per second.
func NewTokenBucketLimiter(capacity, refillRate float64) *TokenBucketLimiter {
	if capacity <= 0 {
		capacity = 100
	}
	if refillRate <= 0 {
		refillRate = 10
	}
	return &TokenBucketLimiter{
		defaultCapacity:   capacity,
		defaultRefillRate: refillRate,
	}
}

func (l *TokenBucketLimiter) getBucket(siteID string) *siteBucket {
	val, ok := l.buckets.Load(siteID)
	if ok {
		return val.(*siteBucket)
	}

	newBucket := &siteBucket{
		tokens:     l.defaultCapacity,
		capacity:   l.defaultCapacity,
		refillRate: l.defaultRefillRate,
		lastRefill: time.Now(),
	}

	actual, _ := l.buckets.LoadOrStore(siteID, newBucket)
	return actual.(*siteBucket)
}

// Allow consumes 1 token if available for the given siteID.
func (l *TokenBucketLimiter) Allow(ctx context.Context, siteID string) (bool, RateLimitResult, error) {
	bucket := l.getBucket(siteID)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.lastRefill = now

	// Refill tokens based on elapsed time
	bucket.tokens = math.Min(bucket.capacity, bucket.tokens+elapsed*bucket.refillRate)

	limit := int(bucket.capacity)

	if bucket.tokens >= 1.0 {
		bucket.tokens -= 1.0
		remaining := int(math.Floor(bucket.tokens))
		return true, RateLimitResult{
			Allowed:    true,
			Limit:      limit,
			Remaining:  remaining,
			ResetAfter: 0,
		}, nil
	}

	// Calculate time until at least 1 token is available
	missing := 1.0 - bucket.tokens
	secondsToRefill := missing / bucket.refillRate
	resetAfter := time.Duration(secondsToRefill * float64(time.Second))
	if resetAfter < time.Millisecond {
		resetAfter = time.Millisecond
	}

	return false, RateLimitResult{
		Allowed:    false,
		Limit:      limit,
		Remaining:  0,
		ResetAfter: resetAfter,
	}, nil
}
