package ratelimit

import (
	"context"
	"strconv"
	"testing"
)

// In-memory token bucket only; the Redis-backed limiter needs a server and
// is deliberately not benchmarked here.

func BenchmarkTokenBucketAllow_SingleSite(b *testing.B) {
	l := NewTokenBucketLimiter(1e12, 1e12)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := l.Allow(ctx, "SITE-1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTokenBucketAllow_ManySites(b *testing.B) {
	l := NewTokenBucketLimiter(1e12, 1e12)
	ctx := context.Background()
	sites := make([]string, 1000)
	for i := range sites {
		sites[i] = "SITE-" + strconv.Itoa(i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := l.Allow(ctx, sites[i%len(sites)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTokenBucketAllow_ParallelSameSite(b *testing.B) {
	l := NewTokenBucketLimiter(1e12, 1e12)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := l.Allow(ctx, "SITE-1"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
