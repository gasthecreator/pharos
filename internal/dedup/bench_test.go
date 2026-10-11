package dedup

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// Benchmarks of the in-memory outbox store only (the Cassandra LWT path is
// measured end to end by the k6 load test; see docs/benchmark-results.md).

func BenchmarkMemoryOutbox_ClaimAndPublish(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	payload := []byte(`{"resourceType":"AdverseEvent"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "SITE-B:" + strconv.Itoa(i+1)
		claim, err := store.InsertClaim(ctx, OutboxRecord{IdempotencyKey: key, SiteID: "SITE-B", LocalSeq: uint64(i + 1), Payload: payload}, DefaultLeaseTimeout)
		if err != nil || !claim.Acquired {
			b.Fatalf("claim: acquired=%v err=%v", claim.Acquired, err)
		}
		if err := store.MarkPublished(ctx, key, claim.ClaimedAt, "pharos.events.adverse", 0, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMemoryOutbox_DuplicateHit is the retry fast path: a key that is
// already PUBLISHED must be recognized and refused cheaply.
func BenchmarkMemoryOutbox_DuplicateHit(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	rec := OutboxRecord{IdempotencyKey: "SITE-B:1", SiteID: "SITE-B", LocalSeq: 1, Payload: []byte(`{}`)}
	claim, err := store.InsertClaim(ctx, rec, DefaultLeaseTimeout)
	if err != nil || !claim.Acquired {
		b.Fatal("seed claim failed")
	}
	if err := store.MarkPublished(ctx, rec.IdempotencyKey, claim.ClaimedAt, "t", 0, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := store.InsertClaim(ctx, rec, time.Hour)
		if err != nil || c.Acquired {
			b.Fatalf("duplicate was not recognized: acquired=%v err=%v", c.Acquired, err)
		}
	}
}
