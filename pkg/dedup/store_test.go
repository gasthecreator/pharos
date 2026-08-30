package dedup

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryOutboxStore_InsertClaimHappyPath(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	rec := OutboxRecord{
		IdempotencyKey: "SITE-01:1",
		SiteID:         "SITE-01",
		LocalSeq:       1,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}

	claim, err := store.InsertClaim(ctx, rec, DefaultLeaseTimeout)
	if err != nil {
		t.Fatalf("InsertClaim failed: %v", err)
	}
	if !claim.Acquired {
		t.Fatalf("expected claim acquired = true")
	}
	if claim.Status != StatusPublishing {
		t.Errorf("expected status PUBLISHING, got %s", claim.Status)
	}

	// Mark published
	err = store.MarkPublished(ctx, "SITE-01:1", "test.topic", 0, 100)
	if err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	saved, err := store.GetOutboxRecord(ctx, "SITE-01:1")
	if err != nil {
		t.Fatalf("GetOutboxRecord failed: %v", err)
	}
	if saved.Status != StatusPublished {
		t.Errorf("expected saved status PUBLISHED, got %s", saved.Status)
	}
	if saved.KafkaTopic != "test.topic" || saved.KafkaOffset != 100 {
		t.Errorf("unexpected kafka metadata: %+v", saved)
	}
}

func TestMemoryOutboxStore_DuplicatePublishedIsNoOp(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	rec := OutboxRecord{
		IdempotencyKey: "SITE-01:2",
		SiteID:         "SITE-01",
		LocalSeq:       2,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}

	// 1st insert: succeeds
	claim1, _ := store.InsertClaim(ctx, rec, DefaultLeaseTimeout)
	if !claim1.Acquired {
		t.Fatalf("expected 1st claim acquired")
	}

	// Publish
	_ = store.MarkPublished(ctx, "SITE-01:2", "test.topic", 0, 101)

	// 2nd insert (simulate edge retry after successful publish)
	claim2, err := store.InsertClaim(ctx, rec, DefaultLeaseTimeout)
	if err != nil {
		t.Fatalf("2nd InsertClaim failed: %v", err)
	}
	if claim2.Acquired {
		t.Fatalf("expected 2nd claim acquired = false for already PUBLISHED record")
	}
	if claim2.Status != StatusPublished {
		t.Errorf("expected status PUBLISHED, got %s", claim2.Status)
	}
}

func TestMemoryOutboxStore_ActiveLeaseBlocksConcurrentClaim(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	rec := OutboxRecord{
		IdempotencyKey: "SITE-01:3",
		SiteID:         "SITE-01",
		LocalSeq:       3,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}

	// 1st insert: acquires claim
	claim1, _ := store.InsertClaim(ctx, rec, 30*time.Second)
	if !claim1.Acquired {
		t.Fatalf("expected 1st claim acquired")
	}

	// 2nd insert immediately after (simulate premature retry or racing concurrent duplicate)
	claim2, err := store.InsertClaim(ctx, rec, 30*time.Second)
	if err != nil {
		t.Fatalf("2nd InsertClaim failed: %v", err)
	}
	if claim2.Acquired {
		t.Fatalf("expected 2nd claim acquired = false because active lease is held")
	}
	if claim2.Status != StatusPublishing {
		t.Errorf("expected status PUBLISHING, got %s", claim2.Status)
	}
}

func TestMemoryOutboxStore_ExpiredLeaseCASSteal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	rec := OutboxRecord{
		IdempotencyKey: "SITE-01:4",
		SiteID:         "SITE-01",
		LocalSeq:       4,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}

	// 1st insert: acquires claim with a very short lease timeout
	shortLease := 20 * time.Millisecond
	claim1, _ := store.InsertClaim(ctx, rec, shortLease)
	if !claim1.Acquired {
		t.Fatalf("expected 1st claim acquired")
	}

	// Wait for lease to expire
	time.Sleep(30 * time.Millisecond)

	// 2nd insert (simulates sweeper or retry after crash)
	claim2, err := store.InsertClaim(ctx, rec, shortLease)
	if err != nil {
		t.Fatalf("2nd InsertClaim failed: %v", err)
	}
	if !claim2.Acquired {
		t.Fatalf("expected 2nd claim acquired = true via expired lease CAS steal")
	}
	if claim2.Status != StatusPublishing {
		t.Errorf("expected status PUBLISHING, got %s", claim2.Status)
	}
	if !claim2.ClaimedAt.After(claim1.ClaimedAt) {
		t.Errorf("expected updated claimed_at after steal")
	}
}

func TestMemoryOutboxStore_ConcurrentClaimsExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	const numGoroutines = 50
	idKey := "SITE-RACE:100"

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	winners := make([]bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		idx := i
		go func() {
			defer wg.Done()
			rec := OutboxRecord{
				IdempotencyKey: idKey,
				SiteID:         "SITE-RACE",
				LocalSeq:       100,
				Payload:        []byte(`{"resourceType":"AdverseEvent","concurrent":true}`),
			}
			claim, err := store.InsertClaim(ctx, rec, 30*time.Second)
			if err != nil {
				t.Errorf("concurrent InsertClaim failed: %v", err)
				return
			}
			if claim.Acquired {
				winners[idx] = true
			}
		}()
	}

	wg.Wait()

	totalWinners := 0
	for _, won := range winners {
		if won {
			totalWinners++
		}
	}

	if totalWinners != 1 {
		t.Fatalf("concurrency violation: expected exactly 1 winner out of %d, got %d", numGoroutines, totalWinners)
	}
}

func TestMemoryOutboxStore_DLQClaimSymmetricBehavior(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()

	dlqRec := DLQRecord{
		IdempotencyKey:   "SITE-DLQ:1",
		SiteID:           "SITE-DLQ",
		Payload:          []byte(`{"resourceType":"AdverseEvent","malformed":true}`),
		RejectionReason:  "missing subject reference",
		ValidationErrors: `[{"field":"subject","error":"required"}]`,
		RejectedAt:       time.Now().UTC(),
	}

	// 1st claim
	claim1, err := store.InsertDLQClaim(ctx, dlqRec, DefaultLeaseTimeout)
	if err != nil {
		t.Fatalf("InsertDLQClaim failed: %v", err)
	}
	if !claim1.Acquired {
		t.Fatalf("expected DLQ claim acquired = true")
	}

	// Mark DLQ published
	err = store.MarkDLQPublished(ctx, "SITE-DLQ:1", "pharos.events.dlq", 0, 55)
	if err != nil {
		t.Fatalf("MarkDLQPublished failed: %v", err)
	}

	// Duplicate DLQ claim
	claim2, _ := store.InsertDLQClaim(ctx, dlqRec, DefaultLeaseTimeout)
	if claim2.Acquired {
		t.Fatalf("expected duplicate DLQ claim acquired = false")
	}
	if claim2.Status != StatusPublished {
		t.Errorf("expected status PUBLISHED, got %s", claim2.Status)
	}

	saved, err := store.GetDLQRecord(ctx, "SITE-DLQ:1")
	if err != nil {
		t.Fatalf("GetDLQRecord failed: %v", err)
	}
	if saved.Status != StatusPublished || saved.RejectionReason != "missing subject reference" {
		t.Errorf("unexpected saved DLQ record: %+v", saved)
	}
}
