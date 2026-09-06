package dedup

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
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
	err = store.MarkPublished(ctx, "SITE-01:1", claim.ClaimedAt, "test.topic", 0, 100)
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
	_ = store.MarkPublished(ctx, "SITE-01:2", claim1.ClaimedAt, "test.topic", 0, 101)

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
	err = store.MarkDLQPublished(ctx, "SITE-DLQ:1", claim1.ClaimedAt, "pharos.events.dlq", 0, 55)
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

// TestMemoryOutboxStore_MarkPublishedFencedAfterSteal proves the §2.4,
// Slice 22 fencing fix: a claimant whose lease expired and was legitimately
// stolen by another claimant can no longer finalize with its own (now
// stale) claim, and the record's Kafka lineage bookkeeping reflects
// whichever claimant's finalize actually won -- never silently
// overwritten by the loser. Before this fix, MarkPublished had no fencing
// at all (Memory) and only checked status, not claimed_at (Cassandra), so
// the stale claimant's finalize would have silently succeeded and
// clobbered the real one's Kafka coordinates.
func TestMemoryOutboxStore_MarkPublishedFencedAfterSteal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	simClock := clock.NewSimulated(time.Now().UTC())
	store.SetClock(simClock)

	rec := OutboxRecord{
		IdempotencyKey: "SITE-FENCE:1",
		SiteID:         "SITE-FENCE",
		LocalSeq:       1,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}
	shortLease := 30 * time.Second

	claim1, err := store.InsertClaim(ctx, rec, shortLease)
	if err != nil || !claim1.Acquired {
		t.Fatalf("1st InsertClaim failed: err=%v acquired=%v", err, claim1.Acquired)
	}

	// claim1 sits un-renewed past its lease -- presumed dead.
	simClock.Advance(shortLease + time.Second)

	claim2, err := store.InsertClaim(ctx, rec, shortLease)
	if err != nil || !claim2.Acquired {
		t.Fatalf("steal InsertClaim failed: err=%v acquired=%v", err, claim2.Acquired)
	}
	if claim2.ClaimedAt.Equal(claim1.ClaimedAt) {
		t.Fatalf("test setup: expected the steal to produce a new claimed_at")
	}

	// claim1's own (stale, late) finalize must be fenced off.
	err = store.MarkPublished(ctx, "SITE-FENCE:1", claim1.ClaimedAt, "stale.topic", 9, 999)
	if !errors.Is(err, ErrClaimSuperseded) {
		t.Fatalf("expected ErrClaimSuperseded for the stale claimant's finalize, got %v", err)
	}

	// claim2 (the legitimate current claimant) can still finalize normally.
	if err := store.MarkPublished(ctx, "SITE-FENCE:1", claim2.ClaimedAt, "real.topic", 0, 42); err != nil {
		t.Fatalf("expected the current claimant's MarkPublished to succeed, got %v", err)
	}

	saved, err := store.GetOutboxRecord(ctx, "SITE-FENCE:1")
	if err != nil {
		t.Fatalf("GetOutboxRecord failed: %v", err)
	}
	if saved.Status != StatusPublished {
		t.Fatalf("expected status PUBLISHED, got %s", saved.Status)
	}
	if saved.KafkaTopic != "real.topic" || saved.KafkaOffset != 42 {
		t.Fatalf("CRITICAL: expected the legitimate claimant's Kafka lineage (real.topic/42) to win, got %s/%d -- fencing did not prevent bookkeeping corruption", saved.KafkaTopic, saved.KafkaOffset)
	}
}

// TestMemoryOutboxStore_InsertDLQClaim_ReplayedIsNotStealable is a
// regression test pinning a real bug the §2.4/Slice 22 property test
// TestOutboxProperty_DLQExactlyOnceAndFencing found on its very first
// generated sequence: InsertDLQClaim's steal branch only checked
// `existing.Status == StatusPublished` before treating an
// already-existing record as an abandoned in-flight claim eligible for
// stealing -- meaning a REPLAYED record (a terminal state reached via
// MarkDLQReplayed, e.g. a client retrying a stale cached submission long
// after the original rejection was already fixed and replayed) fell
// through to the steal branch too. It didn't just report Acquired=true
// incorrectly -- it silently reset the REPLAYED record's claimed_at,
// corrupting state on a row that's supposed to be immutable once
// terminal. Fixed by checking `existing.Status != StatusPublishing`
// instead, mirroring the condition Cassandra's own real CAS steal query
// already used (`IF status = 'PUBLISHING'`) -- the in-memory store's own
// doc comment claims to "accurately simulate" that CAS semantics, and
// this specific check was the one place it didn't.
func TestMemoryOutboxStore_InsertDLQClaim_ReplayedIsNotStealable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	simClock := clock.NewSimulated(time.Now().UTC())
	store.SetClock(simClock)

	dlqRec := DLQRecord{
		IdempotencyKey:  "SITE-REPLAY-STEAL:1",
		SiteID:          "SITE-REPLAY-STEAL",
		Payload:         []byte(`{"malformed":true}`),
		RejectionReason: "seeded for regression test",
	}

	claim, err := store.InsertDLQClaim(ctx, dlqRec, 30*time.Second)
	if err != nil || !claim.Acquired {
		t.Fatalf("InsertDLQClaim failed: err=%v acquired=%v", err, claim.Acquired)
	}
	if err := store.MarkDLQPublished(ctx, dlqRec.IdempotencyKey, claim.ClaimedAt, "topic", 0, 1); err != nil {
		t.Fatalf("MarkDLQPublished failed: %v", err)
	}
	if err := store.MarkDLQReplayed(ctx, dlqRec.IdempotencyKey); err != nil {
		t.Fatalf("MarkDLQReplayed failed: %v", err)
	}

	// Long past any plausible lease timeout -- exactly the condition that
	// used to trip the erroneous steal branch.
	simClock.Advance(24 * time.Hour)

	replay, err := store.InsertDLQClaim(ctx, dlqRec, 30*time.Second)
	if err != nil {
		t.Fatalf("InsertDLQClaim after replay failed: %v", err)
	}
	if replay.Acquired {
		t.Fatalf("CRITICAL: InsertDLQClaim acquired a claim for an already-REPLAYED record -- a terminal record must never be steal-able")
	}
	if replay.Status != StatusReplayed {
		t.Errorf("expected reported status REPLAYED, got %s", replay.Status)
	}

	saved, err := store.GetDLQRecord(ctx, dlqRec.IdempotencyKey)
	if err != nil {
		t.Fatalf("GetDLQRecord failed: %v", err)
	}
	if saved.Status != StatusReplayed {
		t.Fatalf("expected the record to remain REPLAYED, got %s", saved.Status)
	}
	if !saved.ClaimedAt.Equal(claim.ClaimedAt) {
		t.Fatalf("CRITICAL: a terminal REPLAYED record's claimed_at was mutated by a no-op InsertDLQClaim call -- expected it to stay %v, got %v", claim.ClaimedAt, saved.ClaimedAt)
	}
}
