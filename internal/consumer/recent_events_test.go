package consumer

import (
	"context"
	"testing"
	"time"
)

// TestMemoryCanonicalStore_ListRecentEvents_NewestFirst proves the "what's
// come in recently, across every site" feed (§2.4, Slice 21) orders by
// ConsumedAt descending, not save order or event order.
func TestMemoryCanonicalStore_ListRecentEvents_NewestFirst(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	base := time.Now().UTC()

	// Saved in an order that deliberately does not match ConsumedAt order.
	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-A:1", ConsumedAt: base.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}
	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-B:1", ConsumedAt: base}); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}
	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-C:1", ConsumedAt: base.Add(-1 * time.Minute)}); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}

	recent, err := store.ListRecentEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ListRecentEvents failed: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("expected 3 recent events, got %d", len(recent))
	}
	wantOrder := []string{"SITE-B:1", "SITE-C:1", "SITE-A:1"}
	for i, want := range wantOrder {
		if recent[i].IdempotencyKey != want {
			t.Errorf("position %d: expected %s, got %s", i, want, recent[i].IdempotencyKey)
		}
	}
}

// TestMemoryCanonicalStore_ListRecentEvents_RespectsLimit proves the feed
// caps results rather than returning everything ever saved.
func TestMemoryCanonicalStore_ListRecentEvents_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	base := time.Now().UTC()

	for i := 0; i < 5; i++ {
		key := "SITE-A:" + string(rune('1'+i))
		if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: key, ConsumedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatalf("SaveEvent failed: %v", err)
		}
	}

	recent, err := store.ListRecentEvents(ctx, 2)
	if err != nil {
		t.Fatalf("ListRecentEvents failed: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("expected limit=2 to cap results at 2, got %d", len(recent))
	}
	// The two most recently consumed: index 4 then index 3.
	if recent[0].IdempotencyKey != "SITE-A:5" || recent[1].IdempotencyKey != "SITE-A:4" {
		t.Errorf("expected the 2 newest records, got %s, %s", recent[0].IdempotencyKey, recent[1].IdempotencyKey)
	}
}

// TestMemoryCanonicalStore_ListRecentEvents_UpsertUpdatesConsumedAt proves a
// re-consumed (redelivered) event moves in the feed to its new ConsumedAt
// rather than appearing twice or staying pinned at its original position --
// SaveEvent's existing per-store idempotent-upsert behavior (already relied
// on by byStudy/bySite) must hold for the recent feed too.
func TestMemoryCanonicalStore_ListRecentEvents_UpsertUpdatesConsumedAt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	base := time.Now().UTC()

	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-A:1", ConsumedAt: base.Add(-10 * time.Minute)}); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}
	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-B:1", ConsumedAt: base}); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}
	// Redeliver SITE-A:1 with a fresh ConsumedAt, newer than SITE-B:1's.
	if err := store.SaveEvent(ctx, &CanonicalRecord{IdempotencyKey: "SITE-A:1", ConsumedAt: base.Add(1 * time.Minute)}); err != nil {
		t.Fatalf("SaveEvent (redelivery) failed: %v", err)
	}

	recent, err := store.ListRecentEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ListRecentEvents failed: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("expected exactly 2 distinct entries (no duplicate from redelivery), got %d", len(recent))
	}
	if recent[0].IdempotencyKey != "SITE-A:1" {
		t.Errorf("expected the redelivered event to now be newest, got %s first", recent[0].IdempotencyKey)
	}
}
