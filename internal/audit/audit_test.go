package audit

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_RecordAndListByOperator(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.RecordAccess(ctx, AccessAudit{
		Operator: "alice", Action: "QUERY_EVENT", Resource: "SITE-A:1", Outcome: "SUCCESS",
	}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}
	if err := store.RecordAccess(ctx, AccessAudit{
		Operator: "alice", Action: "QUERY_STUDY", Resource: "STUDY-X", Outcome: "SUCCESS",
	}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}
	if err := store.RecordAccess(ctx, AccessAudit{
		Operator: "bob", Action: "QUERY_EVENT", Resource: "SITE-B:1", Outcome: "SUCCESS",
	}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}

	aliceEntries, err := store.ListByOperator(ctx, "alice", 10)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(aliceEntries) != 2 {
		t.Fatalf("expected 2 entries for alice, got %d", len(aliceEntries))
	}
	// Newest first.
	if aliceEntries[0].Action != "QUERY_STUDY" {
		t.Errorf("expected most recent entry first (QUERY_STUDY), got %s", aliceEntries[0].Action)
	}

	bobEntries, err := store.ListByOperator(ctx, "bob", 10)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(bobEntries) != 1 {
		t.Fatalf("expected 1 entry for bob (isolated from alice's), got %d", len(bobEntries))
	}
}

func TestMemoryStore_ListByOperator_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	for i := 0; i < 5; i++ {
		if err := store.RecordAccess(ctx, AccessAudit{
			Operator: "alice", Action: "QUERY_EVENT", Resource: "R", Outcome: "SUCCESS",
		}); err != nil {
			t.Fatalf("RecordAccess failed: %v", err)
		}
	}
	entries, err := store.ListByOperator(ctx, "alice", 3)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected limit=3 to cap results at 3, got %d", len(entries))
	}
}

func TestMemoryStore_RecordAccess_DefaultsOccurredAt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	before := time.Now().UTC()

	if err := store.RecordAccess(ctx, AccessAudit{Operator: "alice", Action: "QUERY_EVENT", Resource: "R", Outcome: "SUCCESS"}); err != nil {
		t.Fatalf("RecordAccess failed: %v", err)
	}

	entries, _ := store.ListByOperator(ctx, "alice", 1)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry")
	}
	if entries[0].OccurredAt.Before(before) {
		t.Errorf("expected OccurredAt to default to roughly now, got %v (before test started: %v)", entries[0].OccurredAt, before)
	}
}
