package consumer

import (
	"context"
	"testing"
	"time"
)

// TestMemoryCanonicalStore_SaveLateArrivalAudit_IdempotentUpsert proves
// MemoryCanonicalStore mirrors CassandraCanonicalStore's own upsert-by-key
// semantics (§2.4, audit remediation): saving the identical (group, window,
// idempotency key) entry twice -- the shape a redelivered Kafka message
// retrying a previously failed persist produces -- must overwrite in
// place, never append a duplicate.
func TestMemoryCanonicalStore_SaveLateArrivalAudit_IdempotentUpsert(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()

	audit := LateArrivalAudit{
		WindowID:           "WINDOW-1",
		IdempotencyKey:     "SITE-A:1",
		Partition:          0,
		EventTime:          time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC),
		ArrivedAt:          time.Date(2026, 8, 30, 13, 10, 0, 0, time.UTC),
		WatermarkAtArrival: time.Date(2026, 8, 30, 13, 5, 0, 0, time.UTC),
	}

	if err := store.SaveLateArrivalAudit(ctx, "group-1", audit); err != nil {
		t.Fatalf("1st SaveLateArrivalAudit failed: %v", err)
	}
	if err := store.SaveLateArrivalAudit(ctx, "group-1", audit); err != nil {
		t.Fatalf("2nd (idempotent retry) SaveLateArrivalAudit failed: %v", err)
	}

	entries, err := store.ListLateArrivalAudits(ctx, "group-1", 50)
	if err != nil {
		t.Fatalf("ListLateArrivalAudits failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 entry after 2 identical saves, got %d: %+v", len(entries), entries)
	}
}

// TestMemoryCanonicalStore_ListLateArrivalAudits_ScopedByGroup proves
// entries are partitioned per consumer group -- one group's late-arrival
// audit trail must never leak into another's.
func TestMemoryCanonicalStore_ListLateArrivalAudits_ScopedByGroup(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()

	auditA := LateArrivalAudit{WindowID: "WINDOW-A", IdempotencyKey: "SITE-A:1"}
	auditB := LateArrivalAudit{WindowID: "WINDOW-B", IdempotencyKey: "SITE-B:1"}

	if err := store.SaveLateArrivalAudit(ctx, "group-a", auditA); err != nil {
		t.Fatalf("SaveLateArrivalAudit(group-a) failed: %v", err)
	}
	if err := store.SaveLateArrivalAudit(ctx, "group-b", auditB); err != nil {
		t.Fatalf("SaveLateArrivalAudit(group-b) failed: %v", err)
	}

	entriesA, err := store.ListLateArrivalAudits(ctx, "group-a", 50)
	if err != nil {
		t.Fatalf("ListLateArrivalAudits(group-a) failed: %v", err)
	}
	if len(entriesA) != 1 || entriesA[0].IdempotencyKey != "SITE-A:1" {
		t.Fatalf("expected only group-a's own entry, got %+v", entriesA)
	}

	entriesB, err := store.ListLateArrivalAudits(ctx, "group-b", 50)
	if err != nil {
		t.Fatalf("ListLateArrivalAudits(group-b) failed: %v", err)
	}
	if len(entriesB) != 1 || entriesB[0].IdempotencyKey != "SITE-B:1" {
		t.Fatalf("expected only group-b's own entry, got %+v", entriesB)
	}
}

// TestMemoryCanonicalStore_ListLateArrivalAudits_UnknownGroup proves an
// unseen group returns an empty, non-error result -- a fresh consumer
// group with no late arrivals yet is not an error condition.
func TestMemoryCanonicalStore_ListLateArrivalAudits_UnknownGroup(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()

	entries, err := store.ListLateArrivalAudits(ctx, "never-seen-group", 50)
	if err != nil {
		t.Fatalf("expected no error for an unknown group, got %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries for an unknown group, got %d", len(entries))
	}
}
