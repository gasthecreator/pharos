package edge

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/model"
)

func newTestEvent(siteID string) *model.AdverseEvent {
	now := time.Now().UTC()
	return &model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    model.ActualityActual,
		Subject: model.Reference{
			Reference: "Patient/SUBJ-12345",
		},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{
					System:  model.MedDRASystem,
					Code:    "10012345",
					Display: "Severe Nausea",
				},
			},
			Text: "Severe Nausea",
		},
		Date:         now.Add(-10 * time.Minute),
		RecordedDate: now,
		Severity: model.CodeableConcept{
			Coding: []model.Coding{
				{Code: "severe"},
			},
		},
		Study: []model.Reference{
			{Reference: "ResearchStudy/PHAROS-01"},
		},
		Location: model.Reference{
			Reference: "Location/" + siteID,
		},
	}
}

func setupTestStore(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "edge_test.db")

	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	return store, dbPath
}

func TestSQLiteStore_EnqueueAndFetch(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-NG-01"

	event := newTestEvent(siteID)
	record, err := store.Enqueue(ctx, siteID, event)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if record.SiteID != siteID {
		t.Errorf("expected siteID %s, got %s", siteID, record.SiteID)
	}
	if record.LocalSeq != 1 {
		t.Errorf("expected local_seq 1, got %d", record.LocalSeq)
	}
	if record.IdempotencyKey != "SITE-NG-01:1" {
		t.Errorf("expected idempotency_key SITE-NG-01:1, got %s", record.IdempotencyKey)
	}
	if record.Status != StatusPending {
		t.Errorf("expected status PENDING, got %s", record.Status)
	}

	// Fetch pending
	records, err := store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].IdempotencyKey != "SITE-NG-01:1" {
		t.Errorf("fetched record key mismatch: %s", records[0].IdempotencyKey)
	}
}

func TestSQLiteStore_DurabilityAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "durability.db")
	siteID := "SITE-ZA-01"

	// 1. Open store, write 5 events
	store1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to open store1: %v", err)
	}

	for i := 1; i <= 5; i++ {
		ev := newTestEvent(siteID)
		rec, err := store1.Enqueue(ctx, siteID, ev)
		if err != nil {
			t.Fatalf("failed to enqueue event %d: %v", i, err)
		}
		if rec.LocalSeq != uint64(i) {
			t.Fatalf("expected seq %d, got %d", i, rec.LocalSeq)
		}
	}

	// Verify WAL file exists while open
	walPath := dbPath + "-wal"
	if _, err := os.Stat(walPath); err == nil {
		t.Logf("verified SQLite WAL file exists: %s", walPath)
	}

	// Close store1
	if err := store1.Close(); err != nil {
		t.Fatalf("failed to close store1: %v", err)
	}

	// 2. Simulate process restart: open store2 on same file
	store2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to open store2: %v", err)
	}
	defer store2.Close()

	// Verify all 5 records survived with state intact
	records, err := store2.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("store2 FetchPending failed: %v", err)
	}
	if len(records) != 5 {
		t.Fatalf("expected 5 records after restart, got %d", len(records))
	}

	for i, r := range records {
		expectedSeq := uint64(i + 1)
		if r.LocalSeq != expectedSeq {
			t.Errorf("record %d has seq %d, expected %d", i, r.LocalSeq, expectedSeq)
		}
	}

	// Enqueue a 6th event; sequence must resume at 6
	ev6 := newTestEvent(siteID)
	rec6, err := store2.Enqueue(ctx, siteID, ev6)
	if err != nil {
		t.Fatalf("failed to enqueue after restart: %v", err)
	}
	if rec6.LocalSeq != 6 {
		t.Errorf("expected sequence to resume at 6, got %d", rec6.LocalSeq)
	}
}

func TestSQLiteStore_ConcurrentEnqueueMonotonicSequences(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-IN-03"

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	type enqueueResult struct {
		seq uint64
		err error
	}
	results := make(chan enqueueResult, numGoroutines)

	startSignal := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			<-startSignal // ensure all goroutines hit the store concurrently

			ev := newTestEvent(siteID)
			rec, err := store.Enqueue(ctx, siteID, ev)
			if err != nil {
				results <- enqueueResult{err: err}
				return
			}
			results <- enqueueResult{seq: rec.LocalSeq}
		}()
	}

	close(startSignal)
	wg.Wait()
	close(results)

	// Collect sequence numbers
	seenSeqs := make(map[uint64]bool)
	for res := range results {
		if res.err != nil {
			t.Fatalf("concurrent enqueue error: %v", res.err)
		}
		if seenSeqs[res.seq] {
			t.Fatalf("DUPLICATE sequence number detected: %d (violates §2.2 idempotency invariant)", res.seq)
		}
		seenSeqs[res.seq] = true
	}

	// Check that sequences are strictly 1 through numGoroutines with zero gaps
	for seq := uint64(1); seq <= numGoroutines; seq++ {
		if !seenSeqs[seq] {
			t.Fatalf("GAP in sequence numbers: missing sequence %d", seq)
		}
	}
}

func TestSQLiteStore_StateTransitionsAndBackoff(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-UK-02"

	// 1. Enqueue 2 events
	ev1 := newTestEvent(siteID)
	rec1, err := store.Enqueue(ctx, siteID, ev1)
	if err != nil {
		t.Fatalf("failed to enqueue ev1: %v", err)
	}

	ev2 := newTestEvent(siteID)
	rec2, err := store.Enqueue(ctx, siteID, ev2)
	if err != nil {
		t.Fatalf("failed to enqueue ev2: %v", err)
	}

	// 2. Fetch pending
	pending, err := store.FetchPending(ctx, 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d, err: %v", len(pending), err)
	}

	// 3. Mark in-flight
	if err := store.MarkInFlight(ctx, []int64{rec1.ID, rec2.ID}); err != nil {
		t.Fatalf("MarkInFlight failed: %v", err)
	}

	// FetchPending should now return 0 records
	pendingAfterFlight, err := store.FetchPending(ctx, 10)
	if err != nil || len(pendingAfterFlight) != 0 {
		t.Fatalf("expected 0 pending after marking in flight, got %d", len(pendingAfterFlight))
	}

	// 4. Mark rec1 as ACKNOWLEDGED
	if err := store.MarkAcknowledged(ctx, []int64{rec1.ID}); err != nil {
		t.Fatalf("MarkAcknowledged failed: %v", err)
	}

	// 5. Mark rec2 as FAILED with a 1-hour backoff
	if err := store.MarkFailed(ctx, rec2.ID, "HTTP 503 Service Unavailable", 1*time.Hour); err != nil {
		t.Fatalf("MarkFailed failed: %v", err)
	}

	// FetchPending should still return 0 because rec2's next_retry_at is in the future
	pendingBackedOff, err := store.FetchPending(ctx, 10)
	if err != nil || len(pendingBackedOff) != 0 {
		t.Fatalf("expected 0 pending due to future backoff, got %d", len(pendingBackedOff))
	}

	// 6. Check queue stats
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	if stats.AcknowledgedCount != 1 {
		t.Errorf("expected 1 acknowledged, got %d", stats.AcknowledgedCount)
	}
	if stats.FailedCount != 1 {
		t.Errorf("expected 1 failed, got %d", stats.FailedCount)
	}
	if stats.MaxSequence != 2 {
		t.Errorf("expected max sequence 2, got %d", stats.MaxSequence)
	}
}

// TestSQLiteStore_EnqueuePersistsMalformedEvent locks in PLAN.md §2.3: the edge
// collector must durably buffer even FHIR-invalid payloads rather than reject
// them, because validation is Central Ingestion's job, not the edge's. Rejecting
// here would silently and permanently drop the record with no DLQ trail.
func TestSQLiteStore_EnqueuePersistsMalformedEvent(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-BR-01"

	malformed := newTestEvent(siteID)
	malformed.Subject.Reference = ""                                 // required by the FHIR profile
	malformed.Severity = model.CodeableConcept{Text: "catastrophic"} // not a valid severity code

	if err := malformed.Validate(); err == nil {
		t.Fatal("test setup invalid: expected this event to fail FHIR validation")
	}

	record, err := store.Enqueue(ctx, siteID, malformed)
	if err != nil {
		t.Fatalf("Enqueue must durably buffer FHIR-invalid events, not reject them: %v", err)
	}
	if record.Status != StatusPending {
		t.Errorf("expected status PENDING, got %s", record.Status)
	}

	records, err := store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected the malformed event to be retrievable from the queue, got %d records", len(records))
	}
}

func TestSQLiteStore_FIFOOrder(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-JP-01"

	const total = 20
	for i := 0; i < total; i++ {
		ev := newTestEvent(siteID)
		_, err := store.Enqueue(ctx, siteID, ev)
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	// Fetch in batches of 5
	var retrievedSeqs []uint64
	for {
		batch, err := store.FetchPending(ctx, 5)
		if err != nil {
			t.Fatalf("FetchPending batch failed: %v", err)
		}
		if len(batch) == 0 {
			break
		}

		var ids []int64
		for _, r := range batch {
			retrievedSeqs = append(retrievedSeqs, r.LocalSeq)
			ids = append(ids, r.ID)
		}

		// Acknowledge this batch to fetch next
		if err := store.MarkAcknowledged(ctx, ids); err != nil {
			t.Fatalf("MarkAcknowledged failed: %v", err)
		}
	}

	if len(retrievedSeqs) != total {
		t.Fatalf("expected %d retrieved sequences, got %d", total, len(retrievedSeqs))
	}

	for i := 0; i < total; i++ {
		expected := uint64(i + 1)
		if retrievedSeqs[i] != expected {
			t.Errorf("FIFO order violation at index %d: expected %d, got %d", i, expected, retrievedSeqs[i])
		}
	}
}

func TestSQLiteStore_MarkRejected(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-REJ-STORE"

	rec1, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue 1 failed: %v", err)
	}
	rec2, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue 2 failed: %v", err)
	}

	// Mark rec1 acknowledged, rec2 rejected
	if err := store.MarkAcknowledged(ctx, []int64{rec1.ID}); err != nil {
		t.Fatalf("MarkAcknowledged failed: %v", err)
	}
	if err := store.MarkRejected(ctx, []int64{rec2.ID}, "FHIR schema validation failed: missing subject"); err != nil {
		t.Fatalf("MarkRejected failed: %v", err)
	}

	// Verify rejected record is terminal and not fetched by FetchPending
	pending, err := store.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("FetchPending failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending records, got %d", len(pending))
	}

	// Verify stats
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.AcknowledgedCount != 1 {
		t.Errorf("expected 1 acknowledged, got %d", stats.AcknowledgedCount)
	}
	if stats.RejectedCount != 1 {
		t.Errorf("expected 1 rejected, got %d", stats.RejectedCount)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending, got %d", stats.PendingCount)
	}
}
