package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/archive"
	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/google/uuid"
)

// TestArchivalLifecycle_RealCassandra is the flagship test for Slice 11: a
// canonical event and a DLQ record are seeded with an old timestamp, proven
// queryable from the hot tier, archived by the real archival job (moving
// them out of Cassandra entirely, not just copying), then proven still
// queryable through the exact same CassandraService the rest of the system
// uses -- now via its archive fallback instead of Cassandra. This is the
// property this slice actually exists to guarantee: aging out of the hot
// tier must never mean losing access to the record.
func TestArchivalLifecycle_RealCassandra(t *testing.T) {
	ctx := context.Background()
	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-ARCHIVE-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-ARCHIVE-%s", uniqueID)
	archiveDir := t.TempDir()

	oldEventTime := time.Now().UTC().AddDate(0, 0, -200) // 200 days ago
	cutoff := time.Now().UTC().AddDate(0, 0, -90)        // archive anything older than 90 days
	// dead_letter_events_by_site's rejected_at is a clustering (primary key)
	// column -- InsertDLQClaim always stamps it "now" internally, and
	// Cassandra doesn't allow updating a primary-key column's value via
	// UPDATE (it would need a delete+reinsert). Rather than add a
	// test-only backdating path to CassandraOutboxStore's public API, the
	// DLQ half of this test uses a future-dated cutoff instead: the freshly
	// -inserted record's real "now" timestamp is already older than that,
	// which exercises the exact same archival code path without needing to
	// fabricate history.
	dlqCutoff := time.Now().UTC().Add(time.Hour)

	// 1. Seed one canonical record directly (bypassing the consumer engine --
	// this test is about the archive lifecycle, not re-proving ingestion).
	canonicalStore, err := consumer.NewCassandraCanonicalStore(consumer.DefaultCassandraStoreConfig())
	if err != nil {
		t.Fatalf("failed to connect canonical store: %v", err)
	}
	defer canonicalStore.Close()

	idKey := fmt.Sprintf("%s:1", siteID)
	canonicalRec := &consumer.CanonicalRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       1,
		EventTime:      oldEventTime,
		RecordedTime:   oldEventTime,
		IngestionTime:  oldEventTime,
		Severity:       "severe",
		EventCode:      "10012345",
		Subject:        "SUBJ-ARCHIVE-TEST",
		Payload:        `{"resourceType":"AdverseEvent"}`,
	}
	if err := canonicalStore.SaveEvent(ctx, canonicalRec); err != nil {
		t.Fatalf("failed to seed canonical record: %v", err)
	}

	// 2. Seed one DLQ record directly, also with an old rejected_at.
	outboxStore, err := dedup.NewCassandraOutboxStore(dedup.DefaultCassandraConfig())
	if err != nil {
		t.Fatalf("failed to connect outbox store: %v", err)
	}
	defer outboxStore.Close()

	dlqKey := fmt.Sprintf("%s:2", siteID)
	dlqClaim, err := outboxStore.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey:  dlqKey,
		SiteID:          siteID,
		Payload:         []byte(`{"malformed":true}`),
		RejectionReason: "seeded for archive test",
	}, 30*time.Second)
	if err != nil || !dlqClaim.Acquired {
		t.Fatalf("failed to seed DLQ claim: %v (acquired=%v)", err, dlqClaim.Acquired)
	}
	if err := outboxStore.MarkDLQPublished(ctx, dlqKey, "pharos.events.dlq", 0, 0); err != nil {
		t.Fatalf("failed to mark seeded DLQ record published: %v", err)
	}

	// 3. Prove both are queryable from the hot tier before archival.
	svcCfg := DefaultCassandraServiceConfig()
	svcCfg.ArchiveDir = archiveDir
	svc, err := NewCassandraService(svcCfg)
	if err != nil {
		t.Fatalf("failed to connect CassandraService: %v", err)
	}
	defer svc.Close()

	if _, err := svc.GetEvent(ctx, idKey); err != nil {
		t.Fatalf("expected the seeded canonical record to be queryable before archival: %v", err)
	}
	if _, err := svc.GetDLQEvent(ctx, dlqKey); err != nil {
		t.Fatalf("expected the seeded DLQ record to be queryable before archival: %v", err)
	}

	// 4. Run the real archival job -- both canonical and DLQ.
	archiveCfg := archive.Config{Dir: archiveDir}
	canonArchived, canonFailed, err := archive.RunCanonical(ctx, canonicalStore, archiveCfg, cutoff, false)
	if err != nil {
		t.Fatalf("RunCanonical failed: %v", err)
	}
	if canonFailed > 0 {
		t.Fatalf("expected 0 canonical archival failures, got %d", canonFailed)
	}
	if canonArchived < 1 {
		t.Fatalf("expected at least 1 canonical record archived (the one seeded above), got %d", canonArchived)
	}

	dlqArchived, dlqFailed, err := archive.RunDLQ(ctx, outboxStore, archiveCfg, dlqCutoff, false)
	if err != nil {
		t.Fatalf("RunDLQ failed: %v", err)
	}
	if dlqFailed > 0 {
		t.Fatalf("expected 0 DLQ archival failures, got %d", dlqFailed)
	}
	if dlqArchived < 1 {
		t.Fatalf("expected at least 1 DLQ record archived (the one seeded above), got %d", dlqArchived)
	}

	// 5. Prove both are genuinely gone from Cassandra, not just copied.
	if _, err := canonicalStore.GetEvent(ctx, idKey); err == nil {
		t.Fatalf("expected the archived canonical record to be deleted from Cassandra, but GetEvent still found it")
	}
	if _, err := outboxStore.GetDLQRecord(ctx, dlqKey); err == nil {
		t.Fatalf("expected the archived DLQ record to be deleted from Cassandra, but GetDLQRecord still found it")
	}

	// 6. Prove it's still queryable through the same CassandraService --
	// now via the archive fallback, not Cassandra.
	archived, err := svc.GetEvent(ctx, idKey)
	if err != nil {
		t.Fatalf("expected the archived record to still be queryable via fallback: %v", err)
	}
	if archived.StudyID != studyID {
		t.Errorf("expected archived record's StudyID %s, got %s", studyID, archived.StudyID)
	}

	byStudy, err := svc.GetEventsByStudy(ctx, studyID, oldEventTime.Add(-time.Hour), oldEventTime.Add(time.Hour))
	if err != nil {
		t.Fatalf("GetEventsByStudy failed: %v", err)
	}
	if len(byStudy) != 1 || byStudy[0].IdempotencyKey != idKey {
		t.Errorf("expected GetEventsByStudy to find the archived record via fallback, got %+v", byStudy)
	}

	bySite, err := svc.GetEventsBySite(ctx, siteID, 0)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(bySite) != 1 || bySite[0].IdempotencyKey != idKey {
		t.Errorf("expected GetEventsBySite to find the archived record via fallback, got %+v", bySite)
	}

	archivedDLQ, err := svc.GetDLQEvent(ctx, dlqKey)
	if err != nil {
		t.Fatalf("expected the archived DLQ record to still be queryable via fallback: %v", err)
	}
	if archivedDLQ.RejectionReason != "seeded for archive test" {
		t.Errorf("expected the original rejection reason preserved through archival, got %q", archivedDLQ.RejectionReason)
	}

	dlqBySite, err := svc.ListDLQEventsBySite(ctx, siteID, 10)
	if err != nil {
		t.Fatalf("ListDLQEventsBySite failed: %v", err)
	}
	found := false
	for _, r := range dlqBySite {
		if r.IdempotencyKey == dlqKey {
			found = true
		}
	}
	if !found {
		t.Errorf("expected ListDLQEventsBySite to find the archived DLQ record via fallback, got %+v", dlqBySite)
	}
}
