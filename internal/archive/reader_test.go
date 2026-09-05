package archive

import (
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
)

func TestReader_GetEventsByStudy(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	jan := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 10, 12, 0, 0, 0, time.UTC)

	inRange := &consumer.CanonicalRecord{IdempotencyKey: "SITE-A:1", StudyID: "STUDY-01", SiteID: "SITE-A", EventTime: jan}
	outOfRange := &consumer.CanonicalRecord{IdempotencyKey: "SITE-A:2", StudyID: "STUDY-01", SiteID: "SITE-A", EventTime: feb}

	if err := AppendRecord(cfg, KindByStudy, "STUDY-01", jan, inRange); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}
	if err := AppendRecord(cfg, KindByStudy, "STUDY-01", feb, outOfRange); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}

	reader := NewReader(cfg)
	results, err := reader.GetEventsByStudy("STUDY-01", jan.Add(-time.Hour), jan.Add(time.Hour))
	if err != nil {
		t.Fatalf("GetEventsByStudy failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 record within range, got %d", len(results))
	}
	if results[0].IdempotencyKey != "SITE-A:1" {
		t.Errorf("expected SITE-A:1, got %s", results[0].IdempotencyKey)
	}
}

func TestReader_GetEventsBySite_ScansAcrossStudies(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	month := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// The same site can appear across multiple studies' archives -- the
	// archive is physically laid out by study, not by site (see
	// reader.go's GetEventsBySite doc comment).
	rec1 := &consumer.CanonicalRecord{IdempotencyKey: "SITE-B:1", StudyID: "STUDY-01", SiteID: "SITE-B", LocalSeq: 1}
	rec2 := &consumer.CanonicalRecord{IdempotencyKey: "SITE-B:2", StudyID: "STUDY-02", SiteID: "SITE-B", LocalSeq: 2}
	other := &consumer.CanonicalRecord{IdempotencyKey: "SITE-C:1", StudyID: "STUDY-01", SiteID: "SITE-C", LocalSeq: 1}

	for _, r := range []*consumer.CanonicalRecord{rec1, other} {
		if err := AppendRecord(cfg, KindByStudy, "STUDY-01", month, r); err != nil {
			t.Fatalf("AppendRecord failed: %v", err)
		}
	}
	if err := AppendRecord(cfg, KindByStudy, "STUDY-02", month, rec2); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}

	reader := NewReader(cfg)
	results, err := reader.GetEventsBySite("SITE-B", 0)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 records for SITE-B across both studies, got %d: %+v", len(results), results)
	}
}

func TestReader_GetEvent_PointLookup(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	month := time.Now().UTC()
	rec := &consumer.CanonicalRecord{IdempotencyKey: "SITE-D:1", StudyID: "STUDY-03", SiteID: "SITE-D"}
	if err := AppendRecord(cfg, KindByStudy, "STUDY-03", month, rec); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}

	reader := NewReader(cfg)
	found, err := reader.GetEvent("SITE-D:1")
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if found == nil {
		t.Fatal("expected to find the archived event")
	}
	if found.StudyID != "STUDY-03" {
		t.Errorf("expected StudyID STUDY-03, got %s", found.StudyID)
	}

	notFound, err := reader.GetEvent("SITE-D:999")
	if err != nil {
		t.Fatalf("GetEvent for a missing key should not error, got: %v", err)
	}
	if notFound != nil {
		t.Errorf("expected nil for a genuinely missing key, got %+v", notFound)
	}
}

func TestReader_DLQ(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	month := time.Now().UTC()
	rec := &dedup.DLQRecord{IdempotencyKey: "SITE-E:1", SiteID: "SITE-E", RejectionReason: "test"}
	if err := AppendRecord(cfg, KindDLQBySite, "SITE-E", month, rec); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}

	reader := NewReader(cfg)

	list, err := reader.GetDLQEventsBySite("SITE-E", 10)
	if err != nil {
		t.Fatalf("GetDLQEventsBySite failed: %v", err)
	}
	if len(list) != 1 || list[0].IdempotencyKey != "SITE-E:1" {
		t.Fatalf("expected 1 archived DLQ record for SITE-E, got %+v", list)
	}

	found, err := reader.GetDLQEvent("SITE-E:1")
	if err != nil {
		t.Fatalf("GetDLQEvent failed: %v", err)
	}
	if found == nil || found.RejectionReason != "test" {
		t.Fatalf("expected to find the archived DLQ record with its reason intact, got %+v", found)
	}

	// DLQ archives are laid out by site_id, and idempotency_key already
	// embeds it -- GetDLQEvent should go straight to the right partition, no
	// cross-partition scan, so a key for a totally different site is a clean
	// miss without touching SITE-E's files at all.
	notFound, err := reader.GetDLQEvent("SITE-NEVER-ARCHIVED:1")
	if err != nil {
		t.Fatalf("GetDLQEvent for an unarchived site should not error, got: %v", err)
	}
	if notFound != nil {
		t.Errorf("expected nil for a genuinely unarchived site, got %+v", notFound)
	}
}
