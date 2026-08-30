package query

import (
	"context"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/consumer"
)

func TestMemoryService_CanonicalAndDLQQueries(t *testing.T) {
	ctx := context.Background()
	svc := NewMemoryService()

	baseTime := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)

	// 1. Seed canonical records
	rec1 := &consumer.CanonicalRecord{
		IdempotencyKey: "SITE-01:1",
		SiteID:         "SITE-01",
		StudyID:        "STUDY-001",
		LocalSeq:       1,
		EventTime:      baseTime,
		RecordedTime:   baseTime.Add(2 * time.Minute),
		Severity:       "moderate",
		EventCode:      "10013661",
		Subject:        "PATIENT-100",
		Payload:        `{"resourceType":"AdverseEvent"}`,
	}
	rec2 := &consumer.CanonicalRecord{
		IdempotencyKey: "SITE-01:2",
		SiteID:         "SITE-01",
		StudyID:        "STUDY-001",
		LocalSeq:       2,
		EventTime:      baseTime.Add(1 * time.Hour),
		RecordedTime:   baseTime.Add(62 * time.Minute),
		Severity:       "severe",
		EventCode:      "10013662",
		Subject:        "PATIENT-101",
		Payload:        `{"resourceType":"AdverseEvent"}`,
	}
	svc.CanonicalStore().SaveEvent(ctx, rec1)
	svc.CanonicalStore().SaveEvent(ctx, rec2)

	// 2. Seed DLQ record
	dlq1 := &DLQRecord{
		IdempotencyKey:   "SITE-01:99",
		SiteID:           "SITE-01",
		Payload:          `{"resourceType":"Invalid"}`,
		RejectionReason:  "FHIR validation failed",
		ValidationErrors: "missing required subject reference; actuality invalid",
		RejectedAt:       baseTime.Add(30 * time.Minute),
		Status:           "PUBLISHED",
		KafkaTopic:       "pharos.events.dlq",
		KafkaPartition:   0,
		KafkaOffset:      12,
	}
	svc.SaveDLQEvent(dlq1)

	// Test Point Lookup: GetEvent
	gotEvent, err := svc.GetEvent(ctx, "SITE-01:1")
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if gotEvent.Subject != "PATIENT-100" || gotEvent.StudyID != "STUDY-001" {
		t.Errorf("unexpected event content: %+v", gotEvent)
	}

	// Test Study Query: GetEventsByStudy
	studyEvents, err := svc.GetEventsByStudy(ctx, "STUDY-001", baseTime.Add(-10*time.Minute), baseTime.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("GetEventsByStudy failed: %v", err)
	}
	if len(studyEvents) != 2 {
		t.Fatalf("expected 2 study events, got %d", len(studyEvents))
	}

	// Test Site Query: GetEventsBySite
	siteEvents, err := svc.GetEventsBySite(ctx, "SITE-01", 1)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(siteEvents) != 2 {
		t.Fatalf("expected 2 site events, got %d", len(siteEvents))
	}

	// Test DLQ Point Lookup: GetDLQEvent
	gotDLQ, err := svc.GetDLQEvent(ctx, "SITE-01:99")
	if err != nil {
		t.Fatalf("GetDLQEvent failed: %v", err)
	}
	if gotDLQ.RejectionReason != "FHIR validation failed" || gotDLQ.ValidationErrors == "" {
		t.Errorf("unexpected DLQ record: %+v", gotDLQ)
	}

	// Test DLQ List by Site
	siteDLQs, err := svc.ListDLQEventsBySite(ctx, "SITE-01", 10)
	if err != nil {
		t.Fatalf("ListDLQEventsBySite failed: %v", err)
	}
	if len(siteDLQs) != 1 {
		t.Fatalf("expected 1 DLQ event for SITE-01, got %d", len(siteDLQs))
	}

	// Test DLQ List All
	allDLQs, err := svc.ListAllDLQEvents(ctx, 10)
	if err != nil {
		t.Fatalf("ListAllDLQEvents failed: %v", err)
	}
	if len(allDLQs) != 1 {
		t.Fatalf("expected 1 DLQ event in total, got %d", len(allDLQs))
	}
}
