package query

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/consumer"
	"github.com/gasthecreator/pharos/pkg/dedup"
	"github.com/gasthecreator/pharos/pkg/ingestion"
	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/model"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
	"github.com/google/uuid"
)

func TestQueryService_RealCassandraCanonicalQueries(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCassandraServiceConfig()

	svc, err := NewCassandraService(cfg)
	if err != nil {
		t.Fatalf("failed to connect CassandraService to live cluster: %v", err)
	}
	defer svc.Close()

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-QUERY-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-QUERY-%s", uniqueID)
	baseTime := time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)

	// 1. Seed two canonical events: seq 1 at 11:00, seq 2 at 12:00
	idKey1 := fmt.Sprintf("%s:1", siteID)
	idKey2 := fmt.Sprintf("%s:2", siteID)

	rec1 := &consumer.CanonicalRecord{
		IdempotencyKey: idKey1,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       1,
		EventTime:      baseTime,
		RecordedTime:   baseTime.Add(2 * time.Minute),
		IngestionTime:  time.Now().UTC(),
		ConsumedAt:     time.Now().UTC(),
		Severity:       "mild",
		EventCode:      "10013661",
		Subject:        "Patient/P-001",
		Payload:        `{"resourceType":"AdverseEvent","actuality":"actual"}`,
		KafkaTopic:     kafka.MainTopic,
		KafkaPartition: 0,
		KafkaOffset:    10,
		IsLate:         false,
	}

	rec2 := &consumer.CanonicalRecord{
		IdempotencyKey: idKey2,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       2,
		EventTime:      baseTime.Add(1 * time.Hour),
		RecordedTime:   baseTime.Add(62 * time.Minute),
		IngestionTime:  time.Now().UTC(),
		ConsumedAt:     time.Now().UTC(),
		Severity:       "severe",
		EventCode:      "10013662",
		Subject:        "Patient/P-002",
		Payload:        `{"resourceType":"AdverseEvent","actuality":"actual"}`,
		KafkaTopic:     kafka.MainTopic,
		KafkaPartition: 0,
		KafkaOffset:    11,
		IsLate:         false,
	}

	cStoreCfg := consumer.DefaultCassandraStoreConfig()
	cStore, err := consumer.NewCassandraCanonicalStore(cStoreCfg)
	if err != nil {
		t.Fatalf("failed to open canonical store: %v", err)
	}
	defer cStore.Close()

	if err := cStore.SaveEvent(ctx, rec1); err != nil {
		t.Fatalf("SaveEvent 1 failed: %v", err)
	}
	if err := cStore.SaveEvent(ctx, rec2); err != nil {
		t.Fatalf("SaveEvent 2 failed: %v", err)
	}

	// 2. Test GetEvent point lookup
	got1, err := svc.GetEvent(ctx, idKey1)
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if got1.IdempotencyKey != idKey1 || got1.StudyID != studyID || got1.Severity != "mild" {
		t.Errorf("GetEvent returned mismatch: %+v", got1)
	}

	// 3. Test GetEventsByStudy ("all events for trial X in date range Y")
	// Query range covering only rec1: [10:30, 11:30]
	range1Events, err := svc.GetEventsByStudy(ctx, studyID, baseTime.Add(-30*time.Minute), baseTime.Add(30*time.Minute))
	if err != nil {
		t.Fatalf("GetEventsByStudy range1 failed: %v", err)
	}
	if len(range1Events) != 1 || range1Events[0].IdempotencyKey != idKey1 {
		t.Errorf("expected only rec1 in range1, got %d events", len(range1Events))
	}

	// Query range covering both rec1 and rec2: [10:30, 12:30]
	allStudyEvents, err := svc.GetEventsByStudy(ctx, studyID, baseTime.Add(-30*time.Minute), baseTime.Add(90*time.Minute))
	if err != nil {
		t.Fatalf("GetEventsByStudy all failed: %v", err)
	}
	if len(allStudyEvents) != 2 {
		t.Errorf("expected 2 events for study, got %d", len(allStudyEvents))
	}

	// 4. Test GetEventsBySite ("all events from site Z")
	siteEvents, err := svc.GetEventsBySite(ctx, siteID, 1)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(siteEvents) != 2 {
		t.Errorf("expected 2 events for site, got %d", len(siteEvents))
	}
	// Verify descending local_seq order
	if siteEvents[0].LocalSeq != 2 || siteEvents[1].LocalSeq != 1 {
		t.Errorf("expected descending sequence order (2, 1), got (%d, %d)", siteEvents[0].LocalSeq, siteEvents[1].LocalSeq)
	}
}

func TestQueryService_RealCassandraDLQInspectionWithActualValidationFailure(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCassandraServiceConfig()

	svc, err := NewCassandraService(cfg)
	if err != nil {
		t.Fatalf("failed to connect CassandraService: %v", err)
	}
	defer svc.Close()

	// 1. Setup real Central Ingestion with real Cassandra Outbox and Kafka mock
	cCfg := dedup.DefaultCassandraConfig()
	outboxStore, err := dedup.NewCassandraOutboxStore(cCfg)
	if err != nil {
		t.Fatalf("failed to connect outbox store: %v", err)
	}
	defer outboxStore.Close()

	producer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := ingestion.NewHandlerWithOutbox(limiter, outboxStore, producer, 30*time.Second)

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-DLQ-%s", uniqueID)
	idKey, _ := model.NewIdempotencyKey(siteID, 99)

	// 2. Craft a payload that has valid idempotency key but ACTUALLY FAILS FHIR validation (§2.3):
	// - actuality is invalid ("invalid_not_actual")
	// - missing subject reference
	invalidAE := model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    "invalid_not_actual", // FAILS: must be "actual"
		Date:         time.Now().UTC(),
		RecordedDate: time.Now().UTC(),
		Severity:     model.CodeableConcept{Coding: []model.Coding{{Code: "mild"}}},
		Study:        []model.Reference{{Reference: "ResearchStudy/STUDY-001"}},
		Location:     model.Reference{Reference: "Location/" + siteID},
	}
	invalidAE.SetIdempotencyKey(idKey)
	payloadBytes, _ := json.Marshal(invalidAE)

	batchReq := ingestion.BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{payloadBytes},
	}
	reqBody, _ := json.Marshal(batchReq)

	// 3. Post to Central Ingestion endpoint
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.HandleEvents(w, req)

	// Verify ingestion responded with 422 Unprocessable Entity
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected HTTP 422 Unprocessable Entity from ingestion, got %d (body: %s)", w.Code, w.Body.String())
	}

	idKeyStr := idKey.String()

	// 4. Query DLQ through QueryService: GetDLQEvent point lookup
	dlqRec, err := svc.GetDLQEvent(ctx, idKeyStr)
	if err != nil {
		t.Fatalf("GetDLQEvent failed: %v", err)
	}

	// Assertions on actual validation failure details
	if dlqRec.IdempotencyKey != idKeyStr {
		t.Errorf("expected idempotency_key %s, got %s", idKeyStr, dlqRec.IdempotencyKey)
	}
	if dlqRec.SiteID != siteID {
		t.Errorf("expected site_id %s, got %s", siteID, dlqRec.SiteID)
	}
	if dlqRec.RejectionReason == "" || !strings.Contains(dlqRec.RejectionReason, "actuality") {
		t.Errorf("expected rejection reason containing 'actuality', got %s", dlqRec.RejectionReason)
	}
	if dlqRec.ValidationErrors == "" {
		t.Errorf("expected non-empty validation errors")
	}
	// Verify that specific validation error reasons were captured
	t.Logf("Captured validation errors: %s", dlqRec.ValidationErrors)

	// 5. Query DLQ by site: ListDLQEventsBySite
	siteDLQs, err := svc.ListDLQEventsBySite(ctx, siteID, 10)
	if err != nil {
		t.Fatalf("ListDLQEventsBySite failed: %v", err)
	}
	if len(siteDLQs) != 1 {
		t.Fatalf("expected 1 DLQ event for site %s, got %d", siteID, len(siteDLQs))
	}
	if siteDLQs[0].IdempotencyKey != idKeyStr {
		t.Errorf("mismatched record in ListDLQEventsBySite: got %s", siteDLQs[0].IdempotencyKey)
	}

	// 6. Query all DLQ events: ListAllDLQEvents
	allDLQs, err := svc.ListAllDLQEvents(ctx, 100)
	if err != nil {
		t.Fatalf("ListAllDLQEvents failed: %v", err)
	}
	found := false
	for _, r := range allDLQs {
		if r.IdempotencyKey == idKeyStr {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected idKey %s in ListAllDLQEvents, but not found", idKeyStr)
	}
}
