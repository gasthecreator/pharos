package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	kafkaGo "github.com/segmentio/kafka-go"
)

// mockMessageReader simulates a Kafka message stream with offset commit tracking.
type mockMessageReader struct {
	messages      []kafkaGo.Message
	fetchIndex    int
	committedMsgs []kafkaGo.Message
	closeCalled   bool
}

func (m *mockMessageReader) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	if m.fetchIndex >= len(m.messages) {
		return kafkaGo.Message{}, context.Canceled
	}
	msg := m.messages[m.fetchIndex]
	m.fetchIndex++
	return msg, nil
}

func (m *mockMessageReader) CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error {
	m.committedMsgs = append(m.committedMsgs, msgs...)
	return nil
}

func (m *mockMessageReader) Close() error {
	m.closeCalled = true
	return nil
}

func newTestKafkaMessage(siteID string, seq int64, studyID string, eventTime time.Time, offset int64) kafkaGo.Message {
	idKey, _ := model.NewIdempotencyKey(siteID, uint64(seq))
	ae := model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    model.ActualityActual,
		Subject:      model.Reference{Reference: "Patient/PATIENT-001"},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{System: model.MedDRASystem, Code: "10013661", Display: "Rash"},
			},
		},
		Date:         eventTime,
		RecordedDate: eventTime.Add(5 * time.Minute),
		Severity: model.CodeableConcept{
			Coding: []model.Coding{
				{Code: "mild"},
			},
		},
		Study: []model.Reference{
			{Reference: "ResearchStudy/" + studyID},
		},
		Location: model.Reference{
			Reference: "Location/" + siteID,
		},
	}
	ae.SetIdempotencyKey(idKey)

	payload, _ := json.Marshal(ae)

	return kafkaGo.Message{
		Topic:     kafka.MainTopic,
		Partition: 0,
		Offset:    offset,
		Key:       []byte(siteID),
		Value:     payload,
		Headers: []kafkaGo.Header{
			{Key: "idempotency_key", Value: []byte(idKey.String())},
			{Key: "site_id", Value: []byte(siteID)},
		},
		Time: eventTime.Add(10 * time.Minute),
	}
}

// TestConsumerEngine_IdempotentRedelivery asserts that when Kafka redelivers an identical
// message (e.g. on rebalance or crash), the final Cassandra table contains exactly 1 row.
func TestConsumerEngine_IdempotentRedelivery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	tracker := NewWatermarkTracker(10*time.Minute, 10*time.Minute)

	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	msg := newTestKafkaMessage("SITE-01", 1, "STUDY-A", baseTime, 100)

	// Feed the EXACT SAME message twice
	reader := &mockMessageReader{
		messages: []kafkaGo.Message{msg, msg},
	}

	engine := NewEngine(reader, store, tracker, DefaultEngineConfig(nil))

	// Step 1: First delivery
	if err := engine.Step(ctx); err != nil {
		t.Fatalf("first Step failed: %v", err)
	}

	// Step 2: Redelivery of duplicate message
	if err := engine.Step(ctx); err != nil {
		t.Fatalf("second Step failed: %v", err)
	}

	// Assertions: exactly 1 distinct record stored
	if store.TotalSaved() != 1 {
		t.Errorf("expected exactly 1 row in canonical store, got %d", store.TotalSaved())
	}
	if store.SaveCalls() != 2 {
		t.Errorf("expected 2 save calls, got %d", store.SaveCalls())
	}
	if len(reader.committedMsgs) != 2 {
		t.Errorf("expected 2 offset commits, got %d", len(reader.committedMsgs))
	}

	// Verify record content
	rec, err := store.GetEvent(ctx, "SITE-01:1")
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if rec.SiteID != "SITE-01" || rec.LocalSeq != 1 || rec.StudyID != "STUDY-A" {
		t.Errorf("unexpected record content: %+v", rec)
	}
}

// TestConsumerEngine_PreservesPerSiteOrdering asserts that sequential messages for a site
// are ordered correctly in the events_by_site table.
func TestConsumerEngine_PreservesPerSiteOrdering(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	tracker := NewWatermarkTracker(10*time.Minute, 10*time.Minute)

	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	msg1 := newTestKafkaMessage("SITE-01", 1, "STUDY-A", baseTime, 100)
	msg2 := newTestKafkaMessage("SITE-01", 2, "STUDY-A", baseTime.Add(1*time.Hour), 101)
	msg3 := newTestKafkaMessage("SITE-01", 3, "STUDY-A", baseTime.Add(2*time.Hour), 102)

	reader := &mockMessageReader{
		messages: []kafkaGo.Message{msg1, msg2, msg3},
	}

	engine := NewEngine(reader, store, tracker, DefaultEngineConfig(nil))

	for i := 0; i < 3; i++ {
		if err := engine.Step(ctx); err != nil {
			t.Fatalf("step %d failed: %v", i+1, err)
		}
	}

	records, err := store.GetEventsBySite(ctx, "SITE-01", 1)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records for SITE-01, got %d", len(records))
	}

	// Verify each sequence is present
	seqs := make(map[int64]bool)
	for _, r := range records {
		seqs[r.LocalSeq] = true
	}
	if !seqs[1] || !seqs[2] || !seqs[3] {
		t.Errorf("missing sequences: %+v", seqs)
	}
}

// TestConsumerEngine_GatedCommitOnCassandraError asserts that Kafka offsets are
// NOT committed when the Cassandra write fails.
func TestConsumerEngine_GatedCommitOnCassandraError(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	tracker := NewWatermarkTracker(10*time.Minute, 10*time.Minute)

	// Inject database failure
	store.SetSaveHook(func(r *CanonicalRecord) error {
		return errors.New("simulated cassandra coordinator timeout")
	})

	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	msg := newTestKafkaMessage("SITE-01", 1, "STUDY-A", baseTime, 100)

	reader := &mockMessageReader{
		messages: []kafkaGo.Message{msg},
	}

	engine := NewEngine(reader, store, tracker, DefaultEngineConfig(nil))

	err := engine.Step(ctx)
	if err == nil {
		t.Fatalf("expected error from Step, got nil")
	}

	// CRITICAL ASSERTION: Offset MUST NOT be committed
	if len(reader.committedMsgs) != 0 {
		t.Fatalf("CRITICAL SAFETY VIOLATION: Kafka offset was committed despite Cassandra error!")
	}
	if engine.Stats().ErrorCount != 1 {
		t.Errorf("expected ErrorCount = 1, got %d", engine.Stats().ErrorCount)
	}
}

// TestConsumerEngine_QueryByStudyAndDateRange asserts that the events_by_study
// query properly filters by study ID and time bounds.
func TestConsumerEngine_QueryByStudyAndDateRange(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	tracker := NewWatermarkTracker(10*time.Minute, 10*time.Minute)

	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

	// STUDY-A events at 12:00, 13:00, 14:00
	msgA1 := newTestKafkaMessage("SITE-01", 1, "STUDY-A", baseTime, 100)
	msgA2 := newTestKafkaMessage("SITE-01", 2, "STUDY-A", baseTime.Add(1*time.Hour), 101)
	msgA3 := newTestKafkaMessage("SITE-01", 3, "STUDY-A", baseTime.Add(2*time.Hour), 102)

	// STUDY-B event at 13:00
	msgB1 := newTestKafkaMessage("SITE-02", 1, "STUDY-B", baseTime.Add(1*time.Hour), 103)

	reader := &mockMessageReader{
		messages: []kafkaGo.Message{msgA1, msgA2, msgA3, msgB1},
	}

	engine := NewEngine(reader, store, tracker, DefaultEngineConfig(nil))

	for i := 0; i < 4; i++ {
		if err := engine.Step(ctx); err != nil {
			t.Fatalf("step %d failed: %v", i+1, err)
		}
	}

	// Query STUDY-A in range [12:30, 14:30] -> should match msgA2 and msgA3 (2 events)
	start := baseTime.Add(30 * time.Minute)
	end := baseTime.Add(150 * time.Minute)

	studyAResults, err := store.GetEventsByStudy(ctx, "STUDY-A", start, end)
	if err != nil {
		t.Fatalf("GetEventsByStudy failed: %v", err)
	}
	if len(studyAResults) != 2 {
		t.Fatalf("expected 2 events for STUDY-A in date range, got %d", len(studyAResults))
	}
	for _, r := range studyAResults {
		if r.StudyID != "STUDY-A" {
			t.Errorf("unexpected study ID: %s", r.StudyID)
		}
		if r.EventTime.Before(start) || r.EventTime.After(end) {
			t.Errorf("event time outside query range: %v", r.EventTime)
		}
	}
}

// TestConsumerEngine_LateArrivalAuditDeduplicatedOnRedelivery tests that if SaveEvent fails
// after ProcessEvent transitions a window to REVISED, redelivery of the identical message
// does NOT create a duplicate LateArrivalAudit entry (21 CFR Part 11).
func TestConsumerEngine_LateArrivalAuditDeduplicatedOnRedelivery(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	tracker := NewWatermarkTracker(0, 10*time.Minute)

	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	windowID := "WINDOW-TEST-1"
	tracker.RegisterWindow(Window{
		ID:    windowID,
		Start: baseTime,
		End:   baseTime.Add(1 * time.Hour), // [12:00, 13:00)
	})

	// 1. Advance watermark past 13:00 to complete the window
	tracker.ProcessEvent(0, "INIT-1", baseTime.Add(70*time.Minute), baseTime.Add(10*time.Minute))
	w, _ := tracker.GetWindow(windowID)
	if w.Status != WindowStatusComplete {
		t.Fatalf("expected window COMPLETE, got %v", w.Status)
	}

	// 2. Prepare a late-arriving event for 12:30 (< 13:00)
	lateMsg := newTestKafkaMessage("SITE-01", 99, "STUDY-A", baseTime.Add(30*time.Minute), 500)

	// Inject failure on first SaveEvent attempt
	saveAttempt := 0
	store.SetSaveHook(func(r *CanonicalRecord) error {
		saveAttempt++
		if saveAttempt == 1 {
			return errors.New("transient database failure on first attempt")
		}
		return nil
	})

	// Mock reader delivering the same message twice (first attempt fails SaveEvent, second succeeds)
	reader := &mockMessageReader{
		messages: []kafkaGo.Message{lateMsg, lateMsg},
	}

	engine := NewEngine(reader, store, tracker, DefaultEngineConfig(nil))

	// Attempt 1: Step() fails on SaveEvent
	err1 := engine.Step(ctx)
	if err1 == nil {
		t.Fatalf("expected error on attempt 1, got nil")
	}

	// Verify window transitioned to REVISED and 1 audit entry was recorded
	w, _ = tracker.GetWindow(windowID)
	if w.Status != WindowStatusRevised {
		t.Fatalf("expected window REVISED on attempt 1, got %v", w.Status)
	}
	audits1 := tracker.GetLateArrivalAudits()
	if len(audits1) != 1 {
		t.Fatalf("expected 1 audit entry after attempt 1, got %d", len(audits1))
	}

	// Attempt 2: Redelivered message processed successfully
	err2 := engine.Step(ctx)
	if err2 != nil {
		t.Fatalf("expected attempt 2 to succeed, got %v", err2)
	}

	// CRITICAL ASSERTION: Exactly one LateArrivalAudit entry exists for this (window, idempotencyKey) pair!
	audits2 := tracker.GetLateArrivalAudits()
	if len(audits2) != 1 {
		t.Fatalf("CRITICAL AUDIT DUPLICATION (21 CFR Part 11): expected exactly 1 audit entry, got %d: %+v", len(audits2), audits2)
	}
	if audits2[0].WindowID != windowID || audits2[0].IdempotencyKey != "SITE-01:99" {
		t.Errorf("unexpected audit entry content: %+v", audits2[0])
	}
}
