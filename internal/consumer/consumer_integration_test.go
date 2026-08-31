package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/google/uuid"
	kafkaGo "github.com/segmentio/kafka-go"
)

func TestCassandraCanonicalStore_RealIntegration(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCassandraStoreConfig()

	store, err := NewCassandraCanonicalStore(cfg)
	if err != nil {
		t.Fatalf("failed to connect to live Cassandra container: %v", err)
	}
	defer store.Close()

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-INT-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-INT-%s", uniqueID)
	idKey := fmt.Sprintf("%s:1", siteID)
	eventTime := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)

	record := &CanonicalRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       1,
		EventTime:      eventTime,
		RecordedTime:   eventTime.Add(2 * time.Minute),
		IngestionTime:  time.Now().UTC(),
		Severity:       "moderate",
		EventCode:      "10013661",
		Subject:        "PATIENT-999",
		Payload:        `{"resourceType":"AdverseEvent","actuality":"actual"}`,
		KafkaTopic:     kafka.MainTopic,
		KafkaPartition: 0,
		KafkaOffset:    42,
		ConsumedAt:     time.Now().UTC(),
		IsLate:         false,
	}

	// 1. SaveEvent (executes parallel errgroup upserts across all 3 tables)
	if err := store.SaveEvent(ctx, record); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}

	// 2. Point lookup: GetEvent
	gotEvent, err := store.GetEvent(ctx, idKey)
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if gotEvent.IdempotencyKey != idKey || gotEvent.SiteID != siteID || gotEvent.StudyID != studyID {
		t.Errorf("GetEvent returned mismatch: %+v", gotEvent)
	}

	// 3. Clinical query: GetEventsByStudy
	startTime := eventTime.Add(-1 * time.Hour)
	endTime := eventTime.Add(1 * time.Hour)
	studyEvents, err := store.GetEventsByStudy(ctx, studyID, startTime, endTime)
	if err != nil {
		t.Fatalf("GetEventsByStudy failed: %v", err)
	}
	if len(studyEvents) != 1 {
		t.Fatalf("expected 1 event for study, got %d", len(studyEvents))
	}
	if studyEvents[0].IdempotencyKey != idKey {
		t.Errorf("expected idempotencyKey %s, got %s", idKey, studyEvents[0].IdempotencyKey)
	}

	// 4. Site query: GetEventsBySite
	siteEvents, err := store.GetEventsBySite(ctx, siteID, 1)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(siteEvents) != 1 {
		t.Fatalf("expected 1 event for site, got %d", len(siteEvents))
	}

	// 5. Idempotent Redelivery: re-save the exact same record
	if err := store.SaveEvent(ctx, record); err != nil {
		t.Fatalf("second SaveEvent failed: %v", err)
	}

	// Verify no duplicate rows in events_by_study
	studyEvents2, err := store.GetEventsByStudy(ctx, studyID, startTime, endTime)
	if err != nil {
		t.Fatalf("second GetEventsByStudy failed: %v", err)
	}
	if len(studyEvents2) != 1 {
		t.Fatalf("CRITICAL IDEMPOTENCY FAILURE: duplicate row created in events_by_study (count=%d)", len(studyEvents2))
	}
}

func TestConsumerEngine_RealEndToEndKafkaAndCassandra(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Connect to Cassandra
	cCfg := DefaultCassandraStoreConfig()
	store, err := NewCassandraCanonicalStore(cCfg)
	if err != nil {
		t.Fatalf("Cassandra connection failed: %v", err)
	}
	defer store.Close()

	// 2. Setup Kafka Producer
	kCfg := kafka.DefaultConfig([]string{"127.0.0.1:9092"})
	producer := kafka.NewWriterProducer(kCfg)
	defer producer.Close()

	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-E2E-%s", uniqueID)
	studyID := fmt.Sprintf("STUDY-E2E-%s", uniqueID)
	idKey, _ := model.NewIdempotencyKey(siteID, 1)
	eventTime := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)

	ae := model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    model.ActualityActual,
		Subject:      model.Reference{Reference: "Patient/P-100"},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{System: model.MedDRASystem, Code: "10013661", Display: "Rash"},
			},
		},
		Date:         eventTime,
		RecordedDate: eventTime.Add(1 * time.Minute),
		Severity: model.CodeableConcept{
			Coding: []model.Coding{{Code: "mild"}},
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

	// 3. Publish to Kafka topic pharos.events.adverse partitioned by site_id
	meta, err := producer.Publish(ctx, kafka.MainTopic, []byte(siteID), payload, map[string]string{
		"idempotency_key": idKey.String(),
		"site_id":         siteID,
	})
	if err != nil {
		t.Fatalf("Kafka publish failed: %v", err)
	}
	t.Logf("Published message to topic %s partition %d offset %d", meta.Topic, meta.Partition, meta.Offset)

	// 4. Initialize Consumer Engine with a dedicated test consumer group
	engineCfg := DefaultEngineConfig([]string{"127.0.0.1:9092"})
	engineCfg.GroupID = fmt.Sprintf("test-group-%s", uniqueID)
	engineCfg.LatenessTolerance = 10 * time.Minute
	engineCfg.IdleTimeout = 30 * time.Second

	reader := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:        engineCfg.Brokers,
		GroupID:        engineCfg.GroupID,
		Topic:          engineCfg.Topic,
		MinBytes:       10,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafkaGo.FirstOffset,
	})
	defer reader.Close()

	tracker := NewWatermarkTracker(engineCfg.LatenessTolerance, engineCfg.IdleTimeout)
	engine := NewEngine(reader, store, tracker, engineCfg)

	// 5. Consume until our published message is processed
	stepCtx, stepCancel := context.WithTimeout(ctx, 15*time.Second)
	defer stepCancel()

	var rec *CanonicalRecord
	for {
		if err := engine.Step(stepCtx); err != nil {
			t.Fatalf("engine.Step failed: %v", err)
		}

		got, err := store.GetEvent(ctx, idKey.String())
		if err == nil && got != nil {
			rec = got
			break
		}
	}

	// 6. Verify record in Cassandra across tables
	if rec.SiteID != siteID || rec.StudyID != studyID {
		t.Errorf("unexpected record data: %+v", rec)
	}
	if rec.KafkaTopic != kafka.MainTopic {
		t.Errorf("Kafka metadata mismatch: got topic=%s", rec.KafkaTopic)
	}

	// Verify clinical study query
	studyRecords, err := store.GetEventsByStudy(ctx, studyID, eventTime.Add(-10*time.Minute), eventTime.Add(10*time.Minute))
	if err != nil || len(studyRecords) != 1 {
		t.Fatalf("GetEventsByStudy failed (len=%d, err=%v)", len(studyRecords), err)
	}

	// Verify stats
	stats := engine.Stats()
	if stats.ConsumedCount == 0 || stats.CommittedCount == 0 {
		t.Errorf("unexpected stats: %+v", stats)
	}
}
