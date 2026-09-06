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

	// 6. Recent feed: ListRecentEvents (§2.4, Slice 21) -- the record's
	// ConsumedAt is time.Now().UTC(), so it lands in the current hour
	// bucket and must appear in a large-enough recent query. A high limit
	// (real traffic from other tests/processes may also be in the current
	// bucket) proves "contains this record," not "contains only this
	// record," since this test doesn't own the whole bucket exclusively.
	recent, err := store.ListRecentEvents(ctx, 500)
	if err != nil {
		t.Fatalf("ListRecentEvents failed: %v", err)
	}
	foundInRecent := false
	for _, r := range recent {
		if r.IdempotencyKey == idKey {
			foundInRecent = true
			if r.SiteID != siteID || r.StudyID != studyID || r.Severity != "moderate" || r.EventCode != "10013661" || r.Subject != "PATIENT-999" {
				t.Errorf("ListRecentEvents returned a summary record with mismatched fields: %+v", r)
			}
			break
		}
	}
	if !foundInRecent {
		t.Fatalf("expected %s to appear in ListRecentEvents (real events_recent write), it did not", idKey)
	}
}

func TestConsumerEngine_RealEndToEndKafkaAndCassandra(t *testing.T) {
	// 60s, not 30s (§2.4, audit remediation: Cassandra internode +
	// Kafka inter-broker TLS) -- this ctx's own deadline is also the budget
	// stepCtx below carves its own window out of, so real setup time
	// (Cassandra connect, Kafka publish) already spends part of a tight
	// budget before Step() ever runs. Previously bumped from 15s to 30s
	// after this test started failing intermittently specifically when run
	// as part of the full suite (never alone) once two new long-running
	// fault-injection tests extended the suite's total duration -- confirmed
	// via docker stats as genuine host CPU saturation (~750% across
	// containers on a machine with far fewer cores), not a correctness
	// regression. Bumped again here after enabling internode/inter-broker
	// TLS reproduced the identical failure shape even running this test
	// alone (each internode hop and inter-broker replication ack now pays a
	// real, measured TLS/crypto cost) -- confirmed the operations themselves
	// were still correct, not hanging, by rerunning with a 180s/170s budget
	// and observing a clean pass in ~15s; this is the same class of "real
	// infrastructure cost eating into a tight test budget" as the original
	// bump, not a new problem.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// NewKafkaReader (not a raw kafkaGo.NewReader) so the reader's Dialer
	// picks up engineCfg.TLS -- real Kafka's client-facing listener is
	// SSL-only as of Slice 15 (§2.4, ARCHITECTURE_PROPOSALS.md "Slice 15:
	// Auth & TLS"), and a plaintext Dialer against it fails with EOF.
	reader, err := NewKafkaReader(engineCfg)
	if err != nil {
		t.Fatalf("failed to create Kafka reader: %v", err)
	}
	defer reader.Close()

	tracker := NewWatermarkTracker(engineCfg.LatenessTolerance, engineCfg.IdleTimeout)
	engine := NewEngine(reader, store, tracker, engineCfg)

	// 5. Consume until our published message is processed
	stepCtx, stepCancel := context.WithTimeout(ctx, 50*time.Second)
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

// TestCassandraCanonicalStore_LateArrivalAuditPersistence proves the §2.4,
// audit remediation fix against real Cassandra: LateArrivalAudit entries
// are genuinely durable (not just held in WatermarkTracker's in-memory
// state, which is all Slice 13 originally provided despite this type's own
// stated 21 CFR Part 11 purpose), and SaveLateArrivalAudit is a true
// idempotent upsert keyed by (groupID, WindowID, IdempotencyKey) -- the
// exact property Engine.Step depends on to safely retry a persist that
// failed on a prior delivery of the same redelivered Kafka message.
func TestCassandraCanonicalStore_LateArrivalAuditPersistence(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultCassandraStoreConfig()

	store, err := NewCassandraCanonicalStore(cfg)
	if err != nil {
		t.Fatalf("failed to connect to live Cassandra container: %v", err)
	}
	defer store.Close()

	uniqueID := uuid.New().String()[:8]
	group := "test-late-audit-group-" + uniqueID
	audit := LateArrivalAudit{
		WindowID:           "WINDOW-" + uniqueID,
		IdempotencyKey:     "SITE-LATE-" + uniqueID + ":1",
		Partition:          0,
		EventTime:          time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC),
		ArrivedAt:          time.Date(2026, 8, 30, 13, 10, 0, 0, time.UTC),
		WatermarkAtArrival: time.Date(2026, 8, 30, 13, 5, 0, 0, time.UTC),
	}

	if err := store.SaveLateArrivalAudit(ctx, group, audit); err != nil {
		t.Fatalf("SaveLateArrivalAudit (1st write) failed: %v", err)
	}
	// Simulates a redelivered Kafka message retrying a persist that
	// previously failed after this same entry was already written once --
	// must be a harmless overwrite, never a second row.
	if err := store.SaveLateArrivalAudit(ctx, group, audit); err != nil {
		t.Fatalf("SaveLateArrivalAudit (idempotent retry) failed: %v", err)
	}

	entries, err := store.ListLateArrivalAudits(ctx, group, 50)
	if err != nil {
		t.Fatalf("ListLateArrivalAudits failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 durably persisted entry after 2 identical writes, got %d: %+v", len(entries), entries)
	}
	got := entries[0]
	if got.WindowID != audit.WindowID || got.IdempotencyKey != audit.IdempotencyKey || got.Partition != audit.Partition {
		t.Fatalf("persisted entry doesn't match what was saved: got %+v, want %+v", got, audit)
	}
	if !got.EventTime.Equal(audit.EventTime) || !got.ArrivedAt.Equal(audit.ArrivedAt) || !got.WatermarkAtArrival.Equal(audit.WatermarkAtArrival) {
		t.Fatalf("persisted timestamps don't round-trip: got %+v, want %+v", got, audit)
	}

	// A different consumer group must never see another group's entries --
	// this table is partitioned by group_id specifically so that holds.
	otherGroupEntries, err := store.ListLateArrivalAudits(ctx, "unrelated-group-"+uniqueID, 50)
	if err != nil {
		t.Fatalf("ListLateArrivalAudits (unrelated group) failed: %v", err)
	}
	if len(otherGroupEntries) != 0 {
		t.Fatalf("expected 0 entries for an unrelated group, got %d", len(otherGroupEntries))
	}
}
