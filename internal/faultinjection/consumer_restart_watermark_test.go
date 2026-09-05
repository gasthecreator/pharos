package faultinjection

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/google/uuid"
)

// publishFaultInjectionEvent builds and really publishes an AdverseEvent to
// Kafka with an explicit event_time independent of local_seq, so a test can
// construct a message that arrives *later* in Kafka offset order but carries
// an *earlier* clinical event_time -- exactly what Kafka resuming a consumer
// group from its last committed offset can hand a freshly restarted engine.
func publishFaultInjectionEvent(t *testing.T, ctx context.Context, producer kafka.Producer, topic, siteID string, localSeq uint64, eventTime time.Time) string {
	t.Helper()

	idKey, err := model.NewIdempotencyKey(siteID, localSeq)
	if err != nil {
		t.Fatalf("failed to build idempotency key: %v", err)
	}

	ae := model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    model.ActualityActual,
		Subject:      model.Reference{Reference: "Patient/FI-WATERMARK"},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{System: model.MedDRASystem, Code: "10012345", Display: "Consumer Restart Watermark Test Event"},
			},
			Text: "Consumer Restart Watermark Test Event",
		},
		Date:         eventTime,
		RecordedDate: time.Now().UTC(),
		Severity: model.CodeableConcept{
			Coding: []model.Coding{{Code: "moderate"}},
		},
		Study: []model.Reference{
			{Reference: "ResearchStudy/FAULT-INJECTION-WATERMARK"},
		},
		Location: model.Reference{Reference: "Location/" + siteID},
	}
	ae.SetIdempotencyKey(idKey)

	payload, err := json.Marshal(ae)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	if _, err := producer.Publish(ctx, topic, []byte(siteID), payload, map[string]string{
		"idempotency_key": idKey.String(),
		"site_id":         siteID,
	}); err != nil {
		t.Fatalf("failed to publish event: %v", err)
	}
	return idKey.String()
}

// TestConsumerRestart_WatermarkCheckpointPreventsRegression is the flagship
// fault-injection test for Slice 13 (ARCHITECTURE_PROPOSALS.md "Consumer
// Crash/Restart Watermark Continuity"): a real pharos-consumer engine
// processes a real event via real Kafka and real Cassandra, advancing its
// watermark well past baseline; a periodic checkpoint (the same call
// cmd/pharos-consumer's background goroutine makes) persists that state to
// Cassandra; the engine is then discarded entirely (standing in for a
// process crash -- no graceful shutdown save runs) and a brand-new engine
// with a brand-new tracker is constructed under the *same* Kafka consumer
// group. Kafka resumes from the last committed offset and delivers a second,
// real event whose event_time is *earlier* than the first -- exactly the
// scenario that makes an unpersisted watermark regress. The restored
// tracker's watermark must never drop below what was reported before the
// simulated crash, at any point: immediately after Restore, and after
// processing the replayed message.
func TestConsumerRestart_WatermarkCheckpointPreventsRegression(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
	}
	if !isPortOpen("127.0.0.1", 9092) {
		t.Skip("skipping: Kafka port 9092 is not open on 127.0.0.1")
	}

	ctx := context.Background()
	uniqueID := uuid.New().String()[:8]
	siteID := fmt.Sprintf("SITE-WATERMARK-RESTART-%s", uniqueID)
	groupID := fmt.Sprintf("test-watermark-restart-%s", uniqueID)
	// A dedicated, single-partition, per-test topic -- not the shared
	// kafka.MainTopic. A brand-new consumer group on MainTopic would have to
	// wade through this entire session's accumulated backlog (thousands of
	// messages from every other test) before ever reaching this test's own
	// two messages, since NewKafkaReader's StartOffset is FirstOffset (the
	// correct production default for a genuinely new group). A private topic
	// sidesteps that entirely: zero backlog, and a single partition
	// guarantees both of this test's same-keyed messages land on it in
	// publish order.
	testTopic := fmt.Sprintf("test-watermark-restart-%s", uniqueID)

	brokers := []string{"127.0.0.1:9092"}
	if err := kafka.EnsureTopics(ctx, brokers, []kafka.TopicConfig{{
		Name:        testTopic,
		Partitions:  1,
		Replication: 3,
		Retention:   time.Hour,
		RetentionMs: int64(time.Hour / time.Millisecond),
		MaxBytes:    1024 * 1024 * 1024,
	}}); err != nil {
		t.Fatalf("failed to create test topic: %v", err)
	}

	store, err := consumer.NewCassandraCanonicalStore(consumer.DefaultCassandraStoreConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra: %v", err)
	}
	defer store.Close()

	realProducer := kafka.NewWriterProducer(kafka.DefaultConfig(brokers))
	defer realProducer.Close()

	baseTime := time.Now().UTC().Add(-24 * time.Hour) // fixed reference point, well clear of any lateness tolerance
	latenessTolerance := 5 * time.Minute
	idleTimeout := 10 * time.Minute

	engineCfg := consumer.DefaultEngineConfig(brokers)
	engineCfg.Topic = testTopic
	engineCfg.GroupID = groupID
	engineCfg.LatenessTolerance = latenessTolerance
	engineCfg.IdleTimeout = idleTimeout
	engineCfg.PollTimeout = 10 * time.Second

	// 1. "Before the crash": publish and process one real event that advances
	// the watermark well past baseTime.
	key1 := publishFaultInjectionEvent(t, ctx, realProducer, testTopic, siteID, 1, baseTime.Add(60*time.Minute))

	tracker1 := consumer.NewWatermarkTracker(latenessTolerance, idleTimeout)
	reader1, err := consumer.NewKafkaReader(engineCfg)
	if err != nil {
		t.Fatalf("failed to build Kafka reader: %v", err)
	}
	engine1 := consumer.NewEngine(reader1, store, tracker1, engineCfg)

	stepCtx1, stepCancel1 := context.WithTimeout(ctx, 15*time.Second)
	if err := engine1.Step(stepCtx1); err != nil {
		stepCancel1()
		t.Fatalf("engine1.Step failed to process the first event: %v", err)
	}
	stepCancel1()

	if wm := tracker1.CurrentWatermark(time.Now().UTC()); wm.IsZero() {
		t.Fatalf("expected a non-zero watermark after processing the first event")
	}

	// 2. Simulate the periodic checkpoint goroutine firing once, then the
	// process crashing -- no graceful shutdown save, just discarding
	// everything in memory, exactly like a real crash would.
	//
	// preCrashWatermark is read from the checkpoint itself (already truncated
	// to millisecond precision by Snapshot, matching what Cassandra's
	// timestamp column can actually store), not a separate raw
	// CurrentWatermark() call -- comparing against anything finer than what
	// persistence can preserve would flag a spurious "regression" that's
	// really just storage-precision truncation, not a real one.
	preCrashCheckpoint := tracker1.Snapshot()
	preCrashWatermark := preCrashCheckpoint.PreviousEmitted
	if err := store.SaveWatermarkCheckpoint(ctx, groupID, preCrashCheckpoint); err != nil {
		t.Fatalf("failed to save watermark checkpoint: %v", err)
	}
	_ = reader1.Close() // tears down the test's Kafka connection; does not affect the already-saved checkpoint

	// 3. Publish the "replayed" event: a real Kafka message with a LATER
	// offset but an EARLIER event_time than what was already watermarked --
	// exactly what resuming from the last committed offset can produce.
	key2 := publishFaultInjectionEvent(t, ctx, realProducer, testTopic, siteID, 2, baseTime.Add(20*time.Minute))

	// 4. "The restart": brand-new tracker and engine, same consumer group.
	// Restore from the checkpoint that was actually persisted to Cassandra --
	// not an in-memory Go value carried over in the test process, a genuine
	// round trip through the real store.
	loadedCheckpoint, err := store.LoadWatermarkCheckpoint(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to load watermark checkpoint: %v", err)
	}
	if loadedCheckpoint == nil {
		t.Fatalf("expected a saved watermark checkpoint for group %s, got none", groupID)
	}

	tracker2 := consumer.NewWatermarkTracker(latenessTolerance, idleTimeout)
	tracker2.Restore(*loadedCheckpoint)

	// CRITICAL ASSERTION 1: immediately after restore, before any new event,
	// the watermark must already reflect the pre-crash floor, not zero.
	restoredWatermark := tracker2.CurrentWatermark(time.Now().UTC())
	if restoredWatermark.Before(preCrashWatermark) {
		t.Fatalf("CRITICAL REGRESSION: restored watermark %v is below pre-crash watermark %v", restoredWatermark, preCrashWatermark)
	}

	reader2, err := consumer.NewKafkaReader(engineCfg)
	if err != nil {
		t.Fatalf("failed to build Kafka reader: %v", err)
	}
	defer reader2.Close()
	engine2 := consumer.NewEngine(reader2, store, tracker2, engineCfg)

	stepCtx2, stepCancel2 := context.WithTimeout(ctx, 15*time.Second)
	if err := engine2.Step(stepCtx2); err != nil {
		stepCancel2()
		t.Fatalf("engine2.Step failed to process the replayed event: %v", err)
	}
	stepCancel2()

	// CRITICAL ASSERTION 2: after processing the real replayed message (whose
	// event_time is earlier than the pre-crash high point), the watermark
	// must still not have regressed below the pre-crash floor.
	postReplayWatermark := tracker2.CurrentWatermark(time.Now().UTC())
	if postReplayWatermark.Before(preCrashWatermark) {
		t.Fatalf("CRITICAL REGRESSION: watermark %v after replaying an earlier-event-time message dropped below pre-crash floor %v", postReplayWatermark, preCrashWatermark)
	}

	// 5. The replayed event must have been correctly flagged late relative to
	// the restored floor -- both in the engine's own counters and durably in
	// Cassandra, consistent with this project's 21 CFR Part 11 audit framing.
	if got := engine2.Stats().LateEventsCount; got != 1 {
		t.Errorf("expected exactly 1 late event recorded after replay, got %d", got)
	}
	rec2, err := store.GetEvent(ctx, key2)
	if err != nil {
		t.Fatalf("expected the replayed event to be durably saved: %v", err)
	}
	if !rec2.IsLate {
		t.Errorf("expected the replayed event (earlier event_time than the restored watermark floor) to be saved with IsLate=true")
	}

	// 6. Sanity: the first event is unaffected by any of this.
	rec1, err := store.GetEvent(ctx, key1)
	if err != nil {
		t.Fatalf("expected the first event to still be durably saved: %v", err)
	}
	if rec1.IsLate {
		t.Errorf("expected the first event to be IsLate=false (nothing was watermarked before it), got true")
	}
}
