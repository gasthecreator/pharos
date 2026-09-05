package faultinjection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/ingestion"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/gasthecreator/pharos/internal/ratelimit"
	"github.com/google/uuid"
)

// TestOutOfOrderDelivery_QueryLayerOrdersCorrectlyRegardlessOfArrival proves
// PLAN.md §2.4: arrival order at Central Ingestion and at the Kafka consumer
// must never matter for correctness — only the canonical schema's own
// clustering key determines query order. This submits one site's events to
// Central Ingestion in deliberately scrambled local_seq order (as could happen
// from concurrent forwarder retries, or a site's own local clock/queue
// hiccups), lets them flow through the real pipeline, and verifies
// events_by_site returns them correctly ordered regardless.
func TestOutOfOrderDelivery_QueryLayerOrdersCorrectlyRegardlessOfArrival(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
	}
	if !isPortOpen("127.0.0.1", 9092) {
		t.Skip("skipping: Kafka port 9092 is not open on 127.0.0.1")
	}

	ctx := context.Background()
	siteID := fmt.Sprintf("SITE-OOO-%s", uuid.New().String()[:8])

	// Deliberately scrambled local_seq order — not 1,2,3,4,5.
	arrivalOrder := []uint64{5, 2, 4, 1, 3}

	// 1. Real Central Ingestion, wired to real Cassandra + real Kafka.
	outboxStore, err := dedup.NewCassandraOutboxStore(dedup.DefaultCassandraConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra (outbox): %v", err)
	}
	defer outboxStore.Close()

	producer := kafka.NewWriterProducer(kafka.DefaultConfig([]string{"127.0.0.1:9092"}))
	defer producer.Close()

	limiter := ratelimit.NewTokenBucketLimiter(1000, 1000)
	handler := ingestion.NewHandlerWithOutbox(limiter, outboxStore, producer, 5*time.Second)
	server := httptest.NewServer(handler)
	defer server.Close()

	// 2. Build one batch whose events array is in scrambled local_seq order.
	rawEvents := make([]json.RawMessage, 0, len(arrivalOrder))
	expectedKeys := make(map[string]bool, len(arrivalOrder))
	for _, seq := range arrivalOrder {
		ev := newFaultInjectionEvent(siteID, seq)
		idKey, err := model.NewIdempotencyKey(siteID, seq)
		if err != nil {
			t.Fatalf("NewIdempotencyKey failed: %v", err)
		}
		ev.SetIdempotencyKey(idKey)
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal event failed: %v", err)
		}
		rawEvents = append(rawEvents, raw)
		expectedKeys[idKey.String()] = true
	}

	reqBody, err := json.Marshal(ingestion.BatchRequest{SiteID: siteID, Events: rawEvents})
	if err != nil {
		t.Fatalf("marshal batch request failed: %v", err)
	}

	// 3. Submit the single, out-of-order batch to the real handler.
	resp, err := http.Post(server.URL+"/api/v1/events", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST to Central Ingestion failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 for a fully valid out-of-order batch, got %d", resp.StatusCode)
	}

	var batchResp ingestion.BatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("decode batch response failed: %v", err)
	}
	if batchResp.Accepted != len(arrivalOrder) || batchResp.Rejected != 0 {
		t.Fatalf("expected all %d events accepted, got accepted=%d rejected=%d", len(arrivalOrder), batchResp.Accepted, batchResp.Rejected)
	}

	// 4. Real consumer, fresh disposable consumer group, draining the topic
	// (which may carry unrelated backlog from other test runs today — that's
	// fine, we only care about seeing every one of our own expected keys).
	canonicalStore, err := consumer.NewCassandraCanonicalStore(consumer.DefaultCassandraStoreConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra (canonical store): %v", err)
	}
	defer canonicalStore.Close()

	engineCfg := consumer.DefaultEngineConfig([]string{"127.0.0.1:9092"})
	engineCfg.GroupID = fmt.Sprintf("test-ooo-%s", uuid.New().String()[:8])
	engineCfg.LatenessTolerance = 10 * time.Minute
	engineCfg.IdleTimeout = 30 * time.Second

	// NewKafkaReader (not a raw kafkaGo.NewReader) so the reader's Dialer
	// picks up engineCfg.TLS -- real Kafka's client-facing listener is
	// SSL-only as of Slice 15 (§2.4, ARCHITECTURE_PROPOSALS.md "Slice 15:
	// Auth & TLS"), and a plaintext Dialer against it fails with EOF.
	reader, err := consumer.NewKafkaReader(engineCfg)
	if err != nil {
		t.Fatalf("failed to create Kafka reader: %v", err)
	}
	defer reader.Close()

	tracker := consumer.NewWatermarkTracker(engineCfg.LatenessTolerance, engineCfg.IdleTimeout)
	engine := consumer.NewEngine(reader, canonicalStore, tracker, engineCfg)

	// Drain until every expected key has been seen, bounded by a wall-clock
	// timeout so an unexpectedly large backlog fails loudly instead of hanging.
	seen := make(map[string]bool, len(expectedKeys))
	deadline := time.Now().Add(30 * time.Second)
	for len(seen) < len(expectedKeys) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out draining Kafka before seeing all expected keys; saw %d of %d", len(seen), len(expectedKeys))
		}
		stepCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := engine.Step(stepCtx)
		cancel()
		if err != nil {
			continue // likely just a poll timeout against backlog/empty topic; keep draining
		}
		stats := engine.Stats()
		_ = stats // consumed via side effects on canonicalStore; nothing to assert here directly

		// Check which of our expected keys have landed yet.
		for key := range expectedKeys {
			if seen[key] {
				continue
			}
			if _, err := canonicalStore.GetEvent(ctx, key); err == nil {
				seen[key] = true
			}
		}
	}

	// 5. The actual property under test: query events_by_site and verify the
	// returned order is strictly descending by local_seq — matching the
	// schema's clustering key — regardless of the scrambled arrival order.
	records, err := canonicalStore.GetEventsBySite(ctx, siteID, 1)
	if err != nil {
		t.Fatalf("GetEventsBySite failed: %v", err)
	}
	if len(records) != len(arrivalOrder) {
		t.Fatalf("expected %d records for site %s, got %d", len(arrivalOrder), siteID, len(records))
	}

	for i := 1; i < len(records); i++ {
		if records[i].LocalSeq >= records[i-1].LocalSeq {
			t.Fatalf("events_by_site not correctly ordered: record %d (local_seq=%d) is not strictly less than record %d (local_seq=%d) — arrival order was %v",
				i, records[i].LocalSeq, i-1, records[i-1].LocalSeq, arrivalOrder)
		}
	}
	if records[0].LocalSeq != 5 || records[len(records)-1].LocalSeq != 1 {
		t.Fatalf("expected records ordered 5..1 by clustering key, got first=%d last=%d", records[0].LocalSeq, records[len(records)-1].LocalSeq)
	}

	// 6. No data corruption: every record's idempotency key round-trips.
	for _, r := range records {
		expectedKey := fmt.Sprintf("%s:%d", siteID, r.LocalSeq)
		if r.IdempotencyKey != expectedKey {
			t.Errorf("record with local_seq=%d has mismatched idempotency_key: got %s, expected %s", r.LocalSeq, r.IdempotencyKey, expectedKey)
		}
	}
}
