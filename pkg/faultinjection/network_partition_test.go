package faultinjection

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/dedup"
	"github.com/gasthecreator/pharos/pkg/edge"
	"github.com/gasthecreator/pharos/pkg/ingestion"
	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
	"github.com/google/uuid"
)

// networkState models the three conditions the edge<->Central Ingestion link can
// be in for this test (PLAN.md §2.1).
type networkState int32

const (
	// stateHealthy: request and response both succeed normally.
	stateHealthy networkState = iota
	// statePartitioned: the request never leaves the edge at all — a pure
	// network failure, simulating a site with zero connectivity.
	statePartitioned
	// stateResponseLost: the request IS delivered and fully processed by the
	// real Central Ingestion server (a real Cassandra write and a real Kafka
	// publish genuinely happen), but the response never makes it back to the
	// edge — simulating a partition healing asymmetrically. This is the harder
	// and more important case: the edge must retry something that Central
	// Ingestion already durably finished, and that retry must be a safe no-op.
	stateResponseLost
)

// partitionableTransport is a controllable edge.HTTPClient standing in for the
// real network link between the edge forwarder and Central Ingestion.
type partitionableTransport struct {
	state  atomic.Int32
	target http.Handler
}

func newPartitionableTransport(target http.Handler) *partitionableTransport {
	return &partitionableTransport{target: target}
}

func (p *partitionableTransport) setState(s networkState) {
	p.state.Store(int32(s))
}

func (p *partitionableTransport) Do(req *http.Request) (*http.Response, error) {
	switch networkState(p.state.Load()) {
	case statePartitioned:
		return nil, errors.New("simulated network partition: connection refused")
	case stateResponseLost:
		rec := httptest.NewRecorder()
		p.target.ServeHTTP(rec, req)
		return nil, errors.New("simulated network partition: response lost in transit")
	default:
		rec := httptest.NewRecorder()
		p.target.ServeHTTP(rec, req)
		return rec.Result(), nil
	}
}

// TestNetworkPartition_EdgeBuffersAndDrainsWithoutLossOrDuplication is the
// flagship fault-injection test for PLAN.md's Core Challenge #1: a trial site
// losing connectivity to Central Ingestion must not lose or corrupt data, and
// must not create duplicates once connectivity returns — even when the
// partition heals asymmetrically (Central Ingestion processes a request but the
// edge never sees the response, forcing a retry of an already-completed write).
//
// Runs against real Cassandra and real Kafka, exactly like the rest of this
// project's fault-relevant tests, because the property being proven (the
// claim/lease outbox's crash-recovery guarantee) only really means something
// when exercised against the real distributed storage it's built on.
func TestNetworkPartition_EdgeBuffersAndDrainsWithoutLossOrDuplication(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
	}
	if !isPortOpen("127.0.0.1", 9092) {
		t.Skip("skipping: Kafka port 9092 is not open on 127.0.0.1")
	}

	ctx := context.Background()
	siteID := fmt.Sprintf("SITE-PARTITION-%s", uuid.New().String()[:8])
	const numEvents = 5

	// 1. Real Central Ingestion, wired to real Cassandra + real Kafka.
	outboxStore, err := dedup.NewCassandraOutboxStore(dedup.DefaultCassandraConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra: %v", err)
	}
	defer outboxStore.Close()

	realProducer := kafka.NewWriterProducer(kafka.DefaultConfig([]string{"127.0.0.1:9092"}))
	producer := newCountingProducer(realProducer)
	defer producer.Close()

	limiter := ratelimit.NewTokenBucketLimiter(1000, 1000) // generous: not testing rate limiting here
	handler := ingestion.NewHandlerWithOutbox(limiter, outboxStore, producer, 5*time.Second)

	// 2. Controllable network link, starting fully partitioned.
	transport := newPartitionableTransport(handler)
	transport.setState(statePartitioned)

	// 3. Real edge collector: SQLite WAL store + forwarder, pointed at the
	// controllable transport instead of a real HTTP round-trip.
	store, err := edge.NewSQLiteStore(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil {
		t.Fatalf("failed to create edge store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// Note: Forwarder.CalculateBackoff floors every computed backoff at 100ms
	// regardless of BaseBackoff/MaxBackoff, so retry waits below must clear
	// that floor rather than these (still-small, just not tiny) configured values.
	fwdCfg := edge.DefaultForwarderConfig("http://fault-injection/api/v1/events", siteID)
	fwdCfg.BaseBackoff = 20 * time.Millisecond
	fwdCfg.MaxBackoff = 100 * time.Millisecond
	fwdCfg.BatchSize = numEvents
	forwarder := edge.NewForwarder(store, transport, fwdCfg)
	const retryWait = 150 * time.Millisecond // safely above the 100ms backoff floor

	// 4. Capture numEvents locally while the site is completely disconnected.
	idKeys := make([]string, 0, numEvents)
	for i := 1; i <= numEvents; i++ {
		rec, err := store.Enqueue(ctx, siteID, newFaultInjectionEvent(siteID, uint64(i)))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
		idKeys = append(idKeys, rec.IdempotencyKey)
	}

	// 5. While partitioned: every forward attempt must fail, and nothing may be
	// acknowledged or lost. This is the core §2.1 guarantee — durability never
	// depends on the network being up.
	for i := 0; i < 3; i++ {
		count, err := forwarder.Step(ctx)
		if err == nil {
			t.Fatalf("expected forwarder to report an error while partitioned, got nil (count=%d)", count)
		}
		if count != 0 {
			t.Fatalf("expected 0 events forwarded while partitioned, got %d", count)
		}
		time.Sleep(retryWait)
	}

	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.AcknowledgedCount != 0 {
		t.Fatalf("expected 0 acknowledged while partitioned, got %d", stats.AcknowledgedCount)
	}
	if stats.PendingCount+stats.FailedCount+stats.InFlightCount != numEvents {
		t.Fatalf("expected all %d events still accounted for locally, got pending=%d failed=%d in_flight=%d",
			numEvents, stats.PendingCount, stats.FailedCount, stats.InFlightCount)
	}
	for _, idKey := range idKeys {
		if producer.CountFor(idKey) != 0 {
			t.Fatalf("CRITICAL: event %s was published to Kafka while the site was still partitioned", idKey)
		}
	}

	// 6. Partition "heals" asymmetrically: Central Ingestion now fully
	// processes requests (real Cassandra write + real Kafka publish), but the
	// edge never sees the response. The edge must not be fooled into thinking
	// this succeeded, and — critically — must not have caused a duplicate
	// Kafka publish by the time the real retry happens in the next phase.
	transport.setState(stateResponseLost)
	time.Sleep(retryWait)
	count, err := forwarder.Step(ctx)
	if err == nil {
		t.Fatalf("expected forwarder to still report an error (response lost), got nil (count=%d)", count)
	}
	if count != 0 {
		t.Fatalf("expected 0 events acknowledged locally when the response is lost, got %d", count)
	}

	// Central Ingestion genuinely processed this batch server-side even though
	// the edge doesn't know it yet — verify that directly against Cassandra.
	for _, idKey := range idKeys {
		rec, err := outboxStore.GetOutboxRecord(ctx, idKey)
		if err != nil {
			t.Fatalf("expected %s to already be durably recorded server-side after the response-lost attempt: %v", idKey, err)
		}
		if rec.Status != dedup.StatusPublished {
			t.Fatalf("expected %s to already be PUBLISHED server-side after the response-lost attempt, got %s", idKey, rec.Status)
		}
	}
	statsAfterLostResponse, _ := store.GetStats(ctx)
	if statsAfterLostResponse.AcknowledgedCount != 0 {
		t.Fatalf("expected edge to still show 0 acknowledged (it never saw a response), got %d", statsAfterLostResponse.AcknowledgedCount)
	}

	// 7. Partition fully heals: the edge's next retry gets a real response.
	// Central Ingestion sees these idempotency keys are already PUBLISHED and
	// must not republish to Kafka — this retry-of-an-already-completed-write is
	// exactly the scenario the claim/lease outbox exists to make safe.
	transport.setState(stateHealthy)
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, err := store.GetStats(ctx)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}
		if s.AcknowledgedCount == numEvents {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for full drain after healing; stats=%+v", s)
		}
		time.Sleep(retryWait)
		_, _ = forwarder.Step(ctx)
	}

	// 8. Final verification: every event durably PUBLISHED exactly once in
	// Cassandra, and — the critical assertion — exactly one Kafka publish per
	// idempotency key despite the partition, the asymmetric healing, and every
	// retry along the way.
	for _, idKey := range idKeys {
		rec, err := outboxStore.GetOutboxRecord(ctx, idKey)
		if err != nil {
			t.Fatalf("GetOutboxRecord(%s) failed: %v", idKey, err)
		}
		if rec.Status != dedup.StatusPublished {
			t.Errorf("expected %s status PUBLISHED, got %s", idKey, rec.Status)
		}
		if rec.KafkaOffset == 0 && rec.KafkaTopic == "" {
			t.Errorf("expected %s to carry real Kafka publish metadata, got none", idKey)
		}
		if got := producer.CountFor(idKey); got != 1 {
			t.Fatalf("CRITICAL DUPLICATE PUBLISH: expected exactly 1 Kafka publish for %s across the whole partition/heal sequence, got %d", idKey, got)
		}
	}
}
