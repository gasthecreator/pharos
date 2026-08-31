package faultinjection

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/edge"
	"github.com/gasthecreator/pharos/internal/ingestion"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/ratelimit"
	"github.com/google/uuid"
)

// drainForwarder repeatedly steps the forwarder until every enqueued record is
// acknowledged or the deadline passes, matching the drain loop pattern already
// used by TestNetworkPartition_EdgeBuffersAndDrainsWithoutLossOrDuplication.
func drainForwarder(t *testing.T, ctx context.Context, store *edge.SQLiteStore, forwarder *edge.Forwarder, want int, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, err := store.GetStats(ctx)
		if err != nil {
			t.Fatalf("GetStats failed: %v", err)
		}
		if s.AcknowledgedCount == int64(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d acknowledged; stats=%+v", want, s)
		}
		time.Sleep(wait)
		_, _ = forwarder.Step(ctx)
	}
}

// TestEdgeInstanceLoss_DiskReplacementDoesNotDropNewEvents is the regression
// test for the Slice 8 bug (ARCHITECTURE_PROPOSALS.md "Idempotency Key
// Resilience Across Edge Instance Loss"): a trial site's disk failing and
// being replaced with the same --site-id produces a brand-new, empty edge
// SQLite file. Before the fix, local_seq restarted at 1 in that fresh file,
// colliding with idempotency keys the *original* hardware had already used —
// Central Ingestion's dedup layer, working exactly as designed, would treat
// the new events as already-published duplicates and never republish them.
//
// This test simulates exactly that: a first "instance" of the edge submits
// real events and drains them through the real pipeline, then a second,
// completely fresh SQLite file — same site_id, different local database —
// stands in for the replaced disk and submits new events of its own. Runs
// against real Cassandra and real Kafka: the property being proven (that the
// dedup layer treats these as genuinely new, not duplicates) only means
// something when checked against the real claim/lease outbox it's built on.
func TestEdgeInstanceLoss_DiskReplacementDoesNotDropNewEvents(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Skip("skipping: Cassandra port 9042 is not open on 127.0.0.1")
	}
	if !isPortOpen("127.0.0.1", 9092) {
		t.Skip("skipping: Kafka port 9092 is not open on 127.0.0.1")
	}

	ctx := context.Background()
	siteID := fmt.Sprintf("SITE-DISK-REPLACED-%s", uuid.New().String()[:8])
	const eventsPerInstance = 3
	const retryWait = 150 * time.Millisecond // clears Forwarder.CalculateBackoff's 100ms floor

	// Real Central Ingestion, wired to real Cassandra + real Kafka — shared
	// across both simulated instances, exactly as one real Central Ingestion
	// deployment would be shared across a site's hardware lifetime.
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

	runInstance := func(dbPath string, epochMinutes func() uint64, n int) []string {
		t.Helper()
		store, err := edge.NewSQLiteStoreWithEpochSource(dbPath, epochMinutes)
		if err != nil {
			t.Fatalf("failed to create edge store at %s: %v", dbPath, err)
		}
		t.Cleanup(func() { _ = store.Close() })

		httpTransport := newPartitionableTransport(handler)
		httpTransport.setState(stateHealthy)

		fwdCfg := edge.DefaultForwarderConfig("http://fault-injection/api/v1/events", siteID)
		fwdCfg.BaseBackoff = 20 * time.Millisecond
		fwdCfg.MaxBackoff = 100 * time.Millisecond
		fwdCfg.BatchSize = n
		forwarder := edge.NewForwarder(store, httpTransport, fwdCfg)

		idKeys := make([]string, 0, n)
		for i := 1; i <= n; i++ {
			rec, err := store.Enqueue(ctx, siteID, newFaultInjectionEvent(siteID, uint64(i)))
			if err != nil {
				t.Fatalf("enqueue %d failed: %v", i, err)
			}
			idKeys = append(idKeys, rec.IdempotencyKey)
		}

		drainForwarder(t, ctx, store, forwarder, n, retryWait)
		return idKeys
	}

	// 1. First "instance": the site's original hardware. Fixed epoch 100
	// (arbitrary but deterministic) stands in for "whenever this site was
	// first provisioned."
	instance1Keys := runInstance(filepath.Join(t.TempDir(), "instance1.db"), func() uint64 { return 100 }, eventsPerInstance)

	// 2. Disk replacement: a completely fresh SQLite file (different path —
	// nothing carries over from instance 1), same site_id. A different fixed
	// epoch (200, not 100) stands in for real time having passed between the
	// original provisioning and the replacement — sleeping past a real
	// wall-clock minute boundary would prove the same thing far more slowly
	// and no more rigorously, since the epoch source is exactly what's
	// under test here, not the wall clock itself.
	instance2Keys := runInstance(filepath.Join(t.TempDir(), "instance2-replaced-disk.db"), func() uint64 { return 200 }, eventsPerInstance)

	// 3. The actual regression check: no idempotency key collision between
	// the two "instances" of the same site.
	seen := make(map[string]bool, len(instance1Keys))
	for _, k := range instance1Keys {
		seen[k] = true
	}
	for _, k := range instance2Keys {
		if seen[k] {
			t.Fatalf("CRITICAL: instance 2 (replaced disk) issued idempotency key %s, colliding with instance 1 — this is the exact bug Slice 8 fixes", k)
		}
	}

	// 4. The actual bug's symptom, checked directly against real
	// infrastructure: every one of instance 2's "new hardware" events must
	// have reached Kafka exactly once — not zero times (silently dropped as
	// a duplicate) and not more than once.
	for _, k := range instance2Keys {
		rec, err := outboxStore.GetOutboxRecord(ctx, k)
		if err != nil {
			t.Fatalf("expected instance 2's event %s to be durably recorded: %v", k, err)
		}
		if rec.Status != dedup.StatusPublished {
			t.Fatalf("expected instance 2's event %s to be PUBLISHED (genuinely new, not a duplicate), got %s", k, rec.Status)
		}
		if got := producer.CountFor(k); got != 1 {
			t.Fatalf("expected exactly 1 Kafka publish for instance 2's event %s, got %d — 0 means it was silently dropped as a false duplicate", k, got)
		}
	}

	// 5. Sanity: instance 1's events are unaffected by instance 2 existing.
	for _, k := range instance1Keys {
		rec, err := outboxStore.GetOutboxRecord(ctx, k)
		if err != nil {
			t.Fatalf("expected instance 1's event %s to still be durably recorded: %v", k, err)
		}
		if rec.Status != dedup.StatusPublished {
			t.Fatalf("expected instance 1's event %s to still be PUBLISHED, got %s", k, rec.Status)
		}
	}
}
