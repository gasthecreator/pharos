package dedup

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func isPortOpen(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 1*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func TestCassandraOutboxStore_RealIntegration(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9042) {
		t.Fatalf("Cassandra port 9042 is not open on 127.0.0.1")
	}

	cfg := DefaultCassandraConfig()
	cfg.ConnectTimeout = 15 * time.Second

	store, err := NewCassandraOutboxStore(cfg)
	if err != nil {
		t.Fatalf("could not connect to Cassandra cluster: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	testSeq := uint64(time.Now().UnixNano())
	idKey := fmt.Sprintf("SITE-CASS-INT:%d", testSeq)

	rec := OutboxRecord{
		IdempotencyKey: idKey,
		SiteID:         "SITE-CASS-INT",
		LocalSeq:       testSeq,
		Payload:        []byte(`{"resourceType":"AdverseEvent","integration":true}`),
	}

	// 1. Initial LWT insert: must acquire claim
	claim1, err := store.InsertClaim(ctx, rec, 30*time.Second)
	if err != nil {
		t.Fatalf("InsertClaim failed: %v", err)
	}
	if !claim1.Acquired {
		t.Fatalf("expected claim1 acquired = true")
	}
	if claim1.Status != StatusPublishing {
		t.Errorf("expected status PUBLISHING, got %s", claim1.Status)
	}

	// 2. Immediate duplicate insert: must NOT acquire claim because lease is active
	claim2, err := store.InsertClaim(ctx, rec, 30*time.Second)
	if err != nil {
		t.Fatalf("2nd InsertClaim failed: %v", err)
	}
	if claim2.Acquired {
		t.Fatalf("expected claim2 acquired = false due to active lease")
	}

	// 3. Mark published with Kafka coordinates
	err = store.MarkPublished(ctx, idKey, "pharos.events.adverse", 0, 42)
	if err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	// 4. Retrieve saved record and verify status and Kafka coordinates
	saved, err := store.GetOutboxRecord(ctx, idKey)
	if err != nil {
		t.Fatalf("GetOutboxRecord failed: %v", err)
	}
	if saved.Status != StatusPublished {
		t.Errorf("expected status PUBLISHED, got %s", saved.Status)
	}
	if saved.KafkaTopic != "pharos.events.adverse" || saved.KafkaOffset != 42 {
		t.Errorf("unexpected kafka metadata: %+v", saved)
	}

	// 5. Post-publish duplicate: must return Acquired=false, Status=PUBLISHED
	claim3, err := store.InsertClaim(ctx, rec, 30*time.Second)
	if err != nil {
		t.Fatalf("3rd InsertClaim failed: %v", err)
	}
	if claim3.Acquired || claim3.Status != StatusPublished {
		t.Errorf("expected claim3 acquired=false, status=PUBLISHED; got acquired=%v, status=%s", claim3.Acquired, claim3.Status)
	}

	// 6. Test Concurrent LWT Race with Real Cassandra: 10 concurrent goroutines
	raceSeq := uint64(time.Now().UnixNano() + 1)
	raceKey := fmt.Sprintf("SITE-CASS-RACE:%d", raceSeq)
	const racers = 10
	var wg sync.WaitGroup
	wg.Add(racers)

	raceRec := OutboxRecord{
		IdempotencyKey: raceKey,
		SiteID:         "SITE-CASS-RACE",
		LocalSeq:       raceSeq,
		Payload:        []byte(`{"race":true}`),
	}

	wonCounts := make([]bool, racers)
	for i := 0; i < racers; i++ {
		idx := i
		go func() {
			defer wg.Done()
			c, cErr := store.InsertClaim(ctx, raceRec, 30*time.Second)
			if cErr == nil && c.Acquired {
				wonCounts[idx] = true
			}
		}()
	}
	wg.Wait()

	totalWinners := 0
	for _, won := range wonCounts {
		if won {
			totalWinners++
		}
	}
	if totalWinners != 1 {
		t.Fatalf("Real Cassandra LWT race violation: expected exactly 1 winner, got %d", totalWinners)
	}

	// 7. Test DLQ Symmetric Path with Real Cassandra
	dlqKey := fmt.Sprintf("SITE-CASS-DLQ:%d", testSeq)
	dlqRec := DLQRecord{
		IdempotencyKey:   dlqKey,
		SiteID:           "SITE-CASS-DLQ",
		Payload:          []byte(`{"malformed":true}`),
		RejectionReason:  "missing subject",
		ValidationErrors: `["subject required"]`,
		RejectedAt:       time.Now().UTC(),
	}

	dlqClaim, err := store.InsertDLQClaim(ctx, dlqRec, 30*time.Second)
	if err != nil || !dlqClaim.Acquired {
		t.Fatalf("InsertDLQClaim failed: err=%v, claim=%+v", err, dlqClaim)
	}

	err = store.MarkDLQPublished(ctx, dlqKey, "pharos.events.dlq", 0, 99)
	if err != nil {
		t.Fatalf("MarkDLQPublished failed: %v", err)
	}

	savedDLQ, err := store.GetDLQRecord(ctx, dlqKey)
	if err != nil {
		t.Fatalf("GetDLQRecord failed: %v", err)
	}
	if savedDLQ.Status != StatusPublished || savedDLQ.KafkaTopic != "pharos.events.dlq" {
		t.Errorf("unexpected saved DLQ record: %+v", savedDLQ)
	}
}
