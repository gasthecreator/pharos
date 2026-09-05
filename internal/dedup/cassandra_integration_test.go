package dedup

import (
	"context"
	"fmt"
	"math/rand"
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
	// racing an identical IF NOT EXISTS insert. Exactly one must win -- more
	// than one is the actual correctness violation this test exists to
	// catch (Paxos LWT isolation broken), and is a hard, non-retried
	// failure. Zero winners is a different, weaker signal: 10-way Paxos
	// ballot contention against a cluster that only just finished a fresh
	// multi-DC bootstrap (§2.4, Slice 14: Multi-Region Cassandra + Kafka)
	// can occasionally push every attempt into a timeout/retry rather than
	// a resolved winner, without any replica ever actually diverging on who
	// won -- confirmed by hand: this exact scenario reproduced with 0
	// winners against a live cluster immediately after bringing the 2-DC
	// topology up, and passed cleanly on a second attempt once the cluster
	// had settled. Retrying on 0 winners (with a fresh key, so a retry can
	// never collide with a previous attempt's partial state) treats that
	// case as inconclusive rather than a proven bug, while still failing
	// immediately, on the first sight of it, for >1 winners.
	runRace := func() int {
		raceSeq := uint64(time.Now().UnixNano()) + uint64(rand.Int63())
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
		return totalWinners
	}

	const maxRaceAttempts = 3
	var lastWinners int
	for attempt := 1; attempt <= maxRaceAttempts; attempt++ {
		lastWinners = runRace()
		if lastWinners > 1 {
			t.Fatalf("Real Cassandra LWT race violation: expected at most 1 winner, got %d", lastWinners)
		}
		if lastWinners == 1 {
			break
		}
		t.Logf("race attempt %d/%d got 0 winners (inconclusive under contention, not a violation) -- retrying with a fresh key", attempt, maxRaceAttempts)
	}
	if lastWinners != 1 {
		t.Fatalf("Real Cassandra LWT race: got 0 winners across all %d attempts, expected exactly 1 at least once", maxRaceAttempts)
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

	// 8. DLQ Replay (§2.3, Slice 10) against the real cluster -- exercises
	// both EnsureSchema's replayed_at migration (this keyspace didn't have
	// the column until this test's own connection ran EnsureSchema moments
	// ago) and MarkDLQReplayed's CAS precondition, not just the in-memory
	// store's equivalent already covered elsewhere.
	if err := store.MarkDLQReplayed(ctx, dlqKey); err != nil {
		t.Fatalf("MarkDLQReplayed failed against real Cassandra: %v", err)
	}
	replayedDLQ, err := store.GetDLQRecord(ctx, dlqKey)
	if err != nil {
		t.Fatalf("GetDLQRecord after replay failed: %v", err)
	}
	if replayedDLQ.Status != StatusReplayed {
		t.Errorf("expected status REPLAYED after MarkDLQReplayed, got %s", replayedDLQ.Status)
	}
	if replayedDLQ.ReplayedAt.IsZero() {
		t.Errorf("expected ReplayedAt to be set after MarkDLQReplayed")
	}
	// The original rejection reason must still be there -- replay never
	// mutates or deletes the audit trail, only status/replayed_at change.
	if replayedDLQ.RejectionReason != dlqRec.RejectionReason {
		t.Errorf("expected original rejection reason preserved, got %q", replayedDLQ.RejectionReason)
	}

	// Replaying a second time must fail -- the CAS precondition requires
	// status = 'PUBLISHED', and it's now REPLAYED.
	if err := store.MarkDLQReplayed(ctx, dlqKey); err == nil {
		t.Errorf("expected a second MarkDLQReplayed call to fail (already REPLAYED), got nil error")
	}

	// dead_letter_events_by_site must reflect the same replay (the dual-write
	// pattern established for MarkDLQPublished in Slice 5).
	// Note: rejected_at as actually stored is InsertDLQClaim's own internal
	// timestamp, not the caller's dlqRec.RejectedAt field -- savedDLQ (read
	// back via GetDLQRecord above) has the real persisted value.
	var siteStatus string
	if scanErr := store.session.Query(
		`SELECT status FROM dead_letter_events_by_site WHERE site_id = ? AND rejected_at = ? AND idempotency_key = ?;`,
		"SITE-CASS-DLQ", savedDLQ.RejectedAt, dlqKey,
	).WithContext(ctx).Scan(&siteStatus); scanErr != nil {
		t.Fatalf("failed to query dead_letter_events_by_site after replay: %v", scanErr)
	}
	if siteStatus != string(StatusReplayed) {
		t.Errorf("expected dead_letter_events_by_site status REPLAYED, got %s", siteStatus)
	}
}
