package dedup

import (
	"context"
	"errors"
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
	err = store.MarkPublished(ctx, idKey, claim1.ClaimedAt, "pharos.events.adverse", 0, 42)
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

	err = store.MarkDLQPublished(ctx, dlqKey, dlqClaim.ClaimedAt, "pharos.events.dlq", 0, 99)
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

// TestCassandraOutboxStore_MarkPublishedFencedAfterSteal proves the §2.4,
// Slice 22 fencing fix against real Cassandra LWT: a claimant whose lease
// genuinely expired and was stolen by another claimant can no longer
// finalize with its own stale claimed_at, and the real event_outbox row
// ends up with whichever claimant's finalize actually won. Before this
// fix, the CAS only checked `IF status = 'PUBLISHING'`, not claimed_at, and
// its applied result was discarded entirely -- the stale claimant's
// finalize would have silently succeeded and overwritten the real Kafka
// lineage.
func TestCassandraOutboxStore_MarkPublishedFencedAfterSteal(t *testing.T) {
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
	idKey := fmt.Sprintf("SITE-FENCE-CASS:%d", time.Now().UnixNano())
	rec := OutboxRecord{
		IdempotencyKey: idKey,
		SiteID:         "SITE-FENCE-CASS",
		LocalSeq:       1,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}
	shortLease := 200 * time.Millisecond

	claim1, err := store.InsertClaim(ctx, rec, shortLease)
	if err != nil || !claim1.Acquired {
		t.Fatalf("1st InsertClaim failed: err=%v acquired=%v", err, claim1.Acquired)
	}

	time.Sleep(shortLease + 300*time.Millisecond)

	claim2, err := store.InsertClaim(ctx, rec, shortLease)
	if err != nil || !claim2.Acquired {
		t.Fatalf("steal InsertClaim failed: err=%v acquired=%v", err, claim2.Acquired)
	}
	if claim2.ClaimedAt.Equal(claim1.ClaimedAt) {
		t.Fatalf("test setup: expected the steal to produce a new claimed_at")
	}

	// The stale claimant's own (late) finalize must be fenced off.
	err = store.MarkPublished(ctx, idKey, claim1.ClaimedAt, "stale.topic", 9, 999)
	if !errors.Is(err, ErrClaimSuperseded) {
		t.Fatalf("expected ErrClaimSuperseded for the stale claimant's finalize, got %v", err)
	}

	// The legitimate current claimant can still finalize normally.
	if err := store.MarkPublished(ctx, idKey, claim2.ClaimedAt, "real.topic", 0, 42); err != nil {
		t.Fatalf("expected the current claimant's MarkPublished to succeed, got %v", err)
	}

	saved, err := store.GetOutboxRecord(ctx, idKey)
	if err != nil {
		t.Fatalf("GetOutboxRecord failed: %v", err)
	}
	if saved.Status != StatusPublished {
		t.Fatalf("expected status PUBLISHED, got %s", saved.Status)
	}
	if saved.KafkaTopic != "real.topic" || saved.KafkaOffset != 42 {
		t.Fatalf("CRITICAL: expected the legitimate claimant's Kafka lineage (real.topic/42) to win, got %s/%d -- fencing did not prevent bookkeeping corruption in real Cassandra", saved.KafkaTopic, saved.KafkaOffset)
	}
}

// TestCassandraOutboxStore_DLQLeaseStealSyncsBySite proves the §2.3, audit
// remediation fix against real Cassandra: when a DLQ claim's lease expires
// and gets stolen, dead_letter_events_by_site (the query index the
// archival job and any site-scoped DLQ inspection use) reflects the
// post-steal status/claimed_at immediately, not the stale pre-steal values
// InsertDLQClaim's steal branch previously left behind until the eventual
// MarkDLQPublished overwrote them.
func TestCassandraOutboxStore_DLQLeaseStealSyncsBySite(t *testing.T) {
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
	siteID := fmt.Sprintf("SITE-DLQ-STEAL-SYNC:%d", time.Now().UnixNano())
	idKey := siteID + ":1"
	rec := DLQRecord{
		IdempotencyKey:   idKey,
		SiteID:           siteID,
		Payload:          []byte(`{"resourceType":"AdverseEvent"}`),
		RejectionReason:  "test rejection",
		ValidationErrors: "none",
	}
	shortLease := 200 * time.Millisecond

	claim1, err := store.InsertDLQClaim(ctx, rec, shortLease)
	if err != nil || !claim1.Acquired {
		t.Fatalf("1st InsertDLQClaim failed: err=%v acquired=%v", err, claim1.Acquired)
	}

	time.Sleep(shortLease + 300*time.Millisecond)

	claim2, err := store.InsertDLQClaim(ctx, rec, shortLease)
	if err != nil || !claim2.Acquired {
		t.Fatalf("steal InsertDLQClaim failed: err=%v acquired=%v", err, claim2.Acquired)
	}
	if claim2.ClaimedAt.Equal(claim1.ClaimedAt) {
		t.Fatalf("test setup: expected the steal to produce a new claimed_at")
	}

	// Cassandra's timestamp column is millisecond-precision; claim2.ClaimedAt
	// carries Go's finer in-memory resolution, so compare truncated to
	// milliseconds rather than exact equality.
	wantClaimedAt := claim2.ClaimedAt.Truncate(time.Millisecond)

	dlq, err := store.GetDLQRecord(ctx, idKey)
	if err != nil {
		t.Fatalf("GetDLQRecord failed: %v", err)
	}
	if dlq.Status != StatusPublishing || !dlq.ClaimedAt.Equal(wantClaimedAt) {
		t.Fatalf("dead_letter_events: expected status=PUBLISHING claimedAt=%v after steal, got status=%s claimedAt=%v", wantClaimedAt, dlq.Status, dlq.ClaimedAt)
	}

	var bySiteStatus string
	var bySiteClaimedAt time.Time
	if err := store.session.Query(
		`SELECT status, claimed_at FROM dead_letter_events_by_site WHERE site_id = ? AND rejected_at = ? AND idempotency_key = ?;`,
		siteID, claim1.ClaimedAt, idKey,
	).WithContext(ctx).Scan(&bySiteStatus, &bySiteClaimedAt); err != nil {
		t.Fatalf("failed to query dead_letter_events_by_site: %v", err)
	}
	if bySiteStatus != string(StatusPublishing) {
		t.Fatalf("STALE INDEX: dead_letter_events_by_site.status = %s after steal, want %s", bySiteStatus, StatusPublishing)
	}
	if !bySiteClaimedAt.Equal(wantClaimedAt) {
		t.Fatalf("STALE INDEX: dead_letter_events_by_site.claimed_at = %v after steal, want the post-steal value %v (still showing the pre-steal claim)", bySiteClaimedAt, wantClaimedAt)
	}
}

// TestCassandraOutboxStore_OutboxPruning proves the §2.2, audit remediation
// fix against real Cassandra: event_outbox_by_site is populated at
// MarkPublished time, ListOutboxBySiteOlderThan respects the cutoff in both
// directions (excludes a too-recent row, includes an aged one), and
// DeletePrunedOutboxRecord actually removes the row from both event_outbox
// and event_outbox_by_site -- not just one of the two, which would leave
// the index pointing at a row that no longer exists.
func TestCassandraOutboxStore_OutboxPruning(t *testing.T) {
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
	siteID := fmt.Sprintf("SITE-OUTBOX-PRUNE:%d", time.Now().UnixNano())
	idKey := siteID + ":1"
	rec := OutboxRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		LocalSeq:       1,
		Payload:        []byte(`{"resourceType":"AdverseEvent"}`),
	}

	claim, err := store.InsertClaim(ctx, rec, DefaultLeaseTimeout)
	if err != nil || !claim.Acquired {
		t.Fatalf("InsertClaim failed: err=%v acquired=%v", err, claim.Acquired)
	}
	if err := store.MarkPublished(ctx, idKey, claim.ClaimedAt, "test-topic", 0, 0); err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	// Safety check: a cutoff before this row's real publish time must never
	// see it as prunable -- a bug here would delete recently-published
	// records, not just old ones.
	tooRecentCutoff := time.Now().UTC().Add(-1 * time.Hour)
	notYetPrunable, err := store.ListOutboxBySiteOlderThan(ctx, siteID, tooRecentCutoff)
	if err != nil {
		t.Fatalf("ListOutboxBySiteOlderThan (too-recent cutoff) failed: %v", err)
	}
	for _, r := range notYetPrunable {
		if r.IdempotencyKey == idKey {
			t.Fatalf("record %s incorrectly listed as prunable against a cutoff before its publish time", idKey)
		}
	}

	// Backdate published_at directly (simulating real age) rather than
	// waiting -- exactly what a row genuinely older than the retention
	// window looks like.
	oldTime := time.Now().UTC().Add(-100 * 24 * time.Hour)

	var actualPublishedAt time.Time
	if err := store.session.Query(
		`SELECT published_at FROM event_outbox_by_site WHERE site_id = ? AND idempotency_key = ? ALLOW FILTERING;`,
		siteID, idKey,
	).WithContext(ctx).Scan(&actualPublishedAt); err != nil {
		t.Fatalf("failed to look up the real published_at MarkPublished set: %v", err)
	}

	if err := store.session.Query(
		`UPDATE event_outbox SET published_at = ? WHERE idempotency_key = ?;`, oldTime, idKey,
	).WithContext(ctx).Exec(); err != nil {
		t.Fatalf("failed to backdate published_at: %v", err)
	}
	if err := store.session.Query(
		`DELETE FROM event_outbox_by_site WHERE site_id = ? AND published_at = ? AND idempotency_key = ?;`,
		siteID, actualPublishedAt, idKey,
	).WithContext(ctx).Exec(); err != nil {
		t.Fatalf("failed to remove the original (non-backdated) index row: %v", err)
	}
	if err := store.session.Query(
		`INSERT INTO event_outbox_by_site (site_id, published_at, idempotency_key) VALUES (?, ?, ?);`,
		siteID, oldTime, idKey,
	).WithContext(ctx).Exec(); err != nil {
		t.Fatalf("failed to insert the backdated index row: %v", err)
	}

	cutoff90d := time.Now().UTC().Add(-90 * 24 * time.Hour)
	prunable, err := store.ListOutboxBySiteOlderThan(ctx, siteID, cutoff90d)
	if err != nil {
		t.Fatalf("ListOutboxBySiteOlderThan (90d cutoff) failed: %v", err)
	}
	var target *OutboxPruneRecord
	for i := range prunable {
		if prunable[i].IdempotencyKey == idKey {
			target = &prunable[i]
		}
	}
	if target == nil {
		t.Fatalf("expected %s to be listed as prunable past the 90d cutoff, got %d other records", idKey, len(prunable))
	}

	if err := store.DeletePrunedOutboxRecord(ctx, *target); err != nil {
		t.Fatalf("DeletePrunedOutboxRecord failed: %v", err)
	}

	if _, err := store.GetOutboxRecord(ctx, idKey); !errors.Is(err, ErrRecordNotFound) {
		t.Fatalf("expected event_outbox row deleted (ErrRecordNotFound), got %v", err)
	}
	var leftover int
	iter := store.session.Query(`SELECT idempotency_key FROM event_outbox_by_site WHERE site_id = ?;`, siteID).WithContext(ctx).Iter()
	var scanned string
	for iter.Scan(&scanned) {
		leftover++
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("failed to verify event_outbox_by_site cleanup: %v", err)
	}
	if leftover != 0 {
		t.Fatalf("expected event_outbox_by_site fully cleaned up for %s, found %d leftover row(s)", siteID, leftover)
	}
}
