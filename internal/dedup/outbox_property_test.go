package dedup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
	"pgregory.net/rapid"
)

// TestOutboxProperty_ExactlyOnceAndNoDoubleClaim generates random sequences
// of claim/steal/publish/stale-finalize/advance-time operations against a
// single MemoryOutboxStore and asserts the claim/lease outbox's core
// invariants (§2.2, PLAN.md Slice 22 Stage A) hold after *every* generated
// sequence -- not just the specific hand-picked interleavings this
// package's other TestMemoryOutboxStore_* tests construct by hand:
//
//  1. Exactly-once claim: once a key is finalized PUBLISHED, no later
//     InsertClaim on that key may ever return Acquired=true again.
//  2. No double-claim: at most one claim may be "live" (Acquired, unexpired,
//     unfinalized) for a key at any simulated instant -- a second Acquired
//     result for the same key can only happen after the first one's lease
//     has genuinely expired (a legitimate steal), never while it's active.
//  3. Fenced finalize (§2.4, Slice 22 fix): a claim that's been superseded
//     by a steal can never successfully finalize with its own stale
//     claimed_at -- MarkPublished must return ErrClaimSuperseded for it,
//     never silently succeed and overwrite the legitimate claimant's
//     bookkeeping.
//
// Uses a Simulated clock (§2.4, Slice 22) so lease expiry is driven by
// generated time advances, not real sleeping -- thousands of generated
// sequences run in a fraction of a second this way.
func TestOutboxProperty_ExactlyOnceAndNoDoubleClaim(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := NewMemoryOutboxStore()
		simClock := clock.NewSimulated(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		store.SetClock(simClock)
		ctx := context.Background()
		const leaseTimeout = 30 * time.Second

		keys := []string{"K1", "K2", "K3"} // small keyspace to force realistic collisions/steals

		// Reference model, one entry per key: liveClaimedAt is the current
		// legitimate (unfinalized) claim's timestamp, zero if none; abandoned
		// holds every claimed_at a steal has since superseded, for the
		// stale_finalize action to attempt against; published/publishedAt
		// record the terminal state once finalized.
		type modelState struct {
			liveClaimedAt time.Time
			abandoned     []time.Time
			published     bool
			publishedAt   time.Time
		}
		model := make(map[string]*modelState, len(keys))
		for _, k := range keys {
			model[k] = &modelState{}
		}

		t.Repeat(map[string]func(*rapid.T){
			"claim": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				res, err := store.InsertClaim(ctx, OutboxRecord{
					IdempotencyKey: key, SiteID: "S", Payload: []byte(`{"resourceType":"AdverseEvent"}`),
				}, leaseTimeout)
				if err != nil {
					t.Fatalf("InsertClaim(%s) error: %v", key, err)
				}
				m := model[key]
				if res.Acquired {
					if m.published {
						t.Fatalf("CRITICAL: exactly-once violated -- InsertClaim acquired a new claim for already-PUBLISHED key %s", key)
					}
					if !m.liveClaimedAt.IsZero() && simClock.Now().Sub(m.liveClaimedAt) < leaseTimeout {
						t.Fatalf("CRITICAL: double-claim -- InsertClaim acquired for key %s while a previous claim's lease (claimed_at=%v) is still active at %v", key, m.liveClaimedAt, simClock.Now())
					}
					if !m.liveClaimedAt.IsZero() {
						// A genuine steal: the old claim is now abandoned.
						m.abandoned = append(m.abandoned, m.liveClaimedAt)
					}
					m.liveClaimedAt = simClock.Now()
				}
			},
			"publish": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m := model[key]
				if m.liveClaimedAt.IsZero() || m.published {
					return // nothing legitimate to finalize right now
				}
				expected := m.liveClaimedAt
				if err := store.MarkPublished(ctx, key, expected, "topic", 0, 1); err != nil {
					t.Fatalf("CRITICAL: MarkPublished(%s) unexpectedly failed (%v) for the current legitimate claim -- fencing must never reject the claimant it was actually issued to", key, err)
				}
				m.published = true
				m.publishedAt = expected
				m.liveClaimedAt = time.Time{}
			},
			"stale_finalize": func(t *rapid.T) {
				// A claimant whose lease already expired and was stolen,
				// finally getting around to finalizing its own now-obsolete
				// work (§2.4, Slice 22's motivating scenario: "a crash
				// simulated mid-publish" and its slower cousin, a claimant
				// that isn't dead, just very late).
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m := model[key]
				if len(m.abandoned) == 0 {
					return
				}
				idx := rapid.IntRange(0, len(m.abandoned)-1).Draw(t, "abandoned_index")
				staleClaimedAt := m.abandoned[idx]
				err := store.MarkPublished(ctx, key, staleClaimedAt, "stale-topic", 0, 1)
				if !errors.Is(err, ErrClaimSuperseded) {
					t.Fatalf("CRITICAL: fencing failed -- a stale claimant's MarkPublished(%s, claimed_at=%v) should have returned ErrClaimSuperseded, got %v", key, staleClaimedAt, err)
				}
			},
			"advance_time": func(t *rapid.T) {
				d := time.Duration(rapid.IntRange(0, 90).Draw(t, "seconds")) * time.Second
				simClock.Advance(d)
			},
		})

		// Final cross-check against the store's own view: every key's
		// terminal published state must agree between the model and the
		// store, and a published key's stored Kafka lineage must be the
		// legitimate claim's, never a stale one's (belt-and-suspenders on
		// top of the per-call assertions above).
		for _, key := range keys {
			m := model[key]
			rec, err := store.GetOutboxRecord(ctx, key)
			if err != nil {
				if m.published {
					t.Fatalf("model says %s was published but GetOutboxRecord failed: %v", key, err)
				}
				continue
			}
			if m.published && rec.Status != StatusPublished {
				t.Fatalf("model says %s is PUBLISHED but store has status %s", key, rec.Status)
			}
			if m.published && rec.KafkaTopic != "topic" {
				t.Fatalf("CRITICAL: %s's stored Kafka lineage is %q, expected the legitimate claimant's \"topic\" -- a stale finalize corrupted it", key, rec.KafkaTopic)
			}
		}
	})
}

// TestOutboxProperty_DLQExactlyOnceAndFencing mirrors the event_outbox
// property test above against the symmetric dead_letter_events claim/lease
// path (InsertDLQClaim/MarkDLQPublished), plus MarkDLQReplayed's own
// precondition: a DLQ record can only ever transition PUBLISHED->REPLAYED
// once, from PUBLISHED, never from PUBLISHING or a second time from
// REPLAYED.
func TestOutboxProperty_DLQExactlyOnceAndFencing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := NewMemoryOutboxStore()
		simClock := clock.NewSimulated(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		store.SetClock(simClock)
		ctx := context.Background()
		const leaseTimeout = 30 * time.Second

		keys := []string{"D1", "D2", "D3"}

		type modelState struct {
			liveClaimedAt time.Time
			abandoned     []time.Time
			published     bool
			replayed      bool
		}
		model := make(map[string]*modelState, len(keys))
		for _, k := range keys {
			model[k] = &modelState{}
		}

		t.Repeat(map[string]func(*rapid.T){
			"claim": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				res, err := store.InsertDLQClaim(ctx, DLQRecord{
					IdempotencyKey: key, SiteID: "S", Payload: []byte(`{"malformed":true}`),
					RejectionReason: "property-test", ValidationErrors: "property-test",
				}, leaseTimeout)
				if err != nil {
					t.Fatalf("InsertDLQClaim(%s) error: %v", key, err)
				}
				m := model[key]
				if res.Acquired {
					if m.published {
						t.Fatalf("CRITICAL: exactly-once violated -- InsertDLQClaim acquired for already-PUBLISHED key %s", key)
					}
					if !m.liveClaimedAt.IsZero() && simClock.Now().Sub(m.liveClaimedAt) < leaseTimeout {
						t.Fatalf("CRITICAL: double-claim -- InsertDLQClaim acquired for key %s while a previous claim's lease is still active", key)
					}
					if !m.liveClaimedAt.IsZero() {
						m.abandoned = append(m.abandoned, m.liveClaimedAt)
					}
					m.liveClaimedAt = simClock.Now()
				}
			},
			"publish": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m := model[key]
				if m.liveClaimedAt.IsZero() || m.published {
					return
				}
				expected := m.liveClaimedAt
				if err := store.MarkDLQPublished(ctx, key, expected, "dlq-topic", 0, 1); err != nil {
					t.Fatalf("CRITICAL: MarkDLQPublished(%s) unexpectedly failed (%v) for the current legitimate claim", key, err)
				}
				m.published = true
				m.liveClaimedAt = time.Time{}
			},
			"stale_finalize": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m := model[key]
				if len(m.abandoned) == 0 {
					return
				}
				idx := rapid.IntRange(0, len(m.abandoned)-1).Draw(t, "abandoned_index")
				staleClaimedAt := m.abandoned[idx]
				err := store.MarkDLQPublished(ctx, key, staleClaimedAt, "stale-dlq-topic", 0, 1)
				if !errors.Is(err, ErrClaimSuperseded) {
					t.Fatalf("CRITICAL: fencing failed -- a stale DLQ claimant's finalize should have returned ErrClaimSuperseded, got %v", err)
				}
			},
			"replay": func(t *rapid.T) {
				key := rapid.SampledFrom(keys).Draw(t, "key")
				m := model[key]
				err := store.MarkDLQReplayed(ctx, key)
				switch {
				case m.published && !m.replayed:
					if err != nil {
						t.Fatalf("CRITICAL: MarkDLQReplayed(%s) failed (%v) for a PUBLISHED, not-yet-replayed record", key, err)
					}
					m.replayed = true
				default:
					// Not yet published, or already replayed -- must fail
					// either way (record not found, or precondition unmet).
					if err == nil {
						t.Fatalf("CRITICAL: MarkDLQReplayed(%s) unexpectedly succeeded from an invalid state (published=%v replayed=%v)", key, m.published, m.replayed)
					}
				}
			},
			"advance_time": func(t *rapid.T) {
				d := time.Duration(rapid.IntRange(0, 90).Draw(t, "seconds")) * time.Second
				simClock.Advance(d)
			},
		})

		for _, key := range keys {
			m := model[key]
			rec, err := store.GetDLQRecord(ctx, key)
			if err != nil {
				if m.published {
					t.Fatalf("model says %s was published but GetDLQRecord failed: %v", key, err)
				}
				continue
			}
			wantStatus := StatusPublishing
			if m.replayed {
				wantStatus = StatusReplayed
			} else if m.published {
				wantStatus = StatusPublished
			}
			if rec.Status != wantStatus {
				t.Fatalf("model/store status mismatch for %s: model wants %s, store has %s", key, wantStatus, rec.Status)
			}
			if m.published && !m.replayed && rec.KafkaTopic != "dlq-topic" {
				t.Fatalf("CRITICAL: %s's stored Kafka lineage is %q, expected the legitimate claimant's \"dlq-topic\" -- a stale finalize corrupted it", key, rec.KafkaTopic)
			}
		}
	})
}
