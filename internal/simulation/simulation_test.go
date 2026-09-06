package simulation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

const (
	defaultNumSites      = 3
	defaultNumPartitions = 3
	defaultTicksPerSeed  = 150
	defaultDrainMaxTicks = 500
)

// simulationResult is a fingerprint of one seed's run: enough to assert two
// runs of the identical seed produced bit-for-bit identical outcomes
// (§2.4, PLAN.md Slice 22 Stage B's core claim -- "a single integer seed
// deterministically reproduces one exact interleaving"), and enough for
// TestSimulation_ExactlyOnceAndMonotonicWatermarkAcrossManySeeds to check
// its invariants against.
type simulationResult struct {
	acceptedOrder  []string
	publishCounts  map[string]int
	finalWatermark time.Time
	totalSaved     int
}

// runSeed drives one full simulation run for seed: a random interleaving of
// event submissions (including deliberate resubmission of already-accepted
// keys, exercising the outbox's dedup path -- the "retries" scenario
// PLAN.md's Stage A names), broker delivery delay, consumer fetch/commit
// steps, and sweeper reclamation passes, checking the watermark
// monotonicity invariant live as it runs, then draining every in-flight
// message before returning a fingerprint of the final state.
func runSeed(t *testing.T, seed int64, ticksPerSeed int) simulationResult {
	t.Helper()
	h := NewHarness(seed, defaultNumPartitions)
	ctx := context.Background()
	baseTime := h.Clock.Now()

	acceptedKeys := make(map[string]bool)
	var acceptedOrder []string
	eventCounter := 0
	var lastWatermark time.Time

	for tick := 0; tick < ticksPerSeed; tick++ {
		switch h.Rng.Intn(6) {
		case 0, 1: // submit -- weighted so submissions dominate over plumbing actions
			siteID := fmt.Sprintf("SITE-SIM-%d", h.Rng.Intn(defaultNumSites))
			eventCounter++
			localSeq := eventCounter
			// Occasionally resubmit an already-accepted key instead of a
			// fresh one -- the exact "retries" scenario PLAN.md names.
			if len(acceptedOrder) > 0 && h.Rng.Intn(3) == 0 {
				key := acceptedOrder[h.Rng.Intn(len(acceptedOrder))]
				var parsedSite string
				var parsedSeq int
				if n, _ := fmt.Sscanf(key, "%[^:]:%d", &parsedSite, &parsedSeq); n == 2 {
					siteID, localSeq = parsedSite, parsedSeq
				}
			}
			// Event time can land before or after "now" -- late/early
			// arrival, independent of broker delivery timing.
			eventTime := baseTime.Add(time.Duration(h.Rng.Intn(600)-300) * time.Second)
			status := h.SubmitEvent(siteID, localSeq, eventTime, "actual")
			key := fmt.Sprintf("%s:%d", siteID, localSeq)
			if status == http.StatusOK && !acceptedKeys[key] {
				acceptedKeys[key] = true
				acceptedOrder = append(acceptedOrder, key)
			}
		case 2:
			h.Broker.Advance()
		case 3:
			if err := h.StepConsumer(ctx); err != nil {
				t.Fatalf("seed %d tick %d: consumer step failed: %v", seed, tick, err)
			}
			wm := h.Engine.Tracker().CurrentWatermark(h.Clock.Now())
			if !lastWatermark.IsZero() && wm.Before(lastWatermark) {
				t.Fatalf("seed %d tick %d: CRITICAL watermark regressed from %v to %v", seed, tick, lastWatermark, wm)
			}
			lastWatermark = wm
		case 4:
			if _, err := h.Sweeper.Step(ctx); err != nil {
				t.Fatalf("seed %d tick %d: sweeper step failed: %v", seed, tick, err)
			}
		case 5:
			// no-op tick: models "nothing happened this instant," still
			// advances the clock below -- keeps time progressing even in
			// sequences that otherwise skew toward one action type.
		}
		h.Clock.Advance(time.Duration(h.Rng.Intn(5)) * time.Second)
	}

	if err := h.DrainConsumer(ctx, defaultDrainMaxTicks); err != nil {
		t.Fatalf("seed %d: %v", seed, err)
	}

	// Invariant: every accepted key is eventually present in the canonical
	// store (end-to-end delivery, not just "the outbox thinks it
	// published") -- checked here, not by the caller, since the harness
	// (and its canonical store) is scoped to this one run.
	for _, key := range acceptedOrder {
		if _, err := h.Canonical.GetEvent(ctx, key); err != nil {
			t.Fatalf("seed %d: CRITICAL accepted key %s never reached the canonical store: %v", seed, key, err)
		}
	}

	publishCounts := make(map[string]int)
	for _, partition := range h.Broker.partitions {
		for _, msg := range partition {
			var key string
			for _, hdr := range msg.Headers {
				if hdr.Key == "idempotency_key" {
					key = string(hdr.Value)
					break
				}
			}
			if key != "" {
				publishCounts[key]++
			}
		}
	}

	return simulationResult{
		acceptedOrder:  acceptedOrder,
		publishCounts:  publishCounts,
		finalWatermark: h.Engine.Tracker().CurrentWatermark(h.Clock.Now()),
		totalSaved:     h.Canonical.TotalSaved(),
	}
}

// TestSimulation_ExactlyOnceAndMonotonicWatermarkAcrossManySeeds drives the
// REAL ingestion.Handler, ingestion.Sweeper, and consumer.Engine against a
// simulated Kafka broker across many seeds (§2.4, PLAN.md Slice 22 Stage
// B), checking two invariants after every seed's run:
//
//  1. Exactly-once publish: every idempotency key that ever received an
//     HTTP 200 (ingestion's own "accepted" contract -- true for both a
//     genuinely new claim and an idempotent duplicate resubmission, see
//     processOneEvent) must appear in the broker's own committed message
//     log EXACTLY once, never zero (lost) or more than once (a real
//     duplicate publish -- the specific failure category Stage A's fencing
//     fix, and the InsertDLQClaim/InsertClaim REPLAYED-stealable fix,
//     exist to prevent).
//  2. Every accepted key is eventually present in the canonical store
//     (end-to-end delivery, not just "the outbox thinks it published").
//
// Watermark monotonicity is checked live, inside runSeed, on every observed
// value during the run -- not just the final one.
func TestSimulation_ExactlyOnceAndMonotonicWatermarkAcrossManySeeds(t *testing.T) {
	const numSeeds = 500

	for seed := int64(0); seed < numSeeds; seed++ {
		result := runSeed(t, seed, defaultTicksPerSeed)

		for _, key := range result.acceptedOrder {
			if count := result.publishCounts[key]; count != 1 {
				t.Fatalf("seed %d: CRITICAL exactly-once violated for %s: published %d times (want exactly 1)", seed, key, count)
			}
		}
	}
}

// TestSimulation_SameSeedIsDeterministic proves Stage B's actual reason for
// existing: an identical seed reproduces bit-for-bit identical behavior
// (PLAN.md: "A single integer seed deterministically reproduces one exact
// interleaving of message delays, reordering, crash timing... which means
// running 100,000 random seeds is a CI job measured in seconds, not hours,
// and any failing seed is trivially reproducible"). Runs the same seed
// twice, independently, and checks every part of the fingerprint agrees --
// if this ever failed, some part of the harness would be reaching for real
// wall-clock time or another non-deterministic source instead of the
// shared seeded Rng/Clock, silently undermining every other test in this
// package's ability to reproduce a failure.
func TestSimulation_SameSeedIsDeterministic(t *testing.T) {
	const seed = 424242
	first := runSeed(t, seed, defaultTicksPerSeed)
	second := runSeed(t, seed, defaultTicksPerSeed)

	if len(first.acceptedOrder) != len(second.acceptedOrder) {
		t.Fatalf("accepted count differs: %d vs %d", len(first.acceptedOrder), len(second.acceptedOrder))
	}
	for i := range first.acceptedOrder {
		if first.acceptedOrder[i] != second.acceptedOrder[i] {
			t.Fatalf("accepted order differs at index %d: %s vs %s", i, first.acceptedOrder[i], second.acceptedOrder[i])
		}
	}
	if len(first.publishCounts) != len(second.publishCounts) {
		t.Fatalf("distinct published-key count differs: %d vs %d", len(first.publishCounts), len(second.publishCounts))
	}
	for key, count := range first.publishCounts {
		if second.publishCounts[key] != count {
			t.Fatalf("publish count for %s differs: %d vs %d", key, count, second.publishCounts[key])
		}
	}
	if !first.finalWatermark.Equal(second.finalWatermark) {
		t.Fatalf("final watermark differs: %v vs %v", first.finalWatermark, second.finalWatermark)
	}
	if first.totalSaved != second.totalSaved {
		t.Fatalf("canonical store SaveEvent call count differs: %d vs %d", first.totalSaved, second.totalSaved)
	}
}
