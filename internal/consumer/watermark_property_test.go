package consumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
	"pgregory.net/rapid"
)

// TestWatermarkProperty_MonotonicAndAuditDedup generates random sequences of
// window registration, event processing (with event times that can land
// before, at, or after the current simulated instant -- i.e. on-time, late,
// or out-of-order), and time advancement against a single WatermarkTracker,
// asserting the two invariants PLAN.md's Slice 22 Stage A calls out by name
// hold after *every* generated sequence:
//
//  1. The emitted watermark never decreases, whether observed via
//     ProcessEvent's return value or a direct CurrentWatermark query.
//  2. GetLateArrivalAudits() never contains two entries for the same
//     (WindowID, IdempotencyKey) pair.
//
// This package's other watermark tests (watermark_test.go) each construct
// one specific hand-picked scenario (a partition going idle and
// reawakening, a COMPLETE window receiving late data once, a restore from
// checkpoint); this generates many more interleavings of the same
// primitives than a human would think to write down by hand, per PLAN.md's
// own stated reason for this slice existing.
func TestWatermarkProperty_MonotonicAndAuditDedup(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tracker := NewWatermarkTracker(5*time.Second, 60*time.Second)
		simClock := clock.NewSimulated(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		var lastWatermark time.Time
		windowCount := 0

		checkNotRegressed := func(t *rapid.T, wm time.Time, source string) {
			if !lastWatermark.IsZero() && wm.Before(lastWatermark) {
				t.Fatalf("CRITICAL: watermark regressed from %v to %v (observed via %s)", lastWatermark, wm, source)
			}
			lastWatermark = wm
		}

		t.Repeat(map[string]func(*rapid.T){
			"register_window": func(t *rapid.T) {
				if windowCount >= 6 {
					return // cap the window count so the state space stays tractable
				}
				id := fmt.Sprintf("W%d", windowCount)
				windowCount++
				startOffset := time.Duration(rapid.IntRange(0, 600).Draw(t, "start_offset_seconds")) * time.Second
				duration := time.Duration(rapid.IntRange(10, 180).Draw(t, "duration_seconds")) * time.Second
				start := simClock.Now().Add(startOffset)
				tracker.RegisterWindow(Window{ID: id, Start: start, End: start.Add(duration)}, simClock.Now())
			},
			"process_event": func(t *rapid.T) {
				partition := rapid.IntRange(0, 2).Draw(t, "partition")
				keyID := rapid.IntRange(0, 5).Draw(t, "key_id")
				key := fmt.Sprintf("KEY-%d", keyID)
				// Deliberately spans negative (late/out-of-order) and positive
				// (on-time) offsets from the current simulated instant -- the
				// exact "events arriving late or out-of-order" scenario
				// PLAN.md's Stage A calls out by name.
				offsetSec := rapid.IntRange(-180, 30).Draw(t, "event_offset_seconds")
				eventTime := simClock.Now().Add(time.Duration(offsetSec) * time.Second)
				_, wm, _ := tracker.ProcessEvent(partition, key, eventTime, simClock.Now())
				checkNotRegressed(t, wm, "ProcessEvent")
			},
			"query_watermark": func(t *rapid.T) {
				wm := tracker.CurrentWatermark(simClock.Now())
				checkNotRegressed(t, wm, "CurrentWatermark")
			},
			"advance_time": func(t *rapid.T) {
				d := time.Duration(rapid.IntRange(0, 45).Draw(t, "seconds")) * time.Second
				simClock.Advance(d)
			},
		})

		audits := tracker.GetLateArrivalAudits()
		seen := make(map[string]bool, len(audits))
		for _, a := range audits {
			dedupKey := a.WindowID + ":" + a.IdempotencyKey
			if seen[dedupKey] {
				t.Fatalf("CRITICAL: duplicate LateArrivalAudit entry for (window=%s, key=%s) -- 21 CFR Part 11 audit dedup violated", a.WindowID, a.IdempotencyKey)
			}
			seen[dedupKey] = true
		}
	})
}
