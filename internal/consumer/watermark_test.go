package consumer

import (
	"testing"
	"time"
)

// TestWatermarkTracker_MonotonicityOnPartitionReawakening tests the critical gap found in review:
// When an idle partition reawakens with an older backlogged event time, the emitted watermark
// MUST NOT regress.
func TestWatermarkTracker_MonotonicityOnPartitionReawakening(t *testing.T) {
	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	latenessTolerance := 10 * time.Minute
	idleTimeout := 30 * time.Second

	tracker := NewWatermarkTracker(latenessTolerance, idleTimeout)

	t0 := baseTime

	// 1. Both partitions active initially at T=10m
	_, w1 := tracker.ProcessEvent(0, "SITE-01:1", baseTime.Add(10*time.Minute), t0)
	_, w2 := tracker.ProcessEvent(1, "SITE-02:1", baseTime.Add(10*time.Minute), t0)

	expectedInitial := baseTime.Add(10 * time.Minute).Add(-latenessTolerance)
	if !w2.Equal(expectedInitial) {
		t.Fatalf("expected initial watermark %v, got %v (w1: %v)", expectedInitial, w2, w1)
	}

	// 2. Partition 1 goes silent. Time passes: t1 = t0 + 40s (> idleTimeout 30s)
	t1 := t0.Add(40 * time.Second)

	// Partition 0 advances significantly: event time is baseTime + 100m
	_, w3 := tracker.ProcessEvent(0, "SITE-01:2", baseTime.Add(100*time.Minute), t1)

	// Partition 1 is idle (>30s since t0), so candidate is based on Partition 0 only: 100m - 10m = 90m
	expectedAdvanced := baseTime.Add(100 * time.Minute).Add(-latenessTolerance)
	if !w3.Equal(expectedAdvanced) {
		t.Fatalf("expected advanced watermark %v, got %v", expectedAdvanced, w3)
	}

	// 3. Partition 1 reawakens at t2 = t1 + 10s with backlog from baseTime + 20m (< 90m)
	t2 := t1.Add(10 * time.Second)
	isLate, w4 := tracker.ProcessEvent(1, "SITE-02:2", baseTime.Add(20*time.Minute), t2)

	// CRITICAL ASSERTION: The watermark must NEVER regress below w3 (baseTime + 90m)
	if w4.Before(w3) {
		t.Fatalf("CRITICAL MONOTONICITY REGRESSION: watermark dropped from %v to %v", w3, w4)
	}
	if !w4.Equal(w3) {
		t.Fatalf("expected watermark to remain %v, got %v", w3, w4)
	}
	if !isLate {
		t.Errorf("expected backlogged event on reawakened partition to be marked isLate=true")
	}
}

// TestWatermarkTracker_IdlePartitionExclusion verifies that an idle partition does not
// stall the global watermark.
func TestWatermarkTracker_IdlePartitionExclusion(t *testing.T) {
	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	tracker := NewWatermarkTracker(5*time.Minute, 10*time.Second)

	now := baseTime

	// Both partitions produce at baseTime + 10m
	tracker.ProcessEvent(0, "P0:1", baseTime.Add(10*time.Minute), now)
	tracker.ProcessEvent(1, "P1:1", baseTime.Add(10*time.Minute), now)

	// 15 seconds pass (> 10s idleTimeout). Partition 1 has been silent.
	now = now.Add(15 * time.Second)

	// Partition 0 produces at baseTime + 30m
	_, w := tracker.ProcessEvent(0, "P0:2", baseTime.Add(30*time.Minute), now)

	expected := baseTime.Add(30 * time.Minute).Add(-5 * time.Minute) // 25m
	if !w.Equal(expected) {
		t.Errorf("expected watermark %v (idle partition excluded), got %v", expected, w)
	}
}

// TestWatermarkTracker_CompleteToRevisedLifecycle verifies the COMPLETE -> REVISED
// transition when late data arrives for a closed window.
func TestWatermarkTracker_CompleteToRevisedLifecycle(t *testing.T) {
	baseTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	tracker := NewWatermarkTracker(0, 1*time.Minute)

	// Register 1-hour window: [12:00, 13:00)
	windowID := "WINDOW-HOUR-1"
	tracker.RegisterWindow(Window{
		ID:    windowID,
		Start: baseTime,
		End:   baseTime.Add(1 * time.Hour),
	})

	w, ok := tracker.GetWindow(windowID)
	if !ok || w.Status != WindowStatusOpen {
		t.Fatalf("expected window OPEN, got %v (ok=%v)", w.Status, ok)
	}

	// 1. Advance watermark past 13:00 (event at 13:10)
	now := baseTime.Add(10 * time.Minute)
	tracker.ProcessEvent(0, "E1", baseTime.Add(70*time.Minute), now)

	w, _ = tracker.GetWindow(windowID)
	if w.Status != WindowStatusComplete {
		t.Fatalf("expected window COMPLETE after watermark passed 13:00, got %v", w.Status)
	}

	// 2. Late event arrives with event_time 12:45 (< 13:00)
	now = now.Add(5 * time.Minute)
	isLate, _ := tracker.ProcessEvent(0, "LATE-1", baseTime.Add(45*time.Minute), now)
	if !isLate {
		t.Errorf("expected isLate=true for 12:45 event arriving after watermark passed 13:00")
	}

	// 3. Verify window transitioned to REVISED
	w, _ = tracker.GetWindow(windowID)
	if w.Status != WindowStatusRevised {
		t.Fatalf("expected window REVISED after late event, got %v", w.Status)
	}

	// 4. Verify audit trail
	audits := tracker.GetLateArrivalAudits()
	if len(audits) != 1 {
		t.Fatalf("expected 1 late arrival audit, got %d", len(audits))
	}
	if audits[0].WindowID != windowID || audits[0].IdempotencyKey != "LATE-1" {
		t.Errorf("unexpected audit entry: %+v", audits[0])
	}
}

// TestWatermarkTracker_RestoreFromCheckpointPreventsRegression is the
// unit-level core of Slice 13 (ARCHITECTURE_PROPOSALS.md "Consumer
// Crash/Restart Watermark Continuity"): a brand-new tracker (standing in for
// a freshly restarted process) that Restores a checkpoint saved by a prior
// tracker must never report a watermark below what that prior tracker had
// already reported -- even before any new event arrives, and even if the
// first event replayed after restart has an earlier event_time than the
// pre-crash high point (exactly what Kafka resuming from the last committed
// offset can produce).
func TestWatermarkTracker_RestoreFromCheckpointPreventsRegression(t *testing.T) {
	baseTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	latenessTolerance := 5 * time.Minute
	idleTimeout := 10 * time.Minute

	// 1. "Before the crash": a tracker processes events and advances well past
	// baseTime.
	before := NewWatermarkTracker(latenessTolerance, idleTimeout)
	_, preCrashWatermark := before.ProcessEvent(0, "SITE-01:1", baseTime.Add(60*time.Minute), baseTime)
	if preCrashWatermark.IsZero() {
		t.Fatalf("expected a non-zero watermark before the simulated crash")
	}

	checkpoint := before.Snapshot()

	// 2. "The crash": a brand-new tracker, exactly what cmd/pharos-consumer
	// constructs fresh on every process start. Immediately after Restore --
	// before any new event -- CurrentWatermark must already reflect the
	// pre-crash floor, not zero.
	after := NewWatermarkTracker(latenessTolerance, idleTimeout)
	after.Restore(checkpoint)

	restoredWatermark := after.CurrentWatermark(baseTime.Add(61 * time.Minute))
	if restoredWatermark.Before(preCrashWatermark) {
		t.Fatalf("CRITICAL REGRESSION: restored tracker reports watermark %v, below pre-crash watermark %v", restoredWatermark, preCrashWatermark)
	}
	if !restoredWatermark.Equal(preCrashWatermark) {
		t.Fatalf("expected restored watermark to equal pre-crash watermark %v, got %v", preCrashWatermark, restoredWatermark)
	}

	// 3. "Replay from the last committed Kafka offset": the first message
	// delivered after restart has an event_time EARLIER than the pre-crash
	// high point -- entirely plausible, since Kafka resumes from the last
	// committed offset, not from "wherever the in-memory watermark had
	// gotten to." The monotonic guard must hold even here.
	isLate, w := after.ProcessEvent(0, "SITE-01:2", baseTime.Add(30*time.Minute), baseTime.Add(61*time.Minute))
	if w.Before(preCrashWatermark) {
		t.Fatalf("CRITICAL REGRESSION: watermark dropped to %v after replaying an earlier-event-time message, below pre-crash floor %v", w, preCrashWatermark)
	}
	if !isLate {
		t.Errorf("expected the earlier-event-time replayed message to be marked isLate=true against the restored floor")
	}
}

// TestWatermarkTracker_RestoreOfEmptyCheckpointIsNoOp verifies that
// restoring a checkpoint from a tracker that never processed any event
// (the genuinely-fresh-consumer-group case) leaves the new tracker
// behaving exactly like one that was never restored at all.
func TestWatermarkTracker_RestoreOfEmptyCheckpointIsNoOp(t *testing.T) {
	baseTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	fresh := NewWatermarkTracker(5*time.Minute, 10*time.Minute)
	emptyCheckpoint := fresh.Snapshot()

	restored := NewWatermarkTracker(5*time.Minute, 10*time.Minute)
	restored.Restore(emptyCheckpoint)

	if wm := restored.CurrentWatermark(baseTime); !wm.IsZero() {
		t.Fatalf("expected zero watermark after restoring an empty checkpoint, got %v", wm)
	}

	_, w := restored.ProcessEvent(0, "SITE-01:1", baseTime.Add(10*time.Minute), baseTime)
	expected := baseTime.Add(10 * time.Minute).Add(-5 * time.Minute)
	if !w.Equal(expected) {
		t.Errorf("expected first event after empty restore to advance watermark to %v, got %v", expected, w)
	}
}

// TestWatermarkTracker_SnapshotIsIndependentCopy verifies Snapshot returns a
// deep copy: mutating the tracker after taking a snapshot must not change
// the already-taken snapshot's maps, since a checkpoint is meant to be saved
// as-is at a point in time.
func TestWatermarkTracker_SnapshotIsIndependentCopy(t *testing.T) {
	baseTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	tracker := NewWatermarkTracker(5*time.Minute, 10*time.Minute)

	tracker.ProcessEvent(0, "SITE-01:1", baseTime.Add(10*time.Minute), baseTime)
	snap := tracker.Snapshot()
	originalHigh := snap.PartitionHighWatermark[0]

	tracker.ProcessEvent(0, "SITE-01:2", baseTime.Add(90*time.Minute), baseTime.Add(time.Minute))

	if !snap.PartitionHighWatermark[0].Equal(originalHigh) {
		t.Fatalf("expected snapshot's partition high watermark to remain %v after further mutation, got %v", originalHigh, snap.PartitionHighWatermark[0])
	}
}
