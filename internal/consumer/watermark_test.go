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
