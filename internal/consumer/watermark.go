package consumer

import (
	"sync"
	"time"
)

// PartitionWatermarkStat provides diagnostic metrics for an assigned partition.
type PartitionWatermarkStat struct {
	Partition        int       `json:"partition"`
	HighEventTime    time.Time `json:"high_event_time"`
	LastActivityTime time.Time `json:"last_activity_time"`
	IsActive         bool      `json:"is_active"`
}

// WatermarkCheckpoint is a persistable snapshot of a WatermarkTracker's state
// (§2.4, ARCHITECTURE_PROPOSALS.md "Slice 13: Consumer Crash/Restart Watermark
// Continuity") -- without this, a process restart resets PreviousEmitted to
// zero, and the strict monotonic guard's own zero-value escape hatch lets the
// externally observed watermark regress below what was already reported
// pre-crash. All three fields are restored together, not just
// PreviousEmitted, since PartitionLastActivity also feeds the active/idle
// partition classification that computeCandidateWatermarkLocked depends on.
type WatermarkCheckpoint struct {
	PreviousEmitted        time.Time
	PartitionHighWatermark map[int]time.Time
	PartitionLastActivity  map[int]time.Time
}

// WatermarkTracker implements event-time watermarking with idle-source detection
// and monotonic progression guards (§2.4).
type WatermarkTracker struct {
	mu                     sync.RWMutex
	latenessTolerance      time.Duration
	idleTimeout            time.Duration
	partitionHighWatermark map[int]time.Time
	partitionLastActivity  map[int]time.Time
	previousEmitted        time.Time
	windows                map[string]*Window
	lateAudits             []LateArrivalAudit
	// lateAuditKeys maps windowID+":"+idempotencyKey to that entry's index in
	// lateAudits, both for 21 CFR Part 11 audit deduplication and so a
	// redelivered message can look its own entry back up in O(1) -- needed
	// so Engine.Step can retry persisting it to Cassandra (§2.4, audit
	// remediation) idempotently on every delivery, not just the first.
	lateAuditKeys map[string]int
}

// NewWatermarkTracker constructs a WatermarkTracker with specified lateness and idle timeout thresholds.
func NewWatermarkTracker(latenessTolerance, idleTimeout time.Duration) *WatermarkTracker {
	if latenessTolerance < 0 {
		latenessTolerance = 0
	}
	if idleTimeout <= 0 {
		idleTimeout = 10 * time.Minute
	}
	return &WatermarkTracker{
		latenessTolerance:      latenessTolerance,
		idleTimeout:            idleTimeout,
		partitionHighWatermark: make(map[int]time.Time),
		partitionLastActivity:  make(map[int]time.Time),
		windows:                make(map[string]*Window),
		lateAuditKeys:          make(map[string]int),
	}
}

// ProcessEvent records event arrival, updates partition high-watermark & activity,
// and computes the monotonically non-decreasing stream watermark (§2.4).
// touchedAudit, when non-nil, is the LateArrivalAudit entry this call's event
// belongs to (whether just created or already recorded on a prior delivery
// of the same message) -- Engine.Step persists it to Cassandra on every
// call, not just the first, since that persistence is an idempotent upsert
// keyed the same way as lateAuditKeys (§2.4, audit remediation): a
// redelivered message must be able to retry a persist that failed last time,
// and returning the existing entry again (instead of nil) is what makes that
// retry possible.
func (wt *WatermarkTracker) ProcessEvent(partition int, idempotencyKey string, eventTime time.Time, now time.Time) (isLate bool, currentWatermark time.Time, touchedAudit *LateArrivalAudit) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	// 1. Mark partition active at current wall-clock time
	wt.partitionLastActivity[partition] = now

	// 2. Track highest event time seen on this partition
	currentHigh, exists := wt.partitionHighWatermark[partition]
	if !exists || eventTime.After(currentHigh) {
		wt.partitionHighWatermark[partition] = eventTime
	}

	// 3. Advance watermark with monotonic guard
	currentWatermark = wt.advanceWatermarkLocked(now)

	// 4. Check if the incoming event is late relative to the emitted watermark
	if !currentWatermark.IsZero() && eventTime.Before(currentWatermark) {
		isLate = true
	}

	// 5. Check if event belongs to an already closed window (transition COMPLETE -> REVISED)
	for _, w := range wt.windows {
		if !eventTime.Before(w.Start) && eventTime.Before(w.End) {
			switch w.Status {
			case WindowStatusComplete:
				w.Status = WindowStatusRevised
				w.RevisedAt = now
				audit := wt.appendLateAuditIfNotExistsLocked(w.ID, idempotencyKey, partition, eventTime, now, currentWatermark)
				touchedAudit = &audit
			case WindowStatusRevised:
				audit := wt.appendLateAuditIfNotExistsLocked(w.ID, idempotencyKey, partition, eventTime, now, currentWatermark)
				touchedAudit = &audit
			}
		}
	}

	return isLate, currentWatermark, touchedAudit
}

// appendLateAuditIfNotExistsLocked ensures exactly one LateArrivalAudit
// exists per (window, idempotencyKey) pair, and always returns that entry --
// freshly created, or the one already recorded from an earlier delivery of
// the same message -- so a caller can retry persisting it idempotently.
func (wt *WatermarkTracker) appendLateAuditIfNotExistsLocked(windowID, idempotencyKey string, partition int, eventTime, arrivedAt, watermark time.Time) LateArrivalAudit {
	dedupKey := windowID + ":" + idempotencyKey
	if idx, ok := wt.lateAuditKeys[dedupKey]; ok {
		return wt.lateAudits[idx]
	}
	audit := LateArrivalAudit{
		WindowID:           windowID,
		IdempotencyKey:     idempotencyKey,
		Partition:          partition,
		EventTime:          eventTime,
		ArrivedAt:          arrivedAt,
		WatermarkAtArrival: watermark,
	}
	wt.lateAuditKeys[dedupKey] = len(wt.lateAudits)
	wt.lateAudits = append(wt.lateAudits, audit)
	return audit
}

// CurrentWatermark recalculates and returns the latest watermark (e.g. on timer or query).
func (wt *WatermarkTracker) CurrentWatermark(now time.Time) time.Time {
	wt.mu.Lock()
	defer wt.mu.Unlock()
	return wt.advanceWatermarkLocked(now)
}

// computeCandidateWatermarkLocked computes the candidate watermark based on active partitions.
func (wt *WatermarkTracker) computeCandidateWatermarkLocked(now time.Time) (time.Time, bool) {
	if len(wt.partitionHighWatermark) == 0 {
		return time.Time{}, false
	}

	var activeHighWatermarks []time.Time
	var allHighWatermarks []time.Time

	for p, high := range wt.partitionHighWatermark {
		allHighWatermarks = append(allHighWatermarks, high)
		lastAct := wt.partitionLastActivity[p]
		if now.Sub(lastAct) <= wt.idleTimeout {
			activeHighWatermarks = append(activeHighWatermarks, high)
		}
	}

	// If at least one partition is active, min over active partitions
	if len(activeHighWatermarks) > 0 {
		minActive := activeHighWatermarks[0]
		for _, t := range activeHighWatermarks[1:] {
			if t.Before(minActive) {
				minActive = t
			}
		}
		return minActive.Add(-wt.latenessTolerance), true
	}

	// If all partitions are idle, use max over all partitions
	maxAll := allHighWatermarks[0]
	for _, t := range allHighWatermarks[1:] {
		if t.After(maxAll) {
			maxAll = t
		}
	}
	return maxAll.Add(-wt.latenessTolerance), true
}

// advanceWatermarkLocked enforces W_new = max(W_previous_emitted, candidate) and closes completed windows.
func (wt *WatermarkTracker) advanceWatermarkLocked(now time.Time) time.Time {
	candidate, ok := wt.computeCandidateWatermarkLocked(now)
	if ok {
		// Strict Monotonic Guard (§2.4): Never let the emitted watermark fall below previously emitted value
		if wt.previousEmitted.IsZero() || candidate.After(wt.previousEmitted) {
			wt.previousEmitted = candidate
		}
	}

	// Check window completeness
	if !wt.previousEmitted.IsZero() {
		for _, w := range wt.windows {
			if w.Status == WindowStatusOpen && !wt.previousEmitted.Before(w.End) {
				w.Status = WindowStatusComplete
				w.ClosedAt = now
			}
		}
	}

	return wt.previousEmitted
}

// RegisterWindow adds a new analytical window to track. now is the caller's
// current time (§2.4, Slice 22: property-based & deterministic simulation
// testing) -- WatermarkTracker's every other time-dependent method already
// takes an explicit now rather than calling time.Now() itself, so a
// property/simulation test can drive it with a controllable clock; this
// was the one remaining method still reaching for the real wall clock
// directly.
func (wt *WatermarkTracker) RegisterWindow(w Window, now time.Time) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	status := WindowStatusOpen
	var closedAt time.Time
	if !wt.previousEmitted.IsZero() && !wt.previousEmitted.Before(w.End) {
		status = WindowStatusComplete
		closedAt = now
	}

	wt.windows[w.ID] = &Window{
		ID:       w.ID,
		Start:    w.Start,
		End:      w.End,
		Status:   status,
		ClosedAt: closedAt,
	}
}

// GetWindow retrieves the current status of a tracked window.
func (wt *WatermarkTracker) GetWindow(id string) (Window, bool) {
	wt.mu.RLock()
	defer wt.mu.RUnlock()

	w, ok := wt.windows[id]
	if !ok {
		return Window{}, false
	}
	return *w, true
}

// GetLateArrivalAudits returns all recorded late arrival audit records.
func (wt *WatermarkTracker) GetLateArrivalAudits() []LateArrivalAudit {
	wt.mu.RLock()
	defer wt.mu.RUnlock()

	result := make([]LateArrivalAudit, len(wt.lateAudits))
	copy(result, wt.lateAudits)
	return result
}

// Snapshot returns a deep copy of the tracker's persistable state, suitable
// for saving as a WatermarkCheckpoint (§2.4, Slice 13). All timestamps are
// truncated to millisecond precision -- Cassandra's timestamp column type is
// itself only millisecond-precision, so persisting a Go time.Time with finer
// resolution and reading it back would otherwise silently produce a value
// microseconds/nanoseconds *earlier* than the original, which is exactly
// the kind of spurious "regression" the strict monotonic guard exists to
// catch and would incorrectly flag. Truncating here, at the point state is
// about to be persisted, makes Snapshot()'s return value exactly what a
// round trip through the checkpoint store will produce, whether or not
// persistence actually happens. Safe to call concurrently with ProcessEvent.
func (wt *WatermarkTracker) Snapshot() WatermarkCheckpoint {
	wt.mu.RLock()
	defer wt.mu.RUnlock()

	high := make(map[int]time.Time, len(wt.partitionHighWatermark))
	for p, t := range wt.partitionHighWatermark {
		high[p] = t.Truncate(time.Millisecond)
	}
	activity := make(map[int]time.Time, len(wt.partitionLastActivity))
	for p, t := range wt.partitionLastActivity {
		activity[p] = t.Truncate(time.Millisecond)
	}
	return WatermarkCheckpoint{
		PreviousEmitted:        wt.previousEmitted.Truncate(time.Millisecond),
		PartitionHighWatermark: high,
		PartitionLastActivity:  activity,
	}
}

// Restore seeds the tracker's state directly from a previously saved
// checkpoint (§2.4, Slice 13) -- called once at startup, before any live
// event is processed. This deliberately bypasses advanceWatermarkLocked's
// guarded path: restoring is initializing prior state, not a new event
// advancing the watermark, so there is nothing to guard against here. Must
// only be called before the first ProcessEvent/CurrentWatermark call on a
// freshly constructed tracker -- calling it afterward would silently
// overwrite whatever the tracker had already computed from live data.
func (wt *WatermarkTracker) Restore(cp WatermarkCheckpoint) {
	wt.mu.Lock()
	defer wt.mu.Unlock()

	wt.previousEmitted = cp.PreviousEmitted
	wt.partitionHighWatermark = make(map[int]time.Time, len(cp.PartitionHighWatermark))
	for p, t := range cp.PartitionHighWatermark {
		wt.partitionHighWatermark[p] = t
	}
	wt.partitionLastActivity = make(map[int]time.Time, len(cp.PartitionLastActivity))
	for p, t := range cp.PartitionLastActivity {
		wt.partitionLastActivity[p] = t
	}
}

// PartitionStats returns live partition activity stats.
func (wt *WatermarkTracker) PartitionStats(now time.Time) []PartitionWatermarkStat {
	wt.mu.RLock()
	defer wt.mu.RUnlock()

	var stats []PartitionWatermarkStat
	for p, high := range wt.partitionHighWatermark {
		lastAct := wt.partitionLastActivity[p]
		isActive := now.Sub(lastAct) <= wt.idleTimeout
		stats = append(stats, PartitionWatermarkStat{
			Partition:        p,
			HighEventTime:    high,
			LastActivityTime: lastAct,
			IsActive:         isActive,
		})
	}
	return stats
}
