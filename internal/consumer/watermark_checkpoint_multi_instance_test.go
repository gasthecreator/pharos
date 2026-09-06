package consumer

import (
	"context"
	"testing"
	"time"
)

// TestSaveWatermarkCheckpoint_MergesAcrossInstances proves the actual
// property PLAN.md's Slice 19 (Multi-instance scaling) exists to verify:
// with 2+ pharos-consumer instances sharing one Kafka consumer group
// (required for rebalancing to split partitions between them at all),
// each instance's own WatermarkTracker only ever observes the partitions
// Kafka assigned to *it*. A naive overwrite of the checkpoint row keyed
// by group_id alone -- what this code did before Slice 19 -- means
// whichever instance saves last replaces the *other* instance's
// partitions with nothing, silently discarding real crash-recovery data.
// This test simulates exactly that: two "instances" (A owns partitions
// 0-1, B owns partition 2) checkpointing independently, and asserts the
// merged result has *all three* partitions, not just whichever saved
// last.
func TestSaveWatermarkCheckpoint_MergesAcrossInstances(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCanonicalStore()
	groupID := "test-multi-instance-group"

	t0 := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

	// Instance A checkpoints its own view: partitions 0 and 1 only.
	instanceA := WatermarkCheckpoint{
		PreviousEmitted: t0,
		PartitionHighWatermark: map[int]time.Time{
			0: t0,
			1: t0.Add(1 * time.Second),
		},
		PartitionLastActivity: map[int]time.Time{
			0: t0,
			1: t0.Add(1 * time.Second),
		},
	}
	if err := store.SaveWatermarkCheckpoint(ctx, groupID, instanceA); err != nil {
		t.Fatalf("instance A checkpoint failed: %v", err)
	}

	// Instance B checkpoints its own view: partition 2 only, further
	// ahead in event time than what A has seen (a realistic scenario --
	// different partitions progress at different rates).
	instanceB := WatermarkCheckpoint{
		PreviousEmitted: t0.Add(5 * time.Second),
		PartitionHighWatermark: map[int]time.Time{
			2: t0.Add(5 * time.Second),
		},
		PartitionLastActivity: map[int]time.Time{
			2: t0.Add(5 * time.Second),
		},
	}
	if err := store.SaveWatermarkCheckpoint(ctx, groupID, instanceB); err != nil {
		t.Fatalf("instance B checkpoint failed: %v", err)
	}

	// The merged checkpoint must contain ALL THREE partitions -- if B's
	// save clobbered A's data (the bug this test exists to catch),
	// partitions 0 and 1 would be missing here.
	merged, err := store.LoadWatermarkCheckpoint(ctx, groupID)
	if err != nil {
		t.Fatalf("LoadWatermarkCheckpoint failed: %v", err)
	}
	if merged == nil {
		t.Fatalf("expected a checkpoint to exist after two saves")
	}
	if len(merged.PartitionHighWatermark) != 3 {
		t.Fatalf("CRITICAL: expected all 3 partitions (0, 1, 2) in the merged checkpoint, got %d: %+v",
			len(merged.PartitionHighWatermark), merged.PartitionHighWatermark)
	}
	for p, want := range map[int]time.Time{0: t0, 1: t0.Add(1 * time.Second), 2: t0.Add(5 * time.Second)} {
		got, ok := merged.PartitionHighWatermark[p]
		if !ok {
			t.Errorf("partition %d missing from merged checkpoint entirely", p)
			continue
		}
		if !got.Equal(want) {
			t.Errorf("partition %d: expected %v, got %v", p, want, got)
		}
	}

	// previous_emitted must reflect the FURTHEST-ahead value seen by
	// either instance (B's), not regress to A's smaller, earlier value
	// just because A happened to save at some point.
	if !merged.PreviousEmitted.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("expected previous_emitted to be the max across instances (%v), got %v",
			t0.Add(5*time.Second), merged.PreviousEmitted)
	}

	// Now instance A saves AGAIN with a smaller previous_emitted than
	// what's already persisted (its own local view genuinely is behind
	// B's) -- this must NOT regress the shared value.
	instanceASecondSave := WatermarkCheckpoint{
		PreviousEmitted: t0.Add(2 * time.Second), // still less than B's 5s
		PartitionHighWatermark: map[int]time.Time{
			0: t0.Add(2 * time.Second),
			1: t0.Add(2 * time.Second),
		},
		PartitionLastActivity: map[int]time.Time{
			0: t0.Add(2 * time.Second),
			1: t0.Add(2 * time.Second),
		},
	}
	if err := store.SaveWatermarkCheckpoint(ctx, groupID, instanceASecondSave); err != nil {
		t.Fatalf("instance A second checkpoint failed: %v", err)
	}
	afterSecondSave, err := store.LoadWatermarkCheckpoint(ctx, groupID)
	if err != nil {
		t.Fatalf("LoadWatermarkCheckpoint failed: %v", err)
	}
	if !afterSecondSave.PreviousEmitted.Equal(t0.Add(5 * time.Second)) {
		t.Fatalf("CRITICAL: previous_emitted regressed from %v to %v after an instance with a smaller local view saved -- this is exactly the regression Slice 13's monotonic guard exists to prevent",
			t0.Add(5*time.Second), afterSecondSave.PreviousEmitted)
	}
	// But partition 2 (B's, untouched by A's second save) must still be present.
	if _, ok := afterSecondSave.PartitionHighWatermark[2]; !ok {
		t.Fatalf("CRITICAL: partition 2 (instance B's) was lost after instance A's second, unrelated save")
	}
}
