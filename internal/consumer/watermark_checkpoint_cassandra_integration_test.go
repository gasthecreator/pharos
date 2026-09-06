package consumer

import (
	"context"
	"testing"
	"time"
)

// TestCassandraCanonicalStore_WatermarkCheckpointMergesAcrossInstances is
// TestSaveWatermarkCheckpoint_MergesAcrossInstances against real Cassandra
// (§2.4, PLAN.md Slice 19: Multi-instance scaling) -- the memory-store
// version alone doesn't prove the actual CQL map-addition syntax
// (`col + {...}`) genuinely merges rather than overwrites against a real
// cluster, which is the only thing that matters for production
// correctness.
func TestCassandraCanonicalStore_WatermarkCheckpointMergesAcrossInstances(t *testing.T) {
	ctx := context.Background()
	store, err := NewCassandraCanonicalStore(DefaultCassandraStoreConfig())
	if err != nil {
		t.Fatalf("failed to connect to real Cassandra: %v", err)
	}
	defer store.Close()

	groupID := "test-multi-instance-cassandra-" + time.Now().UTC().Format("20060102T150405.000000000")
	t0 := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

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

	merged, err := store.LoadWatermarkCheckpoint(ctx, groupID)
	if err != nil {
		t.Fatalf("LoadWatermarkCheckpoint failed: %v", err)
	}
	if merged == nil {
		t.Fatalf("expected a checkpoint to exist after two saves")
	}
	if len(merged.PartitionHighWatermark) != 3 {
		t.Fatalf("CRITICAL: expected all 3 partitions (0, 1, 2) in the merged checkpoint against real Cassandra, got %d: %+v",
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
	if !merged.PreviousEmitted.Equal(t0.Add(5 * time.Second)) {
		t.Errorf("expected previous_emitted to be the max across instances (%v), got %v",
			t0.Add(5*time.Second), merged.PreviousEmitted)
	}

	// Instance A saves again with a smaller previous_emitted -- must not regress.
	instanceASecondSave := WatermarkCheckpoint{
		PreviousEmitted: t0.Add(2 * time.Second),
		PartitionHighWatermark: map[int]time.Time{
			0: t0.Add(2 * time.Second),
		},
		PartitionLastActivity: map[int]time.Time{
			0: t0.Add(2 * time.Second),
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
		t.Fatalf("CRITICAL: previous_emitted regressed from %v to %v against real Cassandra after an instance with a smaller local view saved",
			t0.Add(5*time.Second), afterSecondSave.PreviousEmitted)
	}
	if _, ok := afterSecondSave.PartitionHighWatermark[2]; !ok {
		t.Fatalf("CRITICAL: partition 2 (instance B's) was lost from real Cassandra after instance A's second, unrelated save")
	}
}
