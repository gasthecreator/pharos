package dedup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
)

// TestMemoryOutboxStore_FetchStaleClaims is the failure-path coverage the
// sweeper depends on: which in-flight claims count as abandoned, which
// terminal states must never be returned, how the limit applies, and what
// a closed store reports. FetchStaleClaims had no unit coverage before.
func TestMemoryOutboxStore_FetchStaleClaims(t *testing.T) {
	const lease = 30 * time.Second
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		advance     time.Duration // elapsed since the claim was taken
		finalize    string        // "", "published", "replayed"
		limit       int
		wantOutbox  int
		wantDLQ     int
		closeBefore bool
		wantErr     error
	}{
		{name: "fresh lease is not stale", advance: lease - time.Second, wantOutbox: 0, wantDLQ: 0},
		{name: "expired lease is stale", advance: lease, wantOutbox: 2, wantDLQ: 2},
		{name: "long-expired lease is stale", advance: 24 * time.Hour, wantOutbox: 2, wantDLQ: 2},
		{name: "published claim is never stale", advance: 24 * time.Hour, finalize: "published", wantOutbox: 0, wantDLQ: 0},
		{name: "replayed DLQ record is never stale", advance: 24 * time.Hour, finalize: "replayed", wantOutbox: 0, wantDLQ: 0},
		{name: "limit of one caps each list", advance: lease, limit: 1, wantOutbox: 1, wantDLQ: 1},
		{name: "closed store reports ErrStoreClosed", advance: lease, closeBefore: true, wantErr: ErrStoreClosed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			clk := clock.NewSimulated(start)
			store := NewMemoryOutboxStore()
			store.SetClock(clk)

			// Two outbox claims and two DLQ claims, so that limit=1 is observable.
			for _, key := range []string{"SITE-A:1", "SITE-A:2"} {
				if tt.finalize == "replayed" {
					break // REPLAYED exists for DLQ rows only; this case seeds no outbox rows
				}
				claim, err := store.InsertClaim(ctx, OutboxRecord{IdempotencyKey: key, SiteID: "SITE-A", Payload: []byte(`{}`)}, lease)
				if err != nil || !claim.Acquired {
					t.Fatalf("InsertClaim(%s): acquired=%v err=%v", key, claim.Acquired, err)
				}
				if tt.finalize == "published" {
					if err := store.MarkPublished(ctx, key, claim.ClaimedAt, "t", 0, 1); err != nil {
						t.Fatalf("MarkPublished(%s): %v", key, err)
					}
				}
			}
			for _, key := range []string{"SITE-A:10", "SITE-A:11"} {
				claim, err := store.InsertDLQClaim(ctx, DLQRecord{IdempotencyKey: key, SiteID: "SITE-A", Payload: []byte(`{}`), RejectionReason: "r"}, lease)
				if err != nil || !claim.Acquired {
					t.Fatalf("InsertDLQClaim(%s): acquired=%v err=%v", key, claim.Acquired, err)
				}
				switch tt.finalize {
				case "published", "replayed":
					if err := store.MarkDLQPublished(ctx, key, claim.ClaimedAt, "t", 0, 1); err != nil {
						t.Fatalf("MarkDLQPublished(%s): %v", key, err)
					}
				}
				if tt.finalize == "replayed" {
					if err := store.MarkDLQReplayed(ctx, key); err != nil {
						t.Fatalf("MarkDLQReplayed(%s): %v", key, err)
					}
				}
			}

			clk.Advance(tt.advance)
			if tt.closeBefore {
				if err := store.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			}

			outbox, dlq, err := store.FetchStaleClaims(ctx, lease, tt.limit)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FetchStaleClaims: %v", err)
			}
			if len(outbox) != tt.wantOutbox || len(dlq) != tt.wantDLQ {
				t.Fatalf("got %d outbox / %d dlq stale claims, want %d / %d", len(outbox), len(dlq), tt.wantOutbox, tt.wantDLQ)
			}
		})
	}
}

// TestMemoryOutboxStore_OperationsAfterClose pins that every operation on a
// closed store fails with ErrStoreClosed rather than silently succeeding.
func TestMemoryOutboxStore_OperationsAfterClose(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ops := map[string]func() error{
		"InsertClaim": func() error {
			_, err := store.InsertClaim(ctx, OutboxRecord{IdempotencyKey: "S:1", SiteID: "S"}, time.Second)
			return err
		},
		"InsertDLQClaim": func() error {
			_, err := store.InsertDLQClaim(ctx, DLQRecord{IdempotencyKey: "S:1", SiteID: "S"}, time.Second)
			return err
		},
		"GetOutboxRecord": func() error { _, err := store.GetOutboxRecord(ctx, "S:1"); return err },
		"GetDLQRecord":    func() error { _, err := store.GetDLQRecord(ctx, "S:1"); return err },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			if err := op(); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("%s on a closed store returned %v, want ErrStoreClosed", name, err)
			}
		})
	}
}
