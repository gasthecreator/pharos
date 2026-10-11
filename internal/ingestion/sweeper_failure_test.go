package ingestion

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
)

// faultyStore wraps a real in-memory outbox store and injects errors into
// individual operations, so the sweeper's failure paths (an unreachable
// Cassandra mid-sweep) can be exercised without infrastructure.
type faultyStore struct {
	dedup.OutboxStore
	fetchErr      error
	insertErr     error
	markErr       error
	fetchCalls    atomic.Int64
	markPublished atomic.Int64
}

func (f *faultyStore) FetchStaleClaims(ctx context.Context, lease time.Duration, limit int) ([]dedup.OutboxRecord, []dedup.DLQRecord, error) {
	f.fetchCalls.Add(1)
	if f.fetchErr != nil {
		return nil, nil, f.fetchErr
	}
	return f.OutboxStore.FetchStaleClaims(ctx, lease, limit)
}

func (f *faultyStore) InsertClaim(ctx context.Context, rec dedup.OutboxRecord, lease time.Duration) (dedup.ClaimResult, error) {
	if f.insertErr != nil {
		return dedup.ClaimResult{}, f.insertErr
	}
	return f.OutboxStore.InsertClaim(ctx, rec, lease)
}

func (f *faultyStore) MarkPublished(ctx context.Context, key string, expected time.Time, topic string, partition int, offset int64) error {
	f.markPublished.Add(1)
	if f.markErr != nil {
		return f.markErr
	}
	return f.OutboxStore.MarkPublished(ctx, key, expected, topic, partition, offset)
}

const sweepLease = 30 * time.Second

// abandonedClaim seeds one in-flight claim and advances the simulated clock
// past its lease, as if the claiming worker crashed mid-publish.
func abandonedClaim(t *testing.T, key string) (*faultyStore, *clock.Simulated) {
	t.Helper()
	clk := clock.NewSimulated(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	mem := dedup.NewMemoryOutboxStore()
	mem.SetClock(clk)
	claim, err := mem.InsertClaim(context.Background(), dedup.OutboxRecord{IdempotencyKey: key, SiteID: "SITE-SW", Payload: []byte(`{"x":1}`)}, sweepLease)
	if err != nil || !claim.Acquired {
		t.Fatalf("seed claim: acquired=%v err=%v", claim.Acquired, err)
	}
	clk.Advance(sweepLease + time.Second)
	return &faultyStore{OutboxStore: mem}, clk
}

func TestSweeper_StepFailurePaths(t *testing.T) {
	tests := []struct {
		name          string
		configure     func(f *faultyStore, p *kafka.MockProducer)
		wantErr       error
		wantRecovered int
		wantPublishes int
		wantStatus    dedup.OutboxStatus // status of the seeded record after the sweep
	}{
		{
			name:          "healthy: abandoned claim is republished and finalized",
			configure:     func(*faultyStore, *kafka.MockProducer) {},
			wantRecovered: 1, wantPublishes: 1, wantStatus: dedup.StatusPublished,
		},
		{
			name:          "kafka publish fails: claim stays PUBLISHING for the next sweep",
			configure:     func(_ *faultyStore, p *kafka.MockProducer) { p.FailNext(1) },
			wantRecovered: 0, wantPublishes: 0, wantStatus: dedup.StatusPublishing,
		},
		{
			name:          "cassandra unreachable while listing stale claims",
			configure:     func(f *faultyStore, _ *kafka.MockProducer) { f.fetchErr = errors.New("cassandra unavailable") },
			wantErr:       errors.New("failed to fetch stale claims"),
			wantRecovered: 0, wantPublishes: 0, wantStatus: dedup.StatusPublishing,
		},
		{
			name:          "listing is cancelled mid-sweep",
			configure:     func(f *faultyStore, _ *kafka.MockProducer) { f.fetchErr = context.Canceled },
			wantErr:       context.Canceled,
			wantRecovered: 0, wantPublishes: 0, wantStatus: dedup.StatusPublishing,
		},
		{
			name:          "cannot steal the lease (claim insert errors): nothing is published",
			configure:     func(f *faultyStore, _ *kafka.MockProducer) { f.insertErr = errors.New("LWT timed out") },
			wantRecovered: 0, wantPublishes: 0, wantStatus: dedup.StatusPublishing,
		},
		{
			name:          "published but finalize fails: left PUBLISHING (at-least-once)",
			configure:     func(f *faultyStore, _ *kafka.MockProducer) { f.markErr = errors.New("write timeout") },
			wantRecovered: 1, wantPublishes: 1, wantStatus: dedup.StatusPublishing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := abandonedClaim(t, "SITE-SW:1")
			producer := kafka.NewMockProducer()
			tt.configure(store, producer)

			sweeper := NewSweeper(store, producer, time.Hour, sweepLease)
			recovered, err := sweeper.Step(context.Background())

			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				if errors.Is(tt.wantErr, context.Canceled) && !errors.Is(err, context.Canceled) {
					t.Fatalf("err = %v, want it to wrap context.Canceled", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if recovered != tt.wantRecovered {
				t.Errorf("recovered = %d, want %d", recovered, tt.wantRecovered)
			}
			if got := producer.TotalPublishes(); got != tt.wantPublishes {
				t.Errorf("kafka publishes = %d, want %d", got, tt.wantPublishes)
			}
			rec, gerr := store.GetOutboxRecord(context.Background(), "SITE-SW:1")
			if gerr != nil {
				t.Fatalf("GetOutboxRecord: %v", gerr)
			}
			if rec.Status != tt.wantStatus {
				t.Errorf("record status = %s, want %s", rec.Status, tt.wantStatus)
			}
		})
	}
}

// TestSweeper_RecoversOnTheNextSweepAfterATransientFailure: a failed publish
// must not strand the record; once Kafka is back and the new lease lapses, the
// following sweep finishes the job with exactly one successful publish.
func TestSweeper_RecoversOnTheNextSweepAfterATransientFailure(t *testing.T) {
	store, clk := abandonedClaim(t, "SITE-SW:2")
	producer := kafka.NewMockProducer()
	sweeper := NewSweeper(store, producer, time.Hour, sweepLease)
	ctx := context.Background()

	producer.FailNext(1)
	if n, err := sweeper.Step(ctx); err != nil || n != 0 {
		t.Fatalf("first sweep: recovered=%d err=%v, want 0 and nil", n, err)
	}
	clk.Advance(sweepLease + time.Second) // the sweeper's own re-claim now expires too
	if n, err := sweeper.Step(ctx); err != nil || n != 1 {
		t.Fatalf("second sweep: recovered=%d err=%v, want 1 and nil", n, err)
	}
	if got := producer.KeyPublishCount("SITE-SW"); got != 1 {
		t.Fatalf("publishes for the site key = %d, want exactly 1", got)
	}
	rec, err := store.GetOutboxRecord(ctx, "SITE-SW:2")
	if err != nil || rec.Status != dedup.StatusPublished {
		t.Fatalf("record = %+v err=%v, want PUBLISHED", rec, err)
	}
}

// TestSweeper_StartStopsOnContextCancellation: cancelling the context ends the
// background loop, so a shutting-down service does not leak a sweeping goroutine
// that keeps calling into a closed store.
func TestSweeper_StartStopsOnContextCancellation(t *testing.T) {
	store := &faultyStore{OutboxStore: dedup.NewMemoryOutboxStore()}
	sweeper := NewSweeper(store, kafka.NewMockProducer(), 5*time.Millisecond, sweepLease)

	ctx, cancel := context.WithCancel(context.Background())
	sweeper.Start(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for store.fetchCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("sweeper never ran")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	time.Sleep(50 * time.Millisecond) // let any in-flight tick finish
	settled := store.fetchCalls.Load()
	time.Sleep(100 * time.Millisecond)
	if after := store.fetchCalls.Load(); after != settled {
		t.Fatalf("sweeper kept running after cancellation: %d -> %d fetches", settled, after)
	}
}
