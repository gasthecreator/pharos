package edge

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// TestForwarder_RunKeepsRetryingThroughAnOutageAndNeverDropsTheRecord:
// Run's contract is "retry forever, lose nothing, stop only when told to".
// With Central Ingestion unreachable, the record must stay queued (not
// acknowledged, not rejected), attempts must keep being made, and Run must
// return the context's error as soon as the context ends.
func TestForwarder_RunKeepsRetryingThroughAnOutageAndNeverDropsTheRecord(t *testing.T) {
	store, _ := setupTestStore(t)
	siteID := "SITE-RUN-01"
	if _, err := store.Enqueue(context.Background(), siteID, newTestEvent(siteID)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var attempts atomic.Int64
	client := &mockHTTPClient{doFunc: func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, errors.New("dial tcp: connection refused")
	}}
	cfg := DefaultForwarderConfig("http://central.invalid/api/v1/events", siteID)
	cfg.PollInterval = 5 * time.Millisecond
	cfg.BaseBackoff = time.Millisecond
	cfg.MaxBackoff = 5 * time.Millisecond
	forwarder := NewForwarder(store, client, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- forwarder.Run(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after its context expired")
	}

	if got := attempts.Load(); got < 2 {
		t.Errorf("only %d delivery attempt(s); Run should keep retrying through an outage", got)
	}
	stats, err := store.GetStats(context.Background())
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.AcknowledgedCount != 0 || stats.RejectedCount != 0 {
		t.Errorf("record was finalized during an outage: acked=%d rejected=%d", stats.AcknowledgedCount, stats.RejectedCount)
	}
	if stats.PendingCount+stats.InFlightCount+stats.FailedCount != 1 {
		t.Errorf("the record is no longer queued for retry: %+v", stats)
	}
}

// TestForwarder_RunReturnsImmediatelyWhenAlreadyCancelled covers the
// shutdown-before-start edge.
func TestForwarder_RunReturnsImmediatelyWhenAlreadyCancelled(t *testing.T) {
	store, _ := setupTestStore(t)
	client := &mockHTTPClient{doFunc: func(*http.Request) (*http.Response, error) {
		t.Error("no delivery should be attempted with a cancelled context")
		return nil, errors.New("unexpected")
	}}
	forwarder := NewForwarder(store, client, DefaultForwarderConfig("http://central.invalid/api/v1/events", "SITE-RUN-02"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := forwarder.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}
