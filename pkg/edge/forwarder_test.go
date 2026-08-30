package edge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/ingestion"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

type mockHTTPClient struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func TestForwarder_HappyPathDelivery(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-FORWARD-01"

	// 1. Enqueue 3 events
	for i := 0; i < 3; i++ {
		_, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
		if err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	// 2. Mock Central Ingestion returning HTTP 200
	var receivedCount int32
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			atomic.AddInt32(&receivedCount, 1)
			body := `{"total": 3, "accepted": 3, "rejected": 0, "results": [{"status":"ACCEPTED"}]}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(body)),
				Header:     make(http.Header),
			}, nil
		},
	}

	cfg := DefaultForwarderConfig("http://mock-central/api/v1/events", siteID)
	forwarder := NewForwarder(store, client, cfg)

	count, err := forwarder.Step(ctx)
	if err != nil {
		t.Fatalf("Step failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 forwarded, got %d", count)
	}

	// Verify all 3 records are now ACKNOWLEDGED
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.AcknowledgedCount != 3 {
		t.Errorf("expected 3 acknowledged, got %d", stats.AcknowledgedCount)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending, got %d", stats.PendingCount)
	}
}

func TestForwarder_Server5xxRetryAndExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-5XX"

	// Enqueue 1 event
	rec, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Mock server returning HTTP 503 Service Unavailable
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader(`{"error":"central pipeline unavailable"}`)),
				Header:     make(http.Header),
			}, nil
		},
	}

	cfg := DefaultForwarderConfig("http://mock-central/api/v1/events", siteID)
	cfg.BaseBackoff = 2 * time.Second
	forwarder := NewForwarder(store, client, cfg)

	count, err := forwarder.Step(ctx)
	if err == nil {
		t.Fatalf("expected Step to return error on HTTP 503")
	}
	if count != 0 {
		t.Errorf("expected 0 forwarded, got %d", count)
	}

	// Record should now be FAILED with attempts = 1 and future next_retry_at
	stats, _ := store.GetStats(ctx)
	if stats.FailedCount != 1 {
		t.Errorf("expected 1 failed record in stats, got %d", stats.FailedCount)
	}

	// Second immediate Step should fetch 0 records because backoff is active
	count2, err2 := forwarder.Step(ctx)
	if err2 != nil {
		t.Fatalf("step 2 should not fail since no records ready: %v", err2)
	}
	if count2 != 0 {
		t.Errorf("expected 0 records fetched during backoff, got %d", count2)
	}

	_ = rec
}

func TestForwarder_NetworkTimeoutAndConnectionRefused(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-NETWORK-FAIL"

	_, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Mock client returning network connection refused
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("dial tcp 127.0.0.1:9999: connect: connection refused")
		},
	}

	cfg := DefaultForwarderConfig("http://unreachable:9999/api/v1/events", siteID)
	forwarder := NewForwarder(store, client, cfg)

	count, err := forwarder.Step(ctx)
	if err == nil {
		t.Fatal("expected error on connection refused")
	}
	if count != 0 {
		t.Errorf("expected 0 forwarded, got %d", count)
	}

	stats, _ := store.GetStats(ctx)
	if stats.FailedCount != 1 {
		t.Errorf("expected failed count 1, got %d", stats.FailedCount)
	}
}

func TestForwarder_RateLimit429WithRetryAfter(t *testing.T) {
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-429"

	_, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// Mock server returning HTTP 429 with Retry-After: 4
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			header := make(http.Header)
			header.Set("Retry-After", "4")
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(strings.NewReader(`{"error":"rate limit exceeded"}`)),
				Header:     header,
			}, nil
		},
	}

	cfg := DefaultForwarderConfig("http://mock-central/api/v1/events", siteID)
	forwarder := NewForwarder(store, client, cfg)

	_, err = forwarder.Step(ctx)
	if err == nil {
		t.Fatal("expected error on 429")
	}

	stats, _ := store.GetStats(ctx)
	if stats.FailedCount != 1 {
		t.Errorf("expected failed count 1, got %d", stats.FailedCount)
	}

	// Verify retry is scheduled in the future
	pending, _ := store.FetchPending(ctx, 10)
	if len(pending) != 0 {
		t.Errorf("expected 0 pending due to Retry-After window, got %d", len(pending))
	}
}

func TestForwarder_ValidationRejectionHandling(t *testing.T) {
	// Tests that when Central Ingestion returns HTTP 422 (malformed FHIR rejection),
	// the edge marks the record acknowledged so it does not loop in an infinite retry poison-pill.
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-REJECT"

	_, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			respBody := `{"total":1,"accepted":0,"rejected":1,"results":[{"idempotency_key":"SITE-REJECT:1","status":"REJECTED","error":"subject reference required"}]}`
			return &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Body:       io.NopCloser(strings.NewReader(respBody)),
				Header:     make(http.Header),
			}, nil
		},
	}

	cfg := DefaultForwarderConfig("http://mock-central/api/v1/events", siteID)
	forwarder := NewForwarder(store, client, cfg)

	count, err := forwarder.Step(ctx)
	if err != nil {
		t.Fatalf("expected rejected event to be acknowledged and not return error, got: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 record processed, got %d", count)
	}

	stats, _ := store.GetStats(ctx)
	if stats.AcknowledgedCount != 1 {
		t.Errorf("expected rejected event to be acknowledged, got %d", stats.AcknowledgedCount)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending, got %d", stats.PendingCount)
	}
}

func TestForwarder_EndToEndWithCentralIngestion(t *testing.T) {
	// Full Integration: Real SQLite Store -> Real Forwarder -> Real HTTP Ingestion Handler
	ctx := context.Background()
	store, _ := setupTestStore(t)
	siteID := "SITE-E2E"

	// Spin up Central Ingestion test server
	limiter := ratelimit.NewTokenBucketLimiter(100, 10)
	ingestHandler := ingestion.NewHandler(limiter)
	server := httptest.NewServer(ingestHandler)
	defer server.Close()

	// Enqueue 5 valid events locally
	for i := 0; i < 5; i++ {
		_, err := store.Enqueue(ctx, siteID, newTestEvent(siteID))
		if err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}

	// Configure Forwarder pointing to the real Central Ingestion HTTP test server
	cfg := DefaultForwarderConfig(fmt.Sprintf("%s/api/v1/events", server.URL), siteID)
	cfg.BatchSize = 10
	forwarder := NewForwarder(store, server.Client(), cfg)

	// Execute forwarder step
	forwarded, err := forwarder.Step(ctx)
	if err != nil {
		t.Fatalf("Step failed in E2E: %v", err)
	}
	if forwarded != 5 {
		t.Fatalf("expected 5 forwarded, got %d", forwarded)
	}

	// Verify all 5 events were delivered, accepted, and acknowledged
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.AcknowledgedCount != 5 {
		t.Errorf("expected 5 acknowledged in store, got %d", stats.AcknowledgedCount)
	}
	if stats.PendingCount != 0 {
		t.Errorf("expected 0 pending in store, got %d", stats.PendingCount)
	}

	accepted, rejected, throttled := ingestHandler.Stats()
	if accepted != 5 {
		t.Errorf("expected 5 accepted by Central Ingestion, got %d", accepted)
	}
	if rejected != 0 {
		t.Errorf("expected 0 rejected, got %d", rejected)
	}
	if throttled != 0 {
		t.Errorf("expected 0 throttled, got %d", throttled)
	}
}
