package edge

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gasthecreator/pharos/internal/model"
)

func TestServer_CaptureValidEvent(t *testing.T) {
	store, _ := setupTestStore(t)
	siteID := "SITE-US-01"
	server := NewServer(store, siteID)

	ev := newTestEvent(siteID)
	body, _ := json.Marshal(ev)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/adverse-events", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected HTTP 201, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp CaptureResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != "QUEUED" {
		t.Errorf("expected status QUEUED, got %s", resp.Status)
	}
	if resp.IdempotencyKey != "SITE-US-01:1" {
		t.Errorf("expected idempotency key SITE-US-01:1, got %s", resp.IdempotencyKey)
	}
	if resp.LocalSeq != 1 {
		t.Errorf("expected local seq 1, got %d", resp.LocalSeq)
	}
}

func TestServer_CaptureMalformedFHIREvent(t *testing.T) {
	// Conformance test (§2.3): The edge MUST accept and durably persist
	// even FHIR-malformed events, assigning an idempotency key and buffering locally.
	store, _ := setupTestStore(t)
	siteID := "SITE-NG-02"
	server := NewServer(store, siteID)

	malformed := newTestEvent(siteID)
	malformed.Subject.Reference = "" // Missing required subject
	malformed.Severity = model.CodeableConcept{Text: "invalid-level"}

	if err := malformed.Validate(); err == nil {
		t.Fatal("setup invalid: event should fail FHIR validation")
	}

	body, _ := json.Marshal(malformed)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/adverse-events", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("edge must buffer malformed FHIR events with HTTP 201, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp CaptureResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.IdempotencyKey != "SITE-NG-02:1" {
		t.Errorf("expected idempotency key SITE-NG-02:1, got %s", resp.IdempotencyKey)
	}

	// Verify that the record is indeed in the SQLite store and retrievable
	pending, err := store.FetchPending(t.Context(), 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected 1 pending record in store, got %d (err: %v)", len(pending), err)
	}
}

func TestServer_MalformedJSON(t *testing.T) {
	store, _ := setupTestStore(t)
	server := NewServer(store, "SITE-01")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/adverse-events", bytes.NewReader([]byte(`{not json`)))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for unparseable JSON syntax, got %d", rec.Code)
	}
}

func TestServer_EmptyBody(t *testing.T) {
	store, _ := setupTestStore(t)
	server := NewServer(store, "SITE-01")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/adverse-events", bytes.NewReader(nil))
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for empty body, got %d", rec.Code)
	}
}

func TestServer_StatsEndpoint(t *testing.T) {
	store, _ := setupTestStore(t)
	siteID := "SITE-STATS"
	server := NewServer(store, siteID)

	// Enqueue 2 events directly
	_, _ = store.Enqueue(t.Context(), siteID, newTestEvent(siteID))
	_, _ = store.Enqueue(t.Context(), siteID, newTestEvent(siteID))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for stats, got %d", rec.Code)
	}

	var stats QueueStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if stats.PendingCount != 2 {
		t.Errorf("expected 2 pending events in stats, got %d", stats.PendingCount)
	}
	if stats.MaxSequence != 2 {
		t.Errorf("expected max sequence 2 in stats, got %d", stats.MaxSequence)
	}
}
