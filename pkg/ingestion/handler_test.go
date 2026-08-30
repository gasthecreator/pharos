package ingestion

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/model"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

func validEvent(siteID string, seq uint64) model.AdverseEvent {
	now := time.Now().UTC()
	return model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Identifier: []model.Identifier{
			{
				System: model.IdempotencyKeySystem,
				Value:  model.IdempotencyKey{SiteID: siteID, LocalSeq: seq}.String(),
			},
		},
		Actuality: model.ActualityActual,
		Subject: model.Reference{
			Reference: "Patient/SUBJ-001",
		},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{System: model.MedDRASystem, Code: "10002198", Display: "Anaphylaxis"},
			},
			Text: "Anaphylaxis",
		},
		Date:         now.Add(-5 * time.Minute),
		RecordedDate: now,
		Severity: model.CodeableConcept{
			Coding: []model.Coding{{Code: "severe"}},
		},
		Study: []model.Reference{
			{Reference: "ResearchStudy/PHAROS-01"},
		},
		Location: model.Reference{
			Reference: "Location/" + siteID,
		},
	}
}

func TestHandler_AllValidBatch(t *testing.T) {
	limiter := ratelimit.NewTokenBucketLimiter(10, 1)
	h := NewHandler(limiter)
	siteID := "SITE-US-01"

	events := []model.AdverseEvent{
		validEvent(siteID, 1),
		validEvent(siteID, 2),
	}

	reqBody, _ := json.Marshal(BatchRequest{SiteID: siteID, Events: events})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()

	h.HandleEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp BatchResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Total != 2 || resp.Accepted != 2 || resp.Rejected != 0 {
		t.Errorf("unexpected counts: total=%d, accepted=%d, rejected=%d", resp.Total, resp.Accepted, resp.Rejected)
	}
	for i, r := range resp.Results {
		if r.Status != StatusAccepted {
			t.Errorf("result %d: expected ACCEPTED, got %s", i, r.Status)
		}
	}
}

func TestHandler_ValidationRejectionStructuredErrors(t *testing.T) {
	limiter := ratelimit.NewTokenBucketLimiter(10, 1)
	h := NewHandler(limiter)
	siteID := "SITE-NG-01"

	// Create an event that fails multiple validation rules
	malformed := validEvent(siteID, 1)
	malformed.Subject.Reference = "" // missing subject
	malformed.Severity = model.CodeableConcept{Text: "unknown-level"} // invalid severity

	reqBody, _ := json.Marshal(BatchRequest{SiteID: siteID, Events: []model.AdverseEvent{malformed}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()

	h.HandleEvents(rec, req)

	// All failed -> HTTP 422 with structured errors
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected HTTP 422 for all-rejected batch, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp BatchResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Rejected != 1 || resp.Accepted != 0 {
		t.Errorf("expected rejected=1, accepted=0; got rejected=%d, accepted=%d", resp.Rejected, resp.Accepted)
	}

	if len(resp.Results) != 1 {
		t.Fatalf("expected 1 result detail, got %d", len(resp.Results))
	}

	res := resp.Results[0]
	if res.Status != StatusRejected {
		t.Errorf("expected status REJECTED, got %s", res.Status)
	}
	if res.Error == "" {
		t.Errorf("expected structured error message, got empty")
	}
	if res.IdempotencyKey != "SITE-NG-01:1" {
		t.Errorf("expected idempotency key SITE-NG-01:1, got %s", res.IdempotencyKey)
	}
}

func TestHandler_PartialValidationBatch(t *testing.T) {
	limiter := ratelimit.NewTokenBucketLimiter(10, 1)
	h := NewHandler(limiter)
	siteID := "SITE-BR-01"

	valid := validEvent(siteID, 1)
	malformed := validEvent(siteID, 2)
	malformed.Date = time.Time{} // missing date

	reqBody, _ := json.Marshal(BatchRequest{SiteID: siteID, Events: []model.AdverseEvent{valid, malformed}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()

	h.HandleEvents(rec, req)

	// Partial failure -> HTTP 207 Multi-Status
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("expected HTTP 207 Multi-Status, got %d", rec.Code)
	}

	var resp BatchResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)

	if resp.Total != 2 || resp.Accepted != 1 || resp.Rejected != 1 {
		t.Errorf("unexpected counts: total=%d, accepted=%d, rejected=%d", resp.Total, resp.Accepted, resp.Rejected)
	}
	if resp.Results[0].Status != StatusAccepted {
		t.Errorf("result 0 should be ACCEPTED, got %s", resp.Results[0].Status)
	}
	if resp.Results[1].Status != StatusRejected {
		t.Errorf("result 1 should be REJECTED, got %s", resp.Results[1].Status)
	}
}

func TestHandler_RateLimitingAndHeaders(t *testing.T) {
	// Limiter with capacity 2
	limiter := ratelimit.NewTokenBucketLimiter(2, 0.1)
	h := NewHandler(limiter)
	siteID := "SITE-BURST-01"

	reqBody, _ := json.Marshal(BatchRequest{
		SiteID: siteID,
		Events: []model.AdverseEvent{validEvent(siteID, 1)},
	})

	// 1st request: OK
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec1 := httptest.NewRecorder()
	h.HandleEvents(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("req 1 expected 200, got %d", rec1.Code)
	}

	// 2nd request: OK
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec2 := httptest.NewRecorder()
	h.HandleEvents(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("req 2 expected 200, got %d", rec2.Code)
	}

	// 3rd request: Throttled -> HTTP 429
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	rec3 := httptest.NewRecorder()
	h.HandleEvents(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("req 3 expected HTTP 429, got %d", rec3.Code)
	}

	retryAfterStr := rec3.Header().Get("Retry-After")
	if retryAfterStr == "" {
		t.Errorf("expected Retry-After header on 429 response")
	}
	retryAfter, err := strconv.Atoi(retryAfterStr)
	if err != nil || retryAfter <= 0 {
		t.Errorf("invalid Retry-After value: %s", retryAfterStr)
	}

	limitHeader := rec3.Header().Get("X-RateLimit-Limit")
	if limitHeader != "2" {
		t.Errorf("expected X-RateLimit-Limit = 2, got %s", limitHeader)
	}
}

func TestHandler_MalformedJSON(t *testing.T) {
	h := NewHandler(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`{not valid json`)))
	rec := httptest.NewRecorder()

	h.HandleEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for malformed json, got %d", rec.Code)
	}
}

func TestHandler_MissingSiteID(t *testing.T) {
	h := NewHandler(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader([]byte(`[]`)))
	rec := httptest.NewRecorder()

	h.HandleEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 for empty events and missing siteID, got %d", rec.Code)
	}
}
