package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/query"
)

func newTestHandler(t *testing.T, svc query.Service, centralURL string) *Handler {
	t.Helper()
	h, err := NewHandler(svc, centralURL, "http://localhost:3000", "", ChaosOptions{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	return h
}

func newTestMux(t *testing.T, svc query.Service, centralURL string) *http.ServeMux {
	t.Helper()
	h := newTestHandler(t, svc, centralURL)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func seedMemoryService(t *testing.T) *query.MemoryService {
	t.Helper()
	svc := query.NewMemoryService()
	ctx := context.Background()
	base := time.Now().UTC().Add(-1 * time.Hour)

	if err := svc.CanonicalStore().SaveEvent(ctx, &consumer.CanonicalRecord{
		IdempotencyKey: "SITE-DASH-01:1",
		SiteID:         "SITE-DASH-01",
		StudyID:        "STUDY-DASH",
		LocalSeq:       1,
		EventTime:      base,
		RecordedTime:   base.Add(time.Minute),
		IngestionTime:  base.Add(2 * time.Minute),
		ConsumedAt:     base.Add(3 * time.Minute),
		Severity:       "severe",
		EventCode:      "10002198",
		Subject:        "Patient/P-1",
		Payload:        `{"resourceType":"AdverseEvent"}`,
		IsLate:         false,
	}); err != nil {
		t.Fatalf("failed to seed canonical event: %v", err)
	}

	svc.SaveDLQEvent(&query.DLQRecord{
		IdempotencyKey:   "SITE-DASH-01:99",
		SiteID:           "SITE-DASH-01",
		Payload:          `{"resourceType":"AdverseEvent","actuality":"bad"}`,
		RejectionReason:  "actuality must be 'actual'",
		ValidationErrors: "actuality must be 'actual'",
		RejectedAt:       base.Add(4 * time.Minute),
		Status:           "PUBLISHED",
	})

	return svc
}

func TestHandleIndex_RendersRecentEvents(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "SITE-DASH-01") {
		t.Errorf("expected the seeded event's site to appear in the rendered page, got:\n%s", rec.Body.String())
	}
}

func TestHandleIndex_UnmatchedPathIs404(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/favicon.ico", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	// stdlib ServeMux's "/" pattern is a subtree catch-all; handleIndex
	// must reject anything that isn't the literal root itself.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an unmatched path, got %d", rec.Code)
	}
}

func TestHandleQuery_NoIDShowsBlankForm(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "No matching events found") {
		t.Errorf("expected no query to have been executed yet (no id supplied)")
	}
}

func TestHandleQuery_ByEvent(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/query?type=event&id=SITE-DASH-01:1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "STUDY-DASH") {
		t.Errorf("expected the event's study to appear in the rendered page, got:\n%s", rec.Body.String())
	}
}

func TestHandleQuery_ByEventNotFound(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/query?type=event&id=NOPE:999", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline, not an HTTP error), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not found") {
		t.Errorf("expected a not-found message in the rendered page, got:\n%s", rec.Body.String())
	}
}

func TestHandleQuery_BySite(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/query?type=site&id=SITE-DASH-01", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "SITE-DASH-01:1") {
		t.Errorf("expected the seeded event's key to appear, got:\n%s", rec.Body.String())
	}
}

func TestHandleQuery_InvalidFromTimeShowsError(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/query?type=study&id=STUDY-DASH&from=not-a-time", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Invalid --from time") {
		t.Errorf("expected an invalid-time error message, got:\n%s", rec.Body.String())
	}
}

func TestHandleDLQList_AndDetail(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	listReq := httptest.NewRequest(http.MethodGet, "/dlq?site=SITE-DASH-01", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for dlq list, got %d", listRec.Code)
	}
	if !strings.Contains(listRec.Body.String(), "SITE-DASH-01:99") {
		t.Errorf("expected the seeded DLQ record to appear in the list, got:\n%s", listRec.Body.String())
	}

	detailReq := httptest.NewRequest(http.MethodGet, "/dlq/SITE-DASH-01:99", nil)
	detailRec := httptest.NewRecorder()
	mux.ServeHTTP(detailRec, detailReq)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("expected 200 for dlq detail, got %d: %s", detailRec.Code, detailRec.Body.String())
	}
	if !strings.Contains(detailRec.Body.String(), "actuality must be") {
		t.Errorf("expected the rejection reason to appear, got:\n%s", detailRec.Body.String())
	}
	// PUBLISHED status must show the replay form.
	if !strings.Contains(detailRec.Body.String(), "Replay this event") {
		t.Errorf("expected a replay form for a PUBLISHED record, got:\n%s", detailRec.Body.String())
	}
}

func TestHandleDLQDetail_NotFoundIs404(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	req := httptest.NewRequest(http.MethodGet, "/dlq/NOPE:1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

// TestHandleDLQReplay_MissingCredentialsNeverCallsIngestion proves the
// dashboard refuses to even attempt a replay without both a site ID and API
// key -- and, critically, never calls out to Central Ingestion in that
// case (asserted via a test server that fails the test if hit at all).
func TestHandleDLQReplay_MissingCredentialsNeverCallsIngestion(t *testing.T) {
	svc := seedMemoryService(t)
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("Central Ingestion should never be called when credentials are missing")
	}))
	defer ingestion.Close()
	mux := newTestMux(t, svc, ingestion.URL)

	form := url.Values{}
	req := httptest.NewRequest(http.MethodPost, "/dlq/SITE-DASH-01:99/replay", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "requires both Site ID and API Key") {
		t.Errorf("expected a missing-credentials message, got:\n%s", rec.Body.String())
	}
}

// TestHandleDLQReplay_ForwardsHeadersAndShowsSuccess proves the dashboard
// forwards the human-supplied Site ID/API Key as the exact same
// X-Site-ID/X-API-Key headers pharos-cli's (Slice 20-fixed) replay path
// uses, holding no standing credential of its own.
func TestHandleDLQReplay_ForwardsHeadersAndShowsSuccess(t *testing.T) {
	svc := seedMemoryService(t)
	var gotSiteHeader, gotKeyHeader string
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSiteHeader = r.Header.Get("X-Site-ID")
		gotKeyHeader = r.Header.Get("X-API-Key")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"idempotency_key":"SITE-DASH-01:99","status":"ACCEPTED"}`))
	}))
	defer ingestion.Close()
	mux := newTestMux(t, svc, ingestion.URL)

	form := url.Values{"site_id": {"SITE-DASH-01"}, "api_key": {"phk_test123"}}
	req := httptest.NewRequest(http.MethodPost, "/dlq/SITE-DASH-01:99/replay", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if gotSiteHeader != "SITE-DASH-01" {
		t.Errorf("expected X-Site-ID header 'SITE-DASH-01', got %q", gotSiteHeader)
	}
	if gotKeyHeader != "phk_test123" {
		t.Errorf("expected X-API-Key header 'phk_test123', got %q", gotKeyHeader)
	}
	if !strings.Contains(rec.Body.String(), "REPLAYED") {
		t.Errorf("expected a REPLAYED success message, got:\n%s", rec.Body.String())
	}
}

// TestHandleDLQReplay_StillRejectedShownAsError proves a non-200 from
// Central Ingestion is surfaced as an error, not silently treated as
// success.
func TestHandleDLQReplay_StillRejectedShownAsError(t *testing.T) {
	svc := seedMemoryService(t)
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"idempotency_key":"SITE-DASH-01:99","status":"REJECTED","error":"still invalid"}`))
	}))
	defer ingestion.Close()
	mux := newTestMux(t, svc, ingestion.URL)

	form := url.Values{"site_id": {"SITE-DASH-01"}, "api_key": {"phk_test123"}}
	req := httptest.NewRequest(http.MethodPost, "/dlq/SITE-DASH-01:99/replay", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "still invalid") {
		t.Errorf("expected the rejection error to be shown, got:\n%s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "REPLAYED:") {
		t.Errorf("must not claim success for a non-200 response")
	}
}

// TestHandleSubmitPost_RejectsInvalidJSONWithoutCallingIngestion proves
// malformed event JSON is caught locally, never forwarded.
func TestHandleSubmitPost_RejectsInvalidJSONWithoutCallingIngestion(t *testing.T) {
	svc := seedMemoryService(t)
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("Central Ingestion should never be called with invalid JSON")
	}))
	defer ingestion.Close()
	mux := newTestMux(t, svc, ingestion.URL)

	form := url.Values{"site_id": {"SITE-DASH-01"}, "api_key": {"phk_test123"}, "payload": {"{not valid json"}}
	req := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not valid JSON") {
		t.Errorf("expected an invalid-JSON message, got:\n%s", rec.Body.String())
	}
}

// TestHandleSubmitPost_ForwardsToIngestion proves a valid submission is
// wrapped in the real BatchRequest shape and forwarded with the supplied
// site credentials, showing back whatever Central Ingestion responds with.
func TestHandleSubmitPost_ForwardsToIngestion(t *testing.T) {
	svc := seedMemoryService(t)
	var gotSiteHeader string
	var gotBody string
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSiteHeader = r.Header.Get("X-Site-ID")
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"total":1,"accepted":1,"rejected":0,"results":[{"idempotency_key":"SITE-DASH-01:2","status":"ACCEPTED"}]}`))
	}))
	defer ingestion.Close()
	mux := newTestMux(t, svc, ingestion.URL)

	form := url.Values{
		"site_id": {"SITE-DASH-01"}, "api_key": {"phk_test123"},
		"payload": {`{"resourceType":"AdverseEvent","identifier":[{"system":"urn:pharos:idempotency-key","value":"SITE-DASH-01:2"}]}`},
	}
	req := httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if gotSiteHeader != "SITE-DASH-01" {
		t.Errorf("expected X-Site-ID header 'SITE-DASH-01', got %q", gotSiteHeader)
	}
	if !strings.Contains(gotBody, `"site_id":"SITE-DASH-01"`) || !strings.Contains(gotBody, "SITE-DASH-01:2") {
		t.Errorf("expected the forwarded body to be a real BatchRequest wrapping the submitted event, got: %s", gotBody)
	}
	if !strings.Contains(rec.Body.String(), "ACCEPTED") {
		t.Errorf("expected Central Ingestion's response to be shown, got:\n%s", rec.Body.String())
	}
}
