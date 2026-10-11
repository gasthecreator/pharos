package ingestion

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

// FuzzHandleEvents drives the real HTTP intake handler (in-memory outbox and
// mock Kafka, no infrastructure) with arbitrary request bodies and site
// headers. Whatever arrives, the handler must not panic, must answer with a
// status from its documented set, and every non-429 answer must be valid JSON.
//
//	go test -fuzz=FuzzHandleEvents -fuzztime=30s ./internal/ingestion/
func FuzzHandleEvents(f *testing.F) {
	valid, _ := json.Marshal(BatchRequest{SiteID: "SITE-F", Events: toRaw(validEvent("SITE-F", 1))})
	f.Add(valid, "SITE-F")
	f.Add([]byte(`[]`), "")
	f.Add([]byte(`{}`), "SITE-F")
	f.Add([]byte(`null`), "x")
	f.Add([]byte(`{"events":[{"resourceType":"AdverseEvent"}]}`), "SITE-F")
	f.Add([]byte(`{"site_id":"A","events":[1,"two",null,[3]]}`), "B")
	f.Add([]byte(strings.Repeat("[", 5000)), "SITE-F")
	f.Add([]byte("\xff\xfe\x00"), "日本")

	allowed := map[int]bool{
		http.StatusOK: true, http.StatusMultiStatus: true, http.StatusBadRequest: true,
		http.StatusUnprocessableEntity: true, http.StatusTooManyRequests: true,
		http.StatusInternalServerError: true, http.StatusServiceUnavailable: true,
	}

	f.Fuzz(func(t *testing.T, body []byte, siteHeader string) {
		// Generous limiter so the fuzzer explores validation, not throttling.
		h := NewHandlerWithOutbox(ratelimit.NewTokenBucketLimiter(1e9, 1e9), dedup.NewMemoryOutboxStore(), kafka.NewMockProducer(), time.Second)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(string(body)))
		if siteHeader != "" && !strings.ContainsAny(siteHeader, "\r\n\x00") {
			req.Header.Set("X-Site-ID", siteHeader)
		}
		rec := httptest.NewRecorder()
		h.HandleEvents(rec, req)

		if !allowed[rec.Code] {
			t.Fatalf("undocumented status %d for body %q", rec.Code, body)
		}
		if rec.Code != http.StatusTooManyRequests && !json.Valid(rec.Body.Bytes()) {
			t.Fatalf("status %d with a non-JSON body: %q", rec.Code, rec.Body.String())
		}
	})
}
