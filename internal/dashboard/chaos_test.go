package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/audit"
	"github.com/gasthecreator/pharos/internal/chaos"
)

func newDisabledChaosHandler(t *testing.T) *Handler {
	t.Helper()
	svc := seedMemoryService(t)
	h, err := NewHandler(svc, audit.NewMemoryStore(), "http://unused.example", "http://localhost:3000", "", ChaosOptions{Enabled: false})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	return h
}

func TestHandleChaosPanel_RendersEvenWhenDisabled(t *testing.T) {
	h := newDisabledChaosHandler(t)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/chaos", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "disabled") {
		t.Errorf("expected a disabled notice on the panel, got:\n%s", rec.Body.String())
	}
}

// TestChaosActions_RefuseWhenDisabled proves every chaos action handler
// refuses before doing anything -- critically, before ever calling out to
// the real internal/chaos package (which would shell out to `docker`) --
// when h.chaos.Enabled is false (§2.4, Slice 23's own safety guard).
func TestChaosActions_RefuseWhenDisabled(t *testing.T) {
	h := newDisabledChaosHandler(t)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	cases := []struct {
		name string
		path string
		form url.Values
	}{
		{"cassandra", "/chaos/cassandra", url.Values{"container": {"pharos-cassandra-1"}, "action": {"stop"}}},
		{"region_partition", "/chaos/region/partition", url.Values{}},
		{"region_heal", "/chaos/region/heal", url.Values{}},
		{"edge_partition", "/chaos/edge/partition", url.Values{"edge_admin_url": {"http://localhost:9"}}},
		{"edge_heal", "/chaos/edge/heal", url.Values{"edge_admin_url": {"http://localhost:9"}}},
		{"duplicate", "/chaos/duplicate", url.Values{"site_id": {"S"}, "api_key": {"k"}, "payload": {"{}"}}},
		{"skew", "/chaos/skew", url.Values{"site_id": {"S"}, "api_key": {"k"}, "skew": {"-1h"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(c.form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 (disabled notice, not an HTTP error), got %d", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), "disabled") {
				t.Errorf("expected a disabled notice, got:\n%s", rec.Body.String())
			}
		})
	}
}

func newEnabledChaosHandler(t *testing.T, centralURL string) *Handler {
	t.Helper()
	svc := seedMemoryService(t)
	h, err := NewHandler(svc, audit.NewMemoryStore(), centralURL, "http://localhost:3000", "", ChaosOptions{Enabled: true})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	return h
}

func TestHandleChaosDuplicate_SubmitsTwiceAndReportsBoth(t *testing.T) {
	var calls int
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"total":1,"accepted":1,"rejected":0}`))
	}))
	defer ingestion.Close()

	h := newEnabledChaosHandler(t, ingestion.URL)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	form := url.Values{"site_id": {"SITE-CHAOS-01"}, "api_key": {"phk_test"}, "payload": {`{"resourceType":"AdverseEvent"}`}}
	req := httptest.NewRequest(http.MethodPost, "/chaos/duplicate", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if calls != 2 {
		t.Fatalf("expected exactly 2 submissions to Central Ingestion, got %d", calls)
	}
	if !strings.Contains(rec.Body.String(), "First submission") || !strings.Contains(rec.Body.String(), "Duplicate") {
		t.Errorf("expected both submission results described, got:\n%s", rec.Body.String())
	}
}

func TestHandleChaosDuplicate_MissingFieldsNeverCallsIngestion(t *testing.T) {
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("Central Ingestion should never be called with missing fields")
	}))
	defer ingestion.Close()

	h := newEnabledChaosHandler(t, ingestion.URL)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	form := url.Values{"site_id": {""}, "api_key": {""}, "payload": {""}}
	req := httptest.NewRequest(http.MethodPost, "/chaos/duplicate", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "required") {
		t.Fatalf("expected a required-fields message, got %d:\n%s", rec.Code, rec.Body.String())
	}
}

func TestHandleChaosSkew_SubmitsWithSkewedEventTime(t *testing.T) {
	var gotBody string
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 8192)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"total":1,"accepted":1}`))
	}))
	defer ingestion.Close()

	h := newEnabledChaosHandler(t, ingestion.URL)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	form := url.Values{"site_id": {"SITE-CHAOS-02"}, "api_key": {"phk_test"}, "skew": {"-3h"}}
	req := httptest.NewRequest(http.MethodPost, "/chaos/skew", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(gotBody, "SITE-CHAOS-02") {
		t.Errorf("expected the forwarded body to reference the site, got: %s", gotBody)
	}
	if !strings.Contains(rec.Body.String(), "skewed -3h") {
		t.Errorf("expected the result to describe the applied skew, got:\n%s", rec.Body.String())
	}
}

func TestHandleChaosSkew_InvalidDurationRejectedLocally(t *testing.T) {
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("Central Ingestion should never be called with an invalid skew duration")
	}))
	defer ingestion.Close()

	h := newEnabledChaosHandler(t, ingestion.URL)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	form := url.Values{"site_id": {"S"}, "api_key": {"k"}, "skew": {"not-a-duration"}}
	req := httptest.NewRequest(http.MethodPost, "/chaos/skew", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Invalid skew duration") {
		t.Fatalf("expected an invalid-duration message, got %d:\n%s", rec.Code, rec.Body.String())
	}
}

// TestHandleChaosCassandra_RejectsUnknownContainer proves validation
// happens (and no real docker call is attempted) for a container name
// outside the known list, even with chaos enabled.
func TestHandleChaosCassandra_RejectsUnknownContainer(t *testing.T) {
	h := newEnabledChaosHandler(t, "http://unused.example")
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	form := url.Values{"container": {"not-a-real-container"}, "action": {"stop"}}
	req := httptest.NewRequest(http.MethodPost, "/chaos/cassandra", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Unknown container") {
		t.Fatalf("expected an unknown-container message, got %d:\n%s", rec.Code, rec.Body.String())
	}
}

// TestRegionAutoHeal_ScheduleAndCancel proves the auto-heal timer is
// actually armed and can be cancelled, using a short override duration
// instead of waiting on the real 60s production window. Doesn't exercise
// the real chaos.HealRegionPartition call the timer fires (that requires
// real Docker, covered by internal/chaos's own integration tests) --
// verifies the scheduling/cancellation bookkeeping itself.
func TestRegionAutoHeal_ScheduleAndCancel(t *testing.T) {
	h := newEnabledChaosHandler(t, "http://unused.example")
	h.regionAutoHealOverride = 20 * time.Millisecond

	h.scheduleRegionAutoHeal()
	h.regionHealMu.Lock()
	timer := h.regionHealTimer
	h.regionHealMu.Unlock()
	if timer == nil {
		t.Fatalf("expected scheduleRegionAutoHeal to arm a timer")
	}

	h.cancelRegionAutoHeal()
	h.regionHealMu.Lock()
	timer = h.regionHealTimer
	h.regionHealMu.Unlock()
	if timer != nil {
		t.Fatalf("expected cancelRegionAutoHeal to clear the timer")
	}
}

// TestChaosContainerList_MatchesInternalChaosPackage guards against the
// dashboard's rendered container dropdown silently drifting from
// internal/chaos's own authoritative list.
func TestChaosContainerList_MatchesInternalChaosPackage(t *testing.T) {
	h := newEnabledChaosHandler(t, "http://unused.example")
	data := h.newChaosData(context.Background())
	if len(data.CassandraContainers) != len(chaos.CassandraContainers) {
		t.Fatalf("expected %d containers, got %d", len(chaos.CassandraContainers), len(data.CassandraContainers))
	}
}
