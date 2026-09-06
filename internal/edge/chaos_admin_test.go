package edge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterChaosAdminRoutes_PartitionAndHeal(t *testing.T) {
	stub := &stubHTTPClient{}
	client := NewChaosClient(stub)
	mux := http.NewServeMux()
	RegisterChaosAdminRoutes(mux, client)

	statusOf := func(rec *httptest.ResponseRecorder) bool {
		var body map[string]bool
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to decode status response: %v", err)
		}
		return body["partitioned"]
	}

	// Initial status: not partitioned.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/chaos/status", nil))
	if rec.Code != http.StatusOK || statusOf(rec) {
		t.Fatalf("expected initial status 200/not-partitioned, got %d/%v", rec.Code, statusOf(rec))
	}

	// Partition.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/chaos/partition", nil))
	if rec.Code != http.StatusOK || !statusOf(rec) {
		t.Fatalf("expected 200/partitioned after POST /admin/chaos/partition, got %d/%v", rec.Code, statusOf(rec))
	}
	if !client.IsPartitioned() {
		t.Errorf("expected the underlying ChaosClient to report partitioned")
	}

	// Heal.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/chaos/heal", nil))
	if rec.Code != http.StatusOK || statusOf(rec) {
		t.Fatalf("expected 200/not-partitioned after POST /admin/chaos/heal, got %d/%v", rec.Code, statusOf(rec))
	}
	if client.IsPartitioned() {
		t.Errorf("expected the underlying ChaosClient to report healed")
	}
}
