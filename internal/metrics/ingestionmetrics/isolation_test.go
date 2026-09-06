package ingestionmetrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gasthecreator/pharos/internal/metrics"
	_ "github.com/gasthecreator/pharos/internal/metrics/ingestionmetrics"
)

// TestMetricsIsolation_IngestionOnly is the ingestion-side mirror of
// edgemetrics' own isolation test -- see that test's docs for the full
// history. This file's import list matches cmd/pharos-ingestion and
// internal/ingestion exactly (internal/metrics + this package only).
func TestMetricsIsolation_IngestionOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read /metrics response: %v", err)
	}
	text := string(body)

	if !strings.Contains(text, "pharos_ingestion_") {
		t.Fatalf("expected pharos_ingestion_* metrics in /metrics output, found none -- ingestionmetrics may not be registering at all")
	}
	for _, leaked := range []string{"pharos_edge_", "pharos_consumer_"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("CROSS-SERVICE METRIC LEAK: /metrics output contains %q even though this test only imports internal/metrics/ingestionmetrics -- some import path is pulling in another service's metrics package again", leaked)
		}
	}
}
