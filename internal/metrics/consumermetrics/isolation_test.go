package consumermetrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gasthecreator/pharos/internal/metrics"
	_ "github.com/gasthecreator/pharos/internal/metrics/consumermetrics"
)

// TestMetricsIsolation_ConsumerOnly is the consumer-side mirror of
// edgemetrics' own isolation test -- see that test's docs for the full
// history. This file's import list matches cmd/pharos-consumer and
// internal/consumer exactly (internal/metrics + this package only).
func TestMetricsIsolation_ConsumerOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read /metrics response: %v", err)
	}
	text := string(body)

	if !strings.Contains(text, "pharos_consumer_") {
		t.Fatalf("expected pharos_consumer_* metrics in /metrics output, found none -- consumermetrics may not be registering at all")
	}
	for _, leaked := range []string{"pharos_edge_", "pharos_ingestion_"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("CROSS-SERVICE METRIC LEAK: /metrics output contains %q even though this test only imports internal/metrics/consumermetrics -- some import path is pulling in another service's metrics package again", leaked)
		}
	}
}
