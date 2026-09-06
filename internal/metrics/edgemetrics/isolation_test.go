package edgemetrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gasthecreator/pharos/internal/metrics"
	_ "github.com/gasthecreator/pharos/internal/metrics/edgemetrics"
)

// TestMetricsIsolation_EdgeOnly proves the §2.4, audit remediation fix:
// this file's own import list is exactly what cmd/pharos-edge and
// internal/edge import for metrics -- internal/metrics (for Handler()) and
// internal/metrics/edgemetrics (for this service's own collectors), never
// consumermetrics or ingestionmetrics. Before the internal/wire extraction
// and metrics-package split, internal/edge transitively imported
// internal/ingestion just for wire types, and every service's collectors
// lived in one shared internal/metrics package -- so importing either one
// registered every OTHER service's Prometheus metrics too (zero-valued),
// entirely because Go runs a package's init-time side effects (promauto's
// registration) on import, regardless of which symbols the importer
// actually references. If this test binary's own transitive imports ever
// regress back to pulling in consumermetrics/ingestionmetrics, this test
// catches it directly from the scraped /metrics text, without needing a
// running binary or real infrastructure.
func TestMetricsIsolation_EdgeOnly(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	metrics.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("failed to read /metrics response: %v", err)
	}
	text := string(body)

	if !strings.Contains(text, "pharos_edge_") {
		t.Fatalf("expected pharos_edge_* metrics in /metrics output, found none -- edgemetrics may not be registering at all")
	}
	for _, leaked := range []string{"pharos_ingestion_", "pharos_consumer_"} {
		if strings.Contains(text, leaked) {
			t.Fatalf("CROSS-SERVICE METRIC LEAK: /metrics output contains %q even though this test only imports internal/metrics/edgemetrics -- some import path is pulling in another service's metrics package again", leaked)
		}
	}
}
