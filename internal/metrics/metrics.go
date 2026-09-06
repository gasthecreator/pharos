// Package metrics provides the shared /metrics HTTP handler every Pharos
// service mounts (§4, PLAN.md Slice 6). It deliberately holds no
// service-specific collectors itself -- those live in the per-service
// subpackages (internal/metrics/ingestionmetrics, .../consumermetrics,
// .../edgemetrics), split out here as audit remediation: a single shared
// package declaring every service's Prometheus vars meant every binary's
// /metrics endpoint exposed every OTHER service's metric names too (zero-
// valued for whatever that binary doesn't itself touch), since importing a
// package runs its package-level var initializers (and promauto's
// registration side effect) regardless of whether the importer references
// those specific vars. Each binary now imports only the subpackage(s) for
// its own metrics, plus this package for Handler().
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler returns the HTTP handler that serves the default Prometheus
// registry in the standard exposition format, for mounting at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
