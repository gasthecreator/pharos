// Package edgemetrics defines pharos-edge's own Prometheus collectors (§4,
// PLAN.md Slice 6) -- split out of the single shared internal/metrics
// package (audit remediation) so importing it doesn't also register (and
// expose, zero-valued, on this service's own /metrics) ingestion's and
// consumer's metric names. Only cmd/pharos-edge and internal/edge import
// this package; internal/metrics itself now only provides the shared
// Handler().
package edgemetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_edge_queue_pending_records",
		Help: "Rows currently PENDING in the local SQLite WAL queue, awaiting forwarding (§2.1).",
	})

	QueueOldestPendingSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_edge_queue_oldest_pending_age_seconds",
		Help: "Age of the oldest still-pending record in the local queue; a proxy for how long this site has been effectively partitioned.",
	})

	ForwarderAttemptsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_edge_forwarder_attempts_total",
		Help: "Batch forward attempts to Central Ingestion.",
	})

	ForwarderOutcomesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pharos_edge_forwarder_outcomes_total",
		Help: "Batch forward attempts by outcome.",
	}, []string{"outcome"}) // "success" | "network_error" | "rate_limited" | "server_error"

	ForwarderLastBackoffSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_edge_forwarder_last_backoff_seconds",
		Help: "Most recently computed retry backoff duration (§2.1's Full Jitter formula).",
	})
)
