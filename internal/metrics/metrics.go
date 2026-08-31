// Package metrics defines the Prometheus metrics exposed by every Pharos
// service (§4 of PLAN.md — Slice 6). Each binary registers to the default
// registry and serves them via Handler() on its own /metrics endpoint;
// there is exactly one process per metric namespace (one edge collector per
// site, one ingestion service, one consumer), so package-level collectors
// are safe here in a way they would not be in a library meant to run
// multiple independent instances in one process.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Central Ingestion (pkg/ingestion) ---

var (
	IngestionRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pharos_ingestion_requests_total",
		Help: "Total HTTP batch requests handled by Central Ingestion, by resulting status code.",
	}, []string{"status_code"})

	IngestionRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pharos_ingestion_request_duration_seconds",
		Help:    "Latency of Central Ingestion batch requests end to end.",
		Buckets: prometheus.DefBuckets,
	})

	RateLimitRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pharos_ingestion_rate_limit_rejections_total",
		Help: "Requests rejected by the per-site token bucket rate limiter (§2.3).",
	}, []string{"site_id"})

	ValidationFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_ingestion_validation_failures_total",
		Help: "Events rejected by FHIR validation or malformed-JSON parsing, routed to the DLQ (§2.3).",
	})

	DedupOutcomesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pharos_ingestion_dedup_outcomes_total",
		Help: "Outbox claim outcomes for accepted events, by outcome (§2.2).",
	}, []string{"outcome"}) // "new_claim" | "duplicate_hit" (already published)

	OutboxPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pharos_ingestion_outbox_publish_duration_seconds",
		Help:    "Latency of the Kafka publish step of the transactional outbox (§2.2, §2.3).",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})

	DLQWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_ingestion_dlq_writes_total",
		Help: "Rejected events successfully published to the Kafka dead-letter topic (§2.3).",
	})
)

// --- Consumer (pkg/consumer) ---

var (
	ConsumerEventsConsumedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_events_consumed_total",
		Help: "Adverse event messages successfully read and committed from Kafka.",
	})

	ConsumerLateArrivalsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_late_arrivals_total",
		Help: "Events that arrived after the emitted watermark had already passed them (§2.4).",
	})

	ConsumerErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_errors_total",
		Help: "Errors encountered in the consumer engine's fetch/save/commit loop.",
	})

	ConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_consumer_kafka_lag",
		Help: "Aggregate consumer-group lag reported by the Kafka client (kafka-go ReaderStats.Lag).",
	})

	ConsumerWatermarkSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_consumer_watermark_unix_seconds",
		Help: "Current emitted event-time watermark (§2.4), as a Unix timestamp; 0 if not yet established.",
	})

	ConsumerPartitionActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pharos_consumer_partition_active",
		Help: "Whether a partition is currently considered active (1) or idle-excluded (0) for watermark purposes (§2.4).",
	}, []string{"partition"})

	CassandraWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pharos_consumer_cassandra_write_duration_seconds",
		Help:    "Latency of the parallel canonical-table upsert (§2.4).",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"}) // "success" | "error"
)

// --- Edge collector (pkg/edge) ---

var (
	EdgeQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_edge_queue_pending_records",
		Help: "Rows currently PENDING in the local SQLite WAL queue, awaiting forwarding (§2.1).",
	})

	EdgeQueueOldestPendingSeconds = promauto.NewGauge(prometheus.GaugeOpts{
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

// Handler returns the HTTP handler that serves the default Prometheus
// registry in the standard exposition format, for mounting at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
