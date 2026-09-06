// Package ingestionmetrics defines Central Ingestion's own Prometheus
// collectors (§4, PLAN.md Slice 6) -- split out of the single shared
// internal/metrics package (audit remediation) so importing it doesn't
// also register (and expose, zero-valued, on this service's own /metrics)
// the consumer's and edge's metric names. Only cmd/pharos-ingestion and
// internal/ingestion import this package; internal/metrics itself now only
// provides the shared Handler().
package ingestionmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pharos_ingestion_requests_total",
		Help: "Total HTTP batch requests handled by Central Ingestion, by resulting status code.",
	}, []string{"status_code"})

	RequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
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
