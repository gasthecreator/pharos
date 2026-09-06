// Package consumermetrics defines pharos-consumer's own Prometheus
// collectors (§4, PLAN.md Slice 6) -- split out of the single shared
// internal/metrics package (audit remediation) so importing it doesn't
// also register (and expose, zero-valued, on this service's own /metrics)
// ingestion's and edge's metric names. Only cmd/pharos-consumer and
// internal/consumer import this package; internal/metrics itself now only
// provides the shared Handler().
package consumermetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsConsumedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_events_consumed_total",
		Help: "Adverse event messages successfully read and committed from Kafka.",
	})

	LateArrivalsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_late_arrivals_total",
		Help: "Events that arrived after the emitted watermark had already passed them (§2.4).",
	})

	ErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pharos_consumer_errors_total",
		Help: "Errors encountered in the consumer engine's fetch/save/commit loop.",
	})

	Lag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_consumer_kafka_lag",
		Help: "Aggregate consumer-group lag reported by the Kafka client (kafka-go ReaderStats.Lag).",
	})

	WatermarkSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pharos_consumer_watermark_unix_seconds",
		Help: "Current emitted event-time watermark (§2.4), as a Unix timestamp; 0 if not yet established.",
	})

	PartitionActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pharos_consumer_partition_active",
		Help: "Whether a partition is currently considered active (1) or idle-excluded (0) for watermark purposes (§2.4).",
	}, []string{"partition"})

	CassandraWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pharos_consumer_cassandra_write_duration_seconds",
		Help:    "Latency of the parallel canonical-table upsert (§2.4).",
		Buckets: prometheus.DefBuckets,
	}, []string{"outcome"}) // "success" | "error"
)
