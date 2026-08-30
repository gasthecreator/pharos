package consumer

import (
	"time"
)

// WindowStatus represents the analytical lifecycle state of a reporting window (§2.4).
type WindowStatus string

const (
	WindowStatusOpen     WindowStatus = "OPEN"
	WindowStatusComplete WindowStatus = "COMPLETE"
	WindowStatusRevised  WindowStatus = "REVISED"
)

// Window defines a closed-open event-time interval [Start, End) for clinical safety aggregation.
type Window struct {
	ID        string       `json:"id"`
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	Status    WindowStatus `json:"status"`
	ClosedAt  time.Time    `json:"closed_at,omitempty"`
	RevisedAt time.Time    `json:"revised_at,omitempty"`
}

// LateArrivalAudit records retroactive data delivery into an already-completed window (21 CFR Part 11).
type LateArrivalAudit struct {
	WindowID           string    `json:"window_id"`
	IdempotencyKey     string    `json:"idempotency_key"`
	Partition          int       `json:"partition"`
	EventTime          time.Time `json:"event_time"`
	ArrivedAt          time.Time `json:"arrived_at"`
	WatermarkAtArrival time.Time `json:"watermark_at_arrival"`
}

// CanonicalRecord represents an accepted adverse event stored across canonical query tables (§2.4, §5).
type CanonicalRecord struct {
	IdempotencyKey string    `json:"idempotency_key"`
	SiteID         string    `json:"site_id"`
	StudyID        string    `json:"study_id"`
	LocalSeq       int64     `json:"local_seq"`
	EventTime      time.Time `json:"event_time"`
	RecordedTime   time.Time `json:"recorded_time"`
	IngestionTime  time.Time `json:"ingestion_time"`
	Severity       string    `json:"severity"`
	EventCode      string    `json:"event_code"`
	Subject        string    `json:"subject"`
	Payload        string    `json:"payload"`
	KafkaTopic     string    `json:"kafka_topic"`
	KafkaPartition int       `json:"kafka_partition"`
	KafkaOffset    int64     `json:"kafka_offset"`
	ConsumedAt     time.Time `json:"consumed_at"`
	IsLate         bool      `json:"is_late"`
}
