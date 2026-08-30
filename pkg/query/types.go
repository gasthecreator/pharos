package query

import (
	"context"
	"time"

	"github.com/gasthecreator/pharos/pkg/consumer"
)

// DLQRecord represents a rejected adverse event captured in Cassandra dead_letter_events (§2.3).
type DLQRecord struct {
	IdempotencyKey   string    `json:"idempotency_key"`
	SiteID           string    `json:"site_id"`
	Payload          string    `json:"payload"`
	RejectionReason  string    `json:"rejection_reason"`
	ValidationErrors string    `json:"validation_errors"`
	RejectedAt       time.Time `json:"rejected_at"`
	Status           string    `json:"status"`
	ClaimedAt        time.Time `json:"claimed_at,omitempty"`
	PublishedAt      time.Time `json:"published_at,omitempty"`
	KafkaTopic       string    `json:"kafka_topic,omitempty"`
	KafkaPartition   int       `json:"kafka_partition,omitempty"`
	KafkaOffset      int64     `json:"kafka_offset,omitempty"`
}

// Service defines the query and DLQ inspection interface for Pharos.
type Service interface {
	// Canonical queries (§2.4, §5)
	GetEvent(ctx context.Context, idempotencyKey string) (*consumer.CanonicalRecord, error)
	GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*consumer.CanonicalRecord, error)
	GetEventsBySite(ctx context.Context, siteID string, minLocalSeq int64) ([]*consumer.CanonicalRecord, error)

	// DLQ inspection queries (§2.3)
	GetDLQEvent(ctx context.Context, idempotencyKey string) (*DLQRecord, error)
	ListDLQEventsBySite(ctx context.Context, siteID string, limit int) ([]*DLQRecord, error)
	ListAllDLQEvents(ctx context.Context, limit int) ([]*DLQRecord, error)

	Close() error
}
