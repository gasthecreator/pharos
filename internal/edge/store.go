package edge

import (
	"context"
	"errors"
	"time"

	"github.com/gasthecreator/pharos/internal/model"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrStoreClosed    = errors.New("queue store is closed")
	ErrEmptyBatch     = errors.New("batch is empty")
)

// RecordStatus represents the forwarding lifecycle state of a locally captured event (§2.1).
type RecordStatus string

const (
	StatusPending      RecordStatus = "PENDING"
	StatusInFlight     RecordStatus = "IN_FLIGHT"
	StatusAcknowledged RecordStatus = "ACKNOWLEDGED"
	StatusFailed       RecordStatus = "FAILED"
	StatusRejected     RecordStatus = "REJECTED"
)

// QueuedRecord represents an adverse event durably stored on the site's local disk.
type QueuedRecord struct {
	ID             int64        `json:"id"`
	SiteID         string       `json:"site_id"`
	LocalSeq       uint64       `json:"local_seq"`
	IdempotencyKey string       `json:"idempotency_key"`
	Payload        []byte       `json:"payload"`
	Status         RecordStatus `json:"status"`
	Attempts       int          `json:"attempts"`
	LastError      string       `json:"last_error,omitempty"`
	NextRetryAt    time.Time    `json:"next_retry_at"`
	CreatedAt      time.Time    `json:"created_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

// QueueStats captures operational metrics for the local edge queue.
type QueueStats struct {
	SiteID            string    `json:"site_id"`
	PendingCount      int64     `json:"pending_count"`
	InFlightCount     int64     `json:"in_flight_count"`
	AcknowledgedCount int64     `json:"acknowledged_count"`
	FailedCount       int64     `json:"failed_count"`
	RejectedCount     int64     `json:"rejected_count"`
	MaxSequence       uint64    `json:"max_sequence"`
	OldestPendingTime time.Time `json:"oldest_pending_time,omitempty"`
}

// QueueStore defines the durable storage operations required by the edge collector.
// Guarantees: atomic monotonic sequence assignment, crash resilience, and FIFO ordering.
type QueueStore interface {
	// Enqueue persists an adverse event, assigns the monotonic local sequence number and
	// client idempotency key, and commits to local disk before any network attempt.
	Enqueue(ctx context.Context, siteID string, event *model.AdverseEvent) (*QueuedRecord, error)

	// FetchPending returns the next batch of records ready for transmission, ordered by local_seq ASC.
	FetchPending(ctx context.Context, batchSize int) ([]*QueuedRecord, error)

	// MarkInFlight transitions records from PENDING/FAILED to IN_FLIGHT when transmission begins.
	MarkInFlight(ctx context.Context, ids []int64) error

	// MarkAcknowledged transitions records to ACKNOWLEDGED upon receipt of HTTP 200/201 from Central Ingestion.
	MarkAcknowledged(ctx context.Context, ids []int64) error

	// MarkRejected transitions records to REJECTED when Central Ingestion permanently rejects them (e.g. malformed FHIR).
	MarkRejected(ctx context.Context, ids []int64, errReason string) error

	// MarkFailed records a transmission failure, increments attempt count, and sets next retry timestamp.
	MarkFailed(ctx context.Context, id int64, errReason string, retryAfter time.Duration) error

	// GetStats provides queue observability (lag, pending volume, sequence progression).
	GetStats(ctx context.Context) (QueueStats, error)

	// Close cleanly flushes WAL checkpoints and closes database handles.
	Close() error
}
