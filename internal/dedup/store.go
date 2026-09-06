package dedup

import (
	"context"
	"errors"
	"time"
)

var (
	ErrRecordNotFound = errors.New("outbox record not found")
	ErrStoreClosed    = errors.New("dedup outbox store is closed")
	// ErrClaimSuperseded is returned by MarkPublished/MarkDLQPublished when
	// the claim being finalized is no longer the current one for its
	// idempotency key -- its lease expired and was legitimately stolen by
	// another claimant before this call arrived (§2.4, Slice 22). Not a
	// hard failure: whatever event this call was finalizing was still
	// genuinely published to Kafka: this only means the finalize lost the
	// race to record that fact, so a peer's record of it (or its own
	// still-authoritative claim) stays intact instead of being overwritten.
	ErrClaimSuperseded = errors.New("outbox claim was superseded before it could be finalized")
)

// DefaultLeaseTimeout is the maximum duration an in-flight worker may hold a PUBLISHING claim (default: 30s).
const DefaultLeaseTimeout = 30 * time.Second

// OutboxStatus represents the three-state lifecycle of a transactional outbox record (§2.2).
// Initial insert directly sets PUBLISHING (winning the LWT is the claim).
type OutboxStatus string

const (
	StatusPublishing OutboxStatus = "PUBLISHING"
	StatusPublished  OutboxStatus = "PUBLISHED"
	// StatusReplayed is DLQ-record-only (§2.3, Slice 10): set on a
	// dead_letter_events row, never on event_outbox, once that rejected
	// event has been successfully resubmitted and accepted. The original
	// row is never deleted or overwritten beyond this status/timestamp —
	// the rejection stays part of the audit trail, matching the "never
	// silently mutate a reported result" principle §2.4 established for
	// late-arriving data.
	StatusReplayed OutboxStatus = "REPLAYED"
)

// OutboxRecord represents an accepted event durably recorded in Cassandra pharos.event_outbox.
type OutboxRecord struct {
	IdempotencyKey string       `json:"idempotency_key"`
	SiteID         string       `json:"site_id"`
	LocalSeq       uint64       `json:"local_seq"`
	Payload        []byte       `json:"payload"` // Raw wire JSON bytes (json.RawMessage)
	Status         OutboxStatus `json:"status"`
	ClaimedAt      time.Time    `json:"claimed_at"`
	CreatedAt      time.Time    `json:"created_at"`
	PublishedAt    time.Time    `json:"published_at,omitempty"`
	KafkaTopic     string       `json:"kafka_topic,omitempty"`
	KafkaPartition int          `json:"kafka_partition,omitempty"`
	KafkaOffset    int64        `json:"kafka_offset,omitempty"`
}

// DLQRecord represents a rejected event durably recorded in Cassandra pharos.dead_letter_events (§2.3).
type DLQRecord struct {
	IdempotencyKey   string       `json:"idempotency_key"`
	SiteID           string       `json:"site_id"`
	Payload          []byte       `json:"payload"` // Raw wire JSON bytes (json.RawMessage)
	RejectionReason  string       `json:"rejection_reason"`
	ValidationErrors string       `json:"validation_errors"`
	RejectedAt       time.Time    `json:"rejected_at"`
	Status           OutboxStatus `json:"status"`
	ClaimedAt        time.Time    `json:"claimed_at"`
	PublishedAt      time.Time    `json:"published_at,omitempty"`
	KafkaTopic       string       `json:"kafka_topic,omitempty"`
	KafkaPartition   int          `json:"kafka_partition,omitempty"`
	KafkaOffset      int64        `json:"kafka_offset,omitempty"`
	ReplayedAt       time.Time    `json:"replayed_at,omitempty"` // Set only when Status == StatusReplayed (§2.3, Slice 10)
}

// OutboxPruneRecord identifies one event_outbox row eligible for pruning
// (§2.2, audit remediation) -- just enough to delete it from both
// event_outbox and event_outbox_by_site; unlike DLQRecord, no payload is
// carried since pruned rows are deleted outright, never archived (see
// migrations/006_event_outbox_pruning.cql's docs for why that's safe).
type OutboxPruneRecord struct {
	IdempotencyKey string
	SiteID         string
	PublishedAt    time.Time
}

// ClaimResult describes the outcome of an atomic outbox claim attempt.
type ClaimResult struct {
	Acquired       bool          `json:"acquired"`
	Status         OutboxStatus  `json:"status"`
	ClaimedAt      time.Time     `json:"claimed_at"`
	ExistingRecord *OutboxRecord `json:"existing_record,omitempty"`
}

// OutboxStore defines the persistent outbox and deduplication operations in Cassandra (§2.2, §2.3).
type OutboxStore interface {
	// InsertClaim executes an atomic LWT insert with status='PUBLISHING' and claimed_at=now.
	// If the key already exists:
	// - If status == 'PUBLISHED': returns Acquired=false, Status='PUBLISHED' (no-op).
	// - If status == 'PUBLISHING' with active lease: returns Acquired=false, Status='PUBLISHING' (in-flight).
	// - If status == 'PUBLISHING' with expired lease: attempts atomic LWT CAS steal. If won, returns Acquired=true.
	InsertClaim(ctx context.Context, rec OutboxRecord, leaseTimeout time.Duration) (ClaimResult, error)

	// MarkPublished finalizes the outbox record to status='PUBLISHED' with
	// Kafka broker metadata, fenced by expectedClaimedAt (the ClaimedAt
	// InsertClaim returned for this claim, §2.4 Slice 22) -- only applies
	// if the record's claimed_at still matches, i.e. nobody has stolen this
	// claim since. Returns ErrClaimSuperseded otherwise (see that error's
	// docs); callers should not treat that as their own request failing,
	// since the underlying event was still genuinely published.
	MarkPublished(ctx context.Context, idempotencyKey string, expectedClaimedAt time.Time, topic string, partition int, offset int64) error

	// InsertDLQClaim executes a symmetric atomic LWT claim on pharos.dead_letter_events.
	InsertDLQClaim(ctx context.Context, rec DLQRecord, leaseTimeout time.Duration) (ClaimResult, error)

	// MarkDLQPublished finalizes the DLQ record to status='PUBLISHED' with
	// Kafka broker metadata, fenced by expectedClaimedAt exactly like
	// MarkPublished (§2.4, Slice 22).
	MarkDLQPublished(ctx context.Context, idempotencyKey string, expectedClaimedAt time.Time, topic string, partition int, offset int64) error

	// MarkDLQReplayed transitions a DLQ record from PUBLISHED to REPLAYED
	// (§2.3, Slice 10) once its stored payload has been successfully
	// resubmitted and accepted. Requires the record to already be PUBLISHED
	// — a still-in-flight or already-replayed record cannot be replayed
	// again. The row is never deleted; only its status/timestamp change.
	MarkDLQReplayed(ctx context.Context, idempotencyKey string) error

	// FetchStaleClaims returns records with expired leases for background sweeper reclamation.
	FetchStaleClaims(ctx context.Context, leaseTimeout time.Duration, limit int) ([]OutboxRecord, []DLQRecord, error)

	// GetOutboxRecord retrieves an outbox record by idempotency key.
	GetOutboxRecord(ctx context.Context, idempotencyKey string) (*OutboxRecord, error)

	// GetDLQRecord retrieves a dead-letter record by idempotency key.
	GetDLQRecord(ctx context.Context, idempotencyKey string) (*DLQRecord, error)

	// Close terminates store connections.
	Close() error
}
