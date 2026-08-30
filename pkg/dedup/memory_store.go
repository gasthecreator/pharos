package dedup

import (
	"context"
	"sync"
	"time"
)

// MemoryOutboxStore provides an in-memory, thread-safe implementation of OutboxStore
// accurately simulating Cassandra LWT Paxos consensus and lease CAS semantics for unit testing.
type MemoryOutboxStore struct {
	mu            sync.Mutex
	events        map[string]*OutboxRecord
	dlqEvents     map[string]*DLQRecord
	pendingKeys   map[string]time.Time
	pendingDLQ    map[string]time.Time
	closed        bool
}

// NewMemoryOutboxStore constructs a new MemoryOutboxStore.
func NewMemoryOutboxStore() *MemoryOutboxStore {
	return &MemoryOutboxStore{
		events:      make(map[string]*OutboxRecord),
		dlqEvents:   make(map[string]*DLQRecord),
		pendingKeys: make(map[string]time.Time),
		pendingDLQ:  make(map[string]time.Time),
	}
}

// InsertClaim simulates Cassandra LWT `INSERT ... IF NOT EXISTS` with status='PUBLISHING'.
func (s *MemoryOutboxStore) InsertClaim(ctx context.Context, rec OutboxRecord, leaseTimeout time.Duration) (ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ClaimResult{}, ErrStoreClosed
	}

	now := time.Now().UTC()
	existing, found := s.events[rec.IdempotencyKey]

	if !found {
		// Won LWT insert
		claimed := rec
		claimed.Status = StatusPublishing
		claimed.ClaimedAt = now
		if claimed.CreatedAt.IsZero() {
			claimed.CreatedAt = now
		}
		s.events[rec.IdempotencyKey] = &claimed
		s.pendingKeys[rec.IdempotencyKey] = now
		return ClaimResult{
			Acquired:  true,
			Status:    StatusPublishing,
			ClaimedAt: now,
		}, nil
	}

	// Key already exists (LWT applied == false)
	if existing.Status == StatusPublished {
		// Sub-case 2a: already published -> no-op
		return ClaimResult{
			Acquired:       false,
			Status:         StatusPublished,
			ClaimedAt:      existing.ClaimedAt,
			ExistingRecord: copyOutboxRecord(existing),
		}, nil
	}

	// Sub-case 2b: PUBLISHING with active lease
	if now.Sub(existing.ClaimedAt) < leaseTimeout {
		return ClaimResult{
			Acquired:       false,
			Status:         StatusPublishing,
			ClaimedAt:      existing.ClaimedAt,
			ExistingRecord: copyOutboxRecord(existing),
		}, nil
	}

	// Sub-case 2c: PUBLISHING with expired lease -> CAS steal
	existing.ClaimedAt = now
	return ClaimResult{
		Acquired:       true,
		Status:         StatusPublishing,
		ClaimedAt:      now,
		ExistingRecord: copyOutboxRecord(existing),
	}, nil
}

// MarkPublished finalizes record to status='PUBLISHED'.
func (s *MemoryOutboxStore) MarkPublished(ctx context.Context, idempotencyKey string, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	rec, found := s.events[idempotencyKey]
	if !found {
		return ErrRecordNotFound
	}

	rec.Status = StatusPublished
	rec.PublishedAt = time.Now().UTC()
	rec.KafkaTopic = topic
	rec.KafkaPartition = partition
	rec.KafkaOffset = offset

	delete(s.pendingKeys, idempotencyKey)
	return nil
}

// InsertDLQClaim simulates Cassandra LWT on dead_letter_events.
func (s *MemoryOutboxStore) InsertDLQClaim(ctx context.Context, rec DLQRecord, leaseTimeout time.Duration) (ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ClaimResult{}, ErrStoreClosed
	}

	now := time.Now().UTC()
	existing, found := s.dlqEvents[rec.IdempotencyKey]

	if !found {
		claimed := rec
		claimed.Status = StatusPublishing
		claimed.ClaimedAt = now
		if claimed.RejectedAt.IsZero() {
			claimed.RejectedAt = now
		}
		s.dlqEvents[rec.IdempotencyKey] = &claimed
		s.pendingDLQ[rec.IdempotencyKey] = now
		return ClaimResult{
			Acquired:  true,
			Status:    StatusPublishing,
			ClaimedAt: now,
		}, nil
	}

	if existing.Status == StatusPublished {
		return ClaimResult{
			Acquired:  false,
			Status:    StatusPublished,
			ClaimedAt: existing.ClaimedAt,
		}, nil
	}

	if now.Sub(existing.ClaimedAt) < leaseTimeout {
		return ClaimResult{
			Acquired:  false,
			Status:    StatusPublishing,
			ClaimedAt: existing.ClaimedAt,
		}, nil
	}

	existing.ClaimedAt = now
	return ClaimResult{
		Acquired:  true,
		Status:    StatusPublishing,
		ClaimedAt: now,
	}, nil
}

// MarkDLQPublished finalizes DLQ record to status='PUBLISHED'.
func (s *MemoryOutboxStore) MarkDLQPublished(ctx context.Context, idempotencyKey string, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	rec, found := s.dlqEvents[idempotencyKey]
	if !found {
		return ErrRecordNotFound
	}

	rec.Status = StatusPublished
	rec.PublishedAt = time.Now().UTC()
	rec.KafkaTopic = topic
	rec.KafkaPartition = partition
	rec.KafkaOffset = offset

	delete(s.pendingDLQ, idempotencyKey)
	return nil
}

// FetchStaleClaims returns records with expired leases.
func (s *MemoryOutboxStore) FetchStaleClaims(ctx context.Context, leaseTimeout time.Duration, limit int) ([]OutboxRecord, []DLQRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, ErrStoreClosed
	}

	now := time.Now().UTC()
	var staleOutbox []OutboxRecord
	var staleDLQ []DLQRecord

	for key := range s.pendingKeys {
		if rec, exists := s.events[key]; exists {
			if rec.Status == StatusPublishing && now.Sub(rec.ClaimedAt) >= leaseTimeout {
				staleOutbox = append(staleOutbox, *copyOutboxRecord(rec))
				if limit > 0 && len(staleOutbox) >= limit {
					break
				}
			}
		}
	}

	for key := range s.pendingDLQ {
		if rec, exists := s.dlqEvents[key]; exists {
			if rec.Status == StatusPublishing && now.Sub(rec.ClaimedAt) >= leaseTimeout {
				staleDLQ = append(staleDLQ, *copyDLQRecord(rec))
				if limit > 0 && len(staleDLQ) >= limit {
					break
				}
			}
		}
	}

	return staleOutbox, staleDLQ, nil
}

// GetOutboxRecord retrieves an outbox record by idempotency key.
func (s *MemoryOutboxStore) GetOutboxRecord(ctx context.Context, idempotencyKey string) (*OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	rec, found := s.events[idempotencyKey]
	if !found {
		return nil, ErrRecordNotFound
	}
	return copyOutboxRecord(rec), nil
}

// GetDLQRecord retrieves a DLQ record by idempotency key.
func (s *MemoryOutboxStore) GetDLQRecord(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	rec, found := s.dlqEvents[idempotencyKey]
	if !found {
		return nil, ErrRecordNotFound
	}
	return copyDLQRecord(rec), nil
}

// Close marks the store as closed.
func (s *MemoryOutboxStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func copyOutboxRecord(r *OutboxRecord) *OutboxRecord {
	if r == nil {
		return nil
	}
	c := *r
	if r.Payload != nil {
		c.Payload = make([]byte, len(r.Payload))
		copy(c.Payload, r.Payload)
	}
	return &c
}

func copyDLQRecord(r *DLQRecord) *DLQRecord {
	if r == nil {
		return nil
	}
	c := *r
	if r.Payload != nil {
		c.Payload = make([]byte, len(r.Payload))
		copy(c.Payload, r.Payload)
	}
	return &c
}
