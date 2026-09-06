package dedup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
)

// MemoryOutboxStore provides an in-memory, thread-safe implementation of OutboxStore
// accurately simulating Cassandra LWT Paxos consensus and lease CAS semantics for unit testing.
type MemoryOutboxStore struct {
	mu          sync.Mutex
	clock       clock.Clock
	events      map[string]*OutboxRecord
	dlqEvents   map[string]*DLQRecord
	pendingKeys map[string]time.Time
	pendingDLQ  map[string]time.Time
	closed      bool
}

// NewMemoryOutboxStore constructs a new MemoryOutboxStore, using the real
// wall clock by default -- see SetClock to substitute a controllable one
// for property/simulation testing (§2.4, Slice 22).
func NewMemoryOutboxStore() *MemoryOutboxStore {
	return &MemoryOutboxStore{
		clock:       clock.Real{},
		events:      make(map[string]*OutboxRecord),
		dlqEvents:   make(map[string]*DLQRecord),
		pendingKeys: make(map[string]time.Time),
		pendingDLQ:  make(map[string]time.Time),
	}
}

// SetClock substitutes the clock this store uses for lease timing, so tests
// can advance simulated time without real sleeping (§2.4, Slice 22:
// property-based & deterministic simulation testing). Never call this on a
// store already handling live traffic -- it's for tests and simulation
// harnesses only, mirroring this project's other post-construction setter
// pattern (e.g. Handler.SetKeyStore) used for optional/test-only wiring.
func (s *MemoryOutboxStore) SetClock(c clock.Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = c
}

// InsertClaim simulates Cassandra LWT `INSERT ... IF NOT EXISTS` with status='PUBLISHING'.
func (s *MemoryOutboxStore) InsertClaim(ctx context.Context, rec OutboxRecord, leaseTimeout time.Duration) (ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ClaimResult{}, ErrStoreClosed
	}

	now := s.clock.Now()
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
	//
	// Sub-case 2a: not currently PUBLISHING (already PUBLISHED, or -- for
	// symmetry with InsertDLQClaim's REPLAYED terminal state, even though
	// event_outbox itself never actually reaches a third status today --
	// any other terminal status) -> no-op, never a steal candidate. Real
	// Cassandra's own CAS steal query conditions on `IF status =
	// 'PUBLISHING'` (see CassandraOutboxStore.InsertClaim), so checking
	// only `== StatusPublished` here (rather than `!= StatusPublishing`)
	// was a genuine divergence from what this store's own doc comment
	// claims to simulate -- caught by a Slice 22 property test finding
	// that a REPLAYED DLQ record's claimed_at could be silently mutated
	// and Acquired incorrectly reported true by InsertDLQClaim (see that
	// method's identical fix).
	if existing.Status != StatusPublishing {
		return ClaimResult{
			Acquired:       false,
			Status:         existing.Status,
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

// MarkPublished finalizes record to status='PUBLISHED', but only if
// expectedClaimedAt still matches the record's current claimed_at (§2.4,
// Slice 22) -- a fencing check mirroring the LWT CAS Cassandra already uses
// for the claim/steal itself (InsertClaim), extended here to the finalize
// step too. Without this, a claimant whose lease was legitimately stolen
// (it was presumed dead after DefaultLeaseTimeout) could still finalize
// its own late, stale publish and silently overwrite whichever claimant's
// finalize actually should have won -- corrupting the record's Kafka
// lineage bookkeeping. Returns ErrClaimSuperseded, not a hard failure: from
// the stale caller's own point of view, its event genuinely was published
// (a real Kafka message exists for this idempotency key, whether from this
// call or a peer's), so callers should generally treat this as an outcome
// to note, not as their own submission failing.
func (s *MemoryOutboxStore) MarkPublished(ctx context.Context, idempotencyKey string, expectedClaimedAt time.Time, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	rec, found := s.events[idempotencyKey]
	if !found {
		return ErrRecordNotFound
	}
	if rec.Status != StatusPublishing || !rec.ClaimedAt.Equal(expectedClaimedAt) {
		return ErrClaimSuperseded
	}

	rec.Status = StatusPublished
	rec.PublishedAt = s.clock.Now()
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

	now := s.clock.Now()
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

	// Not currently PUBLISHING (PUBLISHED, or REPLAYED -- a real,
	// previously-undiscovered gap this exact check closes: a REPLAYED
	// record, e.g. from a client retrying a stale cached submission long
	// after its original rejection was already fixed and replayed, must
	// never be treated as an abandoned in-flight claim eligible for
	// stealing. Real Cassandra's own CAS already only steals `IF status =
	// 'PUBLISHING'`; this mirrors that instead of only checking
	// `== StatusPublished` the way this branch previously did.
	if existing.Status != StatusPublishing {
		return ClaimResult{
			Acquired:  false,
			Status:    existing.Status,
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

// MarkDLQPublished finalizes DLQ record to status='PUBLISHED', fenced by
// expectedClaimedAt exactly like MarkPublished (§2.4, Slice 22) -- see that
// method's docs for the full rationale.
func (s *MemoryOutboxStore) MarkDLQPublished(ctx context.Context, idempotencyKey string, expectedClaimedAt time.Time, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	rec, found := s.dlqEvents[idempotencyKey]
	if !found {
		return ErrRecordNotFound
	}
	if rec.Status != StatusPublishing || !rec.ClaimedAt.Equal(expectedClaimedAt) {
		return ErrClaimSuperseded
	}

	rec.Status = StatusPublished
	rec.PublishedAt = s.clock.Now()
	rec.KafkaTopic = topic
	rec.KafkaPartition = partition
	rec.KafkaOffset = offset

	delete(s.pendingDLQ, idempotencyKey)
	return nil
}

// MarkDLQReplayed transitions a DLQ record from PUBLISHED to REPLAYED (§2.3,
// Slice 10), mirroring CassandraOutboxStore.MarkDLQReplayed's precondition.
func (s *MemoryOutboxStore) MarkDLQReplayed(ctx context.Context, idempotencyKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	rec, found := s.dlqEvents[idempotencyKey]
	if !found {
		return ErrRecordNotFound
	}
	if rec.Status != StatusPublished {
		return fmt.Errorf("cannot mark %s replayed: not in PUBLISHED status", idempotencyKey)
	}

	rec.Status = StatusReplayed
	rec.ReplayedAt = s.clock.Now()
	return nil
}

// FetchStaleClaims returns records with expired leases.
func (s *MemoryOutboxStore) FetchStaleClaims(ctx context.Context, leaseTimeout time.Duration, limit int) ([]OutboxRecord, []DLQRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, nil, ErrStoreClosed
	}

	now := s.clock.Now()
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
