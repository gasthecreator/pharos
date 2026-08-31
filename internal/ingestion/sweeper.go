package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
)

// Sweeper runs periodic background scans to reclaim stale in-flight claims (§2.2).
type Sweeper struct {
	store        dedup.OutboxStore
	producer     kafka.Producer
	interval     time.Duration
	leaseTimeout time.Duration
	stopCh       chan struct{}
}

// NewSweeper creates a background sweeper for transactional outbox tables.
func NewSweeper(store dedup.OutboxStore, producer kafka.Producer, interval, leaseTimeout time.Duration) *Sweeper {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if leaseTimeout <= 0 {
		leaseTimeout = dedup.DefaultLeaseTimeout
	}
	return &Sweeper{
		store:        store,
		producer:     producer,
		interval:     interval,
		leaseTimeout: leaseTimeout,
		stopCh:       make(chan struct{}),
	}
}

// Step performs a single sweep of stale claims. Returns count of recovered events.
func (s *Sweeper) Step(ctx context.Context) (int, error) {
	if s.store == nil || s.producer == nil {
		return 0, nil
	}

	staleEvents, staleDLQ, err := s.store.FetchStaleClaims(ctx, s.leaseTimeout, 100)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch stale claims: %w", err)
	}

	recovered := 0

	// 1. Recover stale event outbox records
	for _, rec := range staleEvents {
		claim, err := s.store.InsertClaim(ctx, rec, s.leaseTimeout)
		if err == nil && claim.Acquired {
			meta, err := s.producer.Publish(ctx, kafka.MainTopic, []byte(rec.SiteID), rec.Payload, map[string]string{
				"idempotency_key": rec.IdempotencyKey,
				"site_id":         rec.SiteID,
			})
			if err == nil {
				_ = s.store.MarkPublished(ctx, rec.IdempotencyKey, meta.Topic, meta.Partition, meta.Offset)
				recovered++
			}
		}
	}

	// 2. Recover stale DLQ records
	for _, dlq := range staleDLQ {
		claim, err := s.store.InsertDLQClaim(ctx, dlq, s.leaseTimeout)
		if err == nil && claim.Acquired {
			meta, err := s.producer.Publish(ctx, kafka.DLQTopic, []byte(dlq.SiteID), dlq.Payload, map[string]string{
				"idempotency_key":  dlq.IdempotencyKey,
				"site_id":          dlq.SiteID,
				"rejection_reason": dlq.RejectionReason,
			})
			if err == nil {
				_ = s.store.MarkDLQPublished(ctx, dlq.IdempotencyKey, meta.Topic, meta.Partition, meta.Offset)
				recovered++
			}
		}
	}

	return recovered, nil
}

// Start runs the sweeper loop in a background goroutine until ctx is cancelled or Stop is called.
func (s *Sweeper) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-ticker.C:
				_, _ = s.Step(ctx)
			}
		}
	}()
}

// Stop stops the sweeper.
func (s *Sweeper) Stop() {
	close(s.stopCh)
}
