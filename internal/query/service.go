package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gocql/gocql"
)

// CassandraServiceConfig contains connection parameters for Cassandra query service.
type CassandraServiceConfig struct {
	Hosts          []string
	Port           int
	Keyspace       string
	Consistency    gocql.Consistency
	ConnectTimeout time.Duration
}

// DefaultCassandraServiceConfig returns standard connection settings for the Pharos Cassandra cluster.
func DefaultCassandraServiceConfig() CassandraServiceConfig {
	return CassandraServiceConfig{
		Hosts:          []string{"127.0.0.1"},
		Port:           9042,
		Keyspace:       "pharos",
		Consistency:    gocql.LocalQuorum, // RF=3, LOCAL_QUORUM reads/writes (Slice 7)
		ConnectTimeout: 10 * time.Second,
	}
}

// CassandraService implements Service against live Apache Cassandra tables.
type CassandraService struct {
	canonicalStore *consumer.CassandraCanonicalStore
	session        *gocql.Session
	mu             sync.RWMutex
	closed         bool
}

// NewCassandraService creates a new CassandraService, connecting and bootstrapping schemas/indexes.
func NewCassandraService(cfg CassandraServiceConfig) (*CassandraService, error) {
	cStoreCfg := consumer.CassandraStoreConfig{
		Hosts:             cfg.Hosts,
		Port:              cfg.Port,
		Keyspace:          cfg.Keyspace,
		Consistency:       cfg.Consistency,
		ConnectTimeout:    cfg.ConnectTimeout,
		ReplicationFactor: 3,
	}

	cStore, err := consumer.NewCassandraCanonicalStore(cStoreCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize canonical store: %w", err)
	}

	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency
	cluster.Keyspace = cfg.Keyspace
	cluster.DisableInitialHostLookup = true

	session, err := cluster.CreateSession()
	if err != nil {
		cStore.Close()
		return nil, fmt.Errorf("failed to open Cassandra session for DLQ inspection: %w", err)
	}

	svc := &CassandraService{
		canonicalStore: cStore,
		session:        session,
	}

	if err := svc.EnsureSchemas(); err != nil {
		svc.Close()
		return nil, fmt.Errorf("failed to ensure DLQ schemas: %w", err)
	}

	return svc, nil
}

// EnsureSchemas bootstraps the dead_letter_events_by_site query table if not exists (§2.3, §4).
func (s *CassandraService) EnsureSchemas() error {
	tableQuery := `
		CREATE TABLE IF NOT EXISTS pharos.dead_letter_events_by_site (
			site_id text,
			rejected_at timestamp,
			idempotency_key text,
			payload text,
			rejection_reason text,
			validation_errors text,
			status text,
			claimed_at timestamp,
			published_at timestamp,
			kafka_topic text,
			kafka_partition int,
			kafka_offset bigint,
			PRIMARY KEY ((site_id), rejected_at, idempotency_key)
		) WITH CLUSTERING ORDER BY (rejected_at DESC, idempotency_key ASC);
	`
	if err := s.session.Query(tableQuery).Exec(); err != nil {
		return fmt.Errorf("failed to create dead_letter_events_by_site: %w", err)
	}
	return nil
}

// GetEvent performs a point lookup by idempotency_key against pharos.canonical_events (§2.4, §5).
func (s *CassandraService) GetEvent(ctx context.Context, idempotencyKey string) (*consumer.CanonicalRecord, error) {
	return s.canonicalStore.GetEvent(ctx, idempotencyKey)
}

// GetEventsByStudy answers "all events for trial X in date range Y" via pharos.events_by_study (§2.4, §5).
func (s *CassandraService) GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*consumer.CanonicalRecord, error) {
	return s.canonicalStore.GetEventsByStudy(ctx, studyID, startTime, endTime)
}

// GetEventsBySite answers "all events from site Z" via pharos.events_by_site (§2.4, §5).
func (s *CassandraService) GetEventsBySite(ctx context.Context, siteID string, minLocalSeq int64) ([]*consumer.CanonicalRecord, error) {
	return s.canonicalStore.GetEventsBySite(ctx, siteID, minLocalSeq)
}

// GetDLQEvent performs a point lookup on a rejected event by idempotency_key (§2.3).
func (s *CassandraService) GetDLQEvent(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fmt.Errorf("service is closed")
	}

	query := `SELECT idempotency_key, site_id, payload, rejection_reason, validation_errors,
	                 rejected_at, status, claimed_at, published_at, kafka_topic, kafka_partition, kafka_offset, replayed_at
	          FROM pharos.dead_letter_events
	          WHERE idempotency_key = ? LIMIT 1`

	var rec DLQRecord
	var claimedAt, publishedAt, replayedAt *time.Time
	var kafkaPartition *int
	var kafkaOffset *int64

	iter := s.session.Query(query, idempotencyKey).WithContext(ctx).Iter()
	if !iter.Scan(
		&rec.IdempotencyKey,
		&rec.SiteID,
		&rec.Payload,
		&rec.RejectionReason,
		&rec.ValidationErrors,
		&rec.RejectedAt,
		&rec.Status,
		&claimedAt,
		&publishedAt,
		&rec.KafkaTopic,
		&kafkaPartition,
		&kafkaOffset,
		&replayedAt,
	) {
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to query dead_letter_events: %w", err)
		}
		return nil, fmt.Errorf("dlq event not found: %s", idempotencyKey)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close iter: %w", err)
	}

	if claimedAt != nil {
		rec.ClaimedAt = *claimedAt
	}
	if publishedAt != nil {
		rec.PublishedAt = *publishedAt
	}
	if kafkaPartition != nil {
		rec.KafkaPartition = *kafkaPartition
	}
	if kafkaOffset != nil {
		rec.KafkaOffset = *kafkaOffset
	}
	if replayedAt != nil {
		rec.ReplayedAt = *replayedAt
	}

	return &rec, nil
}

// ListDLQEventsBySite retrieves rejected events for a specific clinical trial site (§2.3)
// querying pharos.dead_letter_events_by_site directly by partition key site_id.
func (s *CassandraService) ListDLQEventsBySite(ctx context.Context, siteID string, limit int) ([]*DLQRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fmt.Errorf("service is closed")
	}

	if limit <= 0 {
		limit = 50
	}

	query := `SELECT idempotency_key, site_id, payload, rejection_reason, validation_errors,
	                 rejected_at, status, claimed_at, published_at, kafka_topic, kafka_partition, kafka_offset, replayed_at
	          FROM pharos.dead_letter_events_by_site
	          WHERE site_id = ? LIMIT ?`

	iter := s.session.Query(query, siteID, limit).WithContext(ctx).Iter()
	return scanDLQRecords(iter)
}

// ListAllDLQEvents retrieves recently rejected events across all trial sites (§2.3).
func (s *CassandraService) ListAllDLQEvents(ctx context.Context, limit int) ([]*DLQRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, fmt.Errorf("service is closed")
	}

	if limit <= 0 {
		limit = 50
	}

	query := `SELECT idempotency_key, site_id, payload, rejection_reason, validation_errors,
	                 rejected_at, status, claimed_at, published_at, kafka_topic, kafka_partition, kafka_offset, replayed_at
	          FROM pharos.dead_letter_events
	          LIMIT ?`

	iter := s.session.Query(query, limit).WithContext(ctx).Iter()
	return scanDLQRecords(iter)
}

func scanDLQRecords(iter *gocql.Iter) ([]*DLQRecord, error) {
	var records []*DLQRecord

	var idKey, siteID, payload, reason, valErrors, status, topic string
	var rejAt time.Time
	var claimedAt, publishedAt, replayedAt *time.Time
	var part *int
	var off *int64

	for iter.Scan(&idKey, &siteID, &payload, &reason, &valErrors, &rejAt, &status, &claimedAt, &publishedAt, &topic, &part, &off, &replayedAt) {
		r := &DLQRecord{
			IdempotencyKey:   idKey,
			SiteID:           siteID,
			Payload:          payload,
			RejectionReason:  reason,
			ValidationErrors: valErrors,
			RejectedAt:       rejAt,
			Status:           status,
			KafkaTopic:       topic,
		}
		if claimedAt != nil {
			r.ClaimedAt = *claimedAt
		}
		if publishedAt != nil {
			r.PublishedAt = *publishedAt
		}
		if part != nil {
			r.KafkaPartition = *part
		}
		if off != nil {
			r.KafkaOffset = *off
		}
		if replayedAt != nil {
			r.ReplayedAt = *replayedAt
		}
		records = append(records, r)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to scan dlq records: %w", err)
	}

	return records, nil
}

// Close gracefully closes the Cassandra session and underlying canonical store.
func (s *CassandraService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.session != nil {
		s.session.Close()
	}
	if s.canonicalStore != nil {
		return s.canonicalStore.Close()
	}
	return nil
}

// MemoryService provides an in-memory implementation of Service for unit testing.
type MemoryService struct {
	mu             sync.RWMutex
	canonicalStore *consumer.MemoryCanonicalStore
	dlqRecords     map[string]*DLQRecord
}

// NewMemoryService constructs an in-memory query service.
func NewMemoryService() *MemoryService {
	return &MemoryService{
		canonicalStore: consumer.NewMemoryCanonicalStore(),
		dlqRecords:     make(map[string]*DLQRecord),
	}
}

// CanonicalStore returns the underlying MemoryCanonicalStore for test seeding.
func (m *MemoryService) CanonicalStore() *consumer.MemoryCanonicalStore {
	return m.canonicalStore
}

// SaveDLQEvent seeds a DLQ record in the in-memory store.
func (m *MemoryService) SaveDLQEvent(rec *DLQRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlqRecords[rec.IdempotencyKey] = rec
}

func (m *MemoryService) GetEvent(ctx context.Context, idempotencyKey string) (*consumer.CanonicalRecord, error) {
	return m.canonicalStore.GetEvent(ctx, idempotencyKey)
}

func (m *MemoryService) GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*consumer.CanonicalRecord, error) {
	return m.canonicalStore.GetEventsByStudy(ctx, studyID, startTime, endTime)
}

func (m *MemoryService) GetEventsBySite(ctx context.Context, siteID string, minLocalSeq int64) ([]*consumer.CanonicalRecord, error) {
	return m.canonicalStore.GetEventsBySite(ctx, siteID, minLocalSeq)
}

func (m *MemoryService) GetDLQEvent(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, exists := m.dlqRecords[idempotencyKey]
	if !exists {
		return nil, fmt.Errorf("dlq event not found: %s", idempotencyKey)
	}
	return rec, nil
}

func (m *MemoryService) ListDLQEventsBySite(ctx context.Context, siteID string, limit int) ([]*DLQRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var matches []*DLQRecord
	for _, rec := range m.dlqRecords {
		if strings.EqualFold(rec.SiteID, siteID) {
			matches = append(matches, rec)
			if limit > 0 && len(matches) >= limit {
				break
			}
		}
	}
	return matches, nil
}

func (m *MemoryService) ListAllDLQEvents(ctx context.Context, limit int) ([]*DLQRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var all []*DLQRecord
	for _, rec := range m.dlqRecords {
		all = append(all, rec)
		if limit > 0 && len(all) >= limit {
			break
		}
	}
	return all, nil
}

func (m *MemoryService) Close() error {
	return nil
}
