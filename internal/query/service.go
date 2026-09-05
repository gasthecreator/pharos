package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/archive"
	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gocql/gocql"
)

// CassandraServiceConfig contains connection parameters for Cassandra query service.
type CassandraServiceConfig struct {
	Hosts          []string
	Port           int
	Keyspace       string
	Consistency    gocql.Consistency
	ConnectTimeout time.Duration
	// ArchiveDir is the cold-tier directory Slice 11's archival job writes
	// to. Queries fall back here when the hot tier doesn't have what was
	// asked for (or, for range/site-scoped queries, always also check here,
	// since "some of this might have aged into cold storage" is the normal
	// case for those query shapes, not an edge case).
	ArchiveDir string
	// LocalDC and RemoteDCs mirror dedup.CassandraConfig's fields (§2.4,
	// Slice 14: Multi-Region Cassandra + Kafka) -- the query service doesn't
	// bootstrap the keyspace itself, but still needs DC-aware host selection
	// for correct LOCAL_QUORUM read routing.
	LocalDC   string
	RemoteDCs map[string]int
}

// DefaultCassandraServiceConfig returns standard connection settings for the Pharos Cassandra cluster.
func DefaultCassandraServiceConfig() CassandraServiceConfig {
	return CassandraServiceConfig{
		Hosts:          []string{"127.0.0.1"},
		Port:           9042,
		Keyspace:       "pharos",
		Consistency:    gocql.LocalQuorum, // RF=3, LOCAL_QUORUM reads/writes (Slice 7)
		ConnectTimeout: 10 * time.Second,
		ArchiveDir:     archive.DefaultConfig().Dir,
		LocalDC:        "dc-us",
		RemoteDCs:      map[string]int{"dc-eu": 3},
	}
}

// CassandraService implements Service against live Apache Cassandra tables,
// falling back to the Slice 11 archive tier for data that's aged out of them.
type CassandraService struct {
	canonicalStore *consumer.CassandraCanonicalStore
	archiveReader  *archive.Reader
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
		LocalDC:           cfg.LocalDC,
		RemoteDCs:         cfg.RemoteDCs,
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
	if cfg.LocalDC != "" {
		// DC-aware host selection (§2.4, Slice 14) -- see dedup.CassandraConfig's
		// LocalDC docs for why this matters once a second DC genuinely exists.
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
	}

	session, err := cluster.CreateSession()
	if err != nil {
		cStore.Close()
		return nil, fmt.Errorf("failed to open Cassandra session for DLQ inspection: %w", err)
	}

	archiveDir := cfg.ArchiveDir
	if archiveDir == "" {
		archiveDir = archive.DefaultConfig().Dir
	}

	svc := &CassandraService{
		canonicalStore: cStore,
		archiveReader:  archive.NewReader(archive.Config{Dir: archiveDir}),
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

// GetEvent performs a point lookup by idempotency_key against
// pharos.canonical_events (§2.4, §5), falling back to the Slice 11 archive
// tier only on a hot-tier miss -- the overwhelmingly common case (recent
// data) never pays the cost of touching the cold tier at all.
func (s *CassandraService) GetEvent(ctx context.Context, idempotencyKey string) (*consumer.CanonicalRecord, error) {
	rec, err := s.canonicalStore.GetEvent(ctx, idempotencyKey)
	if err == nil {
		return rec, nil
	}
	if s.archiveReader != nil {
		if archived, archErr := s.archiveReader.GetEvent(idempotencyKey); archErr == nil && archived != nil {
			return archived, nil
		}
	}
	return nil, err
}

// GetEventsByStudy answers "all events for trial X in date range Y" via
// pharos.events_by_study (§2.4, §5), always also consulting the Slice 11
// archive tier for the requested range and merging results -- "some of
// what's being asked for might have aged into cold storage" is the normal
// case for a date-range query, not an edge case worth special-casing.
func (s *CassandraService) GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*consumer.CanonicalRecord, error) {
	hot, err := s.canonicalStore.GetEventsByStudy(ctx, studyID, startTime, endTime)
	if err != nil {
		return nil, err
	}
	if s.archiveReader == nil {
		return hot, nil
	}
	cold, err := s.archiveReader.GetEventsByStudy(studyID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("archive fallback failed for study %s: %w", studyID, err)
	}
	return mergeCanonicalRecords(hot, cold), nil
}

// GetEventsBySite answers "all events from site Z" via pharos.events_by_site
// (§2.4, §5), mirroring GetEventsByStudy's always-merge behavior.
func (s *CassandraService) GetEventsBySite(ctx context.Context, siteID string, minLocalSeq int64) ([]*consumer.CanonicalRecord, error) {
	hot, err := s.canonicalStore.GetEventsBySite(ctx, siteID, minLocalSeq)
	if err != nil {
		return nil, err
	}
	if s.archiveReader == nil {
		return hot, nil
	}
	cold, err := s.archiveReader.GetEventsBySite(siteID, minLocalSeq)
	if err != nil {
		return nil, fmt.Errorf("archive fallback failed for site %s: %w", siteID, err)
	}
	return mergeCanonicalRecords(hot, cold), nil
}

// mergeCanonicalRecords combines hot- and cold-tier results, deduplicating by
// idempotency_key. A key can legitimately exist in both tiers briefly -- the
// archival job exports before it deletes, so a row can be in both places for
// the short window between those two steps (or indefinitely, if the delete
// step failed and hasn't been retried yet; see internal/archive/job.go).
// Content is identical either way, so which copy wins doesn't matter -- the
// hot-tier copy is kept for no reason beyond "it was seen first."
func mergeCanonicalRecords(hot, cold []*consumer.CanonicalRecord) []*consumer.CanonicalRecord {
	if len(cold) == 0 {
		return hot
	}
	seen := make(map[string]bool, len(hot))
	merged := make([]*consumer.CanonicalRecord, 0, len(hot)+len(cold))
	for _, r := range hot {
		seen[r.IdempotencyKey] = true
		merged = append(merged, r)
	}
	for _, r := range cold {
		if !seen[r.IdempotencyKey] {
			merged = append(merged, r)
		}
	}
	return merged
}

// GetDLQEvent performs a point lookup on a rejected event by idempotency_key
// (§2.3), falling back to the Slice 11 archive tier only on a hot-tier miss.
func (s *CassandraService) GetDLQEvent(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
	rec, err := s.getDLQEventHot(ctx, idempotencyKey)
	if err == nil {
		return rec, nil
	}
	if s.archiveReader != nil {
		if archived, archErr := s.archiveReader.GetDLQEvent(idempotencyKey); archErr == nil && archived != nil {
			return dlqRecordFromArchive(archived), nil
		}
	}
	return nil, err
}

func (s *CassandraService) getDLQEventHot(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
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

// ListDLQEventsBySite retrieves rejected events for a specific clinical trial
// site (§2.3) querying pharos.dead_letter_events_by_site directly by
// partition key site_id, always also merging in the Slice 11 archive tier --
// this query shape has no time bound, so "some of this site's rejections
// might have aged into cold storage" is the normal case, not an edge case.
func (s *CassandraService) ListDLQEventsBySite(ctx context.Context, siteID string, limit int) ([]*DLQRecord, error) {
	hot, err := s.listDLQEventsBySiteHot(ctx, siteID, limit)
	if err != nil {
		return nil, err
	}
	if s.archiveReader == nil {
		return hot, nil
	}
	cold, err := s.archiveReader.GetDLQEventsBySite(siteID, limit)
	if err != nil {
		return nil, fmt.Errorf("archive fallback failed for site %s: %w", siteID, err)
	}
	return mergeDLQRecords(hot, cold, limit), nil
}

func (s *CassandraService) listDLQEventsBySiteHot(ctx context.Context, siteID string, limit int) ([]*DLQRecord, error) {
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

// dlqRecordFromArchive converts an archived dedup.DLQRecord (raw []byte
// payload, the storage-level shape) into this package's query.DLQRecord
// (string payload, the display-level shape) -- the same conversion
// CassandraService already does implicitly when scanning Cassandra rows,
// just made explicit here since the archive tier hands back the storage type
// directly rather than a Cassandra row.
func dlqRecordFromArchive(r *dedup.DLQRecord) *DLQRecord {
	return &DLQRecord{
		IdempotencyKey:   r.IdempotencyKey,
		SiteID:           r.SiteID,
		Payload:          string(r.Payload),
		RejectionReason:  r.RejectionReason,
		ValidationErrors: r.ValidationErrors,
		RejectedAt:       r.RejectedAt,
		Status:           string(r.Status),
		ClaimedAt:        r.ClaimedAt,
		PublishedAt:      r.PublishedAt,
		KafkaTopic:       r.KafkaTopic,
		KafkaPartition:   r.KafkaPartition,
		KafkaOffset:      r.KafkaOffset,
		ReplayedAt:       r.ReplayedAt,
	}
}

// mergeDLQRecords combines hot- and cold-tier DLQ results, deduplicating by
// idempotency_key (see mergeCanonicalRecords for why a key can legitimately
// appear in both tiers), and honors the same limit the hot-tier query used.
func mergeDLQRecords(hot []*DLQRecord, cold []*dedup.DLQRecord, limit int) []*DLQRecord {
	seen := make(map[string]bool, len(hot))
	merged := make([]*DLQRecord, 0, len(hot)+len(cold))
	for _, r := range hot {
		seen[r.IdempotencyKey] = true
		merged = append(merged, r)
	}
	for _, r := range cold {
		if limit > 0 && len(merged) >= limit {
			break
		}
		if !seen[r.IdempotencyKey] {
			merged = append(merged, dlqRecordFromArchive(r))
		}
	}
	return merged
}

// ListAllDLQEvents retrieves recently rejected events across all trial sites
// (§2.3). Deliberately hot-tier only, unlike the other DLQ query methods:
// merging in the archive here would mean scanning every known site's cold
// storage for what's meant to be a quick "what's rejected right now" check,
// a much heavier cost for a query shape that's about recent activity by
// nature -- anyone who needs archived DLQ history for a specific site
// already has ListDLQEventsBySite, which does merge.
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
