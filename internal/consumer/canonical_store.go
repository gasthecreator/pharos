package consumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// CanonicalStore defines the persistence interface for queryable canonical adverse event tables (§2.4, §5).
type CanonicalStore interface {
	SaveEvent(ctx context.Context, record *CanonicalRecord) error
	GetEvent(ctx context.Context, idempotencyKey string) (*CanonicalRecord, error)
	GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*CanonicalRecord, error)
	GetEventsBySite(ctx context.Context, siteID string, minSeq int64) ([]*CanonicalRecord, error)
	SaveWatermarkCheckpoint(ctx context.Context, groupID string, cp WatermarkCheckpoint) error
	LoadWatermarkCheckpoint(ctx context.Context, groupID string) (*WatermarkCheckpoint, error)
	EnsureSchema() error
	Close() error
}

// CassandraStoreConfig specifies Cassandra connection and consistency parameters (§2.4, §5).
type CassandraStoreConfig struct {
	Hosts             []string
	Port              int
	Keyspace          string
	Consistency       gocql.Consistency
	ConnectTimeout    time.Duration
	ReplicationFactor int
	// LocalDC and RemoteDCs mirror dedup.CassandraConfig's fields (§2.4,
	// Slice 14: Multi-Region Cassandra + Kafka) -- see that type's docs.
	LocalDC   string
	RemoteDCs map[string]int
}

// DefaultCassandraStoreConfig returns defaults for the Pharos 3-node Cassandra cluster.
func DefaultCassandraStoreConfig() CassandraStoreConfig {
	return CassandraStoreConfig{
		Hosts:             []string{"127.0.0.1"},
		Port:              9042,
		Keyspace:          "pharos",
		Consistency:       gocql.LocalQuorum, // RF=3, LOCAL_QUORUM reads/writes (Slice 7, §2.4)
		ConnectTimeout:    10 * time.Second,
		ReplicationFactor: 3,
		LocalDC:           "dc-us",
		RemoteDCs:         map[string]int{"dc-eu": 3},
	}
}

// networkTopologyReplicationMap renders the CQL replication map literal for
// NetworkTopologyStrategy (§2.4, Slice 14). Duplicated from
// internal/dedup/cassandra_store.go rather than shared -- this project has
// no existing common Cassandra-bootstrap package, and each store's keyspace
// bootstrap is already independently duplicated (SimpleStrategy before this
// change, identically, in both files).
func networkTopologyReplicationMap(localDC string, localRF int, remoteDCs map[string]int) string {
	parts := []string{fmt.Sprintf("'%s': %d", localDC, localRF)}
	for dc, rf := range remoteDCs {
		parts = append(parts, fmt.Sprintf("'%s': %d", dc, rf))
	}
	return "{'class': 'NetworkTopologyStrategy', " + strings.Join(parts, ", ") + "}"
}

// CassandraCanonicalStore implements CanonicalStore against Apache Cassandra using parallel idempotent upserts.
type CassandraCanonicalStore struct {
	session *gocql.Session
	cfg     CassandraStoreConfig
	mu      sync.RWMutex
	closed  bool
}

// NewCassandraCanonicalStore connects to Cassandra and bootstraps canonical schemas.
func NewCassandraCanonicalStore(cfg CassandraStoreConfig) (*CassandraCanonicalStore, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency
	cluster.DisableInitialHostLookup = true // Required for Docker-on-Mac localhost port mapping
	if cfg.LocalDC != "" {
		// DC-aware host selection (§2.4, Slice 14) -- see dedup.CassandraConfig's
		// LocalDC docs for why this matters once a second DC genuinely exists.
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
	}

	cluster.Keyspace = cfg.Keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		// Fallback: connect without keyspace to create keyspace if missing
		cluster.Keyspace = ""
		initSession, initErr := cluster.CreateSession()
		if initErr != nil {
			return nil, fmt.Errorf("failed to connect to Cassandra cluster: %w", initErr)
		}

		keyspaceStmt := fmt.Sprintf(`
			CREATE KEYSPACE IF NOT EXISTS %s
			WITH replication = %s;
		`, cfg.Keyspace, networkTopologyReplicationMap(cfg.LocalDC, cfg.ReplicationFactor, cfg.RemoteDCs))

		if err := initSession.Query(keyspaceStmt).Exec(); err != nil {
			initSession.Close()
			return nil, fmt.Errorf("failed to create keyspace %s: %w", cfg.Keyspace, err)
		}
		initSession.Close()

		cluster.Keyspace = cfg.Keyspace
		session, err = cluster.CreateSession()
		if err != nil {
			return nil, fmt.Errorf("failed to connect to keyspace %s: %w", cfg.Keyspace, err)
		}
	}

	store := &CassandraCanonicalStore{
		session: session,
		cfg:     cfg,
	}

	if err := store.EnsureSchema(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to bootstrap canonical schemas: %w", err)
	}

	return store, nil
}

// EnsureSchema bootstraps the three canonical query tables (§2.4, §5).
func (s *CassandraCanonicalStore) EnsureSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS pharos.canonical_events (
			idempotency_key text,
			site_id text,
			study_id text,
			local_seq bigint,
			event_time timestamp,
			recorded_time timestamp,
			ingestion_time timestamp,
			severity text,
			event_code text,
			subject text,
			payload text,
			kafka_topic text,
			kafka_partition int,
			kafka_offset bigint,
			consumed_at timestamp,
			is_late boolean,
			PRIMARY KEY (idempotency_key)
		);`,
		`CREATE TABLE IF NOT EXISTS pharos.events_by_study (
			study_id text,
			event_time timestamp,
			idempotency_key text,
			site_id text,
			local_seq bigint,
			recorded_time timestamp,
			ingestion_time timestamp,
			severity text,
			event_code text,
			subject text,
			payload text,
			is_late boolean,
			PRIMARY KEY ((study_id), event_time, idempotency_key)
		) WITH CLUSTERING ORDER BY (event_time DESC, idempotency_key ASC);`,
		`CREATE TABLE IF NOT EXISTS pharos.events_by_site (
			site_id text,
			local_seq bigint,
			idempotency_key text,
			study_id text,
			event_time timestamp,
			recorded_time timestamp,
			ingestion_time timestamp,
			severity text,
			event_code text,
			subject text,
			payload text,
			is_late boolean,
			PRIMARY KEY ((site_id), local_seq, idempotency_key)
		) WITH CLUSTERING ORDER BY (local_seq DESC, idempotency_key ASC);`,
		`CREATE TABLE IF NOT EXISTS pharos.known_studies (
			study_id text,
			PRIMARY KEY (study_id)
		);`,
		`CREATE TABLE IF NOT EXISTS pharos.consumer_watermark_checkpoints (
			group_id text,
			previous_emitted timestamp,
			partition_high_watermark map<int, timestamp>,
			partition_last_activity map<int, timestamp>,
			PRIMARY KEY (group_id)
		);`,
	}

	for _, q := range queries {
		if err := s.session.Query(q).Exec(); err != nil {
			return fmt.Errorf("schema query failed (%s): %w", q, err)
		}
	}
	return nil
}

// SaveEvent writes the record to all three canonical tables, plus the
// known_studies archive-tracking table (§2.4, Slice 11), concurrently using
// parallel idempotent upserts (§2.4).
func (s *CassandraCanonicalStore) SaveEvent(ctx context.Context, r *CanonicalRecord) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return fmt.Errorf("canonical store is closed")
	}
	session := s.session
	s.mu.RUnlock()

	const insertCanonical = `
		INSERT INTO pharos.canonical_events (
			idempotency_key, site_id, study_id, local_seq,
			event_time, recorded_time, ingestion_time,
			severity, event_code, subject, payload,
			kafka_topic, kafka_partition, kafka_offset,
			consumed_at, is_late
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	const insertByStudy = `
		INSERT INTO pharos.events_by_study (
			study_id, event_time, idempotency_key, site_id, local_seq,
			recorded_time, ingestion_time, severity, event_code,
			subject, payload, is_late
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	const insertBySite = `
		INSERT INTO pharos.events_by_site (
			site_id, local_seq, idempotency_key, study_id,
			event_time, recorded_time, ingestion_time,
			severity, event_code, subject, payload, is_late
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	// 1. Table: canonical_events
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := session.Query(insertCanonical,
			r.IdempotencyKey, r.SiteID, r.StudyID, r.LocalSeq,
			r.EventTime, r.RecordedTime, r.IngestionTime,
			r.Severity, r.EventCode, r.Subject, r.Payload,
			r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
			r.ConsumedAt, r.IsLate,
		).WithContext(ctx).Exec()
		if err != nil {
			errCh <- fmt.Errorf("insert canonical_events failed: %w", err)
		}
	}()

	// 2. Table: events_by_study
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := session.Query(insertByStudy,
			r.StudyID, r.EventTime, r.IdempotencyKey, r.SiteID, r.LocalSeq,
			r.RecordedTime, r.IngestionTime, r.Severity, r.EventCode,
			r.Subject, r.Payload, r.IsLate,
		).WithContext(ctx).Exec()
		if err != nil {
			errCh <- fmt.Errorf("insert events_by_study failed: %w", err)
		}
	}()

	// 3. Table: events_by_site
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := session.Query(insertBySite,
			r.SiteID, r.LocalSeq, r.IdempotencyKey, r.StudyID,
			r.EventTime, r.RecordedTime, r.IngestionTime,
			r.Severity, r.EventCode, r.Subject, r.Payload, r.IsLate,
		).WithContext(ctx).Exec()
		if err != nil {
			errCh <- fmt.Errorf("insert events_by_site failed: %w", err)
		}
	}()

	// 4. Table: known_studies (archive-tracking, §2.4 Slice 11) -- lets the
	// archival job discover which studies to scan without ALLOW FILTERING
	// or a secondary index, both already avoided elsewhere in this project.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := session.Query(`INSERT INTO pharos.known_studies (study_id) VALUES (?);`, r.StudyID).
			WithContext(ctx).Exec()
		if err != nil {
			errCh <- fmt.Errorf("insert known_studies failed: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// GetEvent performs a point lookup by idempotency key against canonical_events.
func (s *CassandraCanonicalStore) GetEvent(ctx context.Context, idempotencyKey string) (*CanonicalRecord, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		SELECT idempotency_key, site_id, study_id, local_seq,
		       event_time, recorded_time, ingestion_time,
		       severity, event_code, subject, payload,
		       kafka_topic, kafka_partition, kafka_offset,
		       consumed_at, is_late
		FROM pharos.canonical_events
		WHERE idempotency_key = ?;
	`

	var r CanonicalRecord
	err := session.Query(query, idempotencyKey).WithContext(ctx).Scan(
		&r.IdempotencyKey, &r.SiteID, &r.StudyID, &r.LocalSeq,
		&r.EventTime, &r.RecordedTime, &r.IngestionTime,
		&r.Severity, &r.EventCode, &r.Subject, &r.Payload,
		&r.KafkaTopic, &r.KafkaPartition, &r.KafkaOffset,
		&r.ConsumedAt, &r.IsLate,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// GetEventsByStudy executes a chronological event-time range scan for a clinical study (§2.4, §5).
func (s *CassandraCanonicalStore) GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*CanonicalRecord, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		SELECT study_id, event_time, idempotency_key, site_id, local_seq,
		       recorded_time, ingestion_time, severity, event_code,
		       subject, payload, is_late
		FROM pharos.events_by_study
		WHERE study_id = ? AND event_time >= ? AND event_time <= ?;
	`

	iter := session.Query(query, studyID, startTime, endTime).WithContext(ctx).Iter()
	var results []*CanonicalRecord

	var r CanonicalRecord
	for iter.Scan(
		&r.StudyID, &r.EventTime, &r.IdempotencyKey, &r.SiteID, &r.LocalSeq,
		&r.RecordedTime, &r.IngestionTime, &r.Severity, &r.EventCode,
		&r.Subject, &r.Payload, &r.IsLate,
	) {
		recordCopy := r
		results = append(results, &recordCopy)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}
	return results, nil
}

// GetEventsBySite executes a sequence-ordered scan for a site to audit continuous monotonic ordering (§2.4, §5).
func (s *CassandraCanonicalStore) GetEventsBySite(ctx context.Context, siteID string, minSeq int64) ([]*CanonicalRecord, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		SELECT site_id, local_seq, idempotency_key, study_id,
		       event_time, recorded_time, ingestion_time,
		       severity, event_code, subject, payload, is_late
		FROM pharos.events_by_site
		WHERE site_id = ? AND local_seq >= ?;
	`

	iter := session.Query(query, siteID, minSeq).WithContext(ctx).Iter()
	var results []*CanonicalRecord

	var r CanonicalRecord
	for iter.Scan(
		&r.SiteID, &r.LocalSeq, &r.IdempotencyKey, &r.StudyID,
		&r.EventTime, &r.RecordedTime, &r.IngestionTime,
		&r.Severity, &r.EventCode, &r.Subject, &r.Payload, &r.IsLate,
	) {
		recordCopy := r
		results = append(results, &recordCopy)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}
	return results, nil
}

// ListKnownStudies returns every study_id ever seen, from the archive-
// tracking table (§2.4, Slice 11) -- lets the archival job discover which
// studies to scan via GetEventsByStudy's already-efficient event_time range
// query, without a secondary index or ALLOW FILTERING across canonical_events.
func (s *CassandraCanonicalStore) ListKnownStudies(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	iter := session.Query(`SELECT study_id FROM pharos.known_studies;`).WithContext(ctx).Iter()
	var studies []string
	var studyID string
	for iter.Scan(&studyID) {
		studies = append(studies, studyID)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list known studies: %w", err)
	}
	return studies, nil
}

// DeleteArchivedEvent removes one record from all three canonical tables
// (§2.4, Slice 11) -- called only after that record has been durably
// exported to the cold tier and the export confirmed flushed to disk, never
// delete-then-write. All three tables mirror the same logical event, so all
// three deletes use exactly the key columns SaveEvent originally wrote.
func (s *CassandraCanonicalStore) DeleteArchivedEvent(ctx context.Context, r *CanonicalRecord) error {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := session.Query(`DELETE FROM pharos.canonical_events WHERE idempotency_key = ?;`, r.IdempotencyKey).WithContext(ctx).Exec(); err != nil {
			errCh <- fmt.Errorf("delete canonical_events failed: %w", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := session.Query(`DELETE FROM pharos.events_by_study WHERE study_id = ? AND event_time = ? AND idempotency_key = ?;`,
			r.StudyID, r.EventTime, r.IdempotencyKey).WithContext(ctx).Exec(); err != nil {
			errCh <- fmt.Errorf("delete events_by_study failed: %w", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := session.Query(`DELETE FROM pharos.events_by_site WHERE site_id = ? AND local_seq = ? AND idempotency_key = ?;`,
			r.SiteID, r.LocalSeq, r.IdempotencyKey).WithContext(ctx).Exec(); err != nil {
			errCh <- fmt.Errorf("delete events_by_site failed: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveWatermarkCheckpoint upserts the single checkpoint row for groupID
// (§2.4, Slice 13) -- Cassandra's native map<int, timestamp> column type is a
// direct fit for the tracker's own in-memory per-partition state, no
// serialization format to invent. Empty (but non-nil) maps are written as
// empty CQL maps, which is fine: a brand-new tracker with no partitions seen
// yet checkpoints to "nothing to restore," exactly matching reality.
func (s *CassandraCanonicalStore) SaveWatermarkCheckpoint(ctx context.Context, groupID string, cp WatermarkCheckpoint) error {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		INSERT INTO pharos.consumer_watermark_checkpoints (
			group_id, previous_emitted, partition_high_watermark, partition_last_activity
		) VALUES (?, ?, ?, ?);
	`
	if err := session.Query(query, groupID, cp.PreviousEmitted, cp.PartitionHighWatermark, cp.PartitionLastActivity).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to save watermark checkpoint for group %s: %w", groupID, err)
	}
	return nil
}

// LoadWatermarkCheckpoint returns the last saved checkpoint for groupID, or
// (nil, nil) if this consumer group has never checkpointed -- a genuinely
// fresh consumer group, which is not an error condition (§2.4, Slice 13).
func (s *CassandraCanonicalStore) LoadWatermarkCheckpoint(ctx context.Context, groupID string) (*WatermarkCheckpoint, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		SELECT previous_emitted, partition_high_watermark, partition_last_activity
		FROM pharos.consumer_watermark_checkpoints
		WHERE group_id = ?;
	`
	var cp WatermarkCheckpoint
	err := session.Query(query, groupID).WithContext(ctx).Scan(
		&cp.PreviousEmitted, &cp.PartitionHighWatermark, &cp.PartitionLastActivity,
	)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load watermark checkpoint for group %s: %w", groupID, err)
	}
	return &cp, nil
}

// Close closes the underlying Cassandra session.
func (s *CassandraCanonicalStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.session != nil {
		s.session.Close()
		s.closed = true
	}
	return nil
}

// MemoryCanonicalStore provides an in-memory implementation of CanonicalStore for fast unit testing.
type MemoryCanonicalStore struct {
	mu          sync.RWMutex
	byKey       map[string]*CanonicalRecord
	byStudy     map[string][]*CanonicalRecord
	bySite      map[string][]*CanonicalRecord
	checkpoints map[string]WatermarkCheckpoint
	saveHook    func(r *CanonicalRecord) error
	saveCalls   int
}

func NewMemoryCanonicalStore() *MemoryCanonicalStore {
	return &MemoryCanonicalStore{
		byKey:       make(map[string]*CanonicalRecord),
		byStudy:     make(map[string][]*CanonicalRecord),
		bySite:      make(map[string][]*CanonicalRecord),
		checkpoints: make(map[string]WatermarkCheckpoint),
	}
}

func (m *MemoryCanonicalStore) SetSaveHook(hook func(r *CanonicalRecord) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveHook = hook
}

func (m *MemoryCanonicalStore) SaveEvent(ctx context.Context, r *CanonicalRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.saveCalls++
	if m.saveHook != nil {
		if err := m.saveHook(r); err != nil {
			return err
		}
	}

	recCopy := *r
	m.byKey[r.IdempotencyKey] = &recCopy

	// Update byStudy (upsert / overwrite if exists)
	studyList := m.byStudy[r.StudyID]
	foundStudy := false
	for i, existing := range studyList {
		if existing.IdempotencyKey == r.IdempotencyKey {
			studyList[i] = &recCopy
			foundStudy = true
			break
		}
	}
	if !foundStudy {
		m.byStudy[r.StudyID] = append(studyList, &recCopy)
	}

	// Update bySite (upsert / overwrite if exists)
	siteList := m.bySite[r.SiteID]
	foundSite := false
	for i, existing := range siteList {
		if existing.IdempotencyKey == r.IdempotencyKey {
			siteList[i] = &recCopy
			foundSite = true
			break
		}
	}
	if !foundSite {
		m.bySite[r.SiteID] = append(siteList, &recCopy)
	}

	return nil
}

func (m *MemoryCanonicalStore) GetEvent(ctx context.Context, idempotencyKey string) (*CanonicalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	r, ok := m.byKey[idempotencyKey]
	if !ok {
		return nil, gocql.ErrNotFound
	}
	recCopy := *r
	return &recCopy, nil
}

func (m *MemoryCanonicalStore) GetEventsByStudy(ctx context.Context, studyID string, startTime, endTime time.Time) ([]*CanonicalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*CanonicalRecord
	for _, r := range m.byStudy[studyID] {
		if (r.EventTime.Equal(startTime) || r.EventTime.After(startTime)) &&
			(r.EventTime.Equal(endTime) || r.EventTime.Before(endTime)) {
			recCopy := *r
			results = append(results, &recCopy)
		}
	}
	return results, nil
}

func (m *MemoryCanonicalStore) GetEventsBySite(ctx context.Context, siteID string, minSeq int64) ([]*CanonicalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*CanonicalRecord
	for _, r := range m.bySite[siteID] {
		if r.LocalSeq >= minSeq {
			recCopy := *r
			results = append(results, &recCopy)
		}
	}
	return results, nil
}

func (m *MemoryCanonicalStore) SaveWatermarkCheckpoint(ctx context.Context, groupID string, cp WatermarkCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	high := make(map[int]time.Time, len(cp.PartitionHighWatermark))
	for p, t := range cp.PartitionHighWatermark {
		high[p] = t
	}
	activity := make(map[int]time.Time, len(cp.PartitionLastActivity))
	for p, t := range cp.PartitionLastActivity {
		activity[p] = t
	}
	m.checkpoints[groupID] = WatermarkCheckpoint{
		PreviousEmitted:        cp.PreviousEmitted,
		PartitionHighWatermark: high,
		PartitionLastActivity:  activity,
	}
	return nil
}

func (m *MemoryCanonicalStore) LoadWatermarkCheckpoint(ctx context.Context, groupID string) (*WatermarkCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cp, ok := m.checkpoints[groupID]
	if !ok {
		return nil, nil
	}
	high := make(map[int]time.Time, len(cp.PartitionHighWatermark))
	for p, t := range cp.PartitionHighWatermark {
		high[p] = t
	}
	activity := make(map[int]time.Time, len(cp.PartitionLastActivity))
	for p, t := range cp.PartitionLastActivity {
		activity[p] = t
	}
	return &WatermarkCheckpoint{
		PreviousEmitted:        cp.PreviousEmitted,
		PartitionHighWatermark: high,
		PartitionLastActivity:  activity,
	}, nil
}

func (m *MemoryCanonicalStore) EnsureSchema() error {
	return nil
}

func (m *MemoryCanonicalStore) Close() error {
	return nil
}

func (m *MemoryCanonicalStore) TotalSaved() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byKey)
}

func (m *MemoryCanonicalStore) SaveCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.saveCalls
}
