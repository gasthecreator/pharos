package consumer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/tlsutil"
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
	// SaveLateArrivalAudit durably records one LateArrivalAudit entry (§2.4,
	// audit remediation) -- an idempotent upsert keyed by (groupID, WindowID,
	// IdempotencyKey), safe to call again with the identical entry on a
	// redelivered message (see Engine.Step's caller docs).
	SaveLateArrivalAudit(ctx context.Context, groupID string, audit LateArrivalAudit) error
	// ListLateArrivalAudits returns up to limit of groupID's most recently
	// recorded late-arrival audit entries.
	ListLateArrivalAudits(ctx context.Context, groupID string, limit int) ([]LateArrivalAudit, error)
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
	// RestrictToLocalDC, when true, scopes gocql's own host awareness to
	// LocalDC only (via gocql.DataCentreHostFilter), so it never attempts
	// background connections to the other DC's nodes at all -- default
	// false, since LOCAL_QUORUM never needs those connections anyway and
	// existing callers' behavior shouldn't change. This matters
	// specifically for a genuine cross-region failover (§2.4, PLAN.md
	// Slice 18: Backup & disaster recovery): reconfiguring LocalDC to the
	// surviving region during a real outage of the other one, without
	// this, still leaves gocql trying (and hanging, sometimes for
	// minutes) to reconnect to the now-genuinely-dead region's hosts as
	// part of its normal peer-awareness -- found by watching
	// TestCrossRegionFailover_DcEuServesLocalQuorumWhenDcUsUnreachable
	// hang on store.Close() waiting for exactly those stuck background
	// reconnect goroutines, not anticipated up front.
	RestrictToLocalDC bool
	// TLS mirrors dedup.CassandraConfig's field -- see that type's docs.
	TLS *tlsutil.ClientConfig
}

// DefaultCassandraStoreConfig returns defaults for the Pharos 3-node Cassandra cluster.
func DefaultCassandraStoreConfig() CassandraStoreConfig {
	var tlsCfg *tlsutil.ClientConfig
	if caCert := tlsutil.DefaultCACertPath(); caCert != "" {
		// Real Cassandra now requires TLS on its client port (§2.4, Slice
		// 15) -- see dedup.DefaultCassandraConfig's docs for why this is a
		// default, not just an opt-in.
		tlsCfg = &tlsutil.ClientConfig{CACertPath: caCert, ServerName: "localhost"}
	}
	return CassandraStoreConfig{
		Hosts:             []string{"127.0.0.1"},
		Port:              9042,
		Keyspace:          "pharos",
		Consistency:       gocql.LocalQuorum, // RF=3, LOCAL_QUORUM reads/writes (Slice 7, §2.4)
		ConnectTimeout:    10 * time.Second,
		ReplicationFactor: 3,
		LocalDC:           "dc-us",
		RemoteDCs:         map[string]int{"dc-eu": 1},
		TLS:               tlsCfg,
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

// newClusterConfig builds a fresh *gocql.ClusterConfig from cfg, optionally
// overriding the keyspace. A *new* ClusterConfig (and, critically, a *new*
// HostSelectionPolicy instance) is required for every CreateSession attempt,
// not just the first: gocql's TokenAwareHostPolicy.Init panics if the same
// policy instance is ever handed to a second session
// ("sharing token aware host selection policy between sessions is not
// supported") -- and a failed CreateSession call still runs Init before it
// fails for some other reason, so reusing one *gocql.ClusterConfig across a
// "try with keyspace, fall back without it" retry sequence panics on the
// second attempt. This bug was latent from the moment DC-aware
// TokenAwareHostPolicy was introduced (§2.4, Slice 14) -- gocql's default
// policy has no such single-use restriction, so nothing surfaced it until a
// connection attempt could actually fail and fall through to a retry.
func newClusterConfig(cfg CassandraStoreConfig, keyspace string) (*gocql.ClusterConfig, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency
	cluster.DisableInitialHostLookup = true // Required for Docker-on-Mac localhost port mapping
	cluster.Keyspace = keyspace
	if cfg.LocalDC != "" {
		// DC-aware host selection (§2.4, Slice 14) -- see dedup.CassandraConfig's
		// LocalDC docs for why this matters once a second DC genuinely exists.
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
		if cfg.RestrictToLocalDC {
			// WhiteListHostFilter (by connect address), not
			// DataCentreHostFilter (by DC name): gocql applies HostFilter
			// to the very first contact host before it has ever
			// connected, so the host's DataCenter() is still empty at
			// that point -- DataCentreHostFilter(cfg.LocalDC) rejects
			// even the host we explicitly gave it, since "" != cfg.LocalDC,
			// leaving zero connections. Matching by address sidesteps
			// that ordering problem entirely. Found by trying
			// DataCentreHostFilter first and getting "no connections were
			// made when creating the session."
			cluster.HostFilter = gocql.WhiteListHostFilter(cfg.Hosts...)
		}
	}
	if cfg.TLS != nil {
		sslOpts, err := cfg.TLS.GocqlSslOptions()
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		cluster.SslOpts = sslOpts
	}
	return cluster, nil
}

// NewCassandraCanonicalStore connects to Cassandra and bootstraps canonical schemas.
func NewCassandraCanonicalStore(cfg CassandraStoreConfig) (*CassandraCanonicalStore, error) {
	cluster, err := newClusterConfig(cfg, cfg.Keyspace)
	if err != nil {
		return nil, err
	}
	session, err := cluster.CreateSession()
	if err != nil {
		// Fallback: connect without keyspace to create keyspace if missing.
		// A fresh cluster config per attempt -- see newClusterConfig's docs.
		initCluster, cErr := newClusterConfig(cfg, "")
		if cErr != nil {
			return nil, cErr
		}
		initSession, initErr := initCluster.CreateSession()
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

		retryCluster, cErr := newClusterConfig(cfg, cfg.Keyspace)
		if cErr != nil {
			return nil, cErr
		}
		session, err = retryCluster.CreateSession()
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
		// events_recent (§2.4, Slice 21: web dashboard) answers "what's come
		// in recently, across every site" -- a query shape neither
		// canonical_events (keyed only by idempotency_key, no ordering) nor
		// events_by_study/events_by_site (both scoped to one partition key)
		// can answer without ALLOW FILTERING or a full-table scan, both
		// already avoided everywhere else in this schema. Bucketed by UTC
		// hour rather than one giant partition: an unbounded single
		// partition would grow forever and eventually blow past Cassandra's
		// per-partition size guidance for a busy pipeline; an hour bucket
		// bounds it and lets ListRecentEvents fan out over a small,
		// predictable number of recent partitions instead.
		`CREATE TABLE IF NOT EXISTS pharos.events_recent (
			hour_bucket text,
			consumed_at timestamp,
			idempotency_key text,
			site_id text,
			study_id text,
			local_seq bigint,
			event_time timestamp,
			severity text,
			event_code text,
			subject text,
			is_late boolean,
			PRIMARY KEY ((hour_bucket), consumed_at, idempotency_key)
		) WITH CLUSTERING ORDER BY (consumed_at DESC, idempotency_key ASC);`,
		`CREATE TABLE IF NOT EXISTS pharos.consumer_watermark_checkpoints (
			group_id text,
			previous_emitted timestamp,
			partition_high_watermark map<int, timestamp>,
			partition_last_activity map<int, timestamp>,
			PRIMARY KEY (group_id)
		);`,
		// consumer_late_arrival_audits (§2.4, audit remediation) durably
		// records LateArrivalAudit entries -- previously in-memory only in
		// WatermarkTracker, despite its stated 21 CFR Part 11 purpose.
		// Partitioned by group_id (mirroring consumer_watermark_checkpoints)
		// and clustered by (window_id, idempotency_key), the exact same
		// composite key WatermarkTracker already uses in-memory for
		// deduplication -- an INSERT here is a plain upsert on that key, so
		// retried persistence after a redelivered message is idempotent.
		`CREATE TABLE IF NOT EXISTS pharos.consumer_late_arrival_audits (
			group_id text,
			window_id text,
			idempotency_key text,
			partition int,
			event_time timestamp,
			arrived_at timestamp,
			watermark_at_arrival timestamp,
			PRIMARY KEY ((group_id), window_id, idempotency_key)
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

	const insertRecent = `
		INSERT INTO pharos.events_recent (
			hour_bucket, consumed_at, idempotency_key, site_id, study_id,
			local_seq, event_time, severity, event_code, subject, is_late
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	var wg sync.WaitGroup
	errCh := make(chan error, 5)

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

	// 4. Table: events_recent (§2.4, Slice 21) -- bucketed by the UTC hour
	// of ConsumedAt (when the pipeline actually processed it, not the
	// clinical EventTime, which can be backfilled/late and would scatter a
	// "what just happened" feed across arbitrary past buckets).
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := session.Query(insertRecent,
			hourBucket(r.ConsumedAt), r.ConsumedAt, r.IdempotencyKey, r.SiteID, r.StudyID,
			r.LocalSeq, r.EventTime, r.Severity, r.EventCode, r.Subject, r.IsLate,
		).WithContext(ctx).Exec()
		if err != nil {
			errCh <- fmt.Errorf("insert events_recent failed: %w", err)
		}
	}()

	// 5. Table: known_studies (archive-tracking, §2.4 Slice 11) -- lets the
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

// hourBucket formats t (which must already be UTC) as events_recent's
// partition key -- one partition per UTC hour, bounding partition size for
// a busy pipeline instead of one unbounded partition growing forever.
func hourBucket(t time.Time) string {
	return t.Format("2006-01-02-15")
}

// ListRecentEvents answers "what's come in recently, across every site" for
// the Slice 21 dashboard's live feed -- fans out over the last hoursBack
// hour-bucket partitions of events_recent (newest first), merges, and caps
// at limit, rather than a single unbounded partition or ALLOW FILTERING
// across canonical_events. Returned records are intentionally partial
// (idempotency key, site/study, sequence, timing, severity/code/subject) --
// events_recent doesn't duplicate the full FHIR payload, since a feed row
// links to GetEvent for full detail rather than needing it inline; Payload/
// RecordedTime/IngestionTime/Kafka* are left zero-valued on every result.
func (s *CassandraCanonicalStore) ListRecentEvents(ctx context.Context, limit int) ([]*CanonicalRecord, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}
	const hoursBack = 48

	const query = `
		SELECT hour_bucket, consumed_at, idempotency_key, site_id, study_id,
		       local_seq, event_time, severity, event_code, subject, is_late
		FROM pharos.events_recent
		WHERE hour_bucket = ?
		LIMIT ?;
	`

	var results []*CanonicalRecord
	now := time.Now().UTC()
	for h := 0; h < hoursBack && len(results) < limit; h++ {
		bucket := hourBucket(now.Add(-time.Duration(h) * time.Hour))
		iter := session.Query(query, bucket, limit-len(results)).WithContext(ctx).Iter()

		var r CanonicalRecord
		var bucketStr string
		for iter.Scan(
			&bucketStr, &r.ConsumedAt, &r.IdempotencyKey, &r.SiteID, &r.StudyID,
			&r.LocalSeq, &r.EventTime, &r.Severity, &r.EventCode, &r.Subject, &r.IsLate,
		) {
			recordCopy := r
			results = append(results, &recordCopy)
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("failed to scan events_recent bucket %s: %w", bucket, err)
		}
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
// SaveWatermarkCheckpoint merges this tracker's own view into the group's
// shared checkpoint row, rather than overwriting it outright (§2.4,
// PLAN.md Slice 19: Multi-instance scaling). With 2+ pharos-consumer
// instances sharing one Kafka consumer group (required for rebalancing to
// split partitions between them at all), each instance's own
// WatermarkTracker only ever observes the partitions Kafka assigned *to
// it* -- a plain overwriting INSERT/UPDATE keyed by group_id alone would
// mean whichever instance saves last replaces the *other* instance's
// partitions' data in partition_high_watermark/partition_last_activity
// with nothing, silently discarding it, and could regress
// previous_emitted to a value lower than what a healthier instance
// already reported externally. Found by reasoning through exactly this
// scenario before running it, not by hitting a failure first -- this
// class of bug (two writers, one shared row, "last write wins" clobbering
// data the other writer owns) doesn't reliably reproduce on a timing
// basis worth waiting for.
//
// Fixed with CQL's native map addition (`col + {...}`), which merges only
// the specific partition keys this instance has data for into the
// existing map, leaving whatever other instances have already written for
// *their* partitions untouched. previous_emitted is advanced via a plain
// read-then-max-then-write rather than a Paxos/LWT conditional update
// (tried first: `IF previous_emitted < ?` genuinely timed out under this
// cluster's real load -- "Operation timed out - received only 1
// responses" -- and this checkpoint is already a periodic, eventually-
// consistent crash-recovery snapshot, not a per-message consistency
// mechanism, so a plain compare-and-set is the right amount of rigor, not
// a shortcut). This leaves a narrow race (two instances' concurrent
// read-then-write could interleave such that a smaller value briefly
// wins) that's acceptable here for the same reason: the next periodic
// checkpoint corrects it, and nothing about §2.4's monotonic *external*
// watermark guarantee depends on this persisted value being exactly
// correct between saves, only close enough to prevent regressing a
// running instance's own live watermark after a crash.
func (s *CassandraCanonicalStore) SaveWatermarkCheckpoint(ctx context.Context, groupID string, cp WatermarkCheckpoint) error {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	// 1. Merge this instance's own partitions into the shared maps --
	// map addition overwrites only the keys present in the right-hand
	// side, never touching keys (partitions) it doesn't mention. Also
	// implicitly creates the row on first use (Cassandra doesn't
	// distinguish INSERT from UPDATE at the storage layer), so no
	// separate row-creation step is needed.
	mergeMaps := `
		UPDATE pharos.consumer_watermark_checkpoints
		SET partition_high_watermark = partition_high_watermark + ?,
		    partition_last_activity = partition_last_activity + ?
		WHERE group_id = ?;
	`
	if err := session.Query(mergeMaps, cp.PartitionHighWatermark, cp.PartitionLastActivity, groupID).
		WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to merge watermark checkpoint partitions for group %s: %w", groupID, err)
	}

	// 2. Advance previous_emitted only if this instance's view is
	// actually newer than what's already persisted -- read-then-max-then-
	// write, not a regression, since a smaller local view (an instance
	// owning only early partitions) must never overwrite a larger value
	// another instance (owning partitions further ahead) already
	// reported.
	existing, err := s.LoadWatermarkCheckpoint(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to read existing watermark checkpoint before advancing for group %s: %w", groupID, err)
	}
	newEmitted := cp.PreviousEmitted
	if existing != nil && existing.PreviousEmitted.After(newEmitted) {
		newEmitted = existing.PreviousEmitted
	}
	advanceEmitted := `
		UPDATE pharos.consumer_watermark_checkpoints
		SET previous_emitted = ?
		WHERE group_id = ?;
	`
	if err := session.Query(advanceEmitted, newEmitted, groupID).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to advance watermark checkpoint for group %s: %w", groupID, err)
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

// SaveLateArrivalAudit upserts one LateArrivalAudit entry (§2.4, audit
// remediation) -- a plain INSERT keyed by (group_id, window_id,
// idempotency_key), the same composite key WatermarkTracker's own in-memory
// dedup already enforces, so writing the identical entry again (a
// redelivered message retrying a previously failed persist) is a harmless
// no-op overwrite, never a duplicate row.
func (s *CassandraCanonicalStore) SaveLateArrivalAudit(ctx context.Context, groupID string, audit LateArrivalAudit) error {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	const query = `
		INSERT INTO pharos.consumer_late_arrival_audits (
			group_id, window_id, idempotency_key, partition,
			event_time, arrived_at, watermark_at_arrival
		) VALUES (?, ?, ?, ?, ?, ?, ?);
	`
	if err := session.Query(query,
		groupID, audit.WindowID, audit.IdempotencyKey, audit.Partition,
		audit.EventTime, audit.ArrivedAt, audit.WatermarkAtArrival,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to save late arrival audit for group %s (window %s, key %s): %w", groupID, audit.WindowID, audit.IdempotencyKey, err)
	}
	return nil
}

// ListLateArrivalAudits returns up to limit of groupID's recorded
// late-arrival audit entries (§2.4, audit remediation). limit <= 0 means no
// limit.
func (s *CassandraCanonicalStore) ListLateArrivalAudits(ctx context.Context, groupID string, limit int) ([]LateArrivalAudit, error) {
	s.mu.RLock()
	session := s.session
	s.mu.RUnlock()

	query := `
		SELECT window_id, idempotency_key, partition, event_time, arrived_at, watermark_at_arrival
		FROM pharos.consumer_late_arrival_audits
		WHERE group_id = ?
	`
	args := []interface{}{groupID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	iter := session.Query(query+";", args...).WithContext(ctx).Iter()

	var results []LateArrivalAudit
	var a LateArrivalAudit
	for iter.Scan(&a.WindowID, &a.IdempotencyKey, &a.Partition, &a.EventTime, &a.ArrivedAt, &a.WatermarkAtArrival) {
		results = append(results, a)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list late arrival audits for group %s: %w", groupID, err)
	}
	return results, nil
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
	recent      []*CanonicalRecord // mirrors events_recent (§2.4, Slice 21); sorted at read time, not write time
	checkpoints map[string]WatermarkCheckpoint
	lateAudits  map[string][]LateArrivalAudit // keyed by groupID, mirrors consumer_late_arrival_audits
	saveHook    func(r *CanonicalRecord) error
	saveCalls   int
}

func NewMemoryCanonicalStore() *MemoryCanonicalStore {
	return &MemoryCanonicalStore{
		byKey:       make(map[string]*CanonicalRecord),
		byStudy:     make(map[string][]*CanonicalRecord),
		bySite:      make(map[string][]*CanonicalRecord),
		checkpoints: make(map[string]WatermarkCheckpoint),
		lateAudits:  make(map[string][]LateArrivalAudit),
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

	foundRecent := false
	for i, existing := range m.recent {
		if existing.IdempotencyKey == r.IdempotencyKey {
			m.recent[i] = &recCopy
			foundRecent = true
			break
		}
	}
	if !foundRecent {
		m.recent = append(m.recent, &recCopy)
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

// ListRecentEvents mirrors CassandraCanonicalStore.ListRecentEvents's
// contract (newest ConsumedAt first, capped at limit) but -- since this
// store has no bucketed-partition equivalent to fan out over -- simply
// sorts every saved record at read time. Unlike the Cassandra
// implementation, records here are returned fully populated (this store
// keeps whole records anyway), not the partial summary shape
// events_recent stores; callers should treat the returned fields as a
// superset, never assume a field is populated only because this store
// happens to populate it.
func (m *MemoryCanonicalStore) ListRecentEvents(ctx context.Context, limit int) ([]*CanonicalRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}
	sorted := make([]*CanonicalRecord, len(m.recent))
	copy(sorted, m.recent)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ConsumedAt.After(sorted[j].ConsumedAt)
	})
	if len(sorted) > limit {
		sorted = sorted[:limit]
	}
	results := make([]*CanonicalRecord, len(sorted))
	for i, r := range sorted {
		recCopy := *r
		results[i] = &recCopy
	}
	return results, nil
}

// SaveWatermarkCheckpoint merges, mirroring CassandraCanonicalStore's own
// map-merge/monotonic-advance semantics (§2.4, PLAN.md Slice 19:
// Multi-instance scaling) -- kept consistent so a test exercising 2+
// simulated consumer instances against MemoryCanonicalStore observes the
// same behavior a real deployment would against Cassandra, rather than
// passing here on a naive overwrite that would silently lose data for
// real.
func (m *MemoryCanonicalStore) SaveWatermarkCheckpoint(ctx context.Context, groupID string, cp WatermarkCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.checkpoints[groupID]
	if !ok {
		existing = WatermarkCheckpoint{
			PartitionHighWatermark: map[int]time.Time{},
			PartitionLastActivity:  map[int]time.Time{},
		}
	}
	merged := WatermarkCheckpoint{
		PreviousEmitted:        existing.PreviousEmitted,
		PartitionHighWatermark: make(map[int]time.Time, len(existing.PartitionHighWatermark)+len(cp.PartitionHighWatermark)),
		PartitionLastActivity:  make(map[int]time.Time, len(existing.PartitionLastActivity)+len(cp.PartitionLastActivity)),
	}
	for p, t := range existing.PartitionHighWatermark {
		merged.PartitionHighWatermark[p] = t
	}
	for p, t := range cp.PartitionHighWatermark {
		merged.PartitionHighWatermark[p] = t
	}
	for p, t := range existing.PartitionLastActivity {
		merged.PartitionLastActivity[p] = t
	}
	for p, t := range cp.PartitionLastActivity {
		merged.PartitionLastActivity[p] = t
	}
	if cp.PreviousEmitted.After(merged.PreviousEmitted) {
		merged.PreviousEmitted = cp.PreviousEmitted
	}
	m.checkpoints[groupID] = merged
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

// SaveLateArrivalAudit mirrors CassandraCanonicalStore's own upsert-by-key
// semantics: overwrite the existing entry for (groupID, WindowID,
// IdempotencyKey) if one exists, append otherwise -- so a redelivered
// message retrying a persist is a no-op here too, not a duplicate.
func (m *MemoryCanonicalStore) SaveLateArrivalAudit(ctx context.Context, groupID string, audit LateArrivalAudit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries := m.lateAudits[groupID]
	for i, existing := range entries {
		if existing.WindowID == audit.WindowID && existing.IdempotencyKey == audit.IdempotencyKey {
			entries[i] = audit
			return nil
		}
	}
	m.lateAudits[groupID] = append(entries, audit)
	return nil
}

func (m *MemoryCanonicalStore) ListLateArrivalAudits(ctx context.Context, groupID string, limit int) ([]LateArrivalAudit, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := m.lateAudits[groupID]
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	result := make([]LateArrivalAudit, len(entries))
	copy(result, entries)
	return result, nil
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
