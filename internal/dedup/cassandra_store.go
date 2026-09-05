package dedup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CassandraConfig defines connection and Paxos consistency options (§2.2).
type CassandraConfig struct {
	Hosts             []string
	Port              int
	Keyspace          string
	Consistency       gocql.Consistency
	SerialConsistency gocql.SerialConsistency
	ConnectTimeout    time.Duration
	ReplicationFactor int
}

// DefaultCassandraConfig provides connection defaults for the Pharos 3-node Cassandra cluster.
func DefaultCassandraConfig() CassandraConfig {
	return CassandraConfig{
		Hosts:             []string{"127.0.0.1"},
		Port:              9042,
		Keyspace:          "pharos",
		Consistency:       gocql.LocalQuorum, // RF=3, LOCAL_QUORUM reads/writes (Slice 7, §2.2)
		SerialConsistency: gocql.LocalSerial, // Confines Paxos LWT rounds to local DC
		ConnectTimeout:    10 * time.Second,
		ReplicationFactor: 3,
	}
}

// CassandraOutboxStore implements OutboxStore using Apache Cassandra 5.0 with Paxos LWT.
type CassandraOutboxStore struct {
	session *gocql.Session
	cfg     CassandraConfig
	closed  bool
}

// NewCassandraOutboxStore creates and bootstraps a new CassandraOutboxStore.
func NewCassandraOutboxStore(cfg CassandraConfig) (*CassandraOutboxStore, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	cluster.Timeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency
	cluster.SerialConsistency = cfg.SerialConsistency
	cluster.DisableInitialHostLookup = true // Critical for Docker-on-Mac to route to 127.0.0.1:9042 directly

	// 1. Attempt direct connection to the target keyspace
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
			WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};
		`, cfg.Keyspace, cfg.ReplicationFactor)

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

	store := &CassandraOutboxStore{
		session: session,
		cfg:     cfg,
	}

	// 2. Ensure required schemas exist
	if err := store.EnsureSchema(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to bootstrap schemas: %w", err)
	}

	return store, nil
}

// EnsureSchema bootstraps event_outbox, dead_letter_events, and pending_outbox tables.
func (s *CassandraOutboxStore) EnsureSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS event_outbox (
			idempotency_key text,
			site_id text,
			local_seq bigint,
			payload text,
			status text,
			claimed_at timestamp,
			created_at timestamp,
			published_at timestamp,
			kafka_topic text,
			kafka_partition int,
			kafka_offset bigint,
			PRIMARY KEY (idempotency_key)
		);`,
		`CREATE TABLE IF NOT EXISTS dead_letter_events (
			idempotency_key text,
			site_id text,
			payload text,
			rejection_reason text,
			validation_errors text,
			rejected_at timestamp,
			status text,
			claimed_at timestamp,
			published_at timestamp,
			kafka_topic text,
			kafka_partition int,
			kafka_offset bigint,
			PRIMARY KEY (idempotency_key)
		);`,
		`CREATE TABLE IF NOT EXISTS pending_outbox (
			bucket text,
			idempotency_key text,
			created_at timestamp,
			PRIMARY KEY (bucket, idempotency_key)
		);`,
		`CREATE TABLE IF NOT EXISTS dead_letter_events_by_site (
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
		) WITH CLUSTERING ORDER BY (rejected_at DESC, idempotency_key ASC);`,
		`CREATE TABLE IF NOT EXISTS known_sites (
			site_id text,
			PRIMARY KEY (site_id)
		);`,
	}

	for _, q := range queries {
		if err := s.session.Query(q).Exec(); err != nil {
			return err
		}
	}

	// Migration for keyspaces that predate replayed_at (§2.3, Slice 10): the
	// CREATE TABLE IF NOT EXISTS statements above are a no-op against
	// already-existing tables, so an already-bootstrapped deployment needs
	// an explicit ALTER TABLE. Checked against system_schema.columns first
	// since ALTER TABLE ADD has no portable "if not exists" across Cassandra
	// versions — this keeps EnsureSchema safe to call on every startup,
	// matching the "bootstrapped automatically, no manual migration step"
	// guarantee this project makes everywhere else.
	for _, table := range []string{"dead_letter_events", "dead_letter_events_by_site"} {
		hasColumn, err := s.hasColumn(table, "replayed_at")
		if err != nil {
			return fmt.Errorf("failed to inspect %s schema: %w", table, err)
		}
		if !hasColumn {
			if err := s.session.Query(fmt.Sprintf(`ALTER TABLE %s ADD replayed_at timestamp;`, table)).Exec(); err != nil {
				return fmt.Errorf("failed to add replayed_at to %s: %w", table, err)
			}
		}
	}

	return nil
}

// hasColumn reports whether the named column already exists on the named
// table in this store's keyspace, so schema migrations can be applied
// idempotently across restarts without erroring on an already-migrated
// keyspace (mirrors the equivalent SQLite-side check in
// internal/edge/sqlite_store.go, added for the same reason in Slice 8).
func (s *CassandraOutboxStore) hasColumn(table, column string) (bool, error) {
	var count int
	err := s.session.Query(
		`SELECT COUNT(*) FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND column_name = ?;`,
		s.cfg.Keyspace, table, column,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func currentBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02-15") // hourly time bucket
}

// InsertClaim executes an atomic LWT insert on pharos.event_outbox with status='PUBLISHING'.
func (s *CassandraOutboxStore) InsertClaim(ctx context.Context, rec OutboxRecord, leaseTimeout time.Duration) (ClaimResult, error) {
	now := time.Now().UTC()
	bucket := currentBucket(now)

	query := `
		INSERT INTO event_outbox (
			idempotency_key, site_id, local_seq, payload, status, claimed_at, created_at
		) VALUES (?, ?, ?, ?, 'PUBLISHING', ?, ?)
		IF NOT EXISTS;
	`

	m := make(map[string]interface{})
	applied, err := s.session.Query(query,
		rec.IdempotencyKey,
		rec.SiteID,
		int64(rec.LocalSeq),
		string(rec.Payload),
		now,
		now,
	).WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(m)

	if err != nil {
		return ClaimResult{}, fmt.Errorf("cassandra outbox LWT insert failed: %w", err)
	}

	if applied {
		// Won LWT insert -> Acquired claim!
		_ = s.session.Query(
			`INSERT INTO pending_outbox (bucket, idempotency_key, created_at) VALUES (?, ?, ?);`,
			bucket, rec.IdempotencyKey, now,
		).WithContext(ctx).Exec()

		return ClaimResult{
			Acquired:  true,
			Status:    StatusPublishing,
			ClaimedAt: now,
		}, nil
	}

	// Key already exists: extract existing row state
	status, _ := m["status"].(string)
	claimedAt, _ := m["claimed_at"].(time.Time)
	siteID, _ := m["site_id"].(string)
	var localSeq int64
	if ls, ok := m["local_seq"].(int64); ok {
		localSeq = ls
	}
	payloadStr, _ := m["payload"].(string)

	if status == "" || claimedAt.IsZero() {
		_ = s.session.Query(
			`SELECT status, claimed_at, site_id, local_seq, payload FROM event_outbox WHERE idempotency_key = ?;`,
			rec.IdempotencyKey,
		).WithContext(ctx).Scan(&status, &claimedAt, &siteID, &localSeq, &payloadStr)
	}

	existingRec := &OutboxRecord{
		IdempotencyKey: rec.IdempotencyKey,
		SiteID:         siteID,
		LocalSeq:       uint64(localSeq),
		Payload:        []byte(payloadStr),
		Status:         OutboxStatus(status),
		ClaimedAt:      claimedAt,
	}

	if status == string(StatusPublished) {
		// Sub-case 2a: already published -> no-op
		return ClaimResult{
			Acquired:       false,
			Status:         StatusPublished,
			ClaimedAt:      claimedAt,
			ExistingRecord: existingRec,
		}, nil
	}

	// Sub-case 2b: in flight with active lease
	if now.Sub(claimedAt) < leaseTimeout {
		return ClaimResult{
			Acquired:       false,
			Status:         StatusPublishing,
			ClaimedAt:      claimedAt,
			ExistingRecord: existingRec,
		}, nil
	}

	// Sub-case 2c: in flight with expired lease -> CAS steal
	stealQuery := `
		UPDATE event_outbox
		SET status = 'PUBLISHING', claimed_at = ?
		WHERE idempotency_key = ?
		IF status = 'PUBLISHING' AND claimed_at = ?;
	`
	stealMap := make(map[string]interface{})
	casApplied, err := s.session.Query(stealQuery, now, rec.IdempotencyKey, claimedAt).
		WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(stealMap)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("failed to execute lease steal CAS: %w", err)
	}

	if casApplied {
		return ClaimResult{
			Acquired:       true,
			Status:         StatusPublishing,
			ClaimedAt:      now,
			ExistingRecord: existingRec,
		}, nil
	}

	return ClaimResult{
		Acquired:       false,
		Status:         StatusPublishing,
		ClaimedAt:      claimedAt,
		ExistingRecord: existingRec,
	}, nil
}

// MarkPublished updates status='PUBLISHED' with Kafka metadata.
func (s *CassandraOutboxStore) MarkPublished(ctx context.Context, idempotencyKey string, topic string, partition int, offset int64) error {
	now := time.Now().UTC()
	query := `
		UPDATE event_outbox
		SET status = 'PUBLISHED', published_at = ?, kafka_topic = ?, kafka_partition = ?, kafka_offset = ?
		WHERE idempotency_key = ?
		IF status = 'PUBLISHING';
	`
	markMap := make(map[string]interface{})
	_, err := s.session.Query(query, now, topic, partition, offset, idempotencyKey).
		WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(markMap)
	if err != nil {
		return fmt.Errorf("failed to mark outbox published: %w", err)
	}

	// Remove from pending_outbox index
	_ = s.session.Query(
		`DELETE FROM pending_outbox WHERE bucket = ? AND idempotency_key = ?;`,
		currentBucket(now), idempotencyKey,
	).WithContext(ctx).Exec()

	return nil
}

// InsertDLQClaim executes an atomic LWT insert on pharos.dead_letter_events with status='PUBLISHING'.
func (s *CassandraOutboxStore) InsertDLQClaim(ctx context.Context, rec DLQRecord, leaseTimeout time.Duration) (ClaimResult, error) {
	now := time.Now().UTC()
	bucket := currentBucket(now)

	query := `
		INSERT INTO dead_letter_events (
			idempotency_key, site_id, payload, rejection_reason, validation_errors,
			rejected_at, status, claimed_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PUBLISHING', ?)
		IF NOT EXISTS;
	`

	m := make(map[string]interface{})
	applied, err := s.session.Query(query,
		rec.IdempotencyKey,
		rec.SiteID,
		string(rec.Payload),
		rec.RejectionReason,
		rec.ValidationErrors,
		now,
		now,
	).WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(m)

	if err != nil {
		return ClaimResult{}, fmt.Errorf("cassandra DLQ outbox LWT insert failed: %w", err)
	}

	if applied {
		_ = s.session.Query(
			`INSERT INTO pending_outbox (bucket, idempotency_key, created_at) VALUES (?, ?, ?);`,
			bucket, rec.IdempotencyKey, now,
		).WithContext(ctx).Exec()

		_ = s.session.Query(
			`INSERT INTO dead_letter_events_by_site (
				site_id, rejected_at, idempotency_key, payload, rejection_reason,
				validation_errors, status, claimed_at
			) VALUES (?, ?, ?, ?, ?, ?, 'PUBLISHING', ?);`,
			rec.SiteID, now, rec.IdempotencyKey, string(rec.Payload),
			rec.RejectionReason, rec.ValidationErrors, now,
		).WithContext(ctx).Exec()

		// Archive-tracking (§2.3, Slice 11): lets the archival job discover
		// which sites to scan via dead_letter_events_by_site's already-
		// efficient rejected_at range query, mirroring known_studies on the
		// canonical side.
		_ = s.session.Query(
			`INSERT INTO known_sites (site_id) VALUES (?);`, rec.SiteID,
		).WithContext(ctx).Exec()

		return ClaimResult{
			Acquired:  true,
			Status:    StatusPublishing,
			ClaimedAt: now,
		}, nil
	}

	status, _ := m["status"].(string)
	claimedAt, _ := m["claimed_at"].(time.Time)
	if status == "" || claimedAt.IsZero() {
		_ = s.session.Query(
			`SELECT status, claimed_at FROM dead_letter_events WHERE idempotency_key = ?;`,
			rec.IdempotencyKey,
		).WithContext(ctx).Scan(&status, &claimedAt)
	}

	if status == string(StatusPublished) {
		return ClaimResult{
			Acquired:  false,
			Status:    StatusPublished,
			ClaimedAt: claimedAt,
		}, nil
	}

	if now.Sub(claimedAt) < leaseTimeout {
		return ClaimResult{
			Acquired:  false,
			Status:    StatusPublishing,
			ClaimedAt: claimedAt,
		}, nil
	}

	stealQuery := `
		UPDATE dead_letter_events
		SET status = 'PUBLISHING', claimed_at = ?
		WHERE idempotency_key = ?
		IF status = 'PUBLISHING' AND claimed_at = ?;
	`
	stealMap := make(map[string]interface{})
	casApplied, err := s.session.Query(stealQuery, now, rec.IdempotencyKey, claimedAt).
		WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(stealMap)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("failed to execute DLQ lease steal CAS: %w", err)
	}

	if casApplied {
		return ClaimResult{
			Acquired:  true,
			Status:    StatusPublishing,
			ClaimedAt: now,
		}, nil
	}

	return ClaimResult{
		Acquired:  false,
		Status:    StatusPublishing,
		ClaimedAt: claimedAt,
	}, nil
}

// MarkDLQPublished updates status='PUBLISHED' on pharos.dead_letter_events and dead_letter_events_by_site.
func (s *CassandraOutboxStore) MarkDLQPublished(ctx context.Context, idempotencyKey string, topic string, partition int, offset int64) error {
	now := time.Now().UTC()
	query := `
		UPDATE dead_letter_events
		SET status = 'PUBLISHED', published_at = ?, kafka_topic = ?, kafka_partition = ?, kafka_offset = ?
		WHERE idempotency_key = ?
		IF status = 'PUBLISHING';
	`
	markMap := make(map[string]interface{})
	_, err := s.session.Query(query, now, topic, partition, offset, idempotencyKey).
		WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(markMap)
	if err != nil {
		return fmt.Errorf("failed to mark DLQ outbox published: %w", err)
	}

	// Update dead_letter_events_by_site
	var siteID string
	var rejectedAt time.Time
	_ = s.session.Query(
		`SELECT site_id, rejected_at FROM dead_letter_events WHERE idempotency_key = ?;`,
		idempotencyKey,
	).WithContext(ctx).Scan(&siteID, &rejectedAt)

	if siteID != "" && !rejectedAt.IsZero() {
		_ = s.session.Query(`
			UPDATE dead_letter_events_by_site
			SET status = 'PUBLISHED', published_at = ?, kafka_topic = ?, kafka_partition = ?, kafka_offset = ?
			WHERE site_id = ? AND rejected_at = ? AND idempotency_key = ?;
		`, now, topic, partition, offset, siteID, rejectedAt, idempotencyKey).WithContext(ctx).Exec()
	}

	_ = s.session.Query(
		`DELETE FROM pending_outbox WHERE bucket = ? AND idempotency_key = ?;`,
		currentBucket(now), idempotencyKey,
	).WithContext(ctx).Exec()

	return nil
}

// MarkDLQReplayed transitions a DLQ record from PUBLISHED to REPLAYED (§2.3,
// Slice 10). The CAS precondition (IF status = 'PUBLISHED') guarantees a
// still-in-flight or already-replayed record can't be replayed a second time
// -- mirroring MarkDLQPublished's own CAS guard, not a new pattern.
func (s *CassandraOutboxStore) MarkDLQReplayed(ctx context.Context, idempotencyKey string) error {
	now := time.Now().UTC()
	query := `
		UPDATE dead_letter_events
		SET status = 'REPLAYED', replayed_at = ?
		WHERE idempotency_key = ?
		IF status = 'PUBLISHED';
	`
	markMap := make(map[string]interface{})
	applied, err := s.session.Query(query, now, idempotencyKey).
		WithContext(ctx).SerialConsistency(s.cfg.SerialConsistency).MapScanCAS(markMap)
	if err != nil {
		return fmt.Errorf("failed to mark DLQ record replayed: %w", err)
	}
	if !applied {
		return fmt.Errorf("cannot mark %s replayed: not in PUBLISHED status", idempotencyKey)
	}

	// Update dead_letter_events_by_site (best-effort, mirrors MarkDLQPublished)
	var siteID string
	var rejectedAt time.Time
	_ = s.session.Query(
		`SELECT site_id, rejected_at FROM dead_letter_events WHERE idempotency_key = ?;`,
		idempotencyKey,
	).WithContext(ctx).Scan(&siteID, &rejectedAt)

	if siteID != "" && !rejectedAt.IsZero() {
		_ = s.session.Query(`
			UPDATE dead_letter_events_by_site
			SET status = 'REPLAYED', replayed_at = ?
			WHERE site_id = ? AND rejected_at = ? AND idempotency_key = ?;
		`, now, siteID, rejectedAt, idempotencyKey).WithContext(ctx).Exec()
	}

	return nil
}

// FetchStaleClaims returns records with expired leases for sweeper reclamation.
func (s *CassandraOutboxStore) FetchStaleClaims(ctx context.Context, leaseTimeout time.Duration, limit int) ([]OutboxRecord, []DLQRecord, error) {
	now := time.Now().UTC()
	cutoff := now.Add(-leaseTimeout)
	bucket := currentBucket(now)

	iter := s.session.Query(
		`SELECT idempotency_key, created_at FROM pending_outbox WHERE bucket = ?;`,
		bucket,
	).WithContext(ctx).Iter()

	var staleOutbox []OutboxRecord
	var staleDLQ []DLQRecord

	var idKey string
	var createdAt time.Time
	for iter.Scan(&idKey, &createdAt) {
		if rec, err := s.GetOutboxRecord(ctx, idKey); err == nil {
			if rec.Status == StatusPublishing && rec.ClaimedAt.Before(cutoff) {
				staleOutbox = append(staleOutbox, *rec)
			}
		} else if dlq, err := s.GetDLQRecord(ctx, idKey); err == nil {
			if dlq.Status == StatusPublishing && dlq.ClaimedAt.Before(cutoff) {
				staleDLQ = append(staleDLQ, *dlq)
			}
		}
		if limit > 0 && (len(staleOutbox)+len(staleDLQ)) >= limit {
			break
		}
	}
	_ = iter.Close()

	return staleOutbox, staleDLQ, nil
}

// GetOutboxRecord reads a record by idempotency key.
func (s *CassandraOutboxStore) GetOutboxRecord(ctx context.Context, idempotencyKey string) (*OutboxRecord, error) {
	var status, siteID, payloadStr string
	var localSeq int64
	var claimedAt, createdAt, publishedAt time.Time
	var topic string
	var partition int
	var offset int64

	query := `
		SELECT site_id, local_seq, payload, status, claimed_at, created_at, published_at,
		       kafka_topic, kafka_partition, kafka_offset
		FROM event_outbox WHERE idempotency_key = ?;
	`
	err := s.session.Query(query, idempotencyKey).WithContext(ctx).
		Scan(&siteID, &localSeq, &payloadStr, &status, &claimedAt, &createdAt, &publishedAt, &topic, &partition, &offset)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	return &OutboxRecord{
		IdempotencyKey: idempotencyKey,
		SiteID:         siteID,
		LocalSeq:       uint64(localSeq),
		Payload:        []byte(payloadStr),
		Status:         OutboxStatus(status),
		ClaimedAt:      claimedAt,
		CreatedAt:      createdAt,
		PublishedAt:    publishedAt,
		KafkaTopic:     topic,
		KafkaPartition: partition,
		KafkaOffset:    offset,
	}, nil
}

// GetDLQRecord reads a dead-letter record by idempotency key.
func (s *CassandraOutboxStore) GetDLQRecord(ctx context.Context, idempotencyKey string) (*DLQRecord, error) {
	var status, siteID, payloadStr, reason, valErrors string
	var claimedAt, rejectedAt, publishedAt, replayedAt time.Time
	var topic string
	var partition int
	var offset int64

	query := `
		SELECT site_id, payload, rejection_reason, validation_errors, rejected_at, status,
		       claimed_at, published_at, kafka_topic, kafka_partition, kafka_offset, replayed_at
		FROM dead_letter_events WHERE idempotency_key = ?;
	`
	err := s.session.Query(query, idempotencyKey).WithContext(ctx).
		Scan(&siteID, &payloadStr, &reason, &valErrors, &rejectedAt, &status, &claimedAt, &publishedAt, &topic, &partition, &offset, &replayedAt)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ErrRecordNotFound
		}
		return nil, err
	}

	return &DLQRecord{
		IdempotencyKey:   idempotencyKey,
		SiteID:           siteID,
		Payload:          []byte(payloadStr),
		RejectionReason:  reason,
		ValidationErrors: valErrors,
		RejectedAt:       rejectedAt,
		Status:           OutboxStatus(status),
		ClaimedAt:        claimedAt,
		PublishedAt:      publishedAt,
		KafkaTopic:       topic,
		KafkaPartition:   partition,
		KafkaOffset:      offset,
		ReplayedAt:       replayedAt,
	}, nil
}

// Close terminates the Cassandra session.
// ListDLQBySiteOlderThan returns PUBLISHED DLQ records for siteID rejected
// before cutoff (§2.3, Slice 11) -- an efficient range query against
// dead_letter_events_by_site's existing rejected_at clustering key, driving
// the archival job's scan the same way GetEventsByStudy already drives the
// canonical side. Only PUBLISHED records are archival candidates: a
// still-PUBLISHING record's rejection isn't durably finished yet, and a
// REPLAYED record already has a live outbox claim elsewhere -- archiving a
// stale copy of either would be misleading.
func (s *CassandraOutboxStore) ListDLQBySiteOlderThan(ctx context.Context, siteID string, cutoff time.Time) ([]*DLQRecord, error) {
	query := `
		SELECT idempotency_key, rejected_at, payload, rejection_reason, validation_errors,
		       status, claimed_at, published_at, kafka_topic, kafka_partition, kafka_offset, replayed_at
		FROM dead_letter_events_by_site
		WHERE site_id = ? AND rejected_at < ?;
	`
	iter := s.session.Query(query, siteID, cutoff).WithContext(ctx).Iter()

	var records []*DLQRecord
	var idKey, payload, reason, valErrors, status, topic string
	var rejectedAt time.Time
	var claimedAt, publishedAt, replayedAt *time.Time
	var partition *int
	var offset *int64

	for iter.Scan(&idKey, &rejectedAt, &payload, &reason, &valErrors, &status, &claimedAt, &publishedAt, &topic, &partition, &offset, &replayedAt) {
		if OutboxStatus(status) != StatusPublished {
			continue
		}
		rec := &DLQRecord{
			IdempotencyKey:   idKey,
			SiteID:           siteID,
			RejectedAt:       rejectedAt,
			Payload:          []byte(payload),
			RejectionReason:  reason,
			ValidationErrors: valErrors,
			Status:           OutboxStatus(status),
			KafkaTopic:       topic,
		}
		if claimedAt != nil {
			rec.ClaimedAt = *claimedAt
		}
		if publishedAt != nil {
			rec.PublishedAt = *publishedAt
		}
		if partition != nil {
			rec.KafkaPartition = *partition
		}
		if offset != nil {
			rec.KafkaOffset = *offset
		}
		if replayedAt != nil {
			rec.ReplayedAt = *replayedAt
		}
		records = append(records, rec)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list DLQ records for %s older than %s: %w", siteID, cutoff, err)
	}
	return records, nil
}

// ListKnownSites returns every site_id ever seen in a DLQ rejection (§2.3,
// Slice 11) -- mirrors CassandraCanonicalStore.ListKnownStudies for the same
// reason: an efficient way for the archival job to discover which
// dead_letter_events_by_site partitions to scan.
func (s *CassandraOutboxStore) ListKnownSites(ctx context.Context) ([]string, error) {
	iter := s.session.Query(`SELECT site_id FROM known_sites;`).WithContext(ctx).Iter()
	var sites []string
	var siteID string
	for iter.Scan(&siteID) {
		sites = append(sites, siteID)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to list known sites: %w", err)
	}
	return sites, nil
}

// DeleteArchivedDLQRecord removes one record from both dead_letter_events and
// dead_letter_events_by_site (§2.3, Slice 11) -- called only after that
// record has been durably exported to the cold tier and the export confirmed
// flushed to disk, never delete-then-write.
func (s *CassandraOutboxStore) DeleteArchivedDLQRecord(ctx context.Context, rec *DLQRecord) error {
	if err := s.session.Query(
		`DELETE FROM dead_letter_events WHERE idempotency_key = ?;`, rec.IdempotencyKey,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("delete dead_letter_events failed: %w", err)
	}
	if err := s.session.Query(
		`DELETE FROM dead_letter_events_by_site WHERE site_id = ? AND rejected_at = ? AND idempotency_key = ?;`,
		rec.SiteID, rec.RejectedAt, rec.IdempotencyKey,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("delete dead_letter_events_by_site failed: %w", err)
	}
	return nil
}

func (s *CassandraOutboxStore) Close() error {
	if s.session != nil && !s.closed {
		s.closed = true
		s.session.Close()
	}
	return nil
}
