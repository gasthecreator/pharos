package edge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/model"
	_ "modernc.org/sqlite"
)

// SQLiteStore implements QueueStore using embedded SQLite with WAL mode (§2.1).
type SQLiteStore struct {
	db     *sql.DB
	dbPath string
	mu     sync.Mutex // serializes write transactions to prevent database lock contention
}

// NewSQLiteStore opens or creates an embedded SQLite store with WAL mode enabled.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db at %s: %w", dbPath, err)
	}

	// SQLite operates best when write transactions are serialized
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{
		db:     db,
		dbPath: dbPath,
	}

	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

func (s *SQLiteStore) initSchema() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA busy_timeout = 5000;

	CREATE TABLE IF NOT EXISTS site_sequence (
		site_id TEXT PRIMARY KEY,
		last_seq INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS queued_events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id TEXT NOT NULL,
		local_seq INTEGER NOT NULL,
		idempotency_key TEXT NOT NULL UNIQUE,
		payload BLOB NOT NULL,
		status TEXT NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		last_error TEXT,
		next_retry_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_queued_events_status_seq ON queued_events(status, local_seq);
	CREATE INDEX IF NOT EXISTS idx_queued_events_site_seq ON queued_events(site_id, local_seq);
	`
	_, err := s.db.Exec(query)
	return err
}

// Enqueue atomically allocates the next sequence number for the site, stamps the event
// with its client-side idempotency key, persists it in WAL, and commits.
func (s *SQLiteStore) Enqueue(ctx context.Context, siteID string, event *model.AdverseEvent) (*QueuedRecord, error) {
	if event == nil {
		return nil, fmt.Errorf("cannot enqueue nil adverse event")
	}

	siteID = strings.TrimSpace(siteID)
	if siteID == "" {
		return nil, model.ErrEmptySiteID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin enqueue transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 1. Allocate next sequence number monotonically
	_, err = tx.ExecContext(ctx, `
		INSERT INTO site_sequence (site_id, last_seq) VALUES (?, 1)
		ON CONFLICT(site_id) DO UPDATE SET last_seq = last_seq + 1;
	`, siteID)
	if err != nil {
		return nil, fmt.Errorf("failed to increment sequence: %w", err)
	}

	var nextSeq uint64
	err = tx.QueryRowContext(ctx, `SELECT last_seq FROM site_sequence WHERE site_id = ?`, siteID).Scan(&nextSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve allocated sequence: %w", err)
	}

	// 2. Form client-side idempotency key (§2.2)
	idempotencyKey, err := model.NewIdempotencyKey(siteID, nextSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to create idempotency key: %w", err)
	}

	// 3. Stamp idempotency key on event
	event.SetIdempotencyKey(idempotencyKey)

	// Ensure location matches siteID if not already set
	if event.Location.Reference == "" {
		event.Location = model.Reference{Reference: fmt.Sprintf("Location/%s", siteID)}
	}

	// Deliberately does NOT call event.Validate() here. Per PLAN.md §2.3, FHIR
	// schema validation is Central Ingestion's job, not the edge's: a site must
	// be able to durably buffer even malformed-looking data rather than lose it
	// locally, and a long-partitioned site may be running edge-binary validation
	// rules that have drifted from what Central Ingestion currently expects.
	// Rejecting here would silently and permanently drop the record with no DLQ
	// trail — exactly the failure mode this project exists to prevent.

	// 4. Serialize event payload
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event payload: %w", err)
	}

	now := time.Now().UTC()

	// 5. Insert record into local queue
	res, err := tx.ExecContext(ctx, `
		INSERT INTO queued_events (
			site_id, local_seq, idempotency_key, payload, status, attempts, next_retry_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
	`, siteID, nextSeq, idempotencyKey.String(), payloadBytes, StatusPending, now, now, now)
	if err != nil {
		return nil, fmt.Errorf("failed to insert queued event: %w", err)
	}

	recordID, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get last insert id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit enqueue transaction: %w", err)
	}

	return &QueuedRecord{
		ID:             recordID,
		SiteID:         siteID,
		LocalSeq:       nextSeq,
		IdempotencyKey: idempotencyKey.String(),
		Payload:        payloadBytes,
		Status:         StatusPending,
		Attempts:       0,
		NextRetryAt:    now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// FetchPending retrieves records ready for transmission, sorted by local_seq ascending.
func (s *SQLiteStore) FetchPending(ctx context.Context, batchSize int) ([]*QueuedRecord, error) {
	if batchSize <= 0 {
		batchSize = 50
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, site_id, local_seq, idempotency_key, payload, status, attempts, COALESCE(last_error, ''), next_retry_at, created_at, updated_at
		FROM queued_events
		WHERE status = ? OR (status = ? AND next_retry_at <= ?)
		ORDER BY local_seq ASC
		LIMIT ?
	`, StatusPending, StatusFailed, now, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending records: %w", err)
	}
	defer rows.Close()

	var records []*QueuedRecord
	for rows.Next() {
		var r QueuedRecord
		var statusStr string
		var nextRetry, created, updated time.Time

		err := rows.Scan(
			&r.ID,
			&r.SiteID,
			&r.LocalSeq,
			&r.IdempotencyKey,
			&r.Payload,
			&statusStr,
			&r.Attempts,
			&r.LastError,
			&nextRetry,
			&created,
			&updated,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan record: %w", err)
		}

		r.Status = RecordStatus(statusStr)
		r.NextRetryAt = nextRetry.UTC()
		r.CreatedAt = created.UTC()
		r.UpdatedAt = updated.UTC()
		records = append(records, &r)
	}

	return records, rows.Err()
}

// MarkInFlight transitions records from PENDING/FAILED to IN_FLIGHT.
func (s *SQLiteStore) MarkInFlight(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)
	args[0] = time.Now().UTC()

	for i, id := range ids {
		placeholders[i] = "?"
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		UPDATE queued_events
		SET status = '%s', updated_at = ?
		WHERE id IN (%s) AND status IN ('%s', '%s')
	`, StatusInFlight, strings.Join(placeholders, ","), StatusPending, StatusFailed)

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// MarkAcknowledged transitions records to ACKNOWLEDGED.
func (s *SQLiteStore) MarkAcknowledged(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)
	args[0] = time.Now().UTC()

	for i, id := range ids {
		placeholders[i] = "?"
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		UPDATE queued_events
		SET status = '%s', updated_at = ?
		WHERE id IN (%s)
	`, StatusAcknowledged, strings.Join(placeholders, ","))

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// MarkRejected transitions records to REJECTED when Central Ingestion permanently rejects them.
func (s *SQLiteStore) MarkRejected(ctx context.Context, ids []int64, errReason string) error {
	if len(ids) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+2)
	args[0] = errReason
	args[1] = time.Now().UTC()

	for i, id := range ids {
		placeholders[i] = "?"
		args[i+2] = id
	}

	query := fmt.Sprintf(`
		UPDATE queued_events
		SET status = '%s', last_error = ?, updated_at = ?
		WHERE id IN (%s)
	`, StatusRejected, strings.Join(placeholders, ","))

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// MarkFailed transitions a record to FAILED with exponential backoff timestamp and error message.
func (s *SQLiteStore) MarkFailed(ctx context.Context, id int64, errReason string, retryAfter time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	nextRetry := now.Add(retryAfter)

	query := `
		UPDATE queued_events
		SET status = ?, attempts = attempts + 1, last_error = ?, next_retry_at = ?, updated_at = ?
		WHERE id = ?
	`
	res, err := s.db.ExecContext(ctx, query, StatusFailed, errReason, nextRetry, now, id)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrRecordNotFound
	}
	return nil
}

// GetStats returns queue statistics for monitoring lag and sequence progression.
func (s *SQLiteStore) GetStats(ctx context.Context) (QueueStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var stats QueueStats

	query := `
		SELECT
			COALESCE(SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'IN_FLIGHT' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'ACKNOWLEDGED' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END), 0),
			COALESCE(MAX(local_seq), 0)
		FROM queued_events;
	`
	err := s.db.QueryRowContext(ctx, query).Scan(
		&stats.PendingCount,
		&stats.InFlightCount,
		&stats.AcknowledgedCount,
		&stats.FailedCount,
		&stats.RejectedCount,
		&stats.MaxSequence,
	)
	if err != nil {
		return stats, fmt.Errorf("failed to query queue statistics: %w", err)
	}

	var oldestPending sql.NullTime
	_ = s.db.QueryRowContext(ctx, `
		SELECT MIN(created_at) FROM queued_events WHERE status = 'PENDING'
	`).Scan(&oldestPending)
	if oldestPending.Valid {
		stats.OldestPendingTime = oldestPending.Time.UTC()
	}

	return stats, nil
}

// Close flushes WAL checkpoints and closes database connection.
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return nil
	}

	// Checkpoint WAL to flush all transactions to main database file
	_, _ = s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)
	return s.db.Close()
}
