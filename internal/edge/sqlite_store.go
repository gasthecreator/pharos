package edge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/model"
	_ "modernc.org/sqlite"
)

// projectEpochUnix anchors the instance-epoch component of local_seq (§2.2,
// ARCHITECTURE_PROPOSALS.md "Slice 8: Idempotency Key Resilience Across Edge
// Instance Loss") to 2026-01-01T00:00:00Z rather than the Unix epoch. Encoding
// minutes since *this* epoch in 31 bits gives ~4,083 years of headroom before
// wrapping; encoding raw Unix seconds in the same 31 bits would wrap in
// January 2038 (2^31 seconds after 1970) and overflow into the sign bit of
// Cassandra's signed 64-bit bigint columns that store local_seq downstream.
const projectEpochUnix = 1767225600 // 2026-01-01T00:00:00Z

// instanceEpochBits is the number of bits reserved for the instance-epoch
// component of a composite local_seq value; the remaining 32 bits are the
// per-instance monotonic counter. 31 + 32 = 63 bits, always leaving bit 63
// (the sign bit, as stored in Cassandra's signed bigint) at zero.
const instanceEpochMask = uint64(1)<<31 - 1

// currentInstanceEpochMinutes returns minutes since projectEpochUnix, clamped
// to [0, instanceEpochMask] so a misconfigured system clock (before the
// project epoch, or implausibly far in the future) can never produce a value
// that touches bit 63 once shifted into place.
func currentInstanceEpochMinutes() uint64 {
	deltaSeconds := time.Now().UTC().Unix() - projectEpochUnix
	if deltaSeconds < 0 {
		return 0
	}
	return uint64(deltaSeconds/60) & instanceEpochMask
}

// SQLiteStore implements QueueStore using embedded SQLite with WAL mode (§2.1).
type SQLiteStore struct {
	db               *sql.DB
	dbPath           string
	mu               sync.Mutex // serializes write transactions to prevent database lock contention
	epochMinutesFunc func() uint64
}

// NewSQLiteStore opens or creates an embedded SQLite store with WAL mode enabled.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	return newSQLiteStore(dbPath, currentInstanceEpochMinutes)
}

// RestoreFromBackupIfMissing copies backupPath over dbPath when dbPath does
// not yet exist and backupPath does (§2.1, ARCHITECTURE_PROPOSALS.md "Slice
// 12: Edge Collector Durability Hardening"). Callers must invoke this before
// NewSQLiteStore/NewSQLiteStoreWithEpochSource: opening a nonexistent path in
// WAL mode immediately creates an empty file, at which point it's too late to
// tell "genuinely new instance" apart from "primary lost, should restore."
//
// If dbPath already exists, or neither path exists, this is a no-op -- in the
// no-primary-no-backup case this is genuinely a fresh instance, which Slice
// 8's epoch-based key resilience already handles safely on its own. An empty
// backupPath also short-circuits to a no-op, so callers can pass through an
// unset flag unconditionally.
func RestoreFromBackupIfMissing(dbPath, backupPath string) error {
	if backupPath == "" {
		return nil
	}
	if _, err := os.Stat(dbPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat %s: %w", dbPath, err)
	}
	if _, err := os.Stat(backupPath); err != nil {
		return nil
	}

	src, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup %s for restore: %w", backupPath, err)
	}
	defer src.Close()

	if dir := filepath.Dir(dbPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", dbPath, err)
		}
	}

	dst, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create %s for restore: %w", dbPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to restore %s from backup %s: %w", dbPath, backupPath, err)
	}
	return dst.Sync()
}

// NewSQLiteStoreWithEpochSource is NewSQLiteStore with an injectable source for
// the instance-epoch component of local_seq (§2.2, Slice 8), instead of the
// real wall clock. Exists for tests that need deterministic control over
// instance_epoch — e.g. simulating two edge instances for the same site_id
// created far enough apart to land in different epochs, without an actual
// wall-clock sleep past a minute boundary. Production code should use
// NewSQLiteStore.
func NewSQLiteStoreWithEpochSource(dbPath string, epochMinutesFunc func() uint64) (*SQLiteStore, error) {
	return newSQLiteStore(dbPath, epochMinutesFunc)
}

func newSQLiteStore(dbPath string, epochMinutesFunc func() uint64) (*SQLiteStore, error) {
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db at %s: %w", dbPath, err)
	}

	// SQLite operates best when write transactions are serialized
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{
		db:               db,
		dbPath:           dbPath,
		epochMinutesFunc: epochMinutesFunc,
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
		last_seq INTEGER NOT NULL DEFAULT 0,
		instance_epoch INTEGER NOT NULL DEFAULT 0
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
	if _, err := s.db.Exec(query); err != nil {
		return err
	}

	// Migration for database files that predate the instance_epoch column
	// (§2.2, Slice 8): CREATE TABLE IF NOT EXISTS above is a no-op against an
	// already-existing site_sequence table, so a pre-existing file needs an
	// explicit ALTER TABLE. Existing rows default instance_epoch to 0, which
	// makes their composite local_seq numerically identical to their current
	// plain value — no renumbering, no discontinuity for already-running sites.
	hasColumn, err := s.hasColumn("site_sequence", "instance_epoch")
	if err != nil {
		return fmt.Errorf("failed to inspect site_sequence schema: %w", err)
	}
	if !hasColumn {
		if _, err := s.db.Exec(`ALTER TABLE site_sequence ADD COLUMN instance_epoch INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("failed to add instance_epoch column: %w", err)
		}
	}

	return nil
}

// hasColumn reports whether the named column already exists on the named
// table, so schema migrations can be applied idempotently across restarts
// without erroring on an already-migrated database file.
func (s *SQLiteStore) hasColumn(table, column string) (bool, error) {
	rows, err := s.db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			colType    string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
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

	// 1. Allocate next sequence number monotonically. instance_epoch is only
	// ever written on the INSERT branch below — the ON CONFLICT UPDATE clause
	// doesn't mention it, so it's left untouched on every call after the
	// first. That first-INSERT moment is exactly "this local database file
	// has never tracked this site_id before," which is the correct signal
	// for minting a fresh epoch: it's true both for a genuinely new site and
	// for a disk-replaced site's fresh file re-encountering its own site_id
	// for the first time (§2.2, Slice 8) — no separate detection needed.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO site_sequence (site_id, last_seq, instance_epoch) VALUES (?, 1, ?)
		ON CONFLICT(site_id) DO UPDATE SET last_seq = last_seq + 1;
	`, siteID, s.epochMinutesFunc())
	if err != nil {
		return nil, fmt.Errorf("failed to increment sequence: %w", err)
	}

	var counter, instanceEpoch uint64
	err = tx.QueryRowContext(ctx, `SELECT last_seq, instance_epoch FROM site_sequence WHERE site_id = ?`, siteID).Scan(&counter, &instanceEpoch)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve allocated sequence: %w", err)
	}

	// The composite value stamped on the outgoing idempotency key and stored
	// as queued_events.local_seq. Still strictly monotonic within this
	// instance's lifetime (instance_epoch is fixed once set; counter keeps
	// incrementing 1-by-1 exactly as before), while being numerically
	// disjoint from any prior instance's range for the same site_id.
	nextSeq := (instanceEpoch << 32) | counter

	// 2. Form client-side idempotency key (§2.2)
	idempotencyKey, err := model.NewIdempotencyKey(siteID, nextSeq)
	if err != nil {
		return nil, fmt.Errorf("failed to create idempotency key: %w", err)
	}

	// 3. Stamp idempotency key on event
	event.SetIdempotencyKey(idempotencyKey)

	// Stamp the wire-format version this binary captures under (§2.3, Slice
	// 9), only if the caller hasn't already set one — this must reflect what
	// *this* edge binary understood at capture time, not be silently
	// overwritten by whatever Central Ingestion's current default happens to
	// be, since different sites may run different edge binary versions.
	if event.SchemaVersion == 0 {
		event.SchemaVersion = model.CurrentSchemaVersion
	}

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

// Backup writes a complete, transactionally-consistent snapshot of the
// current database to path via SQLite's own VACUUM INTO (§2.1,
// ARCHITECTURE_PROPOSALS.md "Slice 12: Edge Collector Durability Hardening")
// -- safe to call while the database is being actively written to, unlike a
// raw copy of a live WAL-mode database file, which can capture an
// inconsistent mid-write state.
//
// VACUUM INTO refuses to run if its target file already exists, so this
// writes to a temporary path first and renames into place atomically -- both
// so a retried Backup call never trips over a leftover target from a prior
// attempt, and so RestoreFromBackupIfMissing can never observe a
// partially-written backup file.
func (s *SQLiteStore) Backup(ctx context.Context, path string) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create backup directory %s: %w", dir, err)
		}
	}

	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)

	s.mu.Lock()
	// VACUUM INTO's target path is a SQL string literal, not a bindable
	// parameter -- escape embedded single quotes defensively. path comes from
	// a trusted operator-supplied CLI flag (cmd/pharos-edge), never from
	// network input, but this keeps a stray quote from producing a confusing
	// SQL syntax error instead of a clean one.
	escaped := strings.ReplaceAll(tmpPath, "'", "''")
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s';", escaped))
	s.mu.Unlock()
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("VACUUM INTO failed: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to finalize backup at %s: %w", path, err)
	}
	return nil
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
