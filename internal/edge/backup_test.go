package edge

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSQLiteStore_BackupProducesRestorableSnapshot proves Backup writes a
// complete, valid SQLite file (via VACUUM INTO) that can itself be opened as
// a store and contains the data written before the backup was taken -- not
// just that a file appears at the target path (§2.1, Slice 12).
func TestSQLiteStore_BackupProducesRestorableSnapshot(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")
	backupPath := filepath.Join(dir, "backup", "snapshot.db")

	store, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	event := newTestEvent("SITE-BACKUP-01")
	if _, err := store.Enqueue(ctx, "SITE-BACKUP-01", event); err != nil {
		t.Fatalf("failed to enqueue event: %v", err)
	}

	if err := store.Backup(ctx, backupPath); err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("expected backup file to exist at %s: %v", backupPath, err)
	}
	if _, err := os.Stat(backupPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected no leftover .tmp file after a successful backup, stat returned: %v", err)
	}

	restored, err := NewSQLiteStore(backupPath)
	if err != nil {
		t.Fatalf("expected the backup file to be openable as a valid SQLite store: %v", err)
	}
	defer restored.Close()

	stats, err := restored.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats on restored backup failed: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected 1 pending record carried into the backup snapshot, got %d", stats.PendingCount)
	}
}

// TestSQLiteStore_BackupOverwritesPriorSnapshot proves a second Backup call to
// the same path succeeds and reflects new data, rather than failing because
// VACUUM INTO refuses to write to an already-existing target -- this is what
// the temp-file-then-rename approach in Backup exists to guarantee.
func TestSQLiteStore_BackupOverwritesPriorSnapshot(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")
	backupPath := filepath.Join(dir, "snapshot.db")

	store, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.Backup(ctx, backupPath); err != nil {
		t.Fatalf("first Backup failed: %v", err)
	}

	if _, err := store.Enqueue(ctx, "SITE-BACKUP-02", newTestEvent("SITE-BACKUP-02")); err != nil {
		t.Fatalf("failed to enqueue event: %v", err)
	}

	if err := store.Backup(ctx, backupPath); err != nil {
		t.Fatalf("second Backup (overwrite) failed: %v", err)
	}

	restored, err := NewSQLiteStore(backupPath)
	if err != nil {
		t.Fatalf("failed to open overwritten backup: %v", err)
	}
	defer restored.Close()

	stats, err := restored.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected the overwritten backup to reflect the record enqueued after the first backup, got PendingCount=%d", stats.PendingCount)
	}
}

// TestRestoreFromBackupIfMissing_RestoresWhenPrimaryAbsent is the core
// property Slice 12 exists to guarantee: if the primary database file is
// gone but a backup exists, RestoreFromBackupIfMissing must copy it into
// place before the store is ever opened, so data survives a lost disk.
func TestRestoreFromBackupIfMissing_RestoresWhenPrimaryAbsent(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")
	backupPath := filepath.Join(dir, "backup.db")

	seed, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to create seed store: %v", err)
	}
	ctx := context.Background()
	if _, err := seed.Enqueue(ctx, "SITE-RESTORE-01", newTestEvent("SITE-RESTORE-01")); err != nil {
		t.Fatalf("failed to enqueue event: %v", err)
	}
	if err := seed.Backup(ctx, backupPath); err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("failed to close seed store: %v", err)
	}

	// Simulate total loss of the primary disk: remove the primary db and its
	// WAL/SHM sidecar files entirely.
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		_ = os.Remove(primaryPath + suffix)
	}
	if _, err := os.Stat(primaryPath); !os.IsNotExist(err) {
		t.Fatalf("expected primary to be gone before restore, stat returned: %v", err)
	}

	if err := RestoreFromBackupIfMissing(primaryPath, backupPath); err != nil {
		t.Fatalf("RestoreFromBackupIfMissing failed: %v", err)
	}

	restored, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to open restored primary: %v", err)
	}
	defer restored.Close()

	stats, err := restored.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats on restored primary failed: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected the restored primary to carry the record from before disk loss, got PendingCount=%d", stats.PendingCount)
	}
}

// TestRestoreFromBackupIfMissing_NoOpWhenPrimaryExists proves the backup is
// never consulted -- let alone allowed to clobber anything -- when the
// primary database is already present, per the approved design ("if the
// primary does exist, the backup is irrelevant and ignored").
func TestRestoreFromBackupIfMissing_NoOpWhenPrimaryExists(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")
	backupPath := filepath.Join(dir, "backup.db")

	store, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	ctx := context.Background()
	if _, err := store.Enqueue(ctx, "SITE-PRIMARY-EXISTS", newTestEvent("SITE-PRIMARY-EXISTS")); err != nil {
		t.Fatalf("failed to enqueue event: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// A backup exists too, but for a store with ZERO records -- if restore
	// mistakenly fired here it would silently wipe out the primary's real data.
	emptySeed, err := NewSQLiteStore(filepath.Join(dir, "empty-seed.db"))
	if err != nil {
		t.Fatalf("failed to create empty seed store: %v", err)
	}
	if err := emptySeed.Backup(ctx, backupPath); err != nil {
		t.Fatalf("Backup of empty seed failed: %v", err)
	}
	if err := emptySeed.Close(); err != nil {
		t.Fatalf("failed to close empty seed store: %v", err)
	}

	if err := RestoreFromBackupIfMissing(primaryPath, backupPath); err != nil {
		t.Fatalf("RestoreFromBackupIfMissing failed: %v", err)
	}

	reopened, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("failed to reopen primary: %v", err)
	}
	defer reopened.Close()

	stats, err := reopened.GetStats(ctx)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.PendingCount != 1 {
		t.Errorf("expected the primary's own record to survive untouched (restore must be a no-op when primary exists), got PendingCount=%d", stats.PendingCount)
	}
}

// TestRestoreFromBackupIfMissing_NoOpWhenNeitherExists proves the genuinely-
// fresh-instance case (no primary, no backup) is left alone, as Slice 8's
// epoch-based key resilience is designed to handle that case safely on its own.
func TestRestoreFromBackupIfMissing_NoOpWhenNeitherExists(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")
	backupPath := filepath.Join(dir, "nonexistent-backup.db")

	if err := RestoreFromBackupIfMissing(primaryPath, backupPath); err != nil {
		t.Fatalf("expected no-op (no error) when neither primary nor backup exist, got: %v", err)
	}
	if _, err := os.Stat(primaryPath); !os.IsNotExist(err) {
		t.Fatalf("expected primary to remain absent, stat returned: %v", err)
	}

	store, err := NewSQLiteStore(primaryPath)
	if err != nil {
		t.Fatalf("expected a genuinely fresh store to still be creatable: %v", err)
	}
	defer store.Close()
}

// TestRestoreFromBackupIfMissing_EmptyBackupPathIsNoOp proves an unset
// --backup-path (empty string) is safe to pass through unconditionally.
func TestRestoreFromBackupIfMissing_EmptyBackupPathIsNoOp(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.db")

	if err := RestoreFromBackupIfMissing(primaryPath, ""); err != nil {
		t.Fatalf("expected empty backupPath to be a no-op, got: %v", err)
	}
}
