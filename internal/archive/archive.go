// Package archive implements the cold storage tier for Pharos's data
// retention & lifecycle policy (§2.4, Slice 11): gzip-compressed JSON Lines
// files, partitioned by the exact same keys (study_id, site_id) the hot
// Cassandra tables already use, deliberately mirroring this project's own
// partition-key-first modeling principle instead of inventing a different
// physical layout for cold storage than the one already proven correct for
// hot storage.
//
// Archived data is never deleted -- only moved from Cassandra to these
// files. See ARCHITECTURE_PROPOSALS.md's Slice 11 entry for the full
// reasoning, including why event_outbox/pending_outbox are deliberately
// excluded (operational bookkeeping, not the data-of-record) and why there's
// no secondary index over the archive files (cold-tier point lookups are
// allowed to be slower; that's the whole point of them being cold).
package archive

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Kind identifies which partition-key scheme an archive file uses.
type Kind string

const (
	KindByStudy   Kind = "by_study"
	KindDLQBySite Kind = "dlq_by_site"
)

// Config controls where archive files live on local disk -- satisfying this
// project's zero-cloud-spend constraint the same way every other piece of
// infrastructure here does.
type Config struct {
	Dir string
}

// DefaultConfig points at ./archive relative to the current working
// directory. Production use should set an explicit, durable path.
func DefaultConfig() Config {
	return Config{Dir: "./archive"}
}

func (c Config) pathFor(kind Kind, partitionKey string, month time.Time) string {
	safeKey := strings.ReplaceAll(partitionKey, string(filepath.Separator), "_")
	return filepath.Join(c.Dir, string(kind), safeKey, month.UTC().Format("2006-01")+".jsonl.gz")
}

func (c Config) partitionDir(kind Kind, partitionKey string) string {
	safeKey := strings.ReplaceAll(partitionKey, string(filepath.Separator), "_")
	return filepath.Join(c.Dir, string(kind), safeKey)
}

// AppendRecord appends one JSON-serializable record to the archive file for
// (kind, partitionKey, month), creating directories/files as needed.
//
// Each call opens a fresh gzip stream at the end of the file. This is valid
// per RFC 1952 -- concatenated gzip streams decompress as one continuous
// byte stream, which is exactly what Go's gzip.Reader does by default
// (Multistream defaults to true) -- so appending never requires
// decompressing and recompressing the whole file, only true appends.
func AppendRecord(cfg Config, kind Kind, partitionKey string, month time.Time, record any) error {
	path := cfg.pathFor(kind, partitionKey, month)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("failed to create archive directory for %s: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open archive file %s: %w", path, err)
	}
	defer f.Close()

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record for archival: %w", err)
	}
	data = append(data, '\n')

	gz := gzip.NewWriter(f)
	if _, err := gz.Write(data); err != nil {
		_ = gz.Close()
		return fmt.Errorf("failed to write archive record to %s: %w", path, err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("failed to flush archive record to %s: %w", path, err)
	}
	// Ensure the write actually reached disk before the caller treats this
	// export as durable and deletes the hot-tier row -- never delete-then-write.
	return f.Sync()
}

// ReadFile decodes every JSON line in the archive file at path, calling
// handleFn for each. A nonexistent file (no archive data yet for this
// partition/month) is not an error -- handleFn is simply never called.
func ReadFile(path string, handleFn func(line []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open archive file %s: %w", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to open gzip reader for %s: %w", path, err)
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if err := handleFn(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// ReadPartition reads every archive file under (kind, partitionKey) across
// all months on disk -- used for queries with no time bound (a point lookup,
// or "all events for this site" with no date range).
func ReadPartition(cfg Config, kind Kind, partitionKey string, handleFn func(line []byte) error) error {
	months, err := ListMonthsForPartition(cfg, kind, partitionKey)
	if err != nil {
		return err
	}
	for _, month := range months {
		if err := ReadFile(cfg.pathFor(kind, partitionKey, month), handleFn); err != nil {
			return err
		}
	}
	return nil
}

// ReadPartitionRange reads only the archive files whose month could overlap
// [start, end] -- used for time-bounded queries (GetEventsByStudy) so a
// years-long archive doesn't get fully scanned for a one-week query.
func ReadPartitionRange(cfg Config, kind Kind, partitionKey string, start, end time.Time, handleFn func(line []byte) error) error {
	for _, month := range MonthsInRange(start, end) {
		if err := ReadFile(cfg.pathFor(kind, partitionKey, month), handleFn); err != nil {
			return err
		}
	}
	return nil
}

// MonthsInRange returns the first-of-month timestamp for every calendar
// month overlapping [start, end], inclusive.
func MonthsInRange(start, end time.Time) []time.Time {
	if end.Before(start) {
		return nil
	}
	start = time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	end = time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)

	var months []time.Time
	for m := start; !m.After(end); m = m.AddDate(0, 1, 0) {
		months = append(months, m)
	}
	return months
}

// listPartitionKeys lists every partition key (study_id or site_id) that has
// at least one archive file under the given kind, by reading the kind's
// directory rather than depending on Cassandra's tracking tables -- the
// archive should be browsable even if the hot tier is unavailable.
func listPartitionKeys(cfg Config, kind Kind) ([]string, error) {
	dir := filepath.Join(cfg.Dir, string(kind))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list archive kind %s: %w", dir, err)
	}
	var keys []string
	for _, e := range entries {
		if e.IsDir() {
			keys = append(keys, e.Name())
		}
	}
	return keys, nil
}

// ListMonthsForPartition lists which months actually have an archive file
// for (kind, partitionKey), by reading the partition's directory rather than
// assuming a range -- needed for queries with no time bound.
func ListMonthsForPartition(cfg Config, kind Kind, partitionKey string) ([]time.Time, error) {
	dir := cfg.partitionDir(kind, partitionKey)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list archive partition %s: %w", dir, err)
	}

	var months []time.Time
	for _, e := range entries {
		name := strings.TrimSuffix(e.Name(), ".jsonl.gz")
		if name == e.Name() {
			continue // not a .jsonl.gz file
		}
		m, err := time.Parse("2006-01", name)
		if err != nil {
			continue
		}
		months = append(months, m)
	}
	return months, nil
}
