package archive

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
)

// Reader answers cold-tier lookups mirroring the shape of the query layer's
// existing hot-tier query patterns (§2.4, Slice 11) -- one method per query
// shape, not a generic "scan everything" API, so each caller only pays for
// the file I/O its specific query shape actually needs.
type Reader struct {
	cfg Config
}

func NewReader(cfg Config) *Reader {
	return &Reader{cfg: cfg}
}

// GetEventsByStudy returns archived canonical records for studyID whose
// event_time falls within [start, end] -- only the archive files whose month
// could overlap that range are read.
func (r *Reader) GetEventsByStudy(studyID string, start, end time.Time) ([]*consumer.CanonicalRecord, error) {
	var out []*consumer.CanonicalRecord
	err := ReadPartitionRange(r.cfg, KindByStudy, studyID, start, end, func(line []byte) error {
		var rec consumer.CanonicalRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("failed to decode archived canonical record: %w", err)
		}
		if !rec.EventTime.Before(start) && !rec.EventTime.After(end) {
			out = append(out, &rec)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetEventsBySite returns archived canonical records for siteID with
// local_seq >= minSeq. There's no time bound to narrow the file scan by
// (this query shape never had one, even against the hot tier), so every
// archived month for this site is read -- an accepted cost for a query
// against what is, by definition, no-longer-active data.
func (r *Reader) GetEventsBySite(siteID string, minSeq int64) ([]*consumer.CanonicalRecord, error) {
	// Canonical records are archived only under KindByStudy (one physical
	// copy, not duplicated per-site) -- this query shape has to scan across
	// every known study's archive to find this site's records, since the
	// archive's physical layout is driven by events_by_study's efficient
	// event_time clustering, which is what the archival job actually scans.
	// Accepted for a cold-tier query against no-longer-active data.
	var out []*consumer.CanonicalRecord
	studies, err := r.listArchivedStudies()
	if err != nil {
		return nil, err
	}
	for _, studyID := range studies {
		err := ReadPartition(r.cfg, KindByStudy, studyID, func(line []byte) error {
			var rec consumer.CanonicalRecord
			if err := json.Unmarshal(line, &rec); err != nil {
				return fmt.Errorf("failed to decode archived canonical record: %w", err)
			}
			if rec.SiteID == siteID && rec.LocalSeq >= minSeq {
				out = append(out, &rec)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// GetEvent performs a point lookup by idempotency_key against the archive.
// Since idempotency_key embeds site_id but the archive is physically laid
// out by study_id, this scans every known study's archive for a matching
// key -- slower than a hot-tier lookup, an accepted tradeoff for cold,
// already-inactive data (see ARCHITECTURE_PROPOSALS.md's Slice 11 entry for
// why no secondary index was added to avoid this).
func (r *Reader) GetEvent(idempotencyKey string) (*consumer.CanonicalRecord, error) {
	studies, err := r.listArchivedStudies()
	if err != nil {
		return nil, err
	}
	for _, studyID := range studies {
		var found *consumer.CanonicalRecord
		err := ReadPartition(r.cfg, KindByStudy, studyID, func(line []byte) error {
			if found != nil {
				return nil
			}
			var rec consumer.CanonicalRecord
			if err := json.Unmarshal(line, &rec); err != nil {
				return fmt.Errorf("failed to decode archived canonical record: %w", err)
			}
			if rec.IdempotencyKey == idempotencyKey {
				found = &rec
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if found != nil {
			return found, nil
		}
	}
	return nil, nil // not found in the archive either -- caller treats as a genuine miss
}

// GetDLQEventsBySite returns archived DLQ records for siteID, across every
// archived month for that site (DLQ site listing has no time bound either,
// same reasoning as GetEventsBySite above).
func (r *Reader) GetDLQEventsBySite(siteID string, limit int) ([]*dedup.DLQRecord, error) {
	var out []*dedup.DLQRecord
	err := ReadPartition(r.cfg, KindDLQBySite, siteID, func(line []byte) error {
		if limit > 0 && len(out) >= limit {
			return nil
		}
		var rec dedup.DLQRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("failed to decode archived DLQ record: %w", err)
		}
		out = append(out, &rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetDLQEvent performs a point lookup by idempotency_key against the DLQ
// archive. Unlike canonical records, DLQ archives are physically laid out
// by site_id, and idempotency_key already embeds site_id -- so this can go
// straight to the right partition, no cross-partition scan needed.
func (r *Reader) GetDLQEvent(idempotencyKey string) (*dedup.DLQRecord, error) {
	siteID := siteIDFromKey(idempotencyKey)
	if siteID == "" {
		return nil, nil
	}
	var found *dedup.DLQRecord
	err := ReadPartition(r.cfg, KindDLQBySite, siteID, func(line []byte) error {
		if found != nil {
			return nil
		}
		var rec dedup.DLQRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("failed to decode archived DLQ record: %w", err)
		}
		if rec.IdempotencyKey == idempotencyKey {
			found = &rec
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// listArchivedStudies lists study_id partitions that actually have archive
// data on disk, by reading the archive/by_study/ directory itself rather
// than depending on Cassandra's known_studies table -- the archive should be
// browsable even if the hot tier (and its tracking table) is unavailable.
func (r *Reader) listArchivedStudies() ([]string, error) {
	return listPartitionKeys(r.cfg, KindByStudy)
}

// siteIDFromKey extracts site_id from a "site_id:local_seq"-shaped
// idempotency key without depending on internal/model (avoiding a
// dependency this package doesn't otherwise need) -- mirrors
// model.ParseIdempotencyKey's own tail-split heuristic.
func siteIDFromKey(key string) string {
	idx := strings.LastIndex(key, ":")
	if idx <= 0 {
		return ""
	}
	return key[:idx]
}
