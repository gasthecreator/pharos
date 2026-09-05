package archive

import (
	"context"
	"fmt"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
)

// Result summarizes one archival job run.
type Result struct {
	CanonicalArchived int
	CanonicalFailed   int
	DLQArchived       int
	DLQFailed         int
}

// RunCanonical archives canonical records older than cutoff (§2.4, Slice 11):
// for every study_id in canonicalStore.ListKnownStudies, scans
// events_by_study's already-efficient event_time range query for rows older
// than cutoff, exports each to the by_study archive file for its month, and
// only deletes the row from all three canonical tables after that export is
// confirmed flushed to disk. Never delete-then-write: a failure between
// export and delete just means the row exists in both tiers until the next
// run, not a lost record.
//
// dryRun exports nothing and deletes nothing -- it only reports what would
// be archived, for operators to check before actually running it.
func RunCanonical(ctx context.Context, canonicalStore *consumer.CassandraCanonicalStore, cfg Config, cutoff time.Time, dryRun bool) (int, int, error) {
	studies, err := canonicalStore.ListKnownStudies(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list known studies: %w", err)
	}

	var archived, failed int
	// A wide-open lower bound: this project has no data before its own
	// existence, and "the beginning of time" as a lower bound is simpler and
	// just as correct as trying to track the true oldest possible event_time.
	epoch := time.Unix(0, 0).UTC()

	for _, studyID := range studies {
		records, err := canonicalStore.GetEventsByStudy(ctx, studyID, epoch, cutoff)
		if err != nil {
			return archived, failed, fmt.Errorf("failed to scan study %s for archival: %w", studyID, err)
		}
		for _, rec := range records {
			if dryRun {
				archived++
				continue
			}
			if err := AppendRecord(cfg, KindByStudy, rec.StudyID, rec.EventTime, rec); err != nil {
				failed++
				continue // don't delete a row whose export didn't succeed
			}
			if err := canonicalStore.DeleteArchivedEvent(ctx, rec); err != nil {
				// Exported but not yet deleted -- safe, non-corrupting state:
				// the row now exists in both tiers and will be picked up
				// again (re-exported harmlessly, gzip append is idempotent
				// at the record level since readers just see a duplicate
				// line) on the next run once whatever's failing is fixed.
				failed++
				continue
			}
			archived++
		}
	}
	return archived, failed, nil
}

// RunDLQ archives PUBLISHED DLQ records older than cutoff (§2.3, Slice 11),
// mirroring RunCanonical's structure exactly: scan by known_sites, export,
// then delete only after the export is confirmed durable.
func RunDLQ(ctx context.Context, outboxStore *dedup.CassandraOutboxStore, cfg Config, cutoff time.Time, dryRun bool) (int, int, error) {
	sites, err := outboxStore.ListKnownSites(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list known sites: %w", err)
	}

	var archived, failed int
	for _, siteID := range sites {
		records, err := outboxStore.ListDLQBySiteOlderThan(ctx, siteID, cutoff)
		if err != nil {
			return archived, failed, fmt.Errorf("failed to scan site %s DLQ for archival: %w", siteID, err)
		}
		for _, rec := range records {
			if dryRun {
				archived++
				continue
			}
			if err := AppendRecord(cfg, KindDLQBySite, rec.SiteID, rec.RejectedAt, rec); err != nil {
				failed++
				continue
			}
			if err := outboxStore.DeleteArchivedDLQRecord(ctx, rec); err != nil {
				failed++
				continue
			}
			archived++
		}
	}
	return archived, failed, nil
}
