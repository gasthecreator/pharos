package archive

import (
	"encoding/json"
	"testing"
	"time"
)

type testRecord struct {
	ID   string    `json:"id"`
	When time.Time `json:"when"`
}

func TestAppendRecord_RoundTrip(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	month := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)

	for i := 1; i <= 3; i++ {
		rec := testRecord{ID: string(rune('a' + i - 1)), When: month}
		if err := AppendRecord(cfg, KindByStudy, "STUDY-01", month, rec); err != nil {
			t.Fatalf("AppendRecord %d failed: %v", i, err)
		}
	}

	var got []testRecord
	err := ReadFile(cfg.pathFor(KindByStudy, "STUDY-01", month), func(line []byte) error {
		var r testRecord
		if err := json.Unmarshal(line, &r); err != nil {
			return err
		}
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 records (each AppendRecord call opens its own gzip stream -- concatenated streams must decode as one continuous stream), got %d", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Errorf("expected records in append order [a b c], got %+v", got)
	}
}

func TestReadFile_NonexistentIsNotAnError(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	calls := 0
	err := ReadFile(cfg.pathFor(KindByStudy, "NEVER-ARCHIVED", time.Now()), func(line []byte) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error for a nonexistent archive file, got: %v", err)
	}
	if calls != 0 {
		t.Errorf("expected handleFn never called, got %d calls", calls)
	}
}

func TestMonthsInRange(t *testing.T) {
	start := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)

	months := MonthsInRange(start, end)
	if len(months) != 3 {
		t.Fatalf("expected 3 months (Jan, Feb, Mar), got %d: %v", len(months), months)
	}
	for i, want := range []time.Month{time.January, time.February, time.March} {
		if months[i].Month() != want {
			t.Errorf("month %d: expected %s, got %s", i, want, months[i].Month())
		}
	}
}

func TestMonthsInRange_EndBeforeStart(t *testing.T) {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if months := MonthsInRange(start, end); months != nil {
		t.Errorf("expected nil for end before start, got %v", months)
	}
}

func TestListMonthsForPartition(t *testing.T) {
	cfg := Config{Dir: t.TempDir()}
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mar := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	if err := AppendRecord(cfg, KindByStudy, "STUDY-02", jan, testRecord{ID: "x"}); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}
	if err := AppendRecord(cfg, KindByStudy, "STUDY-02", mar, testRecord{ID: "y"}); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}

	months, err := ListMonthsForPartition(cfg, KindByStudy, "STUDY-02")
	if err != nil {
		t.Fatalf("ListMonthsForPartition failed: %v", err)
	}
	if len(months) != 2 {
		t.Fatalf("expected 2 months with archive data, got %d: %v", len(months), months)
	}

	// A month with no archive file must not appear, and a partition that was
	// never archived at all must return an empty (not error) result.
	months, err = ListMonthsForPartition(cfg, KindByStudy, "STUDY-NEVER-ARCHIVED")
	if err != nil {
		t.Fatalf("expected no error for an unarchived partition, got: %v", err)
	}
	if len(months) != 0 {
		t.Errorf("expected 0 months for an unarchived partition, got %d", len(months))
	}
}

func TestAppendRecord_PartitionKeyWithSlashIsSanitized(t *testing.T) {
	// site_id/study_id values are free-form strings in this project (they've
	// never been restricted to filesystem-safe characters) -- a value
	// containing a path separator must not escape the intended directory.
	cfg := Config{Dir: t.TempDir()}
	month := time.Now().UTC()
	if err := AppendRecord(cfg, KindDLQBySite, "SITE/WITH/SLASHES", month, testRecord{ID: "z"}); err != nil {
		t.Fatalf("AppendRecord failed: %v", err)
	}
	months, err := ListMonthsForPartition(cfg, KindDLQBySite, "SITE/WITH/SLASHES")
	if err != nil {
		t.Fatalf("ListMonthsForPartition failed: %v", err)
	}
	if len(months) != 1 {
		t.Fatalf("expected the sanitized partition key to round-trip to the same lookup, got %d months", len(months))
	}
}
