package model

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Fuzz targets for the project's input-parsing surfaces. Run one at a time:
//
//	go test -fuzz=FuzzParseIdempotencyKey -fuzztime=30s ./internal/model/
//
// Seed corpora live in testdata/fuzz/<FuzzName>/ and run as ordinary tests
// under `go test` (so CI exercises them on every push).

func FuzzParseIdempotencyKey(f *testing.F) {
	for _, s := range []string{
		"SITE-1:1", "a:b:42", "", "::", ":1", "SITE:0", "SITE:-1", " SITE-1:7 ",
		"SITE:18446744073709551615", "SITE:18446744073709551616",
		"日本:5", "\x00:1", "SITE:" + strings.Repeat("9", 400), strings.Repeat("x", 100000) + ":1",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		k, err := ParseIdempotencyKey(raw)
		k2, err2 := ParseIdempotencyKey(raw)
		if k != k2 || (err == nil) != (err2 == nil) {
			t.Fatalf("not deterministic for %q", raw)
		}
		if err != nil {
			return
		}
		if k.LocalSeq == 0 || strings.TrimSpace(k.SiteID) == "" {
			t.Fatalf("accepted an invalid key from %q: %+v", raw, k)
		}
		// Round trip: re-encoding what was parsed must parse back to the same key.
		rk, err := ParseIdempotencyKey(k.String())
		if err != nil || rk != k {
			t.Fatalf("round trip failed for %q: %+v -> %q -> %+v, %v", raw, k, k.String(), rk, err)
		}
	})
}

func FuzzNewIdempotencyKey(f *testing.F) {
	f.Add("SITE-1", uint64(1))
	f.Add("  padded  ", uint64(7))
	f.Add("a:b", uint64(1<<63))
	f.Add("", uint64(1))
	f.Add("site", uint64(0))
	f.Add("日本", uint64(18446744073709551615))
	f.Fuzz(func(t *testing.T, site string, seq uint64) {
		k, err := NewIdempotencyKey(site, seq)
		if err != nil {
			if !errors.Is(err, ErrEmptySiteID) && !errors.Is(err, ErrZeroSequenceNumber) {
				t.Fatalf("unexpected error type: %v", err)
			}
			return
		}
		if k.SiteID != strings.TrimSpace(site) || k.LocalSeq != seq {
			t.Fatalf("constructor altered its inputs: %+v from (%q, %d)", k, site, seq)
		}
		// Whatever the encoder accepts, the parser must read back identically.
		rk, err := ParseIdempotencyKey(k.String())
		if err != nil || rk != k {
			t.Fatalf("encoder/parser disagree: %+v -> %q -> %+v, %v", k, k.String(), rk, err)
		}
	})
}

const seedEvent = `{"resourceType":"AdverseEvent","actuality":"actual",
 "identifier":[{"system":"urn:pharos:idempotency-key","value":"SITE-1:1"}],
 "subject":{"reference":"Patient/P-1"},
 "event":{"coding":[{"system":"http://hl7.org/fhir/sid/meddra","code":"10012345"}],"text":"Nausea"},
 "date":"2026-08-28T09:00:00+01:00","recordedDate":"2026-08-30T05:13:00Z",
 "severity":{"coding":[{"code":"severe"}]},
 "study":[{"reference":"ResearchStudy/STUDY-1"}],
 "location":{"reference":"Location/SITE-1"}}`

func FuzzAdverseEventValidate(f *testing.F) {
	f.Add([]byte(seedEvent))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"resourceType":"AdverseEvent","date":"0001-01-01T00:00:00Z"}`))
	f.Add([]byte(`{"resourceType":"AdverseEvent","schemaVersion":99}`))
	f.Add([]byte(`{"date":12345,"severity":"mild"}`))
	f.Add([]byte(strings.Replace(seedEvent, "Nausea", strings.Repeat("N", 1<<16), 1)))
	f.Add([]byte(strings.Replace(seedEvent, "SITE-1:1", "日本:\u0000:9", 1)))
	f.Fuzz(func(t *testing.T, data []byte) {
		var ev AdverseEvent
		if json.Unmarshal(data, &ev) != nil {
			return // not an event at all; rejected before validation ever runs
		}
		err1, err2 := ev.Validate(), ev.Validate()
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("Validate is not deterministic")
		}
		// Derived accessors are used on rejected events too (DLQ routing); none may panic.
		_ = ev.SiteID()
		_ = ev.StudyID()
		_ = ev.SeverityCode()
		_ = ev.EventCode()
		_ = ev.SubjectID()
		_ = ev.EventTimeUTC()
		_ = ev.RecordedTimeUTC()
		if err1 != nil {
			return
		}
		// Accepted: the validator must never accept what the encoder would reject.
		key, err := ev.GetIdempotencyKey()
		if err != nil {
			t.Fatalf("accepted an event without a usable idempotency key: %v", err)
		}
		if _, err := NewIdempotencyKey(key.SiteID, key.LocalSeq); err != nil {
			t.Fatalf("validator accepted a key the constructor rejects: %+v: %v", key, err)
		}
		out, err := json.Marshal(&ev)
		if err != nil {
			t.Fatalf("an accepted event cannot be re-encoded: %v", err)
		}
		var back AdverseEvent
		if err := json.Unmarshal(out, &back); err != nil {
			t.Fatalf("re-encoded event does not decode: %v", err)
		}
		if err := back.Validate(); err != nil {
			t.Fatalf("accepted event is rejected after a round trip: %v", err)
		}
	})
}

func FuzzSchemaVersionDispatch(f *testing.F) {
	for _, v := range []int{0, 1, 2, -1, 99, 1 << 30, -(1 << 30)} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, version int) {
		ev := AdverseEvent{SchemaVersion: version}
		err := ev.Validate() // an otherwise-empty event is always invalid; only the reason is under test
		supported := version == 0 || version == SchemaVersionV1
		if supported && errors.Is(err, ErrUnsupportedSchemaVersion) {
			t.Fatalf("supported version %d reported as unsupported", version)
		}
		if !supported && !errors.Is(err, ErrUnsupportedSchemaVersion) {
			t.Fatalf("unsupported version %d not reported as such: %v", version, err)
		}
	})
}
