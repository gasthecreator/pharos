package model

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func validAdverseEvent() *AdverseEvent {
	eventTime, _ := time.Parse(time.RFC3339, "2026-08-29T14:30:00+01:00")
	recordedTime, _ := time.Parse(time.RFC3339, "2026-08-29T14:35:00+01:00")

	return &AdverseEvent{
		ResourceType: ResourceTypeAdverseEvent,
		Identifier: []Identifier{
			{
				System: IdempotencyKeySystem,
				Value:  "SITE-NG-01:1001",
			},
		},
		Actuality: ActualityActual,
		Subject: Reference{
			Reference: "Patient/SUBJ-9481",
		},
		Event: CodeableConcept{
			Coding: []Coding{
				{
					System:  MedDRASystem,
					Code:    "10002198",
					Display: "Anaphylactic reaction",
				},
			},
			Text: "Anaphylaxis",
		},
		Date:         eventTime,
		RecordedDate: recordedTime,
		Severity: CodeableConcept{
			Coding: []Coding{
				{
					Code: "severe",
				},
			},
		},
		Study: []Reference{
			{
				Reference: "ResearchStudy/LILLY-401",
			},
		},
		Location: Reference{
			Reference: "Location/SITE-NG-01",
		},
		SuspectEntity: []SuspectEntity{
			{
				Instance: CodeableConcept{
					Text: "LY-3451238 (Investigational Compound)",
				},
			},
		},
	}
}

func TestAdverseEvent_Validate(t *testing.T) {
	t.Run("valid event", func(t *testing.T) {
		ae := validAdverseEvent()
		if err := ae.Validate(); err != nil {
			t.Fatalf("expected valid, got: %v", err)
		}
	})

	t.Run("invalid resourceType", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.ResourceType = "Observation"
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for invalid resourceType")
		}
	})

	t.Run("invalid actuality", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Actuality = "potential"
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for non-actual actuality")
		}
	})

	t.Run("missing subject", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Subject.Reference = ""
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing subject")
		}
	})

	t.Run("missing event", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Event = CodeableConcept{}
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing event")
		}
	})

	t.Run("missing date", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Date = time.Time{}
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing date")
		}
	})

	t.Run("missing recordedDate", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.RecordedDate = time.Time{}
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing recordedDate")
		}
	})

	t.Run("missing study", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Study = nil
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing study")
		}
	})

	t.Run("missing location", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Location.Reference = ""
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing location")
		}
	})

	t.Run("invalid severity", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Severity = CodeableConcept{Text: "catastrophic"}
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for invalid severity")
		}
	})

	t.Run("missing idempotency key", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.Identifier = nil
		if err := ae.Validate(); err == nil {
			t.Fatal("expected error for missing idempotency key")
		}
	})
}

// TestAdverseEvent_SchemaVersionDispatch locks in §2.3/Slice 9: an
// absent/zero SchemaVersion must validate exactly as it did before this
// field existed (nothing retroactively invalidated), an explicit
// SchemaVersionV1 must behave identically to the implicit case, and any
// version this binary doesn't recognize must be rejected with a specific,
// typed error rather than silently misparsed or accepted.
func TestAdverseEvent_SchemaVersionDispatch(t *testing.T) {
	t.Run("absent schemaVersion (backward compatibility)", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.SchemaVersion = 0
		if err := ae.Validate(); err != nil {
			t.Fatalf("expected an absent schemaVersion to validate as v1, got: %v", err)
		}
	})

	t.Run("explicit SchemaVersionV1", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.SchemaVersion = SchemaVersionV1
		if err := ae.Validate(); err != nil {
			t.Fatalf("expected explicit v1 to validate, got: %v", err)
		}
	})

	t.Run("unrecognized schemaVersion is rejected specifically", func(t *testing.T) {
		ae := validAdverseEvent()
		ae.SchemaVersion = 99
		err := ae.Validate()
		if err == nil {
			t.Fatal("expected an error for an unrecognized schemaVersion")
		}
		if !errors.Is(err, ErrUnsupportedSchemaVersion) {
			t.Fatalf("expected ErrUnsupportedSchemaVersion, got: %v", err)
		}
	})

	t.Run("unrecognized version does not fall through to a different check first", func(t *testing.T) {
		// An event that's ALSO missing other required fields should still
		// report the version problem, not a coincidentally-triggered
		// FHIR-shape error — version dispatch happens before profile
		// validation runs at all.
		ae := &AdverseEvent{SchemaVersion: 42}
		err := ae.Validate()
		if !errors.Is(err, ErrUnsupportedSchemaVersion) {
			t.Fatalf("expected ErrUnsupportedSchemaVersion even for an otherwise-empty event, got: %v", err)
		}
	})
}

func TestAdverseEvent_IdempotencyKeyHelpers(t *testing.T) {
	ae := validAdverseEvent()
	key, err := ae.GetIdempotencyKey()
	if err != nil {
		t.Fatalf("failed to get idempotency key: %v", err)
	}

	if key.SiteID != "SITE-NG-01" || key.LocalSeq != 1001 {
		t.Errorf("unexpected key: %+v", key)
	}

	newKey, _ := NewIdempotencyKey("SITE-NG-01", 1002)
	ae.SetIdempotencyKey(newKey)

	retrieved, err := ae.GetIdempotencyKey()
	if err != nil {
		t.Fatalf("failed to get updated key: %v", err)
	}

	if retrieved.LocalSeq != 1002 {
		t.Errorf("expected local seq 1002, got %d", retrieved.LocalSeq)
	}
}

func TestAdverseEvent_SiteID(t *testing.T) {
	ae := validAdverseEvent()
	if ae.SiteID() != "SITE-NG-01" {
		t.Errorf("expected SITE-NG-01, got %s", ae.SiteID())
	}

	ae.Location.Reference = "SITE-JP-02"
	if ae.SiteID() != "SITE-JP-02" {
		t.Errorf("expected SITE-JP-02, got %s", ae.SiteID())
	}
}

func TestAdverseEvent_TimeNormalization(t *testing.T) {
	ae := validAdverseEvent()
	utcTime := ae.EventTimeUTC()

	// 14:30:00 +01:00 should normalize to 13:30:00 UTC
	if utcTime.Hour() != 13 || utcTime.Minute() != 30 {
		t.Errorf("expected 13:30 UTC, got %v", utcTime)
	}
	if utcTime.Location() != time.UTC {
		t.Errorf("expected UTC location, got %v", utcTime.Location())
	}
}

func TestAdverseEvent_JSONRoundtrip(t *testing.T) {
	original := validAdverseEvent()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded AdverseEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded event failed validation: %v", err)
	}

	if decoded.SiteID() != original.SiteID() {
		t.Errorf("site ID mismatch: %s vs %s", decoded.SiteID(), original.SiteID())
	}
}
