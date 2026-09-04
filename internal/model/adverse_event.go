package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ResourceTypeAdverseEvent = "AdverseEvent"
	IdempotencyKeySystem     = "urn:pharos:idempotency-key"
	ActualityActual          = "actual"
	MedDRASystem             = "http://hl7.org/fhir/sid/meddra"
)

// Wire-format schema versions (§2.3, Slice 9). SchemaVersionV1 is the profile
// this project has used since Slice 1; it's named explicitly, not left as an
// implicit "the only version," specifically so a future SchemaVersionV2 is a
// pure addition to schemaValidators rather than a redesign of this constant
// block. CurrentSchemaVersion is what the edge collector stamps on events it
// captures — see internal/edge/sqlite_store.go's Enqueue.
const (
	SchemaVersionV1      = 1
	CurrentSchemaVersion = SchemaVersionV1
)

var (
	ErrInvalidResourceType      = errors.New("resourceType must be 'AdverseEvent'")
	ErrInvalidActuality         = errors.New("actuality must be 'actual'")
	ErrMissingSubject           = errors.New("subject reference is required (e.g., 'Patient/<id>')")
	ErrMissingEvent             = errors.New("event coding or text is required")
	ErrMissingDate              = errors.New("date (observation event time) is required")
	ErrMissingRecordedDate      = errors.New("recordedDate (site capture time) is required")
	ErrMissingStudy             = errors.New("study reference is required (e.g., 'ResearchStudy/<id>')")
	ErrMissingLocation          = errors.New("location reference is required (e.g., 'Location/<site_id>')")
	ErrMissingSeverity          = errors.New("severity coding is required ('mild', 'moderate', or 'severe')")
	ErrMissingIdempotency       = errors.New("identifier with system 'urn:pharos:idempotency-key' is required")
	ErrUnsupportedSchemaVersion = errors.New("unsupported schemaVersion")
)

// Reference represents a standard FHIR Reference element.
type Reference struct {
	Reference string `json:"reference"`
	Display   string `json:"display,omitempty"`
}

// Coding represents a code defined by a terminology system.
type Coding struct {
	System  string `json:"system,omitempty"`
	Code    string `json:"code,omitempty"`
	Display string `json:"display,omitempty"`
}

// CodeableConcept represents a concept defined by codes or plain text.
type CodeableConcept struct {
	Coding []Coding `json:"coding,omitempty"`
	Text   string   `json:"text,omitempty"`
}

// Identifier represents a business identifier for the resource.
type Identifier struct {
	System string `json:"system"`
	Value  string `json:"value"`
}

// SuspectEntity represents an agent (drug/biological product) suspected of causing the adverse event.
type SuspectEntity struct {
	Instance  CodeableConcept  `json:"instance"`
	Causality *CodeableConcept `json:"causality,omitempty"`
}

// AdverseEvent represents the scoped FHIR R4 AdverseEvent resource profile (§2.3).
type AdverseEvent struct {
	ResourceType string `json:"resourceType"`
	ID           string `json:"id,omitempty"`
	// SchemaVersion is the wire-format version this event was captured under
	// (§2.3, Slice 9). Absent/zero is treated as SchemaVersionV1 for
	// backward compatibility with every event captured before this field
	// existed — nothing gets retroactively invalidated.
	SchemaVersion int               `json:"schemaVersion,omitempty"`
	Identifier    []Identifier      `json:"identifier"`
	Actuality     string            `json:"actuality"`
	Category      []CodeableConcept `json:"category,omitempty"`
	Event         CodeableConcept   `json:"event"`
	Subject       Reference         `json:"subject"`
	Date          time.Time         `json:"date"`
	RecordedDate  time.Time         `json:"recordedDate"`
	Seriousness   *CodeableConcept  `json:"seriousness,omitempty"`
	Severity      CodeableConcept   `json:"severity"`
	Outcome       *CodeableConcept  `json:"outcome,omitempty"`
	Recorder      *Reference        `json:"recorder,omitempty"`
	Study         []Reference       `json:"study"`
	Location      Reference         `json:"location"`
	SuspectEntity []SuspectEntity   `json:"suspectEntity,omitempty"`
}

// schemaValidators dispatches Validate() by wire-format version (§2.3, Slice
// 9). Adding a future SchemaVersionV2 means adding an entry here and a new
// validateV2 function — validateV1 and every existing test stays untouched.
var schemaValidators = map[int]func(*AdverseEvent) error{
	SchemaVersionV1: validateV1,
}

// Validate checks that the AdverseEvent satisfies its declared wire-format
// version's profile constraints, dispatching by SchemaVersion (§2.3, Slice 9).
// An absent/zero SchemaVersion is treated as SchemaVersionV1.
func (ae *AdverseEvent) Validate() error {
	version := ae.SchemaVersion
	if version == 0 {
		version = SchemaVersionV1
	}

	validator, ok := schemaValidators[version]
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnsupportedSchemaVersion, version)
	}
	return validator(ae)
}

// validateV1 is the FHIR R4 AdverseEvent profile this project has used since
// Slice 1 (§2.3).
func validateV1(ae *AdverseEvent) error {
	if ae.ResourceType != ResourceTypeAdverseEvent {
		return fmt.Errorf("%w: got %q", ErrInvalidResourceType, ae.ResourceType)
	}

	if ae.Actuality != ActualityActual {
		return fmt.Errorf("%w: got %q", ErrInvalidActuality, ae.Actuality)
	}

	if strings.TrimSpace(ae.Subject.Reference) == "" {
		return ErrMissingSubject
	}

	if len(ae.Event.Coding) == 0 && strings.TrimSpace(ae.Event.Text) == "" {
		return ErrMissingEvent
	}

	if ae.Date.IsZero() {
		return ErrMissingDate
	}

	if ae.RecordedDate.IsZero() {
		return ErrMissingRecordedDate
	}

	if len(ae.Study) == 0 || strings.TrimSpace(ae.Study[0].Reference) == "" {
		return ErrMissingStudy
	}

	if strings.TrimSpace(ae.Location.Reference) == "" {
		return ErrMissingLocation
	}

	if !isValidSeverity(ae.Severity) {
		return ErrMissingSeverity
	}

	if _, err := ae.GetIdempotencyKey(); err != nil {
		return err
	}

	return nil
}

func isValidSeverity(severity CodeableConcept) bool {
	validCodes := map[string]bool{
		"mild":     true,
		"moderate": true,
		"severe":   true,
	}

	for _, c := range severity.Coding {
		if validCodes[strings.ToLower(strings.TrimSpace(c.Code))] {
			return true
		}
	}
	return validCodes[strings.ToLower(strings.TrimSpace(severity.Text))]
}

// GetIdempotencyKey extracts and parses the IdempotencyKey from the resource's identifiers.
func (ae *AdverseEvent) GetIdempotencyKey() (IdempotencyKey, error) {
	for _, ident := range ae.Identifier {
		if ident.System == IdempotencyKeySystem {
			return ParseIdempotencyKey(ident.Value)
		}
	}
	return IdempotencyKey{}, ErrMissingIdempotency
}

// SetIdempotencyKey attaches or replaces the IdempotencyKey identifier in the resource.
func (ae *AdverseEvent) SetIdempotencyKey(key IdempotencyKey) {
	newIdent := Identifier{
		System: IdempotencyKeySystem,
		Value:  key.String(),
	}

	for i, ident := range ae.Identifier {
		if ident.System == IdempotencyKeySystem {
			ae.Identifier[i] = newIdent
			return
		}
	}

	ae.Identifier = append(ae.Identifier, newIdent)
}

// SiteID extracts the site ID from Location reference (e.g. "Location/SITE-NG-01" -> "SITE-NG-01").
func (ae *AdverseEvent) SiteID() string {
	ref := strings.TrimSpace(ae.Location.Reference)
	if strings.HasPrefix(ref, "Location/") {
		return strings.TrimPrefix(ref, "Location/")
	}
	return ref
}

// StudyID extracts the trial/study ID from Study[0] reference (e.g. "ResearchStudy/STUDY-001" -> "STUDY-001").
func (ae *AdverseEvent) StudyID() string {
	if len(ae.Study) == 0 {
		return "UNKNOWN_STUDY"
	}
	ref := strings.TrimSpace(ae.Study[0].Reference)
	if ref == "" {
		return "UNKNOWN_STUDY"
	}
	if strings.HasPrefix(ref, "ResearchStudy/") {
		return strings.TrimPrefix(ref, "ResearchStudy/")
	}
	parts := strings.Split(ref, "/")
	return parts[len(parts)-1]
}

// SeverityCode returns the normalized severity string ("mild", "moderate", "severe").
func (ae *AdverseEvent) SeverityCode() string {
	for _, c := range ae.Severity.Coding {
		if code := strings.TrimSpace(c.Code); code != "" {
			return strings.ToLower(code)
		}
	}
	if text := strings.TrimSpace(ae.Severity.Text); text != "" {
		return strings.ToLower(text)
	}
	return "unknown"
}

// EventCode returns the primary MedDRA or clinical event code/text.
func (ae *AdverseEvent) EventCode() string {
	for _, c := range ae.Event.Coding {
		if code := strings.TrimSpace(c.Code); code != "" {
			return code
		}
	}
	if text := strings.TrimSpace(ae.Event.Text); text != "" {
		return text
	}
	return "unknown"
}

// SubjectID extracts the subject ID from Subject reference (e.g. "Patient/PT-1234" -> "PT-1234").
func (ae *AdverseEvent) SubjectID() string {
	ref := strings.TrimSpace(ae.Subject.Reference)
	if strings.HasPrefix(ref, "Patient/") {
		return strings.TrimPrefix(ref, "Patient/")
	}
	return ref
}

// EventTimeUTC returns the observation event timestamp normalized to UTC (§2.4).
func (ae *AdverseEvent) EventTimeUTC() time.Time {
	return ae.Date.UTC()
}

// RecordedTimeUTC returns the capture timestamp normalized to UTC.
func (ae *AdverseEvent) RecordedTimeUTC() time.Time {
	return ae.RecordedDate.UTC()
}

// MarshalJSON returns standard JSON representation.
func (ae *AdverseEvent) MarshalJSON() ([]byte, error) {
	type Alias AdverseEvent
	return json.Marshal((*Alias)(ae))
}
