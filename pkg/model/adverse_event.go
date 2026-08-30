package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	ResourceTypeAdverseEvent   = "AdverseEvent"
	IdempotencyKeySystem       = "urn:pharos:idempotency-key"
	ActualityActual            = "actual"
	MedDRASystem               = "http://hl7.org/fhir/sid/meddra"
)

var (
	ErrInvalidResourceType = errors.New("resourceType must be 'AdverseEvent'")
	ErrInvalidActuality    = errors.New("actuality must be 'actual'")
	ErrMissingSubject      = errors.New("subject reference is required (e.g., 'Patient/<id>')")
	ErrMissingEvent        = errors.New("event coding or text is required")
	ErrMissingDate         = errors.New("date (observation event time) is required")
	ErrMissingRecordedDate = errors.New("recordedDate (site capture time) is required")
	ErrMissingStudy        = errors.New("study reference is required (e.g., 'ResearchStudy/<id>')")
	ErrMissingLocation     = errors.New("location reference is required (e.g., 'Location/<site_id>')")
	ErrMissingSeverity     = errors.New("severity coding is required ('mild', 'moderate', or 'severe')")
	ErrMissingIdempotency  = errors.New("identifier with system 'urn:pharos:idempotency-key' is required")
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
	Instance  CodeableConcept `json:"instance"`
	Causality *CodeableConcept `json:"causality,omitempty"`
}

// AdverseEvent represents the scoped FHIR R4 AdverseEvent resource profile (§2.3).
type AdverseEvent struct {
	ResourceType  string          `json:"resourceType"`
	ID            string          `json:"id,omitempty"`
	Identifier    []Identifier    `json:"identifier"`
	Actuality     string          `json:"actuality"`
	Category      []CodeableConcept `json:"category,omitempty"`
	Event         CodeableConcept `json:"event"`
	Subject       Reference       `json:"subject"`
	Date          time.Time       `json:"date"`
	RecordedDate  time.Time       `json:"recordedDate"`
	Seriousness   *CodeableConcept `json:"seriousness,omitempty"`
	Severity      CodeableConcept `json:"severity"`
	Outcome       *CodeableConcept `json:"outcome,omitempty"`
	Recorder      *Reference      `json:"recorder,omitempty"`
	Study         []Reference     `json:"study"`
	Location      Reference       `json:"location"`
	SuspectEntity []SuspectEntity `json:"suspectEntity,omitempty"`
}

// Validate checks that the AdverseEvent satisfies the scoped FHIR R4 profile constraints.
func (ae *AdverseEvent) Validate() error {
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
