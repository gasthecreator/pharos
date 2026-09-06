// Package wire holds the HTTP wire-format types shared between
// internal/edge (the forwarder) and internal/ingestion (Central Ingestion's
// intake handler) -- audit remediation, closing a gap flagged since Slice 6
// (§2.4, PLAN.md): internal/edge previously imported internal/ingestion
// directly just for these types, which meant every service's /metrics
// output carried every OTHER service's metric names too (zero-valued for
// whatever that binary doesn't itself touch), since Go pulls in a package's
// entire transitive dependency graph, side effects (package-level metric
// registration) included, not just the specific symbols referenced. This
// package has, and must keep having, no dependencies on any other internal
// package -- that's the whole point of the extraction, not an incidental
// property.
package wire

import "encoding/json"

// EventStatus constants for ingestion results.
const (
	StatusAccepted = "ACCEPTED"
	StatusRejected = "REJECTED"
	StatusFailed   = "FAILED" // Transient infrastructure failure (Cassandra outbox or Kafka publish error) requiring retry (§2.1, §2.2)
)

// EventResult represents the validation and ingestion result for a single adverse event.
type EventResult struct {
	IdempotencyKey string `json:"idempotency_key"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
}

// BatchRequest represents the wire format for submitting a batch of events to Central Ingestion.
type BatchRequest struct {
	SiteID string            `json:"site_id,omitempty"`
	Events []json.RawMessage `json:"events"`
}

// BatchResponse represents the structured response returned by Central Ingestion.
type BatchResponse struct {
	Total    int           `json:"total"`
	Accepted int           `json:"accepted"`
	Rejected int           `json:"rejected"`
	Failed   int           `json:"failed,omitempty"`
	Results  []EventResult `json:"results"`
	Error    string        `json:"error,omitempty"`
}
