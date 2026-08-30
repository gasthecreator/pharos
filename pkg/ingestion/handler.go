package ingestion

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gasthecreator/pharos/pkg/model"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

// EventStatus constants for ingestion results.
const (
	StatusAccepted = "ACCEPTED"
	StatusRejected = "REJECTED"
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
	Results  []EventResult `json:"results"`
	Error    string        `json:"error,omitempty"`
}

// Handler handles Central Ingestion HTTP intake (§2.3).
type Handler struct {
	rateLimiter ratelimit.RateLimiter

	// Observability counters for Slice 2
	acceptedEvents uint64
	rejectedEvents uint64
	throttledReqs  uint64
}

// NewHandler constructs an Ingestion HTTP handler with the provided RateLimiter.
func NewHandler(limiter ratelimit.RateLimiter) *Handler {
	if limiter == nil {
		limiter = ratelimit.NewTokenBucketLimiter(100, 10)
	}
	return &Handler{
		rateLimiter: limiter,
	}
}

// RegisterRoutes sets up HTTP routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/events", h.HandleEvents)
	mux.HandleFunc("/healthz", h.HandleHealth)
}

// HandleHealth returns simple health status.
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

// HandleEvents processes batch ingestion requests with per-site rate limiting and FHIR validation.
func (h *Handler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil || len(bodyBytes) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "empty or unreadable request body"})
		return
	}

	// 1. Parse batch request (support envelope or direct array of raw event messages)
	var rawEvents []json.RawMessage
	var siteID string

	// Attempt envelope parse: {"site_id": "...", "events": [...]}
	var envelope BatchRequest
	if err := json.Unmarshal(bodyBytes, &envelope); err == nil && len(envelope.Events) > 0 {
		rawEvents = envelope.Events
		siteID = strings.TrimSpace(envelope.SiteID)
	} else {
		// Attempt direct array parse: [{...}, {...}]
		var arrayEvents []json.RawMessage
		if err := json.Unmarshal(bodyBytes, &arrayEvents); err == nil && len(arrayEvents) > 0 {
			rawEvents = arrayEvents
		} else {
			// Also check if single event was sent: {...}
			var singleRaw json.RawMessage
			if err := json.Unmarshal(bodyBytes, &singleRaw); err == nil && len(singleRaw) > 0 && singleRaw[0] == '{' {
				rawEvents = []json.RawMessage{singleRaw}
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "malformed json: expected batch envelope or event array"})
				return
			}
		}
	}

	// 2. Resolve site_id (from header, envelope, or first event's location/idempotency key)
	if siteID == "" {
		siteID = strings.TrimSpace(r.Header.Get("X-Site-ID"))
	}
	if siteID == "" && len(rawEvents) > 0 {
		var firstEvent model.AdverseEvent
		if err := json.Unmarshal(rawEvents[0], &firstEvent); err == nil {
			siteID = firstEvent.SiteID()
			if siteID == "" {
				if key, err := firstEvent.GetIdempotencyKey(); err == nil {
					siteID = key.SiteID
				}
			}
		}
	}

	if siteID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing site_id: must be provided via X-Site-ID header, envelope, or event location"})
		return
	}

	// 3. Apply per-site rate limiting (§2.3)
	allowed, limitRes, err := h.rateLimiter.Allow(r.Context(), siteID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "rate limiter error"})
		return
	}

	// Set standard rate limit headers
	w.Header().Set("X-RateLimit-Limit", strconv.Itoa(limitRes.Limit))
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(limitRes.Remaining))

	if !allowed {
		atomic.AddUint64(&h.throttledReqs, 1)
		resetSecs := int(limitRes.ResetAfter.Seconds())
		if resetSecs < 1 {
			resetSecs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(resetSecs))
		w.Header().Set("X-RateLimit-Reset", strconv.Itoa(resetSecs))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":               "rate limit exceeded for site",
			"site_id":             siteID,
			"retry_after_seconds": resetSecs,
		})
		return
	}

	// 4. Validate each event against scoped FHIR profile (§2.3)
	results := make([]EventResult, len(rawEvents))
	acceptedCount := 0
	rejectedCount := 0

	for i, raw := range rawEvents {
		var ev model.AdverseEvent
		unmarshalErr := json.Unmarshal(raw, &ev)

		// Extract idempotency key
		keyStr := ""
		if unmarshalErr == nil {
			if key, keyErr := ev.GetIdempotencyKey(); keyErr == nil {
				keyStr = key.String()
			}
		} else {
			// Try partial extraction from identifier field
			var partial struct {
				Identifier []model.Identifier `json:"identifier"`
			}
			if err := json.Unmarshal(raw, &partial); err == nil {
				for _, ident := range partial.Identifier {
					if ident.System == model.IdempotencyKeySystem {
						keyStr = ident.Value
						break
					}
				}
			}
		}

		if unmarshalErr != nil {
			rejectedCount++
			results[i] = EventResult{
				IdempotencyKey: keyStr,
				Status:         StatusRejected,
				Error:          "malformed event payload: " + unmarshalErr.Error(),
			}
			continue
		}

		valErr := ev.Validate()
		if valErr != nil {
			rejectedCount++
			results[i] = EventResult{
				IdempotencyKey: keyStr,
				Status:         StatusRejected,
				Error:          valErr.Error(),
			}
		} else {
			acceptedCount++
			results[i] = EventResult{
				IdempotencyKey: keyStr,
				Status:         StatusAccepted,
			}
		}
	}

	atomic.AddUint64(&h.acceptedEvents, uint64(acceptedCount))
	atomic.AddUint64(&h.rejectedEvents, uint64(rejectedCount))

	resp := BatchResponse{
		Total:    len(rawEvents),
		Accepted: acceptedCount,
		Rejected: rejectedCount,
		Results:  results,
	}

	w.Header().Set("Content-Type", "application/json")

	// 5. Determine HTTP response code
	if acceptedCount == 0 && rejectedCount > 0 {
		// All events failed validation: return 422 Unprocessable Entity with structured rejection details
		resp.Error = "all events in batch failed FHIR validation"
		w.WriteHeader(http.StatusUnprocessableEntity)
	} else if acceptedCount > 0 && rejectedCount > 0 {
		// Partial validation failure: return 207 Multi-Status
		w.WriteHeader(http.StatusMultiStatus)
	} else {
		// All valid: return 200 OK
		w.WriteHeader(http.StatusOK)
	}

	_ = json.NewEncoder(w).Encode(resp)
}

// Stats returns the ingestion handler counters.
func (h *Handler) Stats() (accepted uint64, rejected uint64, throttled uint64) {
	return atomic.LoadUint64(&h.acceptedEvents),
		atomic.LoadUint64(&h.rejectedEvents),
		atomic.LoadUint64(&h.throttledReqs)
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
}
