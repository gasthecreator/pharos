package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gasthecreator/pharos/internal/auth"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/metrics"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

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

// Handler handles Central Ingestion HTTP intake (§2.3).
type Handler struct {
	rateLimiter  ratelimit.RateLimiter
	outboxStore  dedup.OutboxStore
	producer     kafka.Producer
	leaseTimeout time.Duration
	keyStore     auth.KeyStore // nil disables per-site API key auth (§2.1, §2.2, Slice 15)
	keyLocks     sync.Map      // In-process per-key mutex map as optimization layer (§2.2)

	// Observability counters
	acceptedEvents uint64
	rejectedEvents uint64
	throttledReqs  uint64
	dedupHits      uint64
	dlqCount       uint64
}

// NewHandler constructs an Ingestion HTTP handler with the provided RateLimiter.
func NewHandler(limiter ratelimit.RateLimiter) *Handler {
	return NewHandlerWithOutbox(limiter, nil, nil, dedup.DefaultLeaseTimeout)
}

// NewHandlerWithOutbox constructs an Ingestion HTTP handler wired with OutboxStore and Kafka Producer (§2.2, §2.3).
func NewHandlerWithOutbox(limiter ratelimit.RateLimiter, outbox dedup.OutboxStore, producer kafka.Producer, leaseTimeout time.Duration) *Handler {
	if limiter == nil {
		limiter = ratelimit.NewTokenBucketLimiter(100, 10)
	}
	if leaseTimeout <= 0 {
		leaseTimeout = dedup.DefaultLeaseTimeout
	}
	return &Handler{
		rateLimiter:  limiter,
		outboxStore:  outbox,
		producer:     producer,
		leaseTimeout: leaseTimeout,
	}
}

// SetKeyStore enables per-site API key authentication (§2.1, §2.2, Slice
// 15) on the site-scoped routes (event submission, DLQ replay) the next
// time RegisterRoutes is called. Kept as a post-construction setter, not a
// constructor parameter, so every existing caller of NewHandler/
// NewHandlerWithOutbox (including this project's own tests) keeps working
// unchanged; auth is opt-in per deployment, not a breaking change to how
// the handler is built.
func (h *Handler) SetKeyStore(store auth.KeyStore) {
	h.keyStore = store
}

func (h *Handler) lockKey(key string) func() {
	if key == "" {
		return func() {}
	}
	val, _ := h.keyLocks.LoadOrStore(key, &sync.Mutex{})
	mtx := val.(*sync.Mutex)
	mtx.Lock()
	return func() {
		mtx.Unlock()
	}
}

// RegisterRoutes sets up HTTP routes on the given mux. If SetKeyStore was
// called, the site-scoped routes (event submission, DLQ replay) require a
// valid X-Site-ID/X-API-Key pair (§2.1, §2.2, Slice 15); /healthz never
// does, since health checks have no site to authenticate as.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	eventsHandler := http.Handler(http.HandlerFunc(h.HandleEvents))
	replayHandler := http.Handler(http.HandlerFunc(h.HandleDLQReplay))
	if h.keyStore != nil {
		requireAuth := auth.RequireAPIKey(h.keyStore)
		eventsHandler = requireAuth(eventsHandler)
		replayHandler = requireAuth(replayHandler)
	}
	mux.Handle("/api/v1/events", eventsHandler)
	mux.Handle("POST /api/v1/dlq/{key}/replay", replayHandler)
	mux.HandleFunc("/healthz", h.HandleHealth)
}

// HandleHealth returns simple health status.
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

// statusRecordingWriter captures the status code ultimately written so callers
// can record it as a metric label without threading a variable through every
// response branch below.
type statusRecordingWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusRecordingWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// eventOutcome classifies how processOneEvent resolved a single event, for the
// caller (HandleEvents's batch loop, or the DLQ replay handler, §2.3 Slice 10)
// to aggregate however it needs to.
type eventOutcome string

const (
	outcomeAccepted eventOutcome = "accepted"
	outcomeRejected eventOutcome = "rejected"
	outcomeFailed   eventOutcome = "failed"
)

// eventProcessResult is what processOneEvent returns for a single event.
type eventProcessResult struct {
	Result  EventResult
	Outcome eventOutcome
}

// processOneEvent runs one event through validation, dedup/outbox claim (or
// DLQ claim on rejection), and Kafka publish — the exact same path whether the
// event arrived in a normal batch (HandleEvents) or is being resubmitted via
// DLQ replay (§2.3, Slice 10). Extracted from HandleEvents's per-event loop
// with no behavior change: every branch below is identical to before the
// extraction, just returning an outcome instead of mutating loop-local
// counters and using `continue`.
func (h *Handler) processOneEvent(ctx context.Context, raw json.RawMessage, siteID string) eventProcessResult {
	var ev model.AdverseEvent
	unmarshalErr := json.Unmarshal(raw, &ev)

	// Extract idempotency key
	keyStr := ""
	if unmarshalErr == nil {
		if key, keyErr := ev.GetIdempotencyKey(); keyErr == nil {
			keyStr = key.String()
		}
	} else {
		// Try partial extraction from identifier field for malformed payloads
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
		metrics.ValidationFailuresTotal.Inc()
		errMsg := "malformed event payload: " + unmarshalErr.Error()
		result := EventResult{
			IdempotencyKey: keyStr,
			Status:         StatusRejected,
			Error:          errMsg,
		}
		outcome := outcomeRejected

		if h.outboxStore != nil && keyStr != "" {
			unlock := h.lockKey(keyStr)
			dlqRec := dedup.DLQRecord{
				IdempotencyKey:   keyStr,
				SiteID:           siteID,
				Payload:          raw, // Raw wire JSON bytes preserved (§2.3)
				RejectionReason:  "malformed JSON payload",
				ValidationErrors: unmarshalErr.Error(),
				RejectedAt:       time.Now().UTC(),
			}
			claim, err := h.outboxStore.InsertDLQClaim(ctx, dlqRec, h.leaseTimeout)
			if err != nil {
				unlock()
				return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
					IdempotencyKey: keyStr,
					Status:         StatusFailed,
					Error:          "dlq outbox storage error: " + err.Error(),
				}}
			}
			if claim.Acquired {
				if h.producer != nil {
					publishStart := time.Now()
					meta, pErr := h.producer.Publish(ctx, kafka.DLQTopic, []byte(siteID), raw, map[string]string{
						"idempotency_key":  keyStr,
						"site_id":          siteID,
						"rejection_reason": "malformed JSON payload",
					})
					metrics.OutboxPublishDuration.WithLabelValues(kafka.DLQTopic).Observe(time.Since(publishStart).Seconds())
					if pErr != nil {
						unlock()
						return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
							IdempotencyKey: keyStr,
							Status:         StatusFailed,
							Error:          "dlq kafka publish error: " + pErr.Error(),
						}}
					}
					_ = h.outboxStore.MarkDLQPublished(ctx, keyStr, meta.Topic, meta.Partition, meta.Offset)
				} else {
					_ = h.outboxStore.MarkDLQPublished(ctx, keyStr, kafka.DLQTopic, 0, 0)
				}
				atomic.AddUint64(&h.dlqCount, 1)
				metrics.DLQWritesTotal.Inc()
			}
			unlock()
		}
		return eventProcessResult{Result: result, Outcome: outcome}
	}

	valErr := ev.Validate()
	if valErr != nil {
		metrics.ValidationFailuresTotal.Inc()
		result := EventResult{
			IdempotencyKey: keyStr,
			Status:         StatusRejected,
			Error:          valErr.Error(),
		}
		outcome := outcomeRejected

		if h.outboxStore != nil && keyStr != "" {
			unlock := h.lockKey(keyStr)
			dlqRec := dedup.DLQRecord{
				IdempotencyKey:   keyStr,
				SiteID:           siteID,
				Payload:          raw, // Raw wire JSON bytes preserved (§2.3)
				RejectionReason:  valErr.Error(),
				ValidationErrors: valErr.Error(),
				RejectedAt:       time.Now().UTC(),
			}
			claim, err := h.outboxStore.InsertDLQClaim(ctx, dlqRec, h.leaseTimeout)
			if err != nil {
				unlock()
				return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
					IdempotencyKey: keyStr,
					Status:         StatusFailed,
					Error:          "dlq outbox storage error: " + err.Error(),
				}}
			}
			if claim.Acquired {
				if h.producer != nil {
					publishStart := time.Now()
					meta, pErr := h.producer.Publish(ctx, kafka.DLQTopic, []byte(siteID), raw, map[string]string{
						"idempotency_key":  keyStr,
						"site_id":          siteID,
						"rejection_reason": valErr.Error(),
					})
					metrics.OutboxPublishDuration.WithLabelValues(kafka.DLQTopic).Observe(time.Since(publishStart).Seconds())
					if pErr != nil {
						unlock()
						return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
							IdempotencyKey: keyStr,
							Status:         StatusFailed,
							Error:          "dlq kafka publish error: " + pErr.Error(),
						}}
					}
					_ = h.outboxStore.MarkDLQPublished(ctx, keyStr, meta.Topic, meta.Partition, meta.Offset)
				} else {
					_ = h.outboxStore.MarkDLQPublished(ctx, keyStr, kafka.DLQTopic, 0, 0)
				}
				atomic.AddUint64(&h.dlqCount, 1)
				metrics.DLQWritesTotal.Inc()
			}
			unlock()
		}
		return eventProcessResult{Result: result, Outcome: outcome}
	}

	// Accept Path (§2.2)
	if h.outboxStore != nil && keyStr != "" {
		unlock := h.lockKey(keyStr)
		outboxRec := dedup.OutboxRecord{
			IdempotencyKey: keyStr,
			SiteID:         siteID,
			Payload:        raw, // Raw wire JSON bytes preserved (§2.2)
		}
		if key, parseErr := model.ParseIdempotencyKey(keyStr); parseErr == nil {
			outboxRec.LocalSeq = key.LocalSeq
		}

		claim, err := h.outboxStore.InsertClaim(ctx, outboxRec, h.leaseTimeout)
		if err != nil {
			unlock()
			return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
				IdempotencyKey: keyStr,
				Status:         StatusFailed,
				Error:          "outbox storage error: " + err.Error(),
			}}
		}

		if claim.Acquired {
			metrics.DedupOutcomesTotal.WithLabelValues("new_claim").Inc()
			if h.producer != nil {
				publishStart := time.Now()
				meta, pErr := h.producer.Publish(ctx, kafka.MainTopic, []byte(siteID), raw, map[string]string{
					"idempotency_key": keyStr,
					"site_id":         siteID,
				})
				metrics.OutboxPublishDuration.WithLabelValues(kafka.MainTopic).Observe(time.Since(publishStart).Seconds())
				if pErr != nil {
					unlock()
					return eventProcessResult{Outcome: outcomeFailed, Result: EventResult{
						IdempotencyKey: keyStr,
						Status:         StatusFailed,
						Error:          "kafka publish error: " + pErr.Error(),
					}}
				}
				_ = h.outboxStore.MarkPublished(ctx, keyStr, meta.Topic, meta.Partition, meta.Offset)
			} else {
				_ = h.outboxStore.MarkPublished(ctx, keyStr, kafka.MainTopic, 0, 0)
			}
		} else {
			// Duplicate or concurrent in-flight
			if claim.Status == dedup.StatusPublished {
				atomic.AddUint64(&h.dedupHits, 1)
				metrics.DedupOutcomesTotal.WithLabelValues("duplicate_hit").Inc()
			}
		}
		unlock()
	}

	return eventProcessResult{Outcome: outcomeAccepted, Result: EventResult{
		IdempotencyKey: keyStr,
		Status:         StatusAccepted,
	}}
}

// HandleEvents processes batch ingestion requests with per-site rate limiting and FHIR validation.
func (h *Handler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &statusRecordingWriter{ResponseWriter: w, statusCode: http.StatusOK}
	w = rw
	defer func() {
		metrics.IngestionRequestsTotal.WithLabelValues(strconv.Itoa(rw.statusCode)).Inc()
		metrics.IngestionRequestDuration.Observe(time.Since(start).Seconds())
	}()

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

	// 2b. If RequireAPIKey ran, the caller has proven ownership of exactly
	// one site_id -- reject if the site_id resolved above (from envelope or
	// event payload, which the middleware never inspects) claims to be a
	// *different* site. Without this check, an authenticated site could
	// submit events on another site's behalf just by putting a different
	// site_id in the payload, defeating the whole point of authenticating
	// per site (§2.1, §2.2, ARCHITECTURE_PROPOSALS.md "Slice 15: Auth & TLS").
	if authenticatedSiteID, ok := auth.SiteIDFromContext(r.Context()); ok && authenticatedSiteID != siteID {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("authenticated as site %s, cannot submit events claiming site %s", authenticatedSiteID, siteID),
		})
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
		metrics.RateLimitRejectionsTotal.WithLabelValues(siteID).Inc()
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

	// 4. Validate and route each event against scoped FHIR profile (§2.2, §2.3)
	results := make([]EventResult, len(rawEvents))
	acceptedCount := 0
	rejectedCount := 0
	failedCount := 0

	for i, raw := range rawEvents {
		outcome := h.processOneEvent(r.Context(), raw, siteID)
		results[i] = outcome.Result
		switch outcome.Outcome {
		case outcomeAccepted:
			acceptedCount++
		case outcomeRejected:
			rejectedCount++
		case outcomeFailed:
			failedCount++
		}
	}

	atomic.AddUint64(&h.acceptedEvents, uint64(acceptedCount))
	atomic.AddUint64(&h.rejectedEvents, uint64(rejectedCount))

	resp := BatchResponse{
		Total:    len(rawEvents),
		Accepted: acceptedCount,
		Rejected: rejectedCount,
		Failed:   failedCount,
		Results:  results,
	}

	w.Header().Set("Content-Type", "application/json")

	// 5. Determine HTTP response code
	if failedCount > 0 && (acceptedCount > 0 || rejectedCount > 0) {
		// Mixed outcome with failures: some events were successfully processed (accepted or rejected),
		// while others hit infrastructure failures. Return 207 Multi-Status so the edge forwarder inspects
		// per-event results, acknowledging successes and retrying failed events (§2.1, §2.2).
		resp.Error = fmt.Sprintf("%d of %d events encountered infrastructure failures and require retry", failedCount, len(rawEvents))
		w.WriteHeader(http.StatusMultiStatus)
	} else if failedCount > 0 && acceptedCount == 0 && rejectedCount == 0 {
		// Every event in the batch hit an infrastructure failure with nothing else to report:
		// Return 503 Service Unavailable so edge forwarder retries the entire batch with backoff (§2.1).
		resp.Error = fmt.Sprintf("%d of %d events encountered infrastructure failures and require retry", failedCount, len(rawEvents))
		w.WriteHeader(http.StatusServiceUnavailable)
	} else if acceptedCount == 0 && rejectedCount > 0 {
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

// HandleDLQReplay resubmits a DLQ'd record's stored raw payload through the
// exact same validation/claim/publish path processOneEvent already uses for a
// fresh submission (§2.3, Slice 10) — not a special trust-it-now backdoor. On
// success, marks the original DLQ record REPLAYED (never deleted — the
// original rejection stays part of the audit trail). If it still fails
// validation, the DLQ record is left untouched and stays rejected with
// whatever reason this attempt produced: processOneEvent's InsertDLQClaim
// call no-ops against an already-PUBLISHED record (see
// CassandraOutboxStore.InsertDLQClaim's IF NOT EXISTS + status check), so a
// failed replay attempt can't corrupt or duplicate the original entry.
func (h *Handler) HandleDLQReplay(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing idempotency key in path"})
		return
	}
	if h.outboxStore == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "outbox store not configured"})
		return
	}

	rec, err := h.outboxStore.GetDLQRecord(r.Context(), key)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "dlq record not found: " + err.Error()})
		return
	}
	if rec.Status != dedup.StatusPublished {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("cannot replay %s: record status is %s, expected PUBLISHED", key, rec.Status),
		})
		return
	}

	// Only the owning site may replay its own rejected event (§2.1, §2.2,
	// ARCHITECTURE_PROPOSALS.md "Slice 15: Auth & TLS") -- without this, any
	// authenticated site could replay any other site's DLQ record.
	if authenticatedSiteID, ok := auth.SiteIDFromContext(r.Context()); ok && authenticatedSiteID != rec.SiteID {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("authenticated as site %s, cannot replay a DLQ record owned by site %s", authenticatedSiteID, rec.SiteID),
		})
		return
	}

	outcome := h.processOneEvent(r.Context(), rec.Payload, rec.SiteID)

	w.Header().Set("Content-Type", "application/json")
	switch outcome.Outcome {
	case outcomeAccepted:
		if markErr := h.outboxStore.MarkDLQReplayed(r.Context(), key); markErr != nil {
			// The event WAS genuinely accepted and published to Kafka -- this
			// only failed to flag the *original* DLQ record. A bookkeeping
			// gap, not a data-loss one, but surfaced clearly rather than
			// silently swallowed.
			w.WriteHeader(http.StatusMultiStatus)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"result":  outcome.Result,
				"warning": "replay succeeded but failed to mark the original DLQ record replayed: " + markErr.Error(),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(outcome.Result)
	case outcomeFailed:
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(outcome.Result)
	default: // outcomeRejected: still invalid, original DLQ record untouched
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(outcome.Result)
	}
}

// Stats returns the ingestion handler counters.
func (h *Handler) Stats() (accepted uint64, rejected uint64, throttled uint64) {
	return atomic.LoadUint64(&h.acceptedEvents),
		atomic.LoadUint64(&h.rejectedEvents),
		atomic.LoadUint64(&h.throttledReqs)
}

// ExtendedStats returns all ingestion counters including deduplication hits and DLQ routing.
func (h *Handler) ExtendedStats() (accepted, rejected, throttled, dedupHits, dlqCount uint64) {
	return atomic.LoadUint64(&h.acceptedEvents),
		atomic.LoadUint64(&h.rejectedEvents),
		atomic.LoadUint64(&h.throttledReqs),
		atomic.LoadUint64(&h.dedupHits),
		atomic.LoadUint64(&h.dlqCount)
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
}
