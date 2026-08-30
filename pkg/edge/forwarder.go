package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gasthecreator/pharos/pkg/ingestion"
)

// HTTPClient interface allows mocking the network layer in tests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ForwarderConfig defines the operational parameters for the edge store-and-forward worker (§2.1).
type ForwarderConfig struct {
	CentralURL     string        `json:"central_url"`
	SiteID         string        `json:"site_id"`
	BatchSize      int           `json:"batch_size"`
	PollInterval   time.Duration `json:"poll_interval"`
	RequestTimeout time.Duration `json:"request_timeout"`
	BaseBackoff    time.Duration `json:"base_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
}

// DefaultForwarderConfig provides standard defaults with Full Jitter parameters.
func DefaultForwarderConfig(centralURL, siteID string) ForwarderConfig {
	return ForwarderConfig{
		CentralURL:     centralURL,
		SiteID:         siteID,
		BatchSize:      50,
		PollInterval:   500 * time.Millisecond,
		RequestTimeout: 5 * time.Second,
		BaseBackoff:    500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
	}
}

// Forwarder runs as a background worker reading pending records from local SQLite WAL
// and streaming them to Central Ingestion over HTTPS (§2.1).
type Forwarder struct {
	store      QueueStore
	client     HTTPClient
	cfg        ForwarderConfig
	rng        *rand.Rand
}

// NewForwarder constructs a new Forwarder instance.
func NewForwarder(store QueueStore, client HTTPClient, cfg ForwarderConfig) *Forwarder {
	if client == nil {
		client = &http.Client{Timeout: cfg.RequestTimeout}
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}

	return &Forwarder{
		store:  store,
		client: client,
		cfg:    cfg,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Step performs a single fetch-and-forward cycle. Returns the number of events forwarded.
func (f *Forwarder) Step(ctx context.Context) (int, error) {
	records, err := f.store.FetchPending(ctx, f.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pending records: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	recordIDs := make([]int64, len(records))
	rawEvents := make([]json.RawMessage, len(records))

	for i, r := range records {
		recordIDs[i] = r.ID
		rawEvents[i] = json.RawMessage(r.Payload)
	}

	// 1. Mark in flight to prevent concurrent workers from claiming the same records
	if err := f.store.MarkInFlight(ctx, recordIDs); err != nil {
		return 0, fmt.Errorf("failed to mark records in flight: %w", err)
	}

	// 2. Assemble batch request directly with raw payload bytes (zero round-trip data loss)
	batchReq := ingestion.BatchRequest{
		SiteID: f.cfg.SiteID,
		Events: rawEvents,
	}

	reqBytes, err := json.Marshal(batchReq)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, f.cfg.RequestTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, f.cfg.CentralURL, bytes.NewReader(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to build http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Site-ID", f.cfg.SiteID)

	// 3. Send HTTP request to Central Ingestion
	resp, err := f.client.Do(httpReq)
	if err != nil {
		// Network error (timeout, connection refused, unreachable network)
		f.handleFailure(ctx, records, fmt.Sprintf("network error: %v", err), 0)
		return 0, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	// 4. Handle response status codes
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusMultiStatus || resp.StatusCode == http.StatusUnprocessableEntity:
		var batchResp ingestion.BatchResponse
		_ = json.Unmarshal(respBytes, &batchResp)

		// Map results by idempotency_key to correlate per-event outcomes (§2.2, §2.3)
		resultsByKey := make(map[string]ingestion.EventResult, len(batchResp.Results))
		for _, res := range batchResp.Results {
			if res.IdempotencyKey != "" {
				resultsByKey[res.IdempotencyKey] = res
			}
		}

		var ackIDs []int64
		rejectedByReason := make(map[string][]int64)
		type failedRecord struct {
			id         int64
			reason     string
			retryAfter time.Duration
		}
		var failedRecords []failedRecord

		for _, r := range records {
			res, found := resultsByKey[r.IdempotencyKey]
			if found && res.Status == ingestion.StatusRejected {
				reason := res.Error
				if reason == "" {
					reason = "rejected by central ingestion"
				}
				rejectedByReason[reason] = append(rejectedByReason[reason], r.ID)
			} else if found && res.Status == ingestion.StatusAccepted {
				ackIDs = append(ackIDs, r.ID)
			} else if found && res.Status == ingestion.StatusFailed {
				reason := res.Error
				if reason == "" {
					reason = "infrastructure failure during ingestion"
				}
				retryAfter := f.CalculateBackoff(r.Attempts)
				failedRecords = append(failedRecords, failedRecord{id: r.ID, reason: reason, retryAfter: retryAfter})
			} else {
				// Fallback if not found in results map
				if resp.StatusCode == http.StatusUnprocessableEntity {
					reason := batchResp.Error
					if reason == "" {
						reason = "batch rejected by central ingestion (HTTP 422)"
					}
					rejectedByReason[reason] = append(rejectedByReason[reason], r.ID)
				} else if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
					// Full batch acceptance (HTTP 200/201)
					ackIDs = append(ackIDs, r.ID)
				} else {
					// Conservative fallback for unmapped records in partial/multi-status responses:
					// Never silently acknowledge unmapped records; mark as failed for retry with backoff (§2.1).
					reason := "event result missing from central ingestion response"
					retryAfter := f.CalculateBackoff(r.Attempts)
					failedRecords = append(failedRecords, failedRecord{id: r.ID, reason: reason, retryAfter: retryAfter})
				}
			}
		}

		if len(ackIDs) > 0 {
			if err := f.store.MarkAcknowledged(ctx, ackIDs); err != nil {
				return 0, fmt.Errorf("failed to mark records acknowledged: %w", err)
			}
		}

		for reason, rejIDs := range rejectedByReason {
			if err := f.store.MarkRejected(ctx, rejIDs, reason); err != nil {
				return 0, fmt.Errorf("failed to mark records rejected: %w", err)
			}
		}

		for _, fr := range failedRecords {
			if err := f.store.MarkFailed(ctx, fr.id, fr.reason, fr.retryAfter); err != nil {
				return 0, fmt.Errorf("failed to mark record failed: %w", err)
			}
		}

		return len(records), nil

	case resp.StatusCode == http.StatusTooManyRequests:
		// Rate limited by Central Ingestion token bucket (§2.3)
		retryAfter := f.parseRetryAfter(resp.Header.Get("Retry-After"))
		f.handleFailure(ctx, records, fmt.Sprintf("rate limited (HTTP 429): %s", string(respBytes)), retryAfter)
		return 0, fmt.Errorf("rate limited (HTTP 429)")

	default:
		// 5xx Server Error or unexpected error
		errMsg := fmt.Sprintf("server error (HTTP %d): %s", resp.StatusCode, string(respBytes))
		f.handleFailure(ctx, records, errMsg, 0)
		return 0, fmt.Errorf("%s", errMsg)
	}
}

func (f *Forwarder) handleFailure(ctx context.Context, records []*QueuedRecord, reason string, explicitRetryAfter time.Duration) {
	for _, r := range records {
		retryAfter := explicitRetryAfter
		if retryAfter <= 0 {
			retryAfter = f.CalculateBackoff(r.Attempts)
		}
		_ = f.store.MarkFailed(ctx, r.ID, reason, retryAfter)
	}
}

func (f *Forwarder) parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}
	secs, err := strconv.Atoi(header)
	if err != nil || secs <= 0 {
		return 0
	}
	dur := time.Duration(secs) * time.Second
	if dur > f.cfg.MaxBackoff {
		dur = f.cfg.MaxBackoff
	}
	return dur
}

// CalculateBackoff computes exponential backoff with Full Jitter:
// sleep = rand(0, min(MaxBackoff, BaseBackoff * 2^attempts))
func (f *Forwarder) CalculateBackoff(attempts int) time.Duration {
	base := float64(f.cfg.BaseBackoff)
	max := float64(f.cfg.MaxBackoff)

	if attempts < 0 {
		attempts = 0
	}
	if attempts > 30 {
		attempts = 30 // prevent overflow
	}

	multiplier := math.Pow(2, float64(attempts))
	ceiling := math.Min(max, base*multiplier)

	// Full jitter: random sleep between 0 and ceiling
	jittered := f.rng.Float64() * ceiling
	if jittered < float64(100*time.Millisecond) {
		jittered = float64(100 * time.Millisecond)
	}

	return time.Duration(jittered)
}

// Run starts the forwarder polling loop until the context is canceled.
func (f *Forwarder) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			count, err := f.Step(ctx)
			if err != nil || count == 0 {
				// Wait for next poll tick if error or queue is empty
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
			// If events were processed, immediately loop to drain the queue without waiting
		}
	}
}
