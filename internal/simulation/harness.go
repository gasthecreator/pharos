package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/gasthecreator/pharos/internal/clock"
	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/ingestion"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

// Harness wires the REAL internal/ingestion.Handler, internal/ingestion.Sweeper,
// and internal/consumer.Engine together against a simulated Broker plus the
// existing in-memory outbox/canonical stores (§2.4, PLAN.md Slice 22 Stage
// B) -- swapping only the I/O boundaries (Kafka, wall-clock time) for
// simulated, seed-controlled ones. No pipeline logic is reimplemented here:
// every invariant this package's tests check is checked against the exact
// production claim/publish/consume/watermark code every other slice
// already relies on.
type Harness struct {
	Handler   *ingestion.Handler
	Sweeper   *ingestion.Sweeper
	Engine    *consumer.Engine
	Reader    *SimulatedReader
	Outbox    *dedup.MemoryOutboxStore
	Canonical *consumer.MemoryCanonicalStore
	Broker    *Broker
	Clock     *clock.Simulated
	Rng       *rand.Rand
}

// NewHarness builds one fully-wired simulation run from seed. Every random
// decision in the run -- broker delivery delay, which harness action fires
// on which tick, generated event content -- traces back to this one seed
// via Rng, so re-running NewHarness(seed, numPartitions) and an identical
// driver sequence reproduces bit-for-bit identical behavior.
func NewHarness(seed int64, numPartitions int) *Harness {
	rng := rand.New(rand.NewSource(seed))
	simClock := clock.NewSimulated(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	outbox := dedup.NewMemoryOutboxStore()
	outbox.SetClock(simClock)

	broker := NewBroker(numPartitions, func() int { return rng.Intn(4) })

	limiter := ratelimit.NewTokenBucketLimiter(1e9, 1e9) // effectively unlimited: rate limiting isn't what this simulation exists to stress (already covered live, Slice 16)
	handler := ingestion.NewHandlerWithOutbox(limiter, outbox, broker, dedup.DefaultLeaseTimeout)
	sweeper := ingestion.NewSweeper(outbox, broker, time.Hour /* unused: the harness calls Step directly, never Start */, dedup.DefaultLeaseTimeout)

	canonical := consumer.NewMemoryCanonicalStore()
	tracker := consumer.NewWatermarkTracker(15*time.Minute, 10*time.Minute)
	reader := broker.NewReader()
	engine := consumer.NewEngine(reader, canonical, tracker, consumer.EngineConfig{Topic: kafka.MainTopic})
	engine.SetClock(simClock)

	return &Harness{
		Handler:   handler,
		Sweeper:   sweeper,
		Engine:    engine,
		Reader:    reader,
		Outbox:    outbox,
		Canonical: canonical,
		Broker:    broker,
		Clock:     simClock,
		Rng:       rng,
	}
}

// SubmitEvent drives a real HTTP submission through Handler.HandleEvents
// (the actual production entry point, exercised via httptest exactly like
// internal/ingestion's own handler tests do) -- not a shortcut call into
// unexported internals. Returns the response status code.
func (h *Harness) SubmitEvent(siteID string, localSeq int, eventTime time.Time, actuality string) int {
	payload := fmt.Sprintf(`{
		"resourceType": "AdverseEvent",
		"id": "ae-%s-%d",
		"identifier": [{"system": "urn:pharos:idempotency-key", "value": "%s:%d"}],
		"actuality": %q,
		"subject": {"reference": "Patient/P-1"},
		"event": {"coding": [{"system": "http://hl7.org/fhir/sid/meddra", "code": "10002198", "display": "Anaphylaxis"}], "text": "Anaphylaxis"},
		"date": %q,
		"recordedDate": %q,
		"severity": {"coding": [{"code": "moderate"}]},
		"study": [{"reference": "ResearchStudy/PHAROS-SIM"}],
		"location": {"reference": "Location/%s"}
	}`, siteID, localSeq, siteID, localSeq, actuality, eventTime.Format(time.RFC3339), eventTime.Format(time.RFC3339), siteID)

	body, _ := json.Marshal(struct {
		SiteID string            `json:"site_id"`
		Events []json.RawMessage `json:"events"`
	}{SiteID: siteID, Events: []json.RawMessage{json.RawMessage(payload)}})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.Handler.HandleEvents(rec, req)
	return rec.Code
}

// StepConsumer runs one Engine.Step, treating "nothing currently visible"
// as a normal no-op rather than a failure -- the harness itself decides
// when to advance the broker's virtual time, so an empty fetch simply means
// this tick had nothing to consume yet.
func (h *Harness) StepConsumer(ctx context.Context) error {
	err := h.Engine.Step(ctx)
	if err != nil && errors.Is(err, ErrNoMessage) {
		return nil
	}
	return err
}

// DrainConsumer repeatedly advances the broker and steps the consumer until
// every published message (visible or still pending delivery) has been
// consumed and committed, or maxTicks is exhausted -- called at the end of
// a simulation run so invariant checks see the pipeline's final settled
// state, not a mid-flight snapshot.
func (h *Harness) DrainConsumer(ctx context.Context, maxTicks int) error {
	for i := 0; i < maxTicks; i++ {
		if h.Broker.PendingCount() == 0 && h.Reader.UnconsumedCount() == 0 {
			return nil
		}
		h.Broker.Advance()
		for {
			err := h.Engine.Step(ctx)
			if err != nil {
				if errors.Is(err, ErrNoMessage) {
					break
				}
				return fmt.Errorf("consumer step failed during drain: %w", err)
			}
		}
	}
	if h.Broker.PendingCount() > 0 || h.Reader.UnconsumedCount() > 0 {
		return fmt.Errorf("drain exhausted %d ticks with %d pending / %d unconsumed messages still undelivered", maxTicks, h.Broker.PendingCount(), h.Reader.UnconsumedCount())
	}
	return nil
}
