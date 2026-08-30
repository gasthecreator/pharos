package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/pkg/dedup"
	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

func createValidTestEventJSON(siteID string, localSeq uint64, extraField string) []byte {
	extraJSON := ""
	if extraField != "" {
		extraJSON = fmt.Sprintf(`,"extra_unmodeled_data":%q`, extraField)
	}
	return []byte(fmt.Sprintf(`{
		"resourceType": "AdverseEvent",
		"id": "ae-%s-%d",
		"identifier": [
			{
				"system": "urn:pharos:idempotency-key",
				"value": "%s:%d"
			}
		],
		"actuality": "actual",
		"subject": {
			"reference": "Patient/P-100"
		},
		"event": {
			"coding": [{"system": "http://hl7.org/fhir/sid/meddra", "code": "10002198", "display": "Anaphylaxis"}],
			"text": "Anaphylaxis"
		},
		"date": "2026-08-30T00:00:00Z",
		"recordedDate": "2026-08-30T00:01:00Z",
		"severity": {
			"coding": [{"code": "severe"}]
		},
		"study": [{"reference": "ResearchStudy/PHAROS-01"}],
		"location": {"reference": "Location/%s"}
		%s
	}`, siteID, localSeq, siteID, localSeq, siteID, extraJSON))
}

func createInvalidTestEventJSON(siteID string, localSeq uint64) []byte {
	return []byte(fmt.Sprintf(`{
		"resourceType": "AdverseEvent",
		"id": "ae-%s-%d",
		"identifier": [
			{
				"system": "urn:pharos:idempotency-key",
				"value": "%s:%d"
			}
		],
		"actuality": "actual",
		"date": "2026-08-30T00:00:00Z"
	}`, siteID, localSeq, siteID, localSeq))
}

// TestConcurrentDuplicateRaces verifies that when multiple concurrent goroutines submit
// the identical idempotency key, EXACTLY ONE Kafka publish actually occurs (§2.2).
func TestConcurrentDuplicateRaces(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(1000, 1000)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-RACE-01"
	localSeq := uint64(42)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	eventJSON := createValidTestEventJSON(siteID, localSeq, "concurrency_test_payload")

	const concurrency = 25
	var wg sync.WaitGroup
	wg.Add(concurrency)

	statusCodes := make([]int, concurrency)
	results := make([]BatchResponse, concurrency)

	for i := 0; i < concurrency; i++ {
		idx := i
		go func() {
			defer wg.Done()
			reqBody := BatchRequest{
				SiteID: siteID,
				Events: []json.RawMessage{eventJSON},
			}
			bodyBytes, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.HandleEvents(w, req)

			statusCodes[idx] = w.Code
			_ = json.Unmarshal(w.Body.Bytes(), &results[idx])
		}()
	}

	wg.Wait()

	// 1. All concurrent requests must receive HTTP 200 OK (accepted)
	for i, code := range statusCodes {
		if code != http.StatusOK {
			t.Fatalf("goroutine %d received non-200 status code: %d", i, code)
		}
		if results[i].Accepted != 1 || results[i].Rejected != 0 {
			t.Fatalf("goroutine %d unexpected batch result: %+v", i, results[i])
		}
	}

	// 2. CRITICAL VERIFICATION: Assert EXACTLY ONE Kafka publish call actually occurred!
	totalPublishes := mockProducer.TotalPublishes()
	if totalPublishes != 1 {
		t.Fatalf("CRITICAL RACE VIOLATION: expected exactly 1 Kafka publish for key %s, got %d", idKey, totalPublishes)
	}

	// 3. Verify Kafka message partition key is siteID
	messages := mockProducer.Messages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message in producer log, got %d", len(messages))
	}
	if messages[0].Key != siteID {
		t.Errorf("expected Kafka partition key to be siteID %s, got %s", siteID, messages[0].Key)
	}

	// 4. Verify Cassandra outbox record is marked PUBLISHED
	outboxRec, err := store.GetOutboxRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("failed to get outbox record: %v", err)
	}
	if outboxRec.Status != dedup.StatusPublished {
		t.Errorf("expected outbox record status PUBLISHED, got %s", outboxRec.Status)
	}
}

// TestCrashWindowResumption verifies that if Kafka publishing fails mid-intake,
// the record remains in PUBLISHING and a subsequent retry resumes and publishes (§2.2).
func TestCrashWindowResumption(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	shortLease := 50 * time.Millisecond
	handler := NewHandlerWithOutbox(limiter, store, mockProducer, shortLease)

	siteID := "SITE-CRASH-01"
	localSeq := uint64(101)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	eventJSON := createValidTestEventJSON(siteID, localSeq, "crash_recovery_payload")

	// Inject 1 Kafka failure to simulate crash window during Kafka publish
	mockProducer.FailNext(1)

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{eventJSON},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	// 1st request: Cassandra write succeeds, but Kafka publish fails!
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	handler.HandleEvents(w1, req1)

	// Must fail with 503 so edge knows to retry
	if w1.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 on Kafka failure, got %d: %s", w1.Code, w1.Body.String())
	}

	// Verify record is in Cassandra with status=PUBLISHING, but Kafka publish count is 0
	outboxRec, err := store.GetOutboxRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("expected record in Cassandra outbox: %v", err)
	}
	if outboxRec.Status != dedup.StatusPublishing {
		t.Fatalf("expected status PUBLISHING after interrupted write, got %s", outboxRec.Status)
	}
	if mockProducer.TotalPublishes() != 0 {
		t.Fatalf("expected 0 successful publishes so far, got %d", mockProducer.TotalPublishes())
	}

	// Wait for lease to expire
	time.Sleep(60 * time.Millisecond)

	// 2nd request (simulating client retry after backoff):
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	handler.HandleEvents(w2, req2)

	// Retry must succeed with HTTP 200
	if w2.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on retry, got %d: %s", w2.Code, w2.Body.String())
	}

	// Verify Kafka now has EXACTLY ONE publish
	if mockProducer.TotalPublishes() != 1 {
		t.Fatalf("expected exactly 1 Kafka publish after retry resumption, got %d", mockProducer.TotalPublishes())
	}

	// Verify Cassandra record is now marked PUBLISHED
	updatedRec, _ := store.GetOutboxRecord(context.Background(), idKey)
	if updatedRec.Status != dedup.StatusPublished {
		t.Fatalf("expected status PUBLISHED after retry, got %s", updatedRec.Status)
	}
}

// TestSequentialDuplicateIdempotency verifies that sequential duplicate submissions
// result in zero duplicate Kafka messages and return HTTP 200 (§2.2).
func TestSequentialDuplicateIdempotency(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-DUP-01"
	localSeq := uint64(5)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	eventJSON := createValidTestEventJSON(siteID, localSeq, "duplicate_test")

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{eventJSON},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	// 1st request: fresh submission
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	w1 := httptest.NewRecorder()
	handler.HandleEvents(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected 1st submission 200 OK, got %d", w1.Code)
	}
	if mockProducer.TotalPublishes() != 1 {
		t.Fatalf("expected 1 publish after 1st submission, got %d", mockProducer.TotalPublishes())
	}

	// 2nd request: duplicate submission
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	w2 := httptest.NewRecorder()
	handler.HandleEvents(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected duplicate submission 200 OK, got %d", w2.Code)
	}

	// Assert NO new Kafka publish occurred!
	if mockProducer.TotalPublishes() != 1 {
		t.Fatalf("expected Kafka publishes to remain 1 on duplicate, got %d", mockProducer.TotalPublishes())
	}

	_, _, _, dedupHits, _ := handler.ExtendedStats()
	if dedupHits != 1 {
		t.Errorf("expected dedupHits counter to be 1, got %d", dedupHits)
	}

	outboxRec, err := store.GetOutboxRecord(context.Background(), idKey)
	if err != nil || outboxRec.Status != dedup.StatusPublished {
		t.Errorf("expected outbox record to be PUBLISHED: %v, rec: %+v", err, outboxRec)
	}
}

// TestStaleLeaseReclamationBySweeper verifies that the background sweeper reclaims
// abandoned in-flight claims and publishes them to Kafka (§2.2).
func TestStaleLeaseReclamationBySweeper(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	ctx := context.Background()

	leaseTimeout := 50 * time.Millisecond
	sweeper := NewSweeper(store, mockProducer, 10*time.Millisecond, leaseTimeout)

	siteID := "SITE-SWEEP-01"
	localSeq := uint64(88)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	rawPayload := createValidTestEventJSON(siteID, localSeq, "sweeper_reclaim_payload")

	// 1. Manually insert a stale record into outbox simulating a crashed worker
	staleRec := dedup.OutboxRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		LocalSeq:       localSeq,
		Payload:        rawPayload,
	}
	claim, err := store.InsertClaim(ctx, staleRec, leaseTimeout)
	if err != nil || !claim.Acquired {
		t.Fatalf("failed to insert initial claim: %v", err)
	}

	// Wait for lease to expire
	time.Sleep(70 * time.Millisecond)

	// 2. Trigger a sweeper step
	recovered, err := sweeper.Step(ctx)
	if err != nil {
		t.Fatalf("sweeper step failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected sweeper to recover 1 stale record, got %d", recovered)
	}

	// 3. Verify Kafka publish occurred
	if mockProducer.TotalPublishes() != 1 {
		t.Fatalf("expected sweeper to publish 1 message to Kafka, got %d", mockProducer.TotalPublishes())
	}
	if mockProducer.Messages()[0].Topic != kafka.MainTopic {
		t.Errorf("expected topic %s, got %s", kafka.MainTopic, mockProducer.Messages()[0].Topic)
	}

	// 4. Verify record in Cassandra is now PUBLISHED
	updatedRec, _ := store.GetOutboxRecord(ctx, idKey)
	if updatedRec.Status != dedup.StatusPublished {
		t.Fatalf("expected record to be PUBLISHED after sweep, got %s", updatedRec.Status)
	}
}

// TestDeadLetterPipeline_DurabilityAndRouting verifies that invalid events are durably stored
// in Cassandra dead_letter_events and published to Kafka DLQ topic BEFORE 422 is returned (§2.3).
func TestDeadLetterPipeline_DurabilityAndRouting(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-DLQ-01"
	localSeq := uint64(999)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	invalidJSON := createInvalidTestEventJSON(siteID, localSeq)

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{invalidJSON},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.HandleEvents(w, req)

	// 1. Response must be 422 Unprocessable Entity
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected HTTP 422 for invalid event, got %d: %s", w.Code, w.Body.String())
	}

	// 2. Kafka DLQ topic must have received 1 message
	if mockProducer.TopicCount(kafka.DLQTopic) != 1 {
		t.Fatalf("expected 1 message in DLQ topic, got %d", mockProducer.TopicCount(kafka.DLQTopic))
	}
	if mockProducer.TopicCount(kafka.MainTopic) != 0 {
		t.Fatalf("expected 0 messages in main topic, got %d", mockProducer.TopicCount(kafka.MainTopic))
	}

	dlqMsg := mockProducer.Messages()[0]
	if dlqMsg.Topic != kafka.DLQTopic {
		t.Errorf("expected message on topic %s, got %s", kafka.DLQTopic, dlqMsg.Topic)
	}
	if dlqMsg.Headers["idempotency_key"] != idKey {
		t.Errorf("expected idempotency_key header %s, got %s", idKey, dlqMsg.Headers["idempotency_key"])
	}

	// 3. Cassandra dead_letter_events must have durably recorded the rejection
	dlqRec, err := store.GetDLQRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("failed to retrieve DLQ record from Cassandra store: %v", err)
	}
	if dlqRec.Status != dedup.StatusPublished {
		t.Errorf("expected DLQ status PUBLISHED, got %s", dlqRec.Status)
	}
	if dlqRec.RejectionReason == "" {
		t.Errorf("expected non-empty rejection reason in DLQ record")
	}

	// 4. DLQ stats counter incremented
	_, rejected, _, _, dlqCount := handler.ExtendedStats()
	if rejected != 1 || dlqCount != 1 {
		t.Errorf("expected rejected=1, dlqCount=1; got rejected=%d, dlqCount=%d", rejected, dlqCount)
	}
}

// TestRawPayloadPreservation verifies that unmodeled and arbitrary fields in the raw JSON payload
// are stored verbatim in both Cassandra and Kafka messages without struct round-trip stripping.
func TestRawPayloadPreservation(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-RAW-01"
	localSeq := uint64(77)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	magicToken := "custom_unmodeled_clinical_extension_xyz_9988"
	rawJSON := createValidTestEventJSON(siteID, localSeq, magicToken)

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{rawJSON},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	handler.HandleEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	// 1. Verify Cassandra stored raw payload contains magicToken
	outboxRec, err := store.GetOutboxRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("failed to get outbox record: %v", err)
	}
	if !bytes.Contains(outboxRec.Payload, []byte(magicToken)) {
		t.Fatalf("CRITICAL DATA LOSS: Cassandra outbox payload stripped unmodeled field %s", magicToken)
	}

	// 2. Verify Kafka message value contains magicToken
	messages := mockProducer.Messages()
	if len(messages) != 1 {
		t.Fatalf("expected 1 message in Kafka, got %d", len(messages))
	}
	if !bytes.Contains(messages[0].Value, []byte(magicToken)) {
		t.Fatalf("CRITICAL DATA LOSS: Kafka message value stripped unmodeled field %s", magicToken)
	}
}

// TestMultiEventBatch_MiddleEventInfraFailureContinuesProcessing verifies that when an
// individual event in a multi-event batch encounters an infrastructure failure (e.g. Kafka publish error),
// the handler DOES NOT abort mid-loop: events before and after it are durably stored, published,
// and marked ACCEPTED; the failed event is marked FAILED; the HTTP status code is 503;
// and a full, honest BatchResponse is returned (§2.1, §2.2).
func TestMultiEventBatch_MiddleEventInfraFailureContinuesProcessing(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-BATCH-TEST"
	key0 := fmt.Sprintf("%s:1", siteID)
	key1 := fmt.Sprintf("%s:2", siteID)
	key2 := fmt.Sprintf("%s:3", siteID)

	ev0 := createValidTestEventJSON(siteID, 1, "first_event")
	ev1 := createValidTestEventJSON(siteID, 2, "middle_event_to_fail")
	ev2 := createValidTestEventJSON(siteID, 3, "third_event")

	// Inject failure for key1 only (middle event)
	mockProducer.FailIdempotencyKey(key1)

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{ev0, ev1, ev2},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	handler.HandleEvents(w, req)

	// 1. HTTP status code must be 207 Multi-Status for mixed outcome (accepted + failed)
	// so that edge forwarder inspects the body for per-item outcomes (§2.1, §2.2).
	if w.Code != http.StatusMultiStatus {
		t.Fatalf("expected HTTP 207 Multi-Status for partial infrastructure failure, got %d: %s", w.Code, w.Body.String())
	}

	// 2. Decode the complete, accurate BatchResponse
	var resp BatchResponse
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	if err != nil {
		t.Fatalf("failed to decode BatchResponse: %v, raw: %s", err, w.Body.String())
	}

	if resp.Total != 3 {
		t.Errorf("expected total=3, got %d", resp.Total)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected accepted=2, got %d", resp.Accepted)
	}
	if resp.Failed != 1 {
		t.Errorf("expected failed=1, got %d", resp.Failed)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("expected 3 results in BatchResponse, got %d", len(resp.Results))
	}

	// Result 0: ACCEPTED
	if resp.Results[0].IdempotencyKey != key0 || resp.Results[0].Status != StatusAccepted {
		t.Errorf("expected result 0 ACCEPTED for %s, got %+v", key0, resp.Results[0])
	}
	// Result 1: FAILED with kafka publish error
	if resp.Results[1].IdempotencyKey != key1 || resp.Results[1].Status != StatusFailed {
		t.Errorf("expected result 1 FAILED for %s, got %+v", key1, resp.Results[1])
	}
	if !strings.Contains(resp.Results[1].Error, "kafka publish error") {
		t.Errorf("expected kafka publish error in result 1, got: %s", resp.Results[1].Error)
	}
	// Result 2: ACCEPTED (crucial: processed despite event 1's failure!)
	if resp.Results[2].IdempotencyKey != key2 || resp.Results[2].Status != StatusAccepted {
		t.Errorf("expected result 2 ACCEPTED for %s, got %+v", key2, resp.Results[2])
	}

	// 3. Verify Outbox Store states
	ctx := context.Background()
	rec0, err := store.GetOutboxRecord(ctx, key0)
	if err != nil || rec0.Status != dedup.StatusPublished {
		t.Errorf("expected rec0 to be PUBLISHED in outbox: err=%v, rec=%+v", err, rec0)
	}

	rec1, err := store.GetOutboxRecord(ctx, key1)
	if err != nil || rec1.Status != dedup.StatusPublishing {
		t.Errorf("expected rec1 to remain in PUBLISHING state for retry: err=%v, rec=%+v", err, rec1)
	}

	rec2, err := store.GetOutboxRecord(ctx, key2)
	if err != nil || rec2.Status != dedup.StatusPublished {
		t.Errorf("expected rec2 to be PUBLISHED in outbox: err=%v, rec=%+v", err, rec2)
	}

	// 4. Verify Kafka publish counts: exactly 1 publish for key0, 0 for key1, 1 for key2
	if mockProducer.TotalPublishes() != 2 {
		t.Fatalf("expected exactly 2 total Kafka publishes, got %d", mockProducer.TotalPublishes())
	}
}

// TestFullBatchInfraFailure_Returns503 verifies that when every event in a batch encounters
// an infrastructure failure (nothing accepted or rejected), the handler returns bare 503.
func TestFullBatchInfraFailure_Returns503(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	mockProducer.FailNext(10) // Fail all publishes
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)

	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-FAIL-ALL"
	ev0 := createValidTestEventJSON(siteID, 1, "e1")
	ev1 := createValidTestEventJSON(siteID, 2, "e2")

	reqBody := BatchRequest{
		SiteID: siteID,
		Events: []json.RawMessage{ev0, ev1},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	handler.HandleEvents(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 Service Unavailable when all events fail, got %d: %s", w.Code, w.Body.String())
	}

	var resp BatchResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Failed != 2 || resp.Accepted != 0 || resp.Rejected != 0 {
		t.Errorf("expected failed=2, accepted=0, rejected=0, got %+v", resp)
	}
}
