package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

func newReplayRequest(idKey string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/dlq/"+idKey+"/replay", nil)
	req.SetPathValue("key", idKey)
	return req
}

// TestHandler_DLQReplaySucceedsAndMarksOriginalRecordReplayed locks in the
// success path of §2.3/Slice 10: a PUBLISHED DLQ record whose stored payload
// now validates is resubmitted through the exact same processOneEvent path a
// fresh submission takes -- lands as a new outbox claim, publishes to the
// main Kafka topic (not the DLQ topic again), and the *original* DLQ record
// is marked REPLAYED, never deleted.
func TestHandler_DLQReplaySucceedsAndMarksOriginalRecordReplayed(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-REPLAY-01"
	idKey := fmt.Sprintf("%s:%d", siteID, 1)
	// A payload that validates successfully -- standing in for "whatever was
	// wrong has since been fixed" without needing two different handler
	// configurations in this test; the mechanism under test is the replay
	// path itself, not why a payload might now be valid.
	payload := createValidTestEventJSON(siteID, 1, "")

	ctx := context.Background()
	claim, err := store.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey:   idKey,
		SiteID:           siteID,
		Payload:          payload,
		RejectionReason:  "originally rejected for test setup",
		ValidationErrors: "originally rejected for test setup",
		RejectedAt:       time.Now().UTC(),
	}, 30*time.Second)
	if err != nil || !claim.Acquired {
		t.Fatalf("test setup: failed to seed DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	if err := store.MarkDLQPublished(ctx, idKey, kafka.DLQTopic, 0, 0); err != nil {
		t.Fatalf("test setup: failed to mark DLQ record published: %v", err)
	}

	req := newReplayRequest(idKey)
	w := httptest.NewRecorder()
	handler.HandleDLQReplay(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for a successful replay, got %d: %s", w.Code, w.Body.String())
	}

	var result EventResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode replay response: %v", err)
	}
	if result.Status != StatusAccepted {
		t.Errorf("expected replay result status ACCEPTED, got %s", result.Status)
	}

	if mockProducer.TopicCount(kafka.MainTopic) != 1 {
		t.Errorf("expected 1 message published to the main topic, got %d", mockProducer.TopicCount(kafka.MainTopic))
	}
	if mockProducer.TopicCount(kafka.DLQTopic) != 0 {
		t.Errorf("expected 0 additional DLQ topic publishes from replay itself, got %d", mockProducer.TopicCount(kafka.DLQTopic))
	}

	outboxRec, err := store.GetOutboxRecord(ctx, idKey)
	if err != nil {
		t.Fatalf("expected a new event_outbox claim for %s: %v", idKey, err)
	}
	if outboxRec.Status != dedup.StatusPublished {
		t.Errorf("expected the new outbox record to be PUBLISHED, got %s", outboxRec.Status)
	}

	dlqRec, err := store.GetDLQRecord(ctx, idKey)
	if err != nil {
		t.Fatalf("expected the original DLQ record to still exist: %v", err)
	}
	if dlqRec.Status != dedup.StatusReplayed {
		t.Errorf("expected the original DLQ record status REPLAYED, got %s", dlqRec.Status)
	}
	if dlqRec.ReplayedAt.IsZero() {
		t.Errorf("expected ReplayedAt to be set")
	}
	if dlqRec.RejectionReason != "originally rejected for test setup" {
		t.Errorf("expected the original rejection reason to remain part of the audit trail, got %q", dlqRec.RejectionReason)
	}
}

// TestHandler_DLQReplayStillInvalidLeavesOriginalRecordUntouched locks in the
// other half of §2.3/Slice 10: if the stored payload still fails validation,
// replay must not corrupt, duplicate, or silently accept the original DLQ
// record -- it stays exactly as rejected as it was before the replay attempt.
func TestHandler_DLQReplayStillInvalidLeavesOriginalRecordUntouched(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-REPLAY-02"
	localSeq := uint64(1)
	idKey := fmt.Sprintf("%s:%d", siteID, localSeq)
	invalidJSON := createInvalidTestEventJSON(siteID, localSeq)

	// Get it into the DLQ for real, through the actual HandleEvents path,
	// rather than seeding it directly -- this test also confirms the DLQ
	// entry HandleEvents produces is exactly what HandleDLQReplay expects.
	reqBody, _ := json.Marshal(BatchRequest{SiteID: siteID, Events: []json.RawMessage{invalidJSON}})
	ingestReq := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	ingestRec := httptest.NewRecorder()
	handler.HandleEvents(ingestRec, ingestReq)
	if ingestRec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("test setup: expected HTTP 422 for the invalid event, got %d: %s", ingestRec.Code, ingestRec.Body.String())
	}

	beforeReplay, err := store.GetDLQRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("test setup: expected a DLQ record for %s: %v", idKey, err)
	}
	if beforeReplay.Status != dedup.StatusPublished {
		t.Fatalf("test setup: expected the DLQ record to be PUBLISHED before replay, got %s", beforeReplay.Status)
	}

	req := newReplayRequest(idKey)
	w := httptest.NewRecorder()
	handler.HandleDLQReplay(w, req)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected HTTP 422 for a still-invalid replay, got %d: %s", w.Code, w.Body.String())
	}

	afterReplay, err := store.GetDLQRecord(context.Background(), idKey)
	if err != nil {
		t.Fatalf("expected the DLQ record to still exist after a failed replay: %v", err)
	}
	if afterReplay.Status != dedup.StatusPublished {
		t.Errorf("expected the DLQ record to remain PUBLISHED (untouched) after a failed replay, got %s", afterReplay.Status)
	}
	if !afterReplay.ReplayedAt.IsZero() {
		t.Errorf("expected ReplayedAt to remain unset after a failed replay")
	}

	if _, err := store.GetOutboxRecord(context.Background(), idKey); err == nil {
		t.Errorf("expected no event_outbox record to be created for a still-invalid replay")
	}
}

// TestHandler_DLQReplayNotFound locks in the 404 case: replaying a key with
// no DLQ record at all.
func TestHandler_DLQReplayNotFound(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := NewHandlerWithOutbox(limiter, store, kafka.NewMockProducer(), 30*time.Second)

	req := newReplayRequest("SITE-NOPE:999")
	w := httptest.NewRecorder()
	handler.HandleDLQReplay(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected HTTP 404 for a nonexistent DLQ record, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandler_DLQReplayNotYetPublishedIsConflict locks in the 409 case: a DLQ
// record still mid-claim (PUBLISHING, not yet PUBLISHED) cannot be replayed --
// its own rejection isn't even durably finished yet.
func TestHandler_DLQReplayNotYetPublishedIsConflict(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := NewHandlerWithOutbox(limiter, store, kafka.NewMockProducer(), 30*time.Second)

	siteID := "SITE-REPLAY-03"
	idKey := fmt.Sprintf("%s:%d", siteID, 1)

	claim, err := store.InsertDLQClaim(context.Background(), dedup.DLQRecord{
		IdempotencyKey:  idKey,
		SiteID:          siteID,
		Payload:         createInvalidTestEventJSON(siteID, 1),
		RejectionReason: "still mid-claim",
		RejectedAt:      time.Now().UTC(),
	}, 30*time.Second)
	if err != nil || !claim.Acquired {
		t.Fatalf("test setup: failed to seed a PUBLISHING DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	// Deliberately not calling MarkDLQPublished -- the record stays PUBLISHING.

	req := newReplayRequest(idKey)
	w := httptest.NewRecorder()
	handler.HandleDLQReplay(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected HTTP 409 for a not-yet-PUBLISHED DLQ record, got %d: %s", w.Code, w.Body.String())
	}
}

// TestHandler_DLQReplayTwiceIsConflictOnSecondAttempt locks in that a record
// already REPLAYED cannot be replayed again -- MarkDLQReplayed's CAS
// precondition (IF status = 'PUBLISHED') is what actually enforces this; this
// test proves it end to end through the HTTP handler.
func TestHandler_DLQReplayTwiceIsConflictOnSecondAttempt(t *testing.T) {
	store := dedup.NewMemoryOutboxStore()
	mockProducer := kafka.NewMockProducer()
	limiter := ratelimit.NewTokenBucketLimiter(100, 100)
	handler := NewHandlerWithOutbox(limiter, store, mockProducer, 30*time.Second)

	siteID := "SITE-REPLAY-04"
	idKey := fmt.Sprintf("%s:%d", siteID, 1)
	payload := createValidTestEventJSON(siteID, 1, "")

	ctx := context.Background()
	claim, err := store.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey: idKey,
		SiteID:         siteID,
		Payload:        payload,
		RejectedAt:     time.Now().UTC(),
	}, 30*time.Second)
	if err != nil || !claim.Acquired {
		t.Fatalf("test setup: failed to seed DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	if err := store.MarkDLQPublished(ctx, idKey, kafka.DLQTopic, 0, 0); err != nil {
		t.Fatalf("test setup: failed to mark DLQ record published: %v", err)
	}

	firstReq := newReplayRequest(idKey)
	firstW := httptest.NewRecorder()
	handler.HandleDLQReplay(firstW, firstReq)
	if firstW.Code != http.StatusOK {
		t.Fatalf("expected the first replay to succeed with HTTP 200, got %d: %s", firstW.Code, firstW.Body.String())
	}

	secondReq := newReplayRequest(idKey)
	secondW := httptest.NewRecorder()
	handler.HandleDLQReplay(secondW, secondReq)
	if secondW.Code != http.StatusConflict {
		t.Fatalf("expected the second replay attempt to be HTTP 409 (already REPLAYED), got %d: %s", secondW.Code, secondW.Body.String())
	}

	if mockProducer.TopicCount(kafka.MainTopic) != 1 {
		t.Errorf("expected exactly 1 main-topic publish across both replay attempts, got %d", mockProducer.TopicCount(kafka.MainTopic))
	}
}
