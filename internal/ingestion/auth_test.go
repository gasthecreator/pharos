package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gasthecreator/pharos/internal/audit"
	"github.com/gasthecreator/pharos/internal/auth"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

// newAuthedMux builds a real mux via RegisterRoutes (not calling HandleEvents
// directly, the way most of this package's other tests do) specifically so
// auth.RequireAPIKey's middleware is genuinely in the request path -- this is
// the only way to exercise the authenticated-site-id-in-context check added
// for Slice 15, since the context key that carries it is deliberately
// unexported outside the auth package.
func newAuthedMux(t *testing.T, keyStore auth.KeyStore) (*http.ServeMux, dedup.OutboxStore) {
	t.Helper()
	limiter := ratelimit.NewTokenBucketLimiter(1000, 1000)
	outbox := dedup.NewMemoryOutboxStore()
	producer := kafka.NewMockProducer()
	h := NewHandlerWithOutbox(limiter, outbox, producer, dedup.DefaultLeaseTimeout)
	h.SetKeyStore(keyStore)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux, outbox
}

// newAuthedMuxWithAudit is newAuthedMux plus a real audit.Store wired via
// SetAuditStore, for tests proving §2.4/Slice 20's "who replayed what"
// actually gets recorded through the genuine RequireAPIKey middleware path
// (not by directly poking the unexported context key).
func newAuthedMuxWithAudit(t *testing.T, keyStore auth.KeyStore, auditStore audit.Store) (*http.ServeMux, dedup.OutboxStore) {
	t.Helper()
	limiter := ratelimit.NewTokenBucketLimiter(1000, 1000)
	outbox := dedup.NewMemoryOutboxStore()
	producer := kafka.NewMockProducer()
	h := NewHandlerWithOutbox(limiter, outbox, producer, dedup.DefaultLeaseTimeout)
	h.SetKeyStore(keyStore)
	h.SetAuditStore(auditStore)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux, outbox
}

func TestRegisterRoutes_AuthenticatedSiteCannotSubmitAsAnotherSite(t *testing.T) {
	ctx := context.Background()
	keyStore := auth.NewMemoryKeyStore()
	plaintext, err := keyStore.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	mux, _ := newAuthedMux(t, keyStore)

	// Authenticated as SITE-A, but the envelope claims to be SITE-B --
	// exactly the gap this slice exists to close (§2.1, §2.2, Slice 15).
	events := []model.AdverseEvent{validEvent("SITE-B", 1)}
	reqBody, _ := json.Marshal(BatchRequest{SiteID: "SITE-B", Events: toRaw(events...)})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	req.Header.Set("X-Site-ID", "SITE-A")
	req.Header.Set("X-API-Key", plaintext)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when authenticated site doesn't match claimed site, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestRegisterRoutes_AuthenticatedSiteCanSubmitAsItself(t *testing.T) {
	ctx := context.Background()
	keyStore := auth.NewMemoryKeyStore()
	plaintext, err := keyStore.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	mux, _ := newAuthedMux(t, keyStore)

	events := []model.AdverseEvent{validEvent("SITE-A", 1)}
	reqBody, _ := json.Marshal(BatchRequest{SiteID: "SITE-A", Events: toRaw(events...)})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	req.Header.Set("X-Site-ID", "SITE-A")
	req.Header.Set("X-API-Key", plaintext)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 when authenticated and claimed site match, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

func TestRegisterRoutes_NoKeyRejectedEntirely(t *testing.T) {
	keyStore := auth.NewMemoryKeyStore()
	mux, _ := newAuthedMux(t, keyStore)

	events := []model.AdverseEvent{validEvent("SITE-A", 1)}
	reqBody, _ := json.Marshal(BatchRequest{SiteID: "SITE-A", Events: toRaw(events...)})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(reqBody))
	// Deliberately no X-Site-ID/X-API-Key headers at all.
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no credentials at all, got %d", rec.Code)
	}
}

func TestRegisterRoutes_HealthzNeverRequiresAuth(t *testing.T) {
	keyStore := auth.NewMemoryKeyStore()
	mux, _ := newAuthedMux(t, keyStore)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected /healthz to remain unauthenticated even with auth enabled, got %d", rec.Code)
	}
}

// TestHandleDLQReplay_OnlyOwningSiteMayReplay proves the same
// authenticated-site check on the DLQ replay path: a site cannot replay
// another site's rejected event just by knowing its idempotency key.
func TestHandleDLQReplay_OnlyOwningSiteMayReplay(t *testing.T) {
	ctx := context.Background()
	keyStore := auth.NewMemoryKeyStore()
	plaintextA, err := keyStore.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	mux, outbox := newAuthedMux(t, keyStore)

	// Seed a DLQ record owned by SITE-B.
	dlqKey := "SITE-B:99"
	claim, err := outbox.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey:  dlqKey,
		SiteID:          "SITE-B",
		Payload:         []byte(`{"malformed":true}`),
		RejectionReason: "seeded for auth test",
	}, dedup.DefaultLeaseTimeout)
	if err != nil || !claim.Acquired {
		t.Fatalf("failed to seed DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	if err := outbox.MarkDLQPublished(ctx, dlqKey, claim.ClaimedAt, "pharos.events.dlq", 0, 0); err != nil {
		t.Fatalf("failed to mark seeded DLQ record published: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/dlq/"+dlqKey+"/replay", nil)
	req.Header.Set("X-Site-ID", "SITE-A")
	req.Header.Set("X-API-Key", plaintextA)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when SITE-A tries to replay SITE-B's DLQ record, got %d (body: %s)", rec.Code, rec.Body.String())
	}
}

// TestHandleDLQReplay_RecordsAccessAuditOnSuccess proves §2.4/Slice 20's
// "who replayed what" actually lands in the access-audit trail, keyed by the
// real authenticated site identity (not a self-declared one) -- through the
// genuine RequireAPIKey middleware, not a direct HandleDLQReplay call, so
// auth.SiteIDFromContext resolves for real.
func TestHandleDLQReplay_RecordsAccessAuditOnSuccess(t *testing.T) {
	ctx := context.Background()
	keyStore := auth.NewMemoryKeyStore()
	plaintextA, err := keyStore.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	auditStore := audit.NewMemoryStore()
	mux, outbox := newAuthedMuxWithAudit(t, keyStore, auditStore)

	dlqKey := "SITE-A:1"
	claim, err := outbox.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey:  dlqKey,
		SiteID:          "SITE-A",
		Payload:         createValidTestEventJSON("SITE-A", 1, ""),
		RejectionReason: "seeded for audit test",
	}, dedup.DefaultLeaseTimeout)
	if err != nil || !claim.Acquired {
		t.Fatalf("failed to seed DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	if err := outbox.MarkDLQPublished(ctx, dlqKey, claim.ClaimedAt, "pharos.events.dlq", 0, 0); err != nil {
		t.Fatalf("failed to mark seeded DLQ record published: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/dlq/"+dlqKey+"/replay", nil)
	req.Header.Set("X-Site-ID", "SITE-A")
	req.Header.Set("X-API-Key", plaintextA)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a valid self-replay, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	entries, err := auditStore.ListByOperator(ctx, "SITE-A", 10)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 access-audit entry for SITE-A, got %d", len(entries))
	}
	if entries[0].Action != "DLQ_REPLAY" {
		t.Errorf("expected Action DLQ_REPLAY, got %s", entries[0].Action)
	}
	if entries[0].Resource != dlqKey {
		t.Errorf("expected Resource %s, got %s", dlqKey, entries[0].Resource)
	}
	if entries[0].Outcome != "SUCCESS" {
		t.Errorf("expected Outcome SUCCESS, got %s", entries[0].Outcome)
	}
}

// TestHandleDLQReplay_RecordsAccessAuditOnForbidden proves a rejected replay
// attempt (a site trying to replay another site's record) is still recorded
// in the audit trail -- "who *tried* to replay what" matters for compliance
// just as much as successful replays do, arguably more so.
func TestHandleDLQReplay_RecordsAccessAuditOnForbidden(t *testing.T) {
	ctx := context.Background()
	keyStore := auth.NewMemoryKeyStore()
	plaintextA, err := keyStore.CreateKey(ctx, "SITE-A")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	auditStore := audit.NewMemoryStore()
	mux, outbox := newAuthedMuxWithAudit(t, keyStore, auditStore)

	dlqKey := "SITE-B:99"
	claim, err := outbox.InsertDLQClaim(ctx, dedup.DLQRecord{
		IdempotencyKey:  dlqKey,
		SiteID:          "SITE-B",
		Payload:         []byte(`{"malformed":true}`),
		RejectionReason: "seeded for audit test",
	}, dedup.DefaultLeaseTimeout)
	if err != nil || !claim.Acquired {
		t.Fatalf("failed to seed DLQ claim: %v (acquired=%v)", err, claim.Acquired)
	}
	if err := outbox.MarkDLQPublished(ctx, dlqKey, claim.ClaimedAt, "pharos.events.dlq", 0, 0); err != nil {
		t.Fatalf("failed to mark seeded DLQ record published: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/dlq/"+dlqKey+"/replay", nil)
	req.Header.Set("X-Site-ID", "SITE-A")
	req.Header.Set("X-API-Key", plaintextA)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	entries, err := auditStore.ListByOperator(ctx, "SITE-A", 10)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 access-audit entry for SITE-A (the attempted, forbidden replay), got %d", len(entries))
	}
	if entries[0].Action != "DLQ_REPLAY" {
		t.Errorf("expected Action DLQ_REPLAY, got %s", entries[0].Action)
	}
	if !strings.Contains(entries[0].Outcome, "forbidden") {
		t.Errorf("expected Outcome to mention forbidden, got %q", entries[0].Outcome)
	}
}
