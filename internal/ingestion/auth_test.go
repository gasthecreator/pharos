package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
	if err := outbox.MarkDLQPublished(ctx, dlqKey, "pharos.events.dlq", 0, 0); err != nil {
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
