package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestProtectedHandler(t *testing.T) (KeyStore, http.Handler) {
	t.Helper()
	store := NewMemoryKeyStore()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		siteID, ok := SiteIDFromContext(r.Context())
		if !ok {
			t.Fatalf("expected an authenticated site_id in context once RequireAPIKey let the request through")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(siteID))
	})
	return store, RequireAPIKey(store)(inner)
}

func TestRequireAPIKey_ValidKeyPasses(t *testing.T) {
	store, handler := newTestProtectedHandler(t)
	ctx := context.Background()
	plaintext, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	req.Header.Set("X-Site-ID", "SITE-001")
	req.Header.Set("X-API-Key", plaintext)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for a valid key, got %d (body: %s)", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "SITE-001" {
		t.Errorf("expected the authenticated site_id to be SITE-001, got %q", rec.Body.String())
	}
}

func TestRequireAPIKey_MissingHeadersRejected(t *testing.T) {
	_, handler := newTestProtectedHandler(t)

	for _, tc := range []struct {
		name   string
		siteID string
		apiKey string
	}{
		{"both missing", "", ""},
		{"missing site id", "", "some-key"},
		{"missing api key", "SITE-001", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
			if tc.siteID != "" {
				req.Header.Set("X-Site-ID", tc.siteID)
			}
			if tc.apiKey != "" {
				req.Header.Set("X-API-Key", tc.apiKey)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", rec.Code)
			}
		})
	}
}

func TestRequireAPIKey_WrongKeyRejected(t *testing.T) {
	store, handler := newTestProtectedHandler(t)
	ctx := context.Background()
	if _, err := store.CreateKey(ctx, "SITE-001"); err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	req.Header.Set("X-Site-ID", "SITE-001")
	req.Header.Set("X-API-Key", "wrong-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a wrong key, got %d", rec.Code)
	}
}

func TestRequireAPIKey_UnknownSiteAndWrongKeyGetIdenticalResponses(t *testing.T) {
	// Deliberately checks the responses are indistinguishable: a caller must
	// not be able to enumerate valid site IDs by noticing a different error
	// for "unknown site" versus "wrong key for a real site".
	store, handler := newTestProtectedHandler(t)
	ctx := context.Background()
	if _, err := store.CreateKey(ctx, "SITE-REAL"); err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}

	doRequest := func(siteID string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
		req.Header.Set("X-Site-ID", siteID)
		req.Header.Set("X-API-Key", "wrong-key")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	unknownSiteResp := doRequest("SITE-NEVER-EXISTED")
	wrongKeyResp := doRequest("SITE-REAL")

	if unknownSiteResp.Code != http.StatusUnauthorized || wrongKeyResp.Code != http.StatusUnauthorized {
		t.Fatalf("expected both to be 401, got %d and %d", unknownSiteResp.Code, wrongKeyResp.Code)
	}
	if unknownSiteResp.Body.String() != wrongKeyResp.Body.String() {
		t.Errorf("expected identical response bodies for unknown-site vs wrong-key (to avoid site enumeration), got %q vs %q",
			unknownSiteResp.Body.String(), wrongKeyResp.Body.String())
	}
}

func TestRequireAPIKey_RevokedKeyRejected(t *testing.T) {
	store, handler := newTestProtectedHandler(t)
	ctx := context.Background()
	plaintext, err := store.CreateKey(ctx, "SITE-001")
	if err != nil {
		t.Fatalf("CreateKey failed: %v", err)
	}
	if err := store.RevokeKey(ctx, "SITE-001"); err != nil {
		t.Fatalf("RevokeKey failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	req.Header.Set("X-Site-ID", "SITE-001")
	req.Header.Set("X-API-Key", plaintext)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a revoked key, got %d", rec.Code)
	}
}

// erroringKeyStore simulates the key store's backing infrastructure being
// unreachable, to prove RequireAPIKey degrades to 503 (retryable) rather
// than either crashing or, worse, treating a store error as "authenticated".
type erroringKeyStore struct{ KeyStore }

func (erroringKeyStore) VerifyKey(ctx context.Context, siteID, plaintextKey string) (bool, error) {
	return false, errors.New("simulated backing store outage")
}

func TestRequireAPIKey_StoreErrorReturns503NotAuthenticated(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("inner handler must not be reached when the key store errors")
	})
	handler := RequireAPIKey(erroringKeyStore{})(inner)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	req.Header.Set("X-Site-ID", "SITE-001")
	req.Header.Set("X-API-Key", "some-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when the key store errors, got %d", rec.Code)
	}
}
