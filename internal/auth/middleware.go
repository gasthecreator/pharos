package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type contextKey string

const siteIDContextKey contextKey = "pharos-authenticated-site-id"

// SiteIDFromContext returns the site_id authenticated by RequireAPIKey for
// this request, and whether one was present. Handlers should use this --
// not a client-supplied header -- for anything security-relevant, since a
// client-supplied X-Site-ID has no proof of ownership behind it on its own.
func SiteIDFromContext(ctx context.Context) (string, bool) {
	siteID, ok := ctx.Value(siteIDContextKey).(string)
	return siteID, ok
}

// RequireAPIKey wraps next with per-site API key authentication (§2.1, §2.2,
// ARCHITECTURE_PROPOSALS.md "Slice 15: Auth & TLS"): a request must carry
// both X-Site-ID (the site it's claiming to be) and X-API-Key (proof of
// that claim). VerifyKey does a single point lookup keyed by the claimed
// site_id, so there's no separate reverse index from key to site to keep
// consistent -- the claimed site_id IS the lookup key, and the key either
// matches that site's stored hash or it doesn't.
//
// Deliberately returns the same 401 body for "unknown site," "missing
// key," and "wrong key" -- distinguishing them would let a caller enumerate
// valid site IDs by trying each one and watching which error changes.
func RequireAPIKey(store KeyStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			siteID := strings.TrimSpace(r.Header.Get("X-Site-ID"))
			apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
			if siteID == "" || apiKey == "" {
				writeUnauthorized(w)
				return
			}

			ok, err := store.VerifyKey(r.Context(), siteID, apiKey)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "authentication service unavailable"})
				return
			}
			if !ok {
				writeUnauthorized(w)
				return
			}

			ctx := context.WithValue(r.Context(), siteIDContextKey, siteID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `APIKey realm="pharos"`)
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing or invalid X-Site-ID/X-API-Key"})
}
