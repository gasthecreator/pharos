package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gasthecreator/pharos/internal/audit"
)

// TestReadHandlers_RedirectToOperatorFormWithoutCookie proves every view of
// real adverse-event data (§2.4, Slice 21's own "Read-side audit
// asymmetry" writeup) refuses to run until an operator identity is set,
// redirecting to /operator with a next param pointing back at the original
// request -- mirroring pharos-cli's own --operator requirement, just
// captured once per browser instead of once per command.
func TestReadHandlers_RedirectToOperatorFormWithoutCookie(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	cases := []struct {
		name string
		path string
	}{
		{"index", "/"},
		{"query", "/query?type=event&id=SITE-DASH-01:1"},
		{"dlq list", "/dlq"},
		{"dlq detail", "/dlq/SITE-DASH-01:99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusFound {
				t.Fatalf("expected 302 redirect to /operator, got %d: %s", rec.Code, rec.Body.String())
			}
			loc := rec.Header().Get("Location")
			if !strings.HasPrefix(loc, "/operator?next=") {
				t.Fatalf("expected redirect to /operator?next=..., got %q", loc)
			}
			wantNext, _ := url.QueryUnescape(strings.TrimPrefix(loc, "/operator?next="))
			if wantNext != tc.path {
				t.Errorf("expected next=%q to point back at the original request, got %q", tc.path, wantNext)
			}
		})
	}
}

// TestOperatorSubmit_SetsCookieAndRedirects proves submitting the operator
// form sets a cookie (no Expires/MaxAge -- a session cookie, not a
// persistent login, per operatorCookieName's own docs) and redirects to
// the originally-requested page, which then renders normally.
func TestOperatorSubmit_SetsCookieAndRedirects(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	form := url.Values{"operator": {"alice@sponsor.example"}, "next": {"/query?type=event&id=SITE-DASH-01:1"}}
	req := httptest.NewRequest(http.MethodPost, "/operator", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect after setting operator, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/query?type=event&id=SITE-DASH-01:1" {
		t.Errorf("expected redirect back to the original next path, got %q", got)
	}

	cookies := rec.Result().Cookies()
	var opCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == operatorCookieName {
			opCookie = c
		}
	}
	if opCookie == nil {
		t.Fatalf("expected a %s cookie to be set, got cookies: %+v", operatorCookieName, cookies)
	}
	if opCookie.Value != "alice@sponsor.example" {
		t.Errorf("expected cookie value %q, got %q", "alice@sponsor.example", opCookie.Value)
	}
	if !opCookie.Expires.IsZero() || opCookie.MaxAge != 0 {
		t.Errorf("expected a session cookie (no Expires/MaxAge), got Expires=%v MaxAge=%d", opCookie.Expires, opCookie.MaxAge)
	}

	// Replay the request the cookie should now satisfy.
	req2 := httptest.NewRequest(http.MethodGet, "/query?type=event&id=SITE-DASH-01:1", nil)
	req2.AddCookie(opCookie)
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 once the operator cookie is present, got %d", rec2.Code)
	}
}

// TestOperatorSubmit_EmptyOperatorReRendersForm proves an empty operator
// name is rejected server-side (not just via the HTML `required` attribute,
// which a direct POST bypasses entirely) -- no cookie gets set, and no
// redirect happens.
func TestOperatorSubmit_EmptyOperatorReRendersForm(t *testing.T) {
	svc := seedMemoryService(t)
	mux := newTestMux(t, svc, "http://unused.example")

	form := url.Values{"operator": {"   "}, "next": {"/"}}
	req := httptest.NewRequest(http.MethodPost, "/operator", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (form re-rendered), got %d", rec.Code)
	}
	for _, c := range rec.Result().Cookies() {
		if c.Name == operatorCookieName {
			t.Fatalf("expected no operator cookie to be set for an empty/whitespace-only name, got %+v", c)
		}
	}
}

// TestSafeNextPath_RejectsOpenRedirects proves the next-path safety check
// this package's operator redirect flow depends on: an absolute or
// protocol-relative URL (the open-redirect shape) always falls back to "/",
// never passed through to http.Redirect verbatim.
func TestSafeNextPath_RejectsOpenRedirects(t *testing.T) {
	cases := map[string]string{
		"":                          "/",
		"/query?type=site&id=X":     "/query?type=site&id=X",
		"//evil.example.com":        "/",
		"http://evil.example.com/":  "/",
		"https://evil.example.com/": "/",
		"not-a-path":                "/",
	}
	for in, want := range cases {
		if got := safeNextPath(in); got != want {
			t.Errorf("safeNextPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestReadHandlers_RecordAccessAudit proves a real view of adverse-event
// data through the dashboard is durably recorded in the access-audit
// trail, keyed by the operator cookie -- the actual gap this closes (§2.4,
// Slice 21's "Read-side audit asymmetry" -- pharos-cli's equivalent
// commands already did this via --operator, the dashboard's own read views
// previously recorded nothing at all).
func TestReadHandlers_RecordAccessAudit(t *testing.T) {
	svc := seedMemoryService(t)
	auditStore := audit.NewMemoryStore()
	h, err := NewHandler(svc, auditStore, "http://unused.example", "http://localhost:3000", "", ChaosOptions{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	const operator = "bob@sponsor.example"
	cookie := &http.Cookie{Name: operatorCookieName, Value: operator}

	cases := []struct {
		name       string
		path       string
		wantAction string
		wantResrc  string
	}{
		{"recent events", "/", "LIST_RECENT", "ALL_SITES"},
		{"query event", "/query?type=event&id=SITE-DASH-01:1", "QUERY_EVENT", "SITE-DASH-01:1"},
		{"query site", "/query?type=site&id=SITE-DASH-01", "QUERY_SITE", "SITE-DASH-01"},
		{"dlq list all", "/dlq", "DLQ_LIST", "ALL_SITES"},
		{"dlq list by site", "/dlq?site=SITE-DASH-01", "DLQ_LIST", "SITE-DASH-01"},
		{"dlq detail", "/dlq/SITE-DASH-01:99", "DLQ_GET", "SITE-DASH-01:99"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.AddCookie(cookie)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d: %s", tc.name, rec.Code, rec.Body.String())
		}
	}

	entries, err := auditStore.ListByOperator(context.Background(), operator, 50)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != len(cases) {
		t.Fatalf("expected %d audit entries for operator %s, got %d: %+v", len(cases), operator, len(entries), entries)
	}
	// MemoryStore prepends (newest first); reverse to match request order.
	for i, tc := range cases {
		got := entries[len(entries)-1-i]
		if got.Action != tc.wantAction || got.Resource != tc.wantResrc {
			t.Errorf("%s: expected audit entry {Action:%s Resource:%s}, got {Action:%s Resource:%s}", tc.name, tc.wantAction, tc.wantResrc, got.Action, got.Resource)
		}
		if got.Outcome != "SUCCESS" {
			t.Errorf("%s: expected outcome SUCCESS, got %s", tc.name, got.Outcome)
		}
	}
}

// TestReadHandlers_RecordAccessAuditEvenOnError proves a failed lookup is
// still recorded -- "who tried to access what," not just successes,
// mirroring cmd/pharos-cli's recordAccess helper exactly.
func TestReadHandlers_RecordAccessAuditEvenOnError(t *testing.T) {
	svc := seedMemoryService(t)
	auditStore := audit.NewMemoryStore()
	h, err := NewHandler(svc, auditStore, "http://unused.example", "http://localhost:3000", "", ChaosOptions{})
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	const operator = "carol@sponsor.example"
	req := httptest.NewRequest(http.MethodGet, "/query?type=event&id=NOPE:999", nil)
	req.AddCookie(&http.Cookie{Name: operatorCookieName, Value: operator})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (error shown inline), got %d", rec.Code)
	}

	entries, err := auditStore.ListByOperator(context.Background(), operator, 50)
	if err != nil {
		t.Fatalf("ListByOperator failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 audit entry for the failed lookup, got %d: %+v", len(entries), entries)
	}
	if entries[0].Action != "QUERY_EVENT" || entries[0].Resource != "NOPE:999" {
		t.Errorf("unexpected audit entry: %+v", entries[0])
	}
	if !strings.HasPrefix(entries[0].Outcome, "ERROR:") {
		t.Errorf("expected an ERROR outcome for a not-found lookup, got %q", entries[0].Outcome)
	}
}
