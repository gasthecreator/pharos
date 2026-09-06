// Package dashboard implements pharos-dashboard's HTTP handlers (§2.4,
// PLAN.md Slice 21: web dashboard) -- a server-rendered presentation layer
// over internal/query.Service, the exact interface pharos-cli already uses,
// so this is new UI over already-built, already-tested query logic, not new
// business logic. Deliberately no authentication of its own (PLAN.md's own
// explicit scope decision: this doesn't change the project's risk posture,
// since it's a UI over data that's already reachable unauthenticated) --
// the one write path (DLQ replay, and submitting a test event) requires the
// human at the browser to supply the real owning site's own credentials
// per request; the dashboard never stores or remembers them.
//
// Every view of real adverse-event data (the recent-events feed, query,
// DLQ list/detail) now records an access-audit entry, keyed by a
// self-declared operator name captured once per browser via a session
// cookie -- closing the asymmetry with pharos-cli's own --operator
// requirement (§2.4, Slice 20) that PLAN.md's Slice 21 writeup originally
// left open, deliberately, pending exactly this. /submit and /chaos are not
// gated: submitting a test event already requires the real owning site's
// own credentials per request (a stronger, per-action accountability than
// operator self-declaration), and the chaos panel is an infrastructure
// action, not a view of patient data.
package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/internal/audit"
	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/query"
	"github.com/gasthecreator/pharos/internal/tlsutil"
)

// operatorCookieName holds the self-declared operator identity captured by
// handleOperatorForm. Deliberately a session cookie (no Max-Age/Expires):
// this is an audit-trail identity, not a "remember me" convenience, so it
// should not silently outlive the browser session it was entered in.
const operatorCookieName = "pharos_operator"

//go:embed templates/*.html
var templateFS embed.FS

// ChaosOptions configures the §2.4, PLAN.md Slice 23 Chaos Control Panel.
// The zero value (Enabled: false) disables it entirely -- chaos actions
// mutate real running infrastructure, so the dashboard must never expose
// them just because it happens to be reachable; this is Slice 23's own
// explicit safety guard, not an assumption left to the caller.
type ChaosOptions struct {
	Enabled             bool
	IngestionMetricsURL string
	ConsumerMetricsURL  string
}

// Handler serves every pharos-dashboard route.
type Handler struct {
	svc        query.Service
	auditStore audit.Store
	centralURL string
	grafanaURL string
	httpClient *http.Client
	templates  map[string]*template.Template

	chaos           ChaosOptions
	regionHealMu    sync.Mutex
	regionHealTimer *time.Timer
	// regionAutoHealOverride, if set, replaces regionPartitionAutoHeal's
	// fixed duration -- test-only (unexported, same-package tests can set
	// it directly), so a determinism/timeout test doesn't have to wait 60
	// real seconds for the production auto-heal window.
	regionAutoHealOverride time.Duration
}

// NewHandler constructs a dashboard Handler. caCert, if non-empty, is used
// to trust this project's own CA when proxying replay/submit requests to
// Central Ingestion over HTTPS -- the same tlsutil helper and the same
// requirement pharos-cli's own dlq replay path needs (§2.4, Slice 20 fixed
// this exact gap there; the dashboard's proxy path needs it for the same
// reason: every real deployment runs Central Ingestion behind this
// project's self-signed CA, not a publicly trusted one). auditStore records
// who viewed real adverse-event data through this dashboard, the same
// trail pharos-cli's own --operator flag writes to.
func NewHandler(svc query.Service, auditStore audit.Store, centralURL, grafanaURL, caCert string, chaos ChaosOptions) (*Handler, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	if caCert != "" {
		tlsCfg, err := (tlsutil.ClientConfig{CACertPath: caCert, ServerName: "localhost"}).StdTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to load --ca-cert: %w", err)
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	templates, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	return &Handler{
		svc:        svc,
		auditStore: auditStore,
		centralURL: strings.TrimRight(centralURL, "/"),
		grafanaURL: grafanaURL,
		httpClient: client,
		templates:  templates,
		chaos:      chaos,
	}, nil
}

// loadTemplates parses each page template together with layout.html into
// its own *template.Template -- each page defines blocks named "title" and
// "content"; keeping every page in a separate template.Template (rather
// than one shared set) avoids those same-named block definitions from
// colliding with each other, since html/template's block/define names are
// only scoped within one template.Template.
func loadTemplates() (map[string]*template.Template, error) {
	pages := []string{"index.html", "query.html", "dlq_list.html", "dlq_detail.html", "submit.html", "chaos.html", "operator.html"}
	out := make(map[string]*template.Template, len(pages))
	for _, page := range pages {
		t, err := template.ParseFS(templateFS, "templates/layout.html", "templates/"+page)
		if err != nil {
			return nil, fmt.Errorf("failed to parse template %s: %w", page, err)
		}
		out[page] = t
	}
	return out, nil
}

func (h *Handler) render(w http.ResponseWriter, page string, data any) {
	t, ok := h.templates[page]
	if !ok {
		http.Error(w, "template not found: "+page, http.StatusInternalServerError)
		return
	}
	// Render into a buffer first: ExecuteTemplate can fail partway through
	// (e.g. a nil-pointer field access from unexpected data shape), and by
	// then http.Error/WriteHeader would be too late -- buffering means a
	// template bug surfaces as a clean 500, not a truncated page with no
	// indication anything went wrong.
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		http.Error(w, fmt.Sprintf("template render failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// RegisterRoutes wires every dashboard route onto mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", h.handleIndex)
	mux.HandleFunc("GET /query", h.handleQuery)
	mux.HandleFunc("GET /dlq", h.handleDLQList)
	mux.HandleFunc("GET /dlq/{key}", h.handleDLQDetail)
	mux.HandleFunc("POST /dlq/{key}/replay", h.handleDLQReplay)
	mux.HandleFunc("GET /submit", h.handleSubmitForm)
	mux.HandleFunc("POST /submit", h.handleSubmitPost)
	mux.HandleFunc("GET /healthz", h.handleHealth)
	mux.HandleFunc("GET /operator", h.handleOperatorForm)
	mux.HandleFunc("POST /operator", h.handleOperatorSubmit)

	// Chaos Control Panel (§2.4, PLAN.md Slice 23) -- registered
	// unconditionally so /chaos always exists and clearly reports itself as
	// disabled rather than 404ing (less confusing than "page not found" for
	// an operator who forgot --enable-chaos); every ACTION handler still
	// individually refuses to do anything unless h.chaos.Enabled, so the
	// safety guard doesn't depend on this registration choice.
	mux.HandleFunc("GET /chaos", h.handleChaosPanel)
	mux.HandleFunc("POST /chaos/cassandra", h.handleChaosCassandra)
	mux.HandleFunc("POST /chaos/region/partition", h.handleChaosRegionPartition)
	mux.HandleFunc("POST /chaos/region/heal", h.handleChaosRegionHeal)
	mux.HandleFunc("POST /chaos/edge/partition", h.handleChaosEdgePartition)
	mux.HandleFunc("POST /chaos/edge/heal", h.handleChaosEdgeHeal)
	mux.HandleFunc("POST /chaos/duplicate", h.handleChaosDuplicate)
	mux.HandleFunc("POST /chaos/skew", h.handleChaosSkew)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// currentOperator returns the self-declared operator identity captured for
// this browser, or "" if none has been set yet.
func (h *Handler) currentOperator(r *http.Request) string {
	c, err := r.Cookie(operatorCookieName)
	if err != nil {
		return ""
	}
	return c.Value
}

// safeNextPath restricts an operator-form redirect target to a same-origin
// relative path -- next comes from a query/form value a caller controls, so
// accepting an absolute URL (e.g. "//evil.example.com") would turn this
// into an open redirect. Anything that doesn't look like a plain local
// path falls back to "/".
func safeNextPath(next string) string {
	if next == "" || !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		return "/"
	}
	return next
}

// requireOperator redirects to the operator-identification form when this
// browser has no operator cookie yet, returning ok=false so the caller
// stops handling the current request. next is the original request the
// caller should return to once an operator is set.
func (h *Handler) requireOperator(w http.ResponseWriter, r *http.Request) (operator string, ok bool) {
	operator = h.currentOperator(r)
	if operator != "" {
		return operator, true
	}
	http.Redirect(w, r, "/operator?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
	return "", false
}

// recordAccess writes one entry to the access-audit trail (§2.4, Slice 20)
// for a real view of adverse-event data through the dashboard -- the exact
// same trail, and the same "record even on error" reasoning, as
// cmd/pharos-cli's own recordAccess helper: an operator who successfully
// viewed real data should still see it even if the audit write itself had
// a transient problem, and a failed lookup is itself worth recording (who
// tried to access what, not just who succeeded).
func (h *Handler) recordAccess(ctx context.Context, operator, action, resource string, err error) {
	outcome := "SUCCESS"
	if err != nil {
		outcome = "ERROR: " + err.Error()
	}
	if auditErr := h.auditStore.RecordAccess(ctx, audit.AccessAudit{
		Operator: operator,
		Action:   action,
		Resource: resource,
		Outcome:  outcome,
	}); auditErr != nil {
		log.Printf("[pharos-dashboard] WARNING: failed to record access-audit entry for %s %s: %v", action, resource, auditErr)
	}
}

// operatorData mirrors what operator.html actually renders. Operator is
// deliberately left unset (unlike every other page's data type) -- the
// "Viewing as X · switch" banner layout.html renders from it would be
// redundant while already on the identify-yourself page itself.
type operatorData struct {
	GrafanaURL      string
	Operator        string
	CurrentOperator string
	Next            string
}

func (h *Handler) handleOperatorForm(w http.ResponseWriter, r *http.Request) {
	h.render(w, "operator.html", operatorData{
		GrafanaURL:      h.grafanaURL,
		CurrentOperator: h.currentOperator(r),
		Next:            safeNextPath(r.URL.Query().Get("next")),
	})
}

func (h *Handler) handleOperatorSubmit(w http.ResponseWriter, r *http.Request) {
	operator := strings.TrimSpace(r.FormValue("operator"))
	next := safeNextPath(r.FormValue("next"))
	if operator == "" {
		h.render(w, "operator.html", operatorData{
			GrafanaURL: h.grafanaURL,
			Next:       next,
		})
		return
	}
	// Deliberately no Expires/MaxAge (see operatorCookieName's own docs):
	// this is an audit identity, not a persistent login.
	http.SetCookie(w, &http.Cookie{
		Name:     operatorCookieName,
		Value:    operator,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, next, http.StatusFound)
}

// indexData mirrors what index.html actually renders.
type indexData struct {
	GrafanaURL string
	Operator   string
	Events     []*consumer.CanonicalRecord
}

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	// "/" as a stdlib ServeMux pattern is a subtree match, catching every
	// otherwise-unmatched path (e.g. /favicon.ico), not just the literal
	// root -- reject anything else with a real 404 rather than silently
	// rendering the events feed for a typo'd URL.
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	operator, ok := h.requireOperator(w, r)
	if !ok {
		return
	}
	events, err := h.svc.ListRecentEvents(r.Context(), 100)
	h.recordAccess(r.Context(), operator, "LIST_RECENT", "ALL_SITES", err)
	if err != nil {
		http.Error(w, "failed to load recent events: "+err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "index.html", indexData{GrafanaURL: h.grafanaURL, Operator: operator, Events: events})
}

// queryData mirrors what query.html actually renders.
type queryData struct {
	GrafanaURL   string
	Operator     string
	Type, ID     string
	From, To     string
	MinSeq       string
	Submitted    bool
	Error        string
	SingleRecord *consumer.CanonicalRecord
	Records      []*consumer.CanonicalRecord
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	operator, ok := h.requireOperator(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	data := queryData{
		GrafanaURL: h.grafanaURL,
		Operator:   operator,
		Type:       q.Get("type"),
		ID:         q.Get("id"),
		From:       q.Get("from"),
		To:         q.Get("to"),
		MinSeq:     q.Get("min_seq"),
	}
	if data.Type == "" {
		data.Type = "study"
	}
	if data.ID == "" {
		h.render(w, "query.html", data)
		return
	}
	data.Submitted = true
	ctx := r.Context()

	switch data.Type {
	case "event":
		rec, err := h.svc.GetEvent(ctx, data.ID)
		h.recordAccess(ctx, operator, "QUERY_EVENT", data.ID, err)
		if err != nil {
			data.Error = "Event not found: " + err.Error()
		} else {
			data.SingleRecord = rec
		}

	case "site":
		minSeq := int64(1)
		if data.MinSeq != "" {
			if parsed, err := strconv.ParseInt(data.MinSeq, 10, 64); err == nil {
				minSeq = parsed
			} else {
				data.Error = "Invalid min_seq: " + err.Error()
				h.render(w, "query.html", data)
				return
			}
		}
		records, err := h.svc.GetEventsBySite(ctx, data.ID, minSeq)
		h.recordAccess(ctx, operator, "QUERY_SITE", data.ID, err)
		if err != nil {
			data.Error = "Query failed: " + err.Error()
		} else {
			data.Records = records
		}

	case "study":
		now := time.Now().UTC()
		startTime := now.Add(-30 * 24 * time.Hour)
		endTime := now
		if data.From != "" {
			parsed, err := time.Parse(time.RFC3339, data.From)
			if err != nil {
				data.Error = "Invalid --from time: " + err.Error()
				h.render(w, "query.html", data)
				return
			}
			startTime = parsed
		}
		if data.To != "" {
			parsed, err := time.Parse(time.RFC3339, data.To)
			if err != nil {
				data.Error = "Invalid --to time: " + err.Error()
				h.render(w, "query.html", data)
				return
			}
			endTime = parsed
		}
		records, err := h.svc.GetEventsByStudy(ctx, data.ID, startTime, endTime)
		h.recordAccess(ctx, operator, "QUERY_STUDY", data.ID, err)
		if err != nil {
			data.Error = "Query failed: " + err.Error()
		} else {
			data.Records = records
		}

	default:
		data.Error = "Unknown query type: " + data.Type
	}

	h.render(w, "query.html", data)
}

// dlqListData mirrors what dlq_list.html actually renders.
type dlqListData struct {
	GrafanaURL string
	Operator   string
	SiteID     string
	Error      string
	Records    []*query.DLQRecord
}

func (h *Handler) handleDLQList(w http.ResponseWriter, r *http.Request) {
	operator, ok := h.requireOperator(w, r)
	if !ok {
		return
	}
	siteID := r.URL.Query().Get("site")
	data := dlqListData{GrafanaURL: h.grafanaURL, Operator: operator, SiteID: siteID}

	var records []*query.DLQRecord
	var err error
	if siteID != "" {
		records, err = h.svc.ListDLQEventsBySite(r.Context(), siteID, 100)
	} else {
		records, err = h.svc.ListAllDLQEvents(r.Context(), 100)
	}
	listResource := siteID
	if listResource == "" {
		listResource = "ALL_SITES"
	}
	h.recordAccess(r.Context(), operator, "DLQ_LIST", listResource, err)
	if err != nil {
		data.Error = "DLQ query failed: " + err.Error()
	} else {
		data.Records = records
	}
	h.render(w, "dlq_list.html", data)
}

// dlqDetailData mirrors what dlq_detail.html actually renders.
type dlqDetailData struct {
	GrafanaURL   string
	Operator     string
	Record       *query.DLQRecord
	ReplayResult string
	ReplayError  bool
}

func (h *Handler) handleDLQDetail(w http.ResponseWriter, r *http.Request) {
	operator, ok := h.requireOperator(w, r)
	if !ok {
		return
	}
	key := r.PathValue("key")
	rec, err := h.svc.GetDLQEvent(r.Context(), key)
	h.recordAccess(r.Context(), operator, "DLQ_GET", key, err)
	if err != nil {
		http.Error(w, "DLQ event not found: "+err.Error(), http.StatusNotFound)
		return
	}
	h.render(w, "dlq_detail.html", dlqDetailData{GrafanaURL: h.grafanaURL, Operator: operator, Record: rec})
}

// dlqReplayResponse mirrors internal/ingestion.Handler.HandleDLQReplay's
// JSON response shape -- kept as its own type, matching this project's
// established per-package view-type pattern (cmd/pharos-cli/main.go's own
// dlqReplayResponse does the same, for the same reason) rather than
// importing internal/ingestion.
type dlqReplayResponse struct {
	IdempotencyKey string `json:"idempotency_key"`
	Status         string `json:"status"`
	Error          string `json:"error"`
}

func (h *Handler) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	siteID := strings.TrimSpace(r.FormValue("site_id"))
	apiKey := r.FormValue("api_key")

	rec, err := h.svc.GetDLQEvent(r.Context(), key)
	if err != nil {
		http.Error(w, "DLQ event not found: "+err.Error(), http.StatusNotFound)
		return
	}

	data := dlqDetailData{GrafanaURL: h.grafanaURL, Record: rec}
	if siteID == "" || apiKey == "" {
		data.ReplayResult = "Replay requires both Site ID and API Key."
		data.ReplayError = true
		h.render(w, "dlq_detail.html", data)
		return
	}

	result, statusCode, err := h.replayDLQRecord(r.Context(), key, siteID, apiKey)
	if err != nil {
		data.ReplayResult = "Replay request failed: " + err.Error()
		data.ReplayError = true
	} else if statusCode == http.StatusOK {
		data.ReplayResult = fmt.Sprintf("REPLAYED: %s is now %s", key, result.Status)
		// Re-fetch: the record this page shows is now REPLAYED, and the
		// "PUBLISHED only" replay form should disappear on this render.
		if refreshed, refreshErr := h.svc.GetDLQEvent(r.Context(), key); refreshErr == nil {
			data.Record = refreshed
		}
	} else {
		data.ReplayResult = fmt.Sprintf("Still rejected (HTTP %d): %s", statusCode, result.Error)
		data.ReplayError = true
	}
	h.render(w, "dlq_detail.html", data)
}

// replayDLQRecord proxies to Central Ingestion's real DLQ replay endpoint
// (§2.3, Slice 10), sending X-Site-ID/X-API-Key exactly as pharos-cli's
// own (fixed in §2.4, Slice 20) replayDLQRecord does -- the dashboard holds
// no standing credential, forwarding only what this one request supplied.
func (h *Handler) replayDLQRecord(ctx context.Context, idempotencyKey, siteID, apiKey string) (*dlqReplayResponse, int, error) {
	url := h.centralURL + "/api/v1/dlq/" + idempotencyKey + "/replay"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-Site-ID", siteID)
	req.Header.Set("X-API-Key", apiKey)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("could not reach Central Ingestion at %s: %w", h.centralURL, err)
	}
	defer resp.Body.Close()

	var result dlqReplayResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to decode replay response: %w", err)
	}
	return &result, resp.StatusCode, nil
}

// submitData mirrors what submit.html actually renders.
type submitData struct {
	GrafanaURL string
	// Operator is always left unset -- /submit is deliberately not
	// operator-gated (see this package's own doc comment for why), so
	// layout.html's "Viewing as" banner never renders here.
	Operator    string
	SiteID      string
	Payload     string
	Result      string
	ResultError bool
}

func (h *Handler) handleSubmitForm(w http.ResponseWriter, r *http.Request) {
	h.render(w, "submit.html", submitData{GrafanaURL: h.grafanaURL, Payload: defaultSamplePayload})
}

func (h *Handler) handleSubmitPost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	siteID := strings.TrimSpace(r.FormValue("site_id"))
	apiKey := r.FormValue("api_key")
	payload := r.FormValue("payload")

	data := submitData{GrafanaURL: h.grafanaURL, SiteID: siteID, Payload: payload}

	if siteID == "" || apiKey == "" || strings.TrimSpace(payload) == "" {
		data.Result = "Site ID, API Key, and event JSON are all required."
		data.ResultError = true
		h.render(w, "submit.html", data)
		return
	}
	if !json.Valid([]byte(payload)) {
		data.Result = "Event JSON is not valid JSON."
		data.ResultError = true
		h.render(w, "submit.html", data)
		return
	}

	statusCode, respBody, err := h.submitRawEvent(r.Context(), siteID, apiKey, []byte(payload))
	if err != nil {
		data.Result = "Could not reach Central Ingestion at " + h.centralURL + ": " + err.Error()
		data.ResultError = true
		h.render(w, "submit.html", data)
		return
	}

	if statusCode == http.StatusOK {
		data.Result = fmt.Sprintf("HTTP %d:\n%s", statusCode, respBody)
	} else {
		data.Result = fmt.Sprintf("HTTP %d:\n%s", statusCode, respBody)
		data.ResultError = true
	}
	h.render(w, "submit.html", data)
}

// submitRawEvent POSTs one FHIR AdverseEvent payload to Central Ingestion's
// real /api/v1/events endpoint, wrapped in the real BatchRequest shape --
// shared by handleSubmitPost and the Chaos Control Panel's duplicate/skew
// actions (§2.4, Slice 23), all of which are "submit this exact event
// through the real pipeline" at heart, differing only in what constructs
// payload and how many times it's called.
func (h *Handler) submitRawEvent(ctx context.Context, siteID, apiKey string, payload []byte) (statusCode int, body string, err error) {
	reqBody, err := json.Marshal(struct {
		SiteID string            `json:"site_id"`
		Events []json.RawMessage `json:"events"`
	}{SiteID: siteID, Events: []json.RawMessage{json.RawMessage(payload)}})
	if err != nil {
		return 0, "", fmt.Errorf("failed to build request: %w", err)
	}

	url := h.centralURL + "/api/v1/events"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return 0, "", fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Site-ID", siteID)
	req.Header.Set("X-API-Key", apiKey)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody), nil
}

const defaultSamplePayload = `{
  "resourceType": "AdverseEvent",
  "id": "ae-example-1",
  "identifier": [{"system": "urn:pharos:idempotency-key", "value": "SITE-US-01:1"}],
  "actuality": "actual",
  "subject": {"reference": "Patient/P-100"},
  "event": {"coding": [{"system": "http://hl7.org/fhir/sid/meddra", "code": "10002198", "display": "Anaphylaxis"}], "text": "Anaphylaxis"},
  "date": "2026-08-30T00:00:00Z",
  "recordedDate": "2026-08-30T00:01:00Z",
  "severity": {"coding": [{"code": "severe"}]},
  "study": [{"reference": "ResearchStudy/PHAROS-01"}],
  "location": {"reference": "Location/SITE-US-01"}
}`
