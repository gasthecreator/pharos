package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gasthecreator/pharos/internal/chaos"
)

// regionPartitionAutoHeal bounds how long a dc-us/dc-eu partition triggered
// from the panel can stay applied without an operator manually healing it
// (§2.4, PLAN.md Slice 23's own "explicit safety guard, not an assumption"):
// a forgotten browser tab must not leave the real cluster partitioned
// indefinitely.
const regionPartitionAutoHeal = 60 * time.Second

// chaosData mirrors what chaos.html actually renders.
type chaosData struct {
	GrafanaURL string
	// Operator is always left unset -- /chaos is deliberately not
	// operator-gated (see internal/dashboard's own package doc for why), so
	// layout.html's "Viewing as" banner never renders here.
	Operator string
	Enabled  bool

	Ledger       *Ledger
	LedgerFailed bool

	Result      string
	ResultError bool

	CassandraContainers []string
	Container           string
	ContainerAction     string

	EdgeAdminURL string

	SiteID  string
	Payload string

	SkewInput string
}

func (h *Handler) newChaosData(ctx context.Context) chaosData {
	data := chaosData{
		GrafanaURL:          h.grafanaURL,
		Enabled:             h.chaos.Enabled,
		CassandraContainers: chaos.CassandraContainers,
		Payload:             defaultSamplePayload,
	}
	if h.chaos.IngestionMetricsURL != "" || h.chaos.ConsumerMetricsURL != "" {
		data.Ledger = FetchLedger(ctx, h.httpClient, h.chaos.IngestionMetricsURL, h.chaos.ConsumerMetricsURL)
		if !data.Ledger.IngestionReachable && !data.Ledger.ConsumerReachable {
			data.LedgerFailed = true
		}
	}
	return data
}

func (h *Handler) handleChaosPanel(w http.ResponseWriter, r *http.Request) {
	h.render(w, "chaos.html", h.newChaosData(r.Context()))
}

// requireChaosEnabled renders the panel with a clear "disabled" result and
// returns false if h.chaos.Enabled is false -- called first by every action
// handler below, so the --enable-chaos gate is enforced uniformly rather
// than repeated ad hoc per action.
func (h *Handler) requireChaosEnabled(w http.ResponseWriter, r *http.Request) bool {
	if h.chaos.Enabled {
		return true
	}
	data := h.newChaosData(r.Context())
	data.Result = "Chaos actions are disabled. Restart pharos-dashboard with --enable-chaos to use this panel."
	data.ResultError = true
	h.render(w, "chaos.html", data)
	return false
}

func chaosCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 30*time.Second)
}

func (h *Handler) handleChaosCassandra(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	container := r.FormValue("container")
	action := r.FormValue("action")

	data := h.newChaosData(r.Context())
	data.Container, data.ContainerAction = container, action

	valid := false
	for _, c := range chaos.CassandraContainers {
		if c == container {
			valid = true
			break
		}
	}
	if !valid {
		data.Result = fmt.Sprintf("Unknown container %q.", container)
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	ctx, cancel := chaosCtx(r)
	defer cancel()

	var err error
	switch action {
	case "stop":
		err = chaos.StopContainer(ctx, container)
	case "start":
		err = chaos.StartContainer(ctx, container)
	case "restart":
		err = chaos.RestartContainer(ctx, container)
	default:
		err = fmt.Errorf("unknown action %q (expected stop/start/restart)", action)
	}
	if err != nil {
		data.Result = "Action failed: " + err.Error()
		data.ResultError = true
	} else {
		data.Result = fmt.Sprintf("%s: %s succeeded.", container, action)
	}
	h.render(w, "chaos.html", data)
}

func (h *Handler) handleChaosRegionPartition(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	data := h.newChaosData(r.Context())

	ctx, cancel := chaosCtx(r)
	defer cancel()

	if err := chaos.PartitionRegions(ctx, chaos.DCUSContainers, chaos.DCEUContainers); err != nil {
		data.Result = "Partition failed: " + err.Error()
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	h.scheduleRegionAutoHeal()
	data.Result = fmt.Sprintf("dc-us/dc-eu partitioned. Auto-heals in %s if not healed manually first.", regionPartitionAutoHeal)
	h.render(w, "chaos.html", data)
}

func (h *Handler) handleChaosRegionHeal(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	data := h.newChaosData(r.Context())

	h.cancelRegionAutoHeal()

	ctx, cancel := chaosCtx(r)
	defer cancel()
	all := append(append([]string{}, chaos.DCUSContainers...), chaos.DCEUContainers...)
	if err := chaos.HealRegionPartition(ctx, all); err != nil {
		data.Result = "Heal failed: " + err.Error()
		data.ResultError = true
	} else {
		data.Result = "dc-us/dc-eu partition healed."
	}
	h.render(w, "chaos.html", data)
}

func (h *Handler) autoHealDuration() time.Duration {
	if h.regionAutoHealOverride > 0 {
		return h.regionAutoHealOverride
	}
	return regionPartitionAutoHeal
}

func (h *Handler) scheduleRegionAutoHeal() {
	h.regionHealMu.Lock()
	defer h.regionHealMu.Unlock()
	if h.regionHealTimer != nil {
		h.regionHealTimer.Stop()
	}
	h.regionHealTimer = time.AfterFunc(h.autoHealDuration(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		all := append(append([]string{}, chaos.DCUSContainers...), chaos.DCEUContainers...)
		_ = chaos.HealRegionPartition(ctx, all)
	})
}

func (h *Handler) cancelRegionAutoHeal() {
	h.regionHealMu.Lock()
	defer h.regionHealMu.Unlock()
	if h.regionHealTimer != nil {
		h.regionHealTimer.Stop()
		h.regionHealTimer = nil
	}
}

// edgeAdminRequest POSTs to a real running pharos-edge instance's
// /admin/chaos/* route (internal/edge/chaos_admin.go, also gated behind
// that process's own --enable-chaos flag) -- the dashboard holds no
// standing knowledge of which edges exist, so the operator supplies the
// target edge's own admin base URL per action, the same "human supplies
// the specific target per request" pattern the replay/submit actions
// already use for site credentials.
func (h *Handler) edgeAdminRequest(ctx context.Context, adminURL, path string) (int, string, error) {
	url := strings.TrimRight(adminURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	return resp.StatusCode, "", nil
}

func (h *Handler) handleChaosEdgePartition(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	adminURL := strings.TrimSpace(r.FormValue("edge_admin_url"))
	data := h.newChaosData(r.Context())
	data.EdgeAdminURL = adminURL

	if adminURL == "" {
		data.Result = "Edge admin URL is required."
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	ctx, cancel := chaosCtx(r)
	defer cancel()
	status, _, err := h.edgeAdminRequest(ctx, adminURL, "/admin/chaos/partition")
	if err != nil {
		data.Result = "Could not reach edge admin at " + adminURL + ": " + err.Error()
		data.ResultError = true
	} else if status != http.StatusOK {
		data.Result = fmt.Sprintf("Edge returned HTTP %d (is --enable-chaos set on that pharos-edge?)", status)
		data.ResultError = true
	} else {
		data.Result = "Site partitioned: this edge can no longer reach Central Ingestion until healed (its store-and-forward queue keeps buffering locally, §2.1)."
	}
	h.render(w, "chaos.html", data)
}

func (h *Handler) handleChaosEdgeHeal(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	adminURL := strings.TrimSpace(r.FormValue("edge_admin_url"))
	data := h.newChaosData(r.Context())
	data.EdgeAdminURL = adminURL

	if adminURL == "" {
		data.Result = "Edge admin URL is required."
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	ctx, cancel := chaosCtx(r)
	defer cancel()
	status, _, err := h.edgeAdminRequest(ctx, adminURL, "/admin/chaos/heal")
	if err != nil {
		data.Result = "Could not reach edge admin at " + adminURL + ": " + err.Error()
		data.ResultError = true
	} else if status != http.StatusOK {
		data.Result = fmt.Sprintf("Edge returned HTTP %d", status)
		data.ResultError = true
	} else {
		data.Result = "Site healed: this edge's forwarder will resume draining its buffered queue."
	}
	h.render(w, "chaos.html", data)
}

// handleChaosDuplicate submits the identical event twice in a row -- the
// exact real mechanism internal/ingestion/outbox_test.go's own
// TestSequentialDuplicateIdempotency already proves at the unit level,
// exercised here against the real running Central Ingestion instead.
func (h *Handler) handleChaosDuplicate(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	siteID := strings.TrimSpace(r.FormValue("site_id"))
	apiKey := r.FormValue("api_key")
	payload := r.FormValue("payload")

	data := h.newChaosData(r.Context())
	data.SiteID, data.Payload = siteID, payload

	if siteID == "" || apiKey == "" || strings.TrimSpace(payload) == "" {
		data.Result = "Site ID, API Key, and event JSON are all required."
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	ctx, cancel := chaosCtx(r)
	defer cancel()

	status1, body1, err := h.submitRawEvent(ctx, siteID, apiKey, []byte(payload))
	if err != nil {
		data.Result = "First submission failed to reach Central Ingestion: " + err.Error()
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}
	status2, body2, err := h.submitRawEvent(ctx, siteID, apiKey, []byte(payload))
	if err != nil {
		data.Result = "Second (duplicate) submission failed to reach Central Ingestion: " + err.Error()
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	data.Result = fmt.Sprintf(
		"First submission: HTTP %d\n%s\n\nDuplicate (identical payload) submission: HTTP %d\n%s\n\nBoth accepted (HTTP 200) with no duplicate Kafka publish is the expected, correct outcome -- the second is deduped by the outbox's claim/lease mechanism (§2.2), not silently dropped or double-processed.",
		status1, body1, status2, body2,
	)
	h.render(w, "chaos.html", data)
}

// handleChaosSkew submits one real event whose clinical date/recordedDate
// are deliberately offset from "now" by the requested skew (parsed as a Go
// duration, e.g. "-2h30m" for a site whose clock runs 2.5 hours behind) --
// exercising the real late-arrival/watermark path live (§2.4) rather than
// only in internal/consumer's own hand-written and property tests.
func (h *Handler) handleChaosSkew(w http.ResponseWriter, r *http.Request) {
	if !h.requireChaosEnabled(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}
	siteID := strings.TrimSpace(r.FormValue("site_id"))
	apiKey := r.FormValue("api_key")
	skewInput := strings.TrimSpace(r.FormValue("skew"))

	data := h.newChaosData(r.Context())
	data.SiteID, data.SkewInput = siteID, skewInput

	if siteID == "" || apiKey == "" || skewInput == "" {
		data.Result = "Site ID, API Key, and skew duration are all required."
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}
	skew, err := time.ParseDuration(skewInput)
	if err != nil {
		data.Result = "Invalid skew duration (expected a Go duration like -2h30m or 90m): " + err.Error()
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}

	eventTime := time.Now().UTC().Add(skew)
	idKey := fmt.Sprintf("%s:%d", siteID, time.Now().UnixNano())
	payload := fmt.Sprintf(`{
		"resourceType": "AdverseEvent",
		"id": "ae-skew-test",
		"identifier": [{"system": "urn:pharos:idempotency-key", "value": %q}],
		"actuality": "actual",
		"subject": {"reference": "Patient/P-100"},
		"event": {"coding": [{"system": "http://hl7.org/fhir/sid/meddra", "code": "10002198", "display": "Anaphylaxis"}], "text": "Anaphylaxis"},
		"date": %q,
		"recordedDate": %q,
		"severity": {"coding": [{"code": "moderate"}]},
		"study": [{"reference": "ResearchStudy/PHAROS-CHAOS"}],
		"location": {"reference": "Location/%s"}
	}`, idKey, eventTime.Format(time.RFC3339), eventTime.Format(time.RFC3339), siteID)
	data.Payload = payload

	ctx, cancel := chaosCtx(r)
	defer cancel()
	status, body, err := h.submitRawEvent(ctx, siteID, apiKey, []byte(payload))
	if err != nil {
		data.Result = "Could not reach Central Ingestion: " + err.Error()
		data.ResultError = true
		h.render(w, "chaos.html", data)
		return
	}
	data.Result = fmt.Sprintf("Submitted with event time skewed %s from now (%s): HTTP %d\n%s\n\nCheck the Correctness Ledger's late-arrival count, or query this event (%s) once consumed, to see the watermark/late-arrival path react.", skewInput, eventTime.Format(time.RFC3339), status, body, idKey)
	h.render(w, "chaos.html", data)
}
