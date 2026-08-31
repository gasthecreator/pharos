package edge

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gasthecreator/pharos/internal/model"
)

// CaptureResponse represents the response returned to local site systems upon successful local enqueuing.
type CaptureResponse struct {
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	LocalSeq       uint64    `json:"local_seq"`
	SiteID         string    `json:"site_id"`
	CreatedAt      time.Time `json:"created_at"`
}

// Server provides the local HTTP capture endpoint for trial site staff and EDC systems (§2.1).
type Server struct {
	store  QueueStore
	siteID string
}

// NewServer creates a new edge capture server.
func NewServer(store QueueStore, siteID string) *Server {
	return &Server{
		store:  store,
		siteID: strings.TrimSpace(siteID),
	}
}

// RegisterRoutes registers endpoints on the provided ServeMux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/adverse-events", s.HandleCapture)
	mux.HandleFunc("/api/v1/events", s.HandleCapture)
	mux.HandleFunc("/api/v1/stats", s.HandleStats)
	mux.HandleFunc("/healthz", s.HandleHealth)
}

// HandleHealth reports service health.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"UP"}`))
}

// HandleStats returns local queue operational metrics.
func (s *Server) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetStats(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

// HandleCapture accepts adverse event reports and persists them to SQLite WAL before acknowledging.
// Note: Per PLAN.md §2.3, this endpoint does NOT reject FHIR-malformed events; it durably buffers
// them to prevent local data loss. Central Ingestion performs validation and DLQ routing.
func (s *Server) HandleCapture(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil || len(bodyBytes) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "empty or unreadable request body"})
		return
	}

	var event model.AdverseEvent
	if err := json.Unmarshal(bodyBytes, &event); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "malformed JSON payload: " + err.Error()})
		return
	}

	// Resolve siteID
	siteID := s.siteID
	if siteID == "" {
		siteID = event.SiteID()
	}
	if siteID == "" {
		siteID = "UNKNOWN-SITE"
	}

	// Durably enqueue to local disk (SQLite WAL)
	record, err := s.store.Enqueue(r.Context(), siteID, &event)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to persist locally: " + err.Error()})
		return
	}

	resp := CaptureResponse{
		Status:         "QUEUED",
		IdempotencyKey: record.IdempotencyKey,
		LocalSeq:       record.LocalSeq,
		SiteID:         record.SiteID,
		CreatedAt:      record.CreatedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	mux.ServeHTTP(w, r)
}
