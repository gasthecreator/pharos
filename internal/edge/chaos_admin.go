package edge

import (
	"encoding/json"
	"net/http"
)

// RegisterChaosAdminRoutes wires the Slice 23 Chaos Control Panel's
// site-partition action onto mux. Callers (cmd/pharos-edge) MUST only call
// this when the operator has explicitly opted in (--enable-chaos) -- these
// routes let any caller who can reach this port cut the site off from
// Central Ingestion, which is exactly why this project's dashboard (the
// only intended caller) itself requires the equivalent --enable-chaos
// opt-in before it will ever call these.
func RegisterChaosAdminRoutes(mux *http.ServeMux, client *ChaosClient) {
	mux.HandleFunc("POST /admin/chaos/partition", func(w http.ResponseWriter, r *http.Request) {
		client.SetPartitioned(true)
		writeChaosStatus(w, client)
	})
	mux.HandleFunc("POST /admin/chaos/heal", func(w http.ResponseWriter, r *http.Request) {
		client.SetPartitioned(false)
		writeChaosStatus(w, client)
	})
	mux.HandleFunc("GET /admin/chaos/status", func(w http.ResponseWriter, r *http.Request) {
		writeChaosStatus(w, client)
	})
}

func writeChaosStatus(w http.ResponseWriter, client *ChaosClient) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"partitioned": client.IsPartitioned()})
}
