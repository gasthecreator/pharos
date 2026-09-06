package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gasthecreator/pharos/internal/dashboard"
	"github.com/gasthecreator/pharos/internal/query"
	"github.com/gasthecreator/pharos/internal/tlsutil"
)

func main() {
	port := flag.Int("port", 8092, "HTTP listen port for the dashboard itself")
	cassandraHosts := flag.String("cassandra-hosts", "127.0.0.1", "Comma-separated Cassandra host addresses")
	cassandraPort := flag.Int("cassandra-port", 9042, "Cassandra port")
	cassandraKeyspace := flag.String("cassandra-keyspace", "pharos", "Cassandra keyspace")
	caCert := flag.String("ca-cert", tlsutil.DefaultCACertPath(), "CA certificate file for verifying TLS connections to Cassandra and Central Ingestion (§2.4, Slice 15)")
	centralURL := flag.String("central-url", "http://localhost:8091", "Central Ingestion base URL, for DLQ replay and test-event submission")
	grafanaURL := flag.String("grafana-url", "http://localhost:3000", "Grafana base URL to link out to for system health (§2.4, Slice 6); empty hides the link")
	useMemory := flag.Bool("memory", false, "Run with in-memory sample data for offline demo (no Cassandra required)")
	flag.Parse()

	log.Printf("[pharos-dashboard] Starting on port %d...", *port)

	var svc query.Service
	if *useMemory {
		log.Println("[pharos-dashboard] Running with in-memory sample data (--memory)")
		svc = query.NewMemoryService()
	} else {
		cfg := query.DefaultCassandraServiceConfig()
		cfg.Hosts = strings.Split(*cassandraHosts, ",")
		cfg.Port = *cassandraPort
		cfg.Keyspace = *cassandraKeyspace
		if *caCert != "" {
			cfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}
		cSvc, err := query.NewCassandraService(cfg)
		if err != nil {
			log.Fatalf("[pharos-dashboard] Error connecting to Cassandra: %v (hint: pass --memory to run offline with sample data)", err)
		}
		svc = cSvc
		log.Printf("[pharos-dashboard] Connected to Cassandra at %s:%d (keyspace: %s)", *cassandraHosts, *cassandraPort, *cassandraKeyspace)
	}
	defer svc.Close()

	handler, err := dashboard.NewHandler(svc, *centralURL, *grafanaURL, *caCert)
	if err != nil {
		log.Fatalf("[pharos-dashboard] Failed to initialize handler: %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Printf("[pharos-dashboard] Ready on http://localhost:%d/", *port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[pharos-dashboard] HTTP server failed: %v", err)
		os.Exit(1)
	}
}
