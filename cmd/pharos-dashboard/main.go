package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gasthecreator/pharos/internal/audit"
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
	enableChaos := flag.Bool("enable-chaos", false, "Enable the Chaos Control Panel's infrastructure-mutating actions (§2.4, Slice 23); off by default -- only ever enable this on a local/demo instance")
	ingestionMetricsURL := flag.String("ingestion-metrics-url", "http://localhost:8091/metrics", "Central Ingestion's /metrics endpoint, for the Correctness Ledger panel")
	consumerMetricsURL := flag.String("consumer-metrics-url", "http://localhost:9091/metrics", "pharos-consumer's /metrics endpoint, for the Correctness Ledger panel")
	flag.Parse()

	log.Printf("[pharos-dashboard] Starting on port %d...", *port)

	var svc query.Service
	var auditStore audit.Store
	if *useMemory {
		log.Println("[pharos-dashboard] Running with in-memory sample data (--memory)")
		svc = query.NewMemoryService()
		auditStore = audit.NewMemoryStore()
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

		// Read-side access-audit trail (§2.4, Slice 20/21: every view of real
		// adverse-event data through this dashboard, the same trail
		// pharos-cli's own --operator flag writes to). Falls back to a
		// process-local MemoryStore on connection failure rather than
		// failing closed: this is a compliance record, not the security
		// control itself (there is none here to begin with -- PLAN.md's own
		// explicit scope decision), so refusing to serve the dashboard over
		// an unreachable audit backend would be a disproportionate outage
		// for what it protects.
		auditCfg := audit.DefaultCassandraConfig()
		auditCfg.Hosts = strings.Split(*cassandraHosts, ",")
		auditCfg.Port = *cassandraPort
		auditCfg.Keyspace = *cassandraKeyspace
		if *caCert != "" {
			auditCfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}
		aStore, err := audit.NewCassandraStore(auditCfg)
		if err != nil {
			log.Printf("[pharos-dashboard] WARNING: access-audit store connection failed: %v. Falling back to a process-local MemoryStore (dashboard access-audit entries will not survive a restart).", err)
			auditStore = audit.NewMemoryStore()
		} else {
			auditStore = aStore
			log.Println("[pharos-dashboard] Read-side access-audit trail connected (§2.4, Slice 20/21).")
		}
	}
	defer svc.Close()
	defer auditStore.Close()

	chaosOpts := dashboard.ChaosOptions{
		Enabled:             *enableChaos,
		IngestionMetricsURL: *ingestionMetricsURL,
		ConsumerMetricsURL:  *consumerMetricsURL,
	}
	if *enableChaos {
		log.Println("[pharos-dashboard] WARNING: --enable-chaos is set -- the Chaos Control Panel can stop/restart real Cassandra nodes and partition dc-us/dc-eu. Only run this on a local/demo instance.")
	}
	handler, err := dashboard.NewHandler(svc, auditStore, *centralURL, *grafanaURL, *caCert, chaosOpts)
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
