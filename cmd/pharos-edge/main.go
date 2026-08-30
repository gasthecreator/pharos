package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gasthecreator/pharos/pkg/edge"
)

func main() {
	siteID := flag.String("site-id", "SITE-NG-01", "Clinical trial site identifier")
	dbPath := flag.String("db-path", "pharos-edge.db", "Path to embedded SQLite database")
	port := flag.Int("port", 8080, "Local HTTP capture server port")
	centralURL := flag.String("central-url", "http://localhost:8081/api/v1/events", "Central Ingestion API URL")
	batchSize := flag.Int("batch-size", 50, "Batch size for upstream forwarding")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "Forwarder queue poll interval")
	flag.Parse()

	log.Printf("[pharos-edge] Initializing edge collector for site: %s (db: %s)", *siteID, *dbPath)

	// 1. Initialize embedded SQLite WAL queue store (§2.1)
	store, err := edge.NewSQLiteStore(*dbPath)
	if err != nil {
		log.Fatalf("[pharos-edge] Failed to initialize SQLite store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("[pharos-edge] Error closing SQLite store: %v", err)
		}
	}()

	stats, err := store.GetStats(context.Background())
	if err != nil {
		log.Printf("[pharos-edge] Warning: failed to query stats: %v", err)
	} else {
		log.Printf("[pharos-edge] Queue initialized: %d pending, %d in-flight, %d acknowledged, max_seq=%d",
			stats.PendingCount, stats.InFlightCount, stats.AcknowledgedCount, stats.MaxSequence)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Start edge forwarder worker in background (§2.1)
	fwdCfg := edge.DefaultForwarderConfig(*centralURL, *siteID)
	fwdCfg.BatchSize = *batchSize
	fwdCfg.PollInterval = *pollInterval

	forwarder := edge.NewForwarder(store, nil, fwdCfg)
	go func() {
		log.Printf("[pharos-edge] Forwarder worker active -> streaming to %s", *centralURL)
		if err := forwarder.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("[pharos-edge] Forwarder worker error: %v", err)
		}
	}()

	// 3. Start local HTTP capture server for site staff/EDC systems
	edgeServer := edge.NewServer(store, *siteID)
	mux := http.NewServeMux()
	edgeServer.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[pharos-edge] HTTP capture endpoint listening on http://localhost:%d/api/v1/adverse-events", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[pharos-edge] HTTP server failed: %v", err)
		}
	}()

	// 4. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Printf("[pharos-edge] Shutting down gracefully...")

	// Cancel forwarder context
	cancel()

	// Shutdown HTTP server with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[pharos-edge] Error during HTTP shutdown: %v", err)
	}

	log.Printf("[pharos-edge] Shutdown complete.")
}
