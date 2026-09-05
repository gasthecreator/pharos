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

	"github.com/gasthecreator/pharos/internal/edge"
	"github.com/gasthecreator/pharos/internal/metrics"
	"github.com/gasthecreator/pharos/internal/tlsutil"
)

func main() {
	siteID := flag.String("site-id", "SITE-NG-01", "Clinical trial site identifier")
	dbPath := flag.String("db-path", "pharos-edge.db", "Path to embedded SQLite database")
	port := flag.Int("port", 8080, "Local HTTP capture server port")
	centralURL := flag.String("central-url", "http://localhost:8081/api/v1/events", "Central Ingestion API URL")
	batchSize := flag.Int("batch-size", 50, "Batch size for upstream forwarding")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "Forwarder queue poll interval")
	backupPath := flag.String("backup-path", "", "Path for periodic SQLite backups (§2.1, Slice 12); empty disables backup/restore")
	backupInterval := flag.Duration("backup-interval", 5*time.Minute, "Interval between periodic SQLite backups")
	apiKey := flag.String("api-key", "", "This site's API key for Central Ingestion (§2.1, §2.2, Slice 15); required unless Central Ingestion runs with --enable-auth=false")
	caCert := flag.String("ca-cert", "", "CA certificate file for verifying Central Ingestion's TLS certificate (§2.1, Slice 15); required if --central-url is https://")
	flag.Parse()

	log.Printf("[pharos-edge] Initializing edge collector for site: %s (db: %s)", *siteID, *dbPath)

	// 0. Restore from backup if the primary database is missing but a backup
	// exists (§2.1, Slice 12) -- must happen before the store is opened, since
	// opening a nonexistent path in WAL mode immediately creates an empty file.
	if err := edge.RestoreFromBackupIfMissing(*dbPath, *backupPath); err != nil {
		log.Fatalf("[pharos-edge] Failed to restore from backup: %v", err)
	}

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
	fwdCfg.APIKey = *apiKey
	if *apiKey == "" {
		log.Printf("[pharos-edge] WARNING: --api-key not set -- requests to Central Ingestion will carry no X-API-Key header.")
	}

	var httpClient edge.HTTPClient
	if *caCert != "" {
		tlsCfg, err := (tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}).StdTLSConfig()
		if err != nil {
			log.Fatalf("[pharos-edge] Failed to load CA cert for Central Ingestion TLS: %v", err)
		}
		httpClient = &http.Client{
			Timeout:   fwdCfg.RequestTimeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		}
	}

	forwarder := edge.NewForwarder(store, httpClient, fwdCfg)
	go func() {
		log.Printf("[pharos-edge] Forwarder worker active -> streaming to %s", *centralURL)
		if err := forwarder.Run(ctx); err != nil && err != context.Canceled {
			log.Printf("[pharos-edge] Forwarder worker error: %v", err)
		}
	}()

	// Periodic queue-depth gauge (Slice 6 — §4): a proxy for how long this site
	// has been effectively partitioned, per §2.1's store-and-forward design.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats, err := store.GetStats(ctx)
				if err != nil {
					continue
				}
				metrics.EdgeQueueDepth.Set(float64(stats.PendingCount))
				if !stats.OldestPendingTime.IsZero() {
					metrics.EdgeQueueOldestPendingSeconds.Set(time.Since(stats.OldestPendingTime).Seconds())
				} else {
					metrics.EdgeQueueOldestPendingSeconds.Set(0)
				}
			}
		}
	}()

	// Periodic SQLite backup (§2.1, Slice 12): bounds the residual data-loss
	// exposure window from Slice 12's design to "whatever changed since the
	// last successful backup," in case the primary disk fails outright.
	if *backupPath != "" {
		go func() {
			log.Printf("[pharos-edge] Periodic backup active -> %s every %s", *backupPath, *backupInterval)
			ticker := time.NewTicker(*backupInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := store.Backup(ctx, *backupPath); err != nil {
						log.Printf("[pharos-edge] Backup failed: %v", err)
					}
				}
			}
		}()
	} else {
		log.Printf("[pharos-edge] Periodic backup disabled (no --backup-path set)")
	}

	// 3. Start local HTTP capture server for site staff/EDC systems
	edgeServer := edge.NewServer(store, *siteID)
	mux := http.NewServeMux()
	edgeServer.RegisterRoutes(mux)
	mux.Handle("/metrics", metrics.Handler())

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
