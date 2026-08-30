package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gasthecreator/pharos/pkg/edge"
)

func main() {
	siteID := flag.String("site-id", "SITE-NG-01", "Clinical trial site identifier")
	dbPath := flag.String("db-path", "pharos-edge.db", "Path to embedded SQLite database")
	flag.Parse()

	log.Printf("[pharos-edge] Starting edge collector for site: %s (db: %s)", *siteID, *dbPath)

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
		log.Printf("[pharos-edge] Warning: failed to read queue stats: %v", err)
	} else {
		log.Printf("[pharos-edge] Queue initialized: %d pending, %d in-flight, %d acknowledged, max_seq=%d",
			stats.PendingCount, stats.InFlightCount, stats.AcknowledgedCount, stats.MaxSequence)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("[pharos-edge] Ready. Press Ctrl+C to terminate.\n")
	<-sigChan

	log.Printf("[pharos-edge] Shutting down gracefully...")
}
