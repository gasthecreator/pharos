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

	"github.com/gasthecreator/pharos/pkg/ingestion"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

func main() {
	port := flag.Int("port", 8081, "HTTP listen port")
	rateLimitCap := flag.Float64("rate-limit-capacity", 100, "Per-site token bucket burst capacity")
	rateLimitRefill := flag.Float64("rate-limit-refill", 10, "Per-site token refill rate (tokens/sec)")
	flag.Parse()

	log.Printf("[pharos-ingestion] Starting Central Ingestion Service on port %d...", *port)

	// 1. Initialize per-site token bucket rate limiter (§2.3)
	limiter := ratelimit.NewTokenBucketLimiter(*rateLimitCap, *rateLimitRefill)

	// 2. Initialize ingestion HTTP handler
	handler := ingestion.NewHandler(limiter)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[pharos-ingestion] Central Ingestion ready on http://localhost:%d/api/v1/events", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[pharos-ingestion] HTTP server failed: %v", err)
		}
	}()

	// 3. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Printf("[pharos-ingestion] Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[pharos-ingestion] Error during shutdown: %v", err)
	}

	accepted, rejected, throttled := handler.Stats()
	log.Printf("[pharos-ingestion] Session totals: %d accepted, %d rejected, %d throttled", accepted, rejected, throttled)
	log.Printf("[pharos-ingestion] Shutdown complete.")
}
