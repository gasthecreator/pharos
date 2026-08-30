package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	port := flag.Int("port", 8080, "HTTP listen port")
	flag.Parse()

	log.Printf("[pharos-ingestion] Starting Central Ingestion Service on port %d...", *port)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("[pharos-ingestion] Service ready on :%d. Press Ctrl+C to terminate.\n", *port)
	<-sigChan

	log.Printf("[pharos-ingestion] Shutting down gracefully...")
}
