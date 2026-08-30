package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gasthecreator/pharos/pkg/dedup"
	"github.com/gasthecreator/pharos/pkg/ingestion"
	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/metrics"
	"github.com/gasthecreator/pharos/pkg/ratelimit"
)

func main() {
	port := flag.Int("port", 8081, "HTTP listen port")
	rateLimitCap := flag.Float64("rate-limit-capacity", 100, "Per-site token bucket burst capacity")
	rateLimitRefill := flag.Float64("rate-limit-refill", 10, "Per-site token refill rate (tokens/sec)")
	cassandraHosts := flag.String("cassandra-hosts", "127.0.0.1", "Comma-separated Cassandra host addresses")
	cassandraPort := flag.Int("cassandra-port", 9042, "Cassandra port")
	cassandraKeyspace := flag.String("cassandra-keyspace", "pharos", "Cassandra keyspace")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "Comma-separated Kafka broker addresses")
	leaseTimeout := flag.Duration("lease-timeout", 30*time.Second, "Outbox publishing claim lease timeout")
	sweeperInterval := flag.Duration("sweeper-interval", 10*time.Second, "Background outbox sweeper interval")
	useMemoryStore := flag.Bool("use-memory-store", false, "Use in-memory outbox store and mock producer (for testing without Docker)")
	flag.Parse()

	log.Printf("[pharos-ingestion] Starting Central Ingestion Service on port %d...", *port)

	// 1. Initialize per-site token bucket rate limiter (§2.3)
	limiter := ratelimit.NewTokenBucketLimiter(*rateLimitCap, *rateLimitRefill)

	// 2. Initialize Cassandra Outbox Store & Kafka Producer (§2.2, §2.3)
	var outboxStore dedup.OutboxStore
	var producer kafka.Producer

	if *useMemoryStore {
		log.Println("[pharos-ingestion] Running with MemoryOutboxStore and MockProducer (standalone mode)")
		outboxStore = dedup.NewMemoryOutboxStore()
		producer = kafka.NewMockProducer()
	} else {
		hosts := strings.Split(*cassandraHosts, ",")
		cCfg := dedup.DefaultCassandraConfig()
		cCfg.Hosts = hosts
		cCfg.Port = *cassandraPort
		cCfg.Keyspace = *cassandraKeyspace

		log.Printf("[pharos-ingestion] Connecting to Cassandra at %s:%d (keyspace: %s)...", hosts[0], *cassandraPort, *cassandraKeyspace)
		cStore, err := dedup.NewCassandraOutboxStore(cCfg)
		if err != nil {
			log.Printf("[pharos-ingestion] WARNING: Cassandra connection failed: %v. Falling back to MemoryOutboxStore.", err)
			outboxStore = dedup.NewMemoryOutboxStore()
		} else {
			outboxStore = cStore
			log.Println("[pharos-ingestion] Cassandra Outbox Store connected and schemas ensured.")
		}

		brokers := strings.Split(*kafkaBrokers, ",")
		if err := kafka.EnsureTopics(context.Background(), brokers, kafka.DefaultTopicConfigs()); err != nil {
			log.Printf("[pharos-ingestion] WARNING: Could not ensure Kafka topic retention configs: %v", err)
		} else {
			log.Printf("[pharos-ingestion] Kafka topics ensured with regulatory retention policies (§4).")
		}
		kCfg := kafka.DefaultConfig(brokers)
		producer = kafka.NewWriterProducer(kCfg)
		log.Printf("[pharos-ingestion] Kafka producer configured for brokers %v", brokers)
	}

	// 3. Initialize background sweeper (§2.2)
	sweeper := ingestion.NewSweeper(outboxStore, producer, *sweeperInterval, *leaseTimeout)
	sweeperCtx, stopSweeper := context.WithCancel(context.Background())
	sweeper.Start(sweeperCtx)
	log.Printf("[pharos-ingestion] Background outbox sweeper started (interval: %s, lease timeout: %s)", *sweeperInterval, *leaseTimeout)

	// 4. Initialize ingestion HTTP handler
	handler := ingestion.NewHandlerWithOutbox(limiter, outboxStore, producer, *leaseTimeout)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	mux.Handle("/metrics", metrics.Handler())

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

	// 5. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Printf("[pharos-ingestion] Shutting down gracefully...")

	stopSweeper()
	sweeper.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[pharos-ingestion] Error during HTTP shutdown: %v", err)
	}

	if producer != nil {
		_ = producer.Close()
	}
	if outboxStore != nil {
		_ = outboxStore.Close()
	}

	accepted, rejected, throttled, dedupHits, dlqCount := handler.ExtendedStats()
	log.Printf("[pharos-ingestion] Session totals: %d accepted (%d duplicates deduped), %d rejected (%d routed to DLQ), %d throttled",
		accepted, dedupHits, rejected, dlqCount, throttled)
	log.Printf("[pharos-ingestion] Shutdown complete.")
}
