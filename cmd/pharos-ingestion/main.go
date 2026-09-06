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

	"github.com/gasthecreator/pharos/internal/audit"
	"github.com/gasthecreator/pharos/internal/auth"
	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/ingestion"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/metrics"
	"github.com/gasthecreator/pharos/internal/ratelimit"
	"github.com/gasthecreator/pharos/internal/tlsutil"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	port := flag.Int("port", 8081, "HTTP listen port")
	rateLimitCap := flag.Float64("rate-limit-capacity", 100, "Per-site token bucket burst capacity")
	rateLimitRefill := flag.Float64("rate-limit-refill", 10, "Per-site token refill rate (tokens/sec)")
	redisAddr := flag.String("redis-addr", "", "Redis address (host:port) for a distributed rate limiter shared across instances (§2.3, §2.4, Slice 19); empty uses an in-process limiter, correct only for a single instance")
	cassandraHosts := flag.String("cassandra-hosts", "127.0.0.1", "Comma-separated Cassandra host addresses")
	cassandraPort := flag.Int("cassandra-port", 9042, "Cassandra port")
	cassandraKeyspace := flag.String("cassandra-keyspace", "pharos", "Cassandra keyspace")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9095", "Comma-separated Kafka broker addresses")
	leaseTimeout := flag.Duration("lease-timeout", 30*time.Second, "Outbox publishing claim lease timeout")
	sweeperInterval := flag.Duration("sweeper-interval", 10*time.Second, "Background outbox sweeper interval")
	useMemoryStore := flag.Bool("use-memory-store", false, "Use in-memory outbox store and mock producer (for testing without Docker)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (enables HTTPS; requires --tls-key too)")
	tlsKey := flag.String("tls-key", "", "TLS private key file (enables HTTPS; requires --tls-cert too)")
	caCert := flag.String("ca-cert", "", "CA certificate file for verifying TLS connections to Cassandra (§2.4, Slice 15)")
	enableAuth := flag.Bool("enable-auth", true, "Require per-site API key authentication on event submission and DLQ replay (§2.1, §2.2, Slice 15); always off with --use-memory-store")
	flag.Parse()

	log.Printf("[pharos-ingestion] Starting Central Ingestion Service on port %d...", *port)

	// 1. Initialize per-site token bucket rate limiter (§2.3) -- Redis-backed
	// and shared across every instance when --redis-addr is set (§2.4,
	// Slice 19: Multi-instance scaling); an in-process bucket is only
	// correct for exactly one instance, since each additional instance
	// behind a load balancer would otherwise enforce the configured limit
	// independently, silently multiplying a site's real limit.
	var limiter ratelimit.RateLimiter
	if *redisAddr != "" {
		redisClient := goredis.NewClient(&goredis.Options{Addr: *redisAddr})
		redisLimiter, err := ratelimit.NewRedisLimiter(context.Background(), redisClient, *rateLimitCap, *rateLimitRefill)
		if err != nil {
			log.Fatalf("[pharos-ingestion] Failed to initialize Redis rate limiter: %v", err)
		}
		limiter = redisLimiter
		log.Printf("[pharos-ingestion] Distributed rate limiting via Redis at %s (shared across instances)", *redisAddr)
	} else {
		limiter = ratelimit.NewTokenBucketLimiter(*rateLimitCap, *rateLimitRefill)
		log.Printf("[pharos-ingestion] WARNING: in-process rate limiting only (--redis-addr not set) -- correct for a single instance only")
	}

	// 2. Initialize Cassandra Outbox Store & Kafka Producer (§2.2, §2.3)
	var outboxStore dedup.OutboxStore
	var producer kafka.Producer
	var keyStore auth.KeyStore
	var auditStore audit.Store

	if *useMemoryStore {
		log.Println("[pharos-ingestion] Running with MemoryOutboxStore and MockProducer (standalone mode)")
		outboxStore = dedup.NewMemoryOutboxStore()
		producer = kafka.NewMockProducer()
		auditStore = audit.NewMemoryStore()
	} else {
		hosts := strings.Split(*cassandraHosts, ",")
		cCfg := dedup.DefaultCassandraConfig()
		cCfg.Hosts = hosts
		cCfg.Port = *cassandraPort
		cCfg.Keyspace = *cassandraKeyspace
		if *caCert != "" {
			cCfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}

		log.Printf("[pharos-ingestion] Connecting to Cassandra at %s:%d (keyspace: %s)...", hosts[0], *cassandraPort, *cassandraKeyspace)
		cStore, err := dedup.NewCassandraOutboxStore(cCfg)
		if err != nil {
			log.Printf("[pharos-ingestion] WARNING: Cassandra connection failed: %v. Falling back to MemoryOutboxStore.", err)
			outboxStore = dedup.NewMemoryOutboxStore()
		} else {
			outboxStore = cStore
			log.Println("[pharos-ingestion] Cassandra Outbox Store connected and schemas ensured.")
		}

		if *enableAuth {
			authCfg := auth.DefaultCassandraConfig()
			authCfg.Hosts = hosts
			authCfg.Port = *cassandraPort
			authCfg.Keyspace = *cassandraKeyspace
			if *caCert != "" {
				authCfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
			}
			ks, err := auth.NewCassandraKeyStore(authCfg)
			if err != nil {
				// Fail closed, not open: a security feature that's silently
				// disabled when its backing store is unreachable is worse
				// than the process refusing to start (§2.1, §2.2, Slice 15).
				log.Fatalf("[pharos-ingestion] --enable-auth is set but the API key store could not be reached: %v", err)
			}
			keyStore = ks
			log.Println("[pharos-ingestion] Per-site API key authentication ENABLED on event submission and DLQ replay.")
		} else {
			log.Println("[pharos-ingestion] WARNING: --enable-auth=false -- any caller can submit events or replay DLQ records as any site.")
		}

		// DLQ replay access-audit trail (§2.4, Slice 20: "who replayed what,"
		// same trail pharos-cli's query/dlq-inspection commands write to).
		// Falls back to a process-local MemoryStore on connection failure
		// rather than failing closed like the API key store above: this is a
		// compliance record for an already-guarded action (the actual
		// security check is the auth middleware's site-ownership match),
		// not the security control itself, so refusing to serve any
		// ingestion traffic over an unreachable audit backend would be a
		// disproportionate outage for what it protects.
		auditCfg := audit.DefaultCassandraConfig()
		auditCfg.Hosts = hosts
		auditCfg.Port = *cassandraPort
		auditCfg.Keyspace = *cassandraKeyspace
		if *caCert != "" {
			auditCfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}
		aStore, err := audit.NewCassandraStore(auditCfg)
		if err != nil {
			log.Printf("[pharos-ingestion] WARNING: access-audit store connection failed: %v. Falling back to a process-local MemoryStore (DLQ replay audit entries will not survive a restart).", err)
			auditStore = audit.NewMemoryStore()
		} else {
			auditStore = aStore
			log.Println("[pharos-ingestion] DLQ replay access-audit trail connected (§2.4, Slice 20).")
		}

		brokers := strings.Split(*kafkaBrokers, ",")
		var kafkaTLS *tlsutil.ClientConfig
		if *caCert != "" {
			kafkaTLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}
		if err := kafka.EnsureTopicsTLS(context.Background(), brokers, kafka.DefaultTopicConfigs(), kafkaTLS); err != nil {
			log.Printf("[pharos-ingestion] WARNING: Could not ensure Kafka topic retention configs: %v", err)
		} else {
			log.Printf("[pharos-ingestion] Kafka topics ensured with regulatory retention policies (§4).")
		}
		kCfg := kafka.DefaultConfig(brokers)
		kCfg.TLS = kafkaTLS
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
	if keyStore != nil {
		handler.SetKeyStore(keyStore)
	}
	if auditStore != nil {
		handler.SetAuditStore(auditStore)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	mux.Handle("/metrics", metrics.Handler())

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	useTLS := *tlsCert != "" && *tlsKey != ""
	go func() {
		var err error
		if useTLS {
			log.Printf("[pharos-ingestion] Central Ingestion ready on https://localhost:%d/api/v1/events", *port)
			err = httpServer.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			log.Printf("[pharos-ingestion] WARNING: --tls-cert/--tls-key not set -- serving plaintext HTTP.")
			log.Printf("[pharos-ingestion] Central Ingestion ready on http://localhost:%d/api/v1/events", *port)
			err = httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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
	if keyStore != nil {
		_ = keyStore.Close()
	}
	if auditStore != nil {
		_ = auditStore.Close()
	}

	accepted, rejected, throttled, dedupHits, dlqCount := handler.ExtendedStats()
	log.Printf("[pharos-ingestion] Session totals: %d accepted (%d duplicates deduped), %d rejected (%d routed to DLQ), %d throttled",
		accepted, dedupHits, rejected, dlqCount, throttled)
	log.Printf("[pharos-ingestion] Shutdown complete.")
}
