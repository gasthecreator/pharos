package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gasthecreator/pharos/internal/consumer"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/metrics"
	"github.com/gasthecreator/pharos/internal/tlsutil"
)

func main() {
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9095", "Comma-separated Kafka broker addresses")
	metricsPort := flag.Int("metrics-port", 9091, "Port to serve /metrics and /healthz on")
	kafkaTopic := flag.String("kafka-topic", kafka.MainTopic, "Kafka topic to consume")
	kafkaGroup := flag.String("kafka-group", "pharos-canonical-sink", "Kafka consumer group ID")
	cassandraHosts := flag.String("cassandra-hosts", "127.0.0.1", "Comma-separated Cassandra host addresses")
	cassandraPort := flag.Int("cassandra-port", 9042, "Cassandra port")
	cassandraKeyspace := flag.String("cassandra-keyspace", "pharos", "Cassandra keyspace")
	caCert := flag.String("ca-cert", "", "CA certificate file for verifying TLS connections to Cassandra (§2.4, Slice 15)")
	latenessTolerance := flag.Duration("lateness-tolerance", 15*time.Minute, "Event-time bounded lateness tolerance")
	idleTimeout := flag.Duration("idle-timeout", 10*time.Minute, "Idle partition exclusion threshold")
	useMemoryStore := flag.Bool("use-memory-store", false, "Use in-memory canonical store (for standalone testing)")
	flag.Parse()

	log.Printf("[pharos-consumer] Starting Downstream Canonical Consumer on group %s (topic: %s)...", *kafkaGroup, *kafkaTopic)

	// 1. Initialize Cassandra Canonical Store (§2.4, §5)
	var store consumer.CanonicalStore
	if *useMemoryStore {
		log.Println("[pharos-consumer] Running with MemoryCanonicalStore (standalone mode)")
		store = consumer.NewMemoryCanonicalStore()
	} else {
		hosts := strings.Split(*cassandraHosts, ",")
		cCfg := consumer.DefaultCassandraStoreConfig()
		cCfg.Hosts = hosts
		cCfg.Port = *cassandraPort
		cCfg.Keyspace = *cassandraKeyspace
		if *caCert != "" {
			cCfg.TLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
		}

		log.Printf("[pharos-consumer] Connecting to Cassandra at %s:%d (keyspace: %s)...", hosts[0], *cassandraPort, *cassandraKeyspace)
		cStore, err := consumer.NewCassandraCanonicalStore(cCfg)
		if err != nil {
			log.Printf("[pharos-consumer] WARNING: Cassandra connection failed: %v. Falling back to MemoryCanonicalStore.", err)
			store = consumer.NewMemoryCanonicalStore()
		} else {
			store = cStore
			log.Println("[pharos-consumer] Cassandra Canonical Store connected and query schemas ensured.")
		}
	}

	// 2. Initialize Watermark Tracker (§2.4), restoring from a prior
	// checkpoint if one exists for this consumer group (§2.4, Slice 13) --
	// must happen before the engine consumes anything, so the strict
	// monotonic guard's floor is in place before any live event is processed.
	tracker := consumer.NewWatermarkTracker(*latenessTolerance, *idleTimeout)
	checkpointCtx, checkpointCancel := context.WithTimeout(context.Background(), 10*time.Second)
	checkpoint, err := store.LoadWatermarkCheckpoint(checkpointCtx, *kafkaGroup)
	checkpointCancel()
	if err != nil {
		log.Printf("[pharos-consumer] WARNING: failed to load watermark checkpoint: %v (starting with an empty tracker)", err)
	} else if checkpoint != nil {
		tracker.Restore(*checkpoint)
		log.Printf("[pharos-consumer] Watermark tracker restored from checkpoint (group: %s, previous_emitted: %s)",
			*kafkaGroup, checkpoint.PreviousEmitted.Format(time.RFC3339))
	} else {
		log.Printf("[pharos-consumer] No prior watermark checkpoint for group %s (fresh consumer group)", *kafkaGroup)
	}
	log.Printf("[pharos-consumer] Watermark tracker initialized (lateness: %s, idle timeout: %s)", *latenessTolerance, *idleTimeout)

	// 3. Initialize Kafka Consumer Engine
	brokers := strings.Split(*kafkaBrokers, ",")
	var kafkaTLS *tlsutil.ClientConfig
	if *caCert != "" {
		kafkaTLS = &tlsutil.ClientConfig{CACertPath: *caCert, ServerName: "localhost"}
	}
	if !*useMemoryStore {
		_ = kafka.EnsureTopicsTLS(context.Background(), brokers, kafka.DefaultTopicConfigs(), kafkaTLS)
	}
	engineCfg := consumer.DefaultEngineConfig(brokers)
	engineCfg.Topic = *kafkaTopic
	engineCfg.GroupID = *kafkaGroup
	engineCfg.LatenessTolerance = *latenessTolerance
	engineCfg.IdleTimeout = *idleTimeout
	if kafkaTLS != nil {
		engineCfg.TLS = kafkaTLS
	}

	reader, err := consumer.NewKafkaReader(engineCfg)
	if err != nil {
		log.Fatalf("[pharos-consumer] Failed to build Kafka reader: %v", err)
	}
	engine := consumer.NewEngine(reader, store, tracker, engineCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Start consumption worker
	go func() {
		log.Println("[pharos-consumer] Consumer engine listening for adverse event messages...")
		if err := engine.Run(ctx); err != nil && err != context.Canceled {
			log.Fatalf("[pharos-consumer] Engine failed: %v", err)
		}
	}()

	// 5. Metrics/health HTTP server (Slice 6 — §4)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	metricsServer := &http.Server{Addr: fmt.Sprintf(":%d", *metricsPort), Handler: metricsMux}
	go func() {
		log.Printf("[pharos-consumer] Metrics endpoint listening on http://localhost:%d/metrics", *metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[pharos-consumer] Metrics server error: %v", err)
		}
	}()

	// 6. Periodic status telemetry — also feeds the watermark/lag/partition-activity gauges (Slice 6)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := engine.Stats()
				now := time.Now().UTC()
				wm := tracker.CurrentWatermark(now)
				log.Printf("[pharos-consumer] Status: consumed=%d committed=%d late=%d errors=%d watermark=%s",
					stats.ConsumedCount, stats.CommittedCount, stats.LateEventsCount, stats.ErrorCount,
					wm.Format(time.RFC3339))

				if !wm.IsZero() {
					metrics.ConsumerWatermarkSeconds.Set(float64(wm.Unix()))
				}
				metrics.ConsumerLag.Set(float64(reader.Stats().Lag))
				for _, ps := range tracker.PartitionStats(now) {
					active := 0.0
					if ps.IsActive {
						active = 1.0
					}
					metrics.ConsumerPartitionActive.WithLabelValues(strconv.Itoa(ps.Partition)).Set(active)
				}
			}
		}
	}()

	// 6b. Periodic watermark checkpoint (§2.4, Slice 13): bounds the residual
	// exposure window (how much watermark progress a crash could still lose)
	// to "whatever advanced since the last successful checkpoint." Tighter
	// than the status ticker above since this state is now load-bearing
	// correctness, not just an operational log line.
	saveWatermarkCheckpoint := func() {
		saveCtx, saveCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer saveCancel()
		if err := store.SaveWatermarkCheckpoint(saveCtx, *kafkaGroup, tracker.Snapshot()); err != nil {
			log.Printf("[pharos-consumer] WARNING: failed to save watermark checkpoint: %v", err)
		}
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				saveWatermarkCheckpoint()
			}
		}
	}()

	// 7. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("[pharos-consumer] Shutting down gracefully...")
	cancel()

	// Save one last checkpoint on the graceful-shutdown path specifically --
	// this is the one path where the exposure window is actually avoidable,
	// since a real crash by definition never reaches here.
	saveWatermarkCheckpoint()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[pharos-consumer] Warning during metrics server shutdown: %v", err)
	}

	if err := engine.Close(); err != nil {
		log.Printf("[pharos-consumer] Warning during shutdown: %v", err)
	}
	log.Println("[pharos-consumer] Shutdown complete.")
}
