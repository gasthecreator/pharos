package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gasthecreator/pharos/pkg/consumer"
	"github.com/gasthecreator/pharos/pkg/kafka"
)

func main() {
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "Comma-separated Kafka broker addresses")
	kafkaTopic := flag.String("kafka-topic", kafka.MainTopic, "Kafka topic to consume")
	kafkaGroup := flag.String("kafka-group", "pharos-canonical-sink", "Kafka consumer group ID")
	cassandraHosts := flag.String("cassandra-hosts", "127.0.0.1", "Comma-separated Cassandra host addresses")
	cassandraPort := flag.Int("cassandra-port", 9042, "Cassandra port")
	cassandraKeyspace := flag.String("cassandra-keyspace", "pharos", "Cassandra keyspace")
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

	// 2. Initialize Watermark Tracker (§2.4)
	tracker := consumer.NewWatermarkTracker(*latenessTolerance, *idleTimeout)
	log.Printf("[pharos-consumer] Watermark tracker initialized (lateness: %s, idle timeout: %s)", *latenessTolerance, *idleTimeout)

	// 3. Initialize Kafka Consumer Engine
	brokers := strings.Split(*kafkaBrokers, ",")
	if !*useMemoryStore {
		_ = kafka.EnsureTopics(context.Background(), brokers, kafka.DefaultTopicConfigs())
	}
	engineCfg := consumer.DefaultEngineConfig(brokers)
	engineCfg.Topic = *kafkaTopic
	engineCfg.GroupID = *kafkaGroup
	engineCfg.LatenessTolerance = *latenessTolerance
	engineCfg.IdleTimeout = *idleTimeout

	reader := consumer.NewKafkaReader(engineCfg)
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

	// 5. Periodic status telemetry
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats := engine.Stats()
				wm := tracker.CurrentWatermark(time.Now().UTC())
				log.Printf("[pharos-consumer] Status: consumed=%d committed=%d late=%d errors=%d watermark=%s",
					stats.ConsumedCount, stats.CommittedCount, stats.LateEventsCount, stats.ErrorCount,
					wm.Format(time.RFC3339))
			}
		}
	}()

	// 6. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("[pharos-consumer] Shutting down gracefully...")
	cancel()

	if err := engine.Close(); err != nil {
		log.Printf("[pharos-consumer] Warning during shutdown: %v", err)
	}
	log.Println("[pharos-consumer] Shutdown complete.")
}
