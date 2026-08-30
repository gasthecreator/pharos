package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/model"
	kafkaGo "github.com/segmentio/kafka-go"
)

// MessageReader defines the minimal Kafka consumer interface required by the Engine.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafkaGo.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkaGo.Message) error
	Close() error
}

// EngineConfig holds configuration for the downstream consumer engine (§2.4).
type EngineConfig struct {
	Brokers           []string
	Topic             string
	GroupID           string
	LatenessTolerance time.Duration
	IdleTimeout       time.Duration
	PollTimeout       time.Duration
}

// DefaultEngineConfig returns production defaults for the canonical consumer group.
func DefaultEngineConfig(brokers []string) EngineConfig {
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092"}
	}
	return EngineConfig{
		Brokers:           brokers,
		Topic:             kafka.MainTopic,
		GroupID:           "pharos-canonical-sink",
		LatenessTolerance: 15 * time.Minute,
		IdleTimeout:       10 * time.Minute,
		PollTimeout:       5 * time.Second,
	}
}

// EngineStats captures operational metrics for the consumer engine.
type EngineStats struct {
	ConsumedCount   uint64 `json:"consumed_count"`
	CommittedCount  uint64 `json:"committed_count"`
	LateEventsCount uint64 `json:"late_events_count"`
	ErrorCount      uint64 `json:"error_count"`
}

// Engine reads events from Kafka, tracks event-time watermarks, and writes to canonical Cassandra tables.
type Engine struct {
	reader  MessageReader
	store   CanonicalStore
	tracker *WatermarkTracker
	cfg     EngineConfig
	stats   EngineStats
}

// NewEngine creates a new Consumer Engine.
func NewEngine(reader MessageReader, store CanonicalStore, tracker *WatermarkTracker, cfg EngineConfig) *Engine {
	if tracker == nil {
		tracker = NewWatermarkTracker(cfg.LatenessTolerance, cfg.IdleTimeout)
	}
	return &Engine{
		reader:  reader,
		store:   store,
		tracker: tracker,
		cfg:     cfg,
	}
}

// NewKafkaReader builds a standard segmentio/kafka-go reader configured for consumer group processing.
func NewKafkaReader(cfg EngineConfig) *kafkaGo.Reader {
	return kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Manual commits only via CommitMessages after Cassandra write
		StartOffset:    kafkaGo.FirstOffset,
	})
}

// Tracker returns the underlying WatermarkTracker.
func (e *Engine) Tracker() *WatermarkTracker {
	return e.tracker
}

// Stats returns a snapshot of engine statistics.
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		ConsumedCount:   atomic.LoadUint64(&e.stats.ConsumedCount),
		CommittedCount:  atomic.LoadUint64(&e.stats.CommittedCount),
		LateEventsCount: atomic.LoadUint64(&e.stats.LateEventsCount),
		ErrorCount:      atomic.LoadUint64(&e.stats.ErrorCount),
	}
}

// Step consumes a single message from Kafka, writes to canonical storage, and commits offset (§2.4).
func (e *Engine) Step(ctx context.Context) error {
	msg, err := e.reader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		atomic.AddUint64(&e.stats.ErrorCount, 1)
		return fmt.Errorf("failed to fetch message from kafka: %w", err)
	}

	// 1. Parse adverse event payload
	var event model.AdverseEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		atomic.AddUint64(&e.stats.ErrorCount, 1)
		return fmt.Errorf("failed to unmarshal message payload: %w", err)
	}

	// 2. Extract idempotency key
	keyStr := ""
	for _, h := range msg.Headers {
		if strings.EqualFold(h.Key, "idempotency_key") {
			keyStr = string(h.Value)
			break
		}
	}
	if keyStr == "" {
		if idKey, err := event.GetIdempotencyKey(); err == nil {
			keyStr = idKey.String()
		} else {
			keyStr = fmt.Sprintf("UNKEYED:%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
		}
	}

	var localSeq int64
	if parsedKey, err := model.ParseIdempotencyKey(keyStr); err == nil {
		localSeq = int64(parsedKey.LocalSeq)
	}

	siteID := string(msg.Key)
	if siteID == "" {
		siteID = event.SiteID()
	}

	studyID := event.StudyID()
	eventTime := event.EventTimeUTC()
	recordedTime := event.RecordedTimeUTC()
	ingestionTime := msg.Time.UTC()
	if ingestionTime.IsZero() {
		ingestionTime = time.Now().UTC()
	}

	now := time.Now().UTC()

	// 3. Process event through watermark tracker
	isLate, _ := e.tracker.ProcessEvent(msg.Partition, keyStr, eventTime, now)
	if isLate {
		atomic.AddUint64(&e.stats.LateEventsCount, 1)
	}

	// 4. Construct canonical record
	record := &CanonicalRecord{
		IdempotencyKey: keyStr,
		SiteID:         siteID,
		StudyID:        studyID,
		LocalSeq:       localSeq,
		EventTime:      eventTime,
		RecordedTime:   recordedTime,
		IngestionTime:  ingestionTime,
		Severity:       event.SeverityCode(),
		EventCode:      event.EventCode(),
		Subject:        event.SubjectID(),
		Payload:        string(msg.Value),
		KafkaTopic:     msg.Topic,
		KafkaPartition: msg.Partition,
		KafkaOffset:    msg.Offset,
		ConsumedAt:     now,
		IsLate:         isLate,
	}

	// 5. Durable parallel upsert to Cassandra query tables
	if err := e.store.SaveEvent(ctx, record); err != nil {
		atomic.AddUint64(&e.stats.ErrorCount, 1)
		// DO NOT COMMIT: uncommitted offset ensures Kafka redelivers on retry (§2.4)
		return fmt.Errorf("failed to save canonical event (offset uncommitted): %w", err)
	}

	// 6. Explicit manual offset commit after successful Cassandra write
	if err := e.reader.CommitMessages(ctx, msg); err != nil {
		atomic.AddUint64(&e.stats.ErrorCount, 1)
		return fmt.Errorf("failed to commit kafka offset: %w", err)
	}

	atomic.AddUint64(&e.stats.ConsumedCount, 1)
	atomic.AddUint64(&e.stats.CommittedCount, 1)
	return nil
}

// Run starts the continuous consumption loop until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := e.Step(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// On transient error, brief backoff before retry
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// Close gracefully closes the underlying Kafka reader and store.
func (e *Engine) Close() error {
	var errs []string
	if err := e.reader.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("reader close: %v", err))
	}
	if err := e.store.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("store close: %v", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}
