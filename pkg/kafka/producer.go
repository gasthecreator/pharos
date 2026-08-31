package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// MainTopic is the default Kafka topic for validated clinical adverse events (§2.2).
const MainTopic = "pharos.events.adverse"

// DLQTopic is the default Kafka topic for rejected/malformed adverse events (§2.3).
const DLQTopic = "pharos.events.dlq"

// KafkaMetadata contains broker acknowledgment coordinates for a published message.
type KafkaMetadata struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// Producer defines the publishing interface for Kafka.
type Producer interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) (KafkaMetadata, error)
	Close() error
}

// Config defines connection and durability parameters for Kafka publishing (§2.4).
type Config struct {
	Brokers       []string
	ClientID      string
	RequiredAcks  kafka.RequiredAcks
	BatchTimeout  time.Duration
	WriteTimeout  time.Duration
	AllowAutoCreate bool
}

// DefaultConfig provides production-ready idempotent producer defaults.
func DefaultConfig(brokers []string) Config {
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092", "127.0.0.1:9094", "127.0.0.1:9095"}
	}
	return Config{
		Brokers:         brokers,
		ClientID:        "pharos-ingestion",
		RequiredAcks:    kafka.RequireAll, // acks = -1 (all ISR replicas)
		BatchTimeout:    10 * time.Millisecond,
		WriteTimeout:    5 * time.Second,
		AllowAutoCreate: true,
	}
}

// WriterProducer implements Producer using segmentio/kafka-go.Writer.
type WriterProducer struct {
	writers map[string]*kafka.Writer
	mu      sync.Mutex
	cfg     Config
	closed  bool
}

// NewWriterProducer creates a new pure-Go Kafka producer.
func NewWriterProducer(cfg Config) *WriterProducer {
	return &WriterProducer{
		writers: make(map[string]*kafka.Writer),
		cfg:     cfg,
	}
}

func (p *WriterProducer) getWriter(topic string) *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	w, exists := p.writers[topic]
	if !exists {
		w = &kafka.Writer{
			Addr:         kafka.TCP(p.cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // Guarantees per-site ordering by partition key (§2.4)
			MaxAttempts:  5,
			RequiredAcks: p.cfg.RequiredAcks,
			Async:        false, // Synchronous durability guarantee for outbox publish step
			BatchTimeout: p.cfg.BatchTimeout,
			WriteTimeout: p.cfg.WriteTimeout,
			Compression:            kafka.Snappy,
			AllowAutoTopicCreation: p.cfg.AllowAutoCreate,
		}
		p.writers[topic] = w
	}
	return w
}

// Publish publishes a message with the specified partition key and headers.
func (p *WriterProducer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) (KafkaMetadata, error) {
	w := p.getWriter(topic)
	if w == nil {
		return KafkaMetadata{}, fmt.Errorf("producer is closed")
	}

	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := kafka.Message{
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
		Time:    time.Now().UTC(),
	}

	err := w.WriteMessages(ctx, msg)
	if err != nil {
		return KafkaMetadata{}, fmt.Errorf("failed to write message to topic %s: %w", topic, err)
	}

	// In segmentio/kafka-go synchronous mode, write success guarantees persistence across ISR
	return KafkaMetadata{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
	}, nil
}

// Close closes all topic writers.
func (p *WriterProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	var firstErr error
	for _, w := range p.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PublishedMessage records an event captured by the MockProducer.
type PublishedMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
	Time    time.Time
}

// MockProducer provides thread-safe in-memory publish tracking and fault injection.
type MockProducer struct {
	mu               sync.Mutex
	messages            []PublishedMessage
	keyPublishCounts    map[string]int
	topicCounts         map[string]int
	failIdempotencyKeys map[string]bool
	totalPublishes      int64
	failNextCount       int32
	failTopic           string
	closed              bool
}

// NewMockProducer creates a new mock Kafka producer for testing.
func NewMockProducer() *MockProducer {
	return &MockProducer{
		messages:            make([]PublishedMessage, 0),
		keyPublishCounts:    make(map[string]int),
		topicCounts:         make(map[string]int),
		failIdempotencyKeys: make(map[string]bool),
	}
}

// FailNext publishes triggers synthetic failures for the next N publish calls.
func (m *MockProducer) FailNext(n int) {
	atomic.StoreInt32(&m.failNextCount, int32(n))
}

// FailTopic triggers failures for a specific topic.
func (m *MockProducer) FailTopic(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failTopic = topic
}

// FailIdempotencyKey triggers failure when publishing an event with the specified idempotency key.
func (m *MockProducer) FailIdempotencyKey(idKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failIdempotencyKeys[idKey] = true
}

// Publish stores the published message and tracks publish statistics.
func (m *MockProducer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) (KafkaMetadata, error) {
	if atomic.LoadInt32(&m.failNextCount) > 0 {
		atomic.AddInt32(&m.failNextCount, -1)
		return KafkaMetadata{}, fmt.Errorf("injected kafka publish failure")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return KafkaMetadata{}, fmt.Errorf("producer is closed")
	}

	if m.failTopic != "" && m.failTopic == topic {
		return KafkaMetadata{}, fmt.Errorf("injected kafka topic failure for %s", topic)
	}

	if idKey, hasKey := headers["idempotency_key"]; hasKey && m.failIdempotencyKeys[idKey] {
		return KafkaMetadata{}, fmt.Errorf("injected kafka failure for idempotency key %s", idKey)
	}

	keyStr := string(key)
	m.keyPublishCounts[keyStr]++
	m.topicCounts[topic]++
	atomic.AddInt64(&m.totalPublishes, 1)

	headersCopy := make(map[string]string, len(headers))
	for k, v := range headers {
		headersCopy[k] = v
	}

	m.messages = append(m.messages, PublishedMessage{
		Topic:   topic,
		Key:     keyStr,
		Value:   value,
		Headers: headersCopy,
		Time:    time.Now().UTC(),
	})

	offset := int64(len(m.messages))
	return KafkaMetadata{
		Topic:     topic,
		Partition: 0,
		Offset:    offset,
	}, nil
}

// TotalPublishes returns the total count of successful publish calls.
func (m *MockProducer) TotalPublishes() int {
	return int(atomic.LoadInt64(&m.totalPublishes))
}

// KeyPublishCount returns how many times a given key (e.g. site_id or idempotency_key) was published.
func (m *MockProducer) KeyPublishCount(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keyPublishCounts[key]
}

// TopicCount returns how many messages were published to a specific topic.
func (m *MockProducer) TopicCount(topic string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.topicCounts[topic]
}

// Messages returns a copy of all published messages.
func (m *MockProducer) Messages() []PublishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]PublishedMessage, len(m.messages))
	copy(copied, m.messages)
	return copied
}

// Close closes the mock producer.
func (m *MockProducer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
