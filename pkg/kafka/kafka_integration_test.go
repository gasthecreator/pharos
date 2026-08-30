package kafka

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func isPortOpen(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 1*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func TestKafkaProducer_RealIntegration(t *testing.T) {
	if !isPortOpen("127.0.0.1", 9092) {
		t.Fatalf("Kafka port 9092 is not open on 127.0.0.1")
	}

	cfg := DefaultConfig([]string{"127.0.0.1:9092"})
	cfg.WriteTimeout = 10 * time.Second

	producer := NewWriterProducer(cfg)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	siteID := "SITE-KAFKA-INT"
	payload := []byte(`{"resourceType":"AdverseEvent","kafka_real":true}`)

	meta, err := producer.Publish(ctx, MainTopic, []byte(siteID), payload, map[string]string{
		"idempotency_key": "SITE-KAFKA-INT:1",
		"site_id":         siteID,
	})
	if err != nil {
		t.Fatalf("real Kafka publish failed: %v", err)
	}

	if meta.Topic != MainTopic {
		t.Errorf("expected topic %s, got %s", MainTopic, meta.Topic)
	}
}
