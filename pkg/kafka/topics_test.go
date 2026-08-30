package kafka

import (
	"context"
	"testing"
	"time"
)

func TestEnsureTopics_RealKafkaIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	brokers := []string{"127.0.0.1:9092"}
	configs := DefaultTopicConfigs()

	if err := EnsureTopics(ctx, brokers, configs); err != nil {
		t.Fatalf("EnsureTopics failed against live Kafka: %v", err)
	}

	t.Log("Successfully ensured dynamic topic retention configurations on live Kafka")
}
