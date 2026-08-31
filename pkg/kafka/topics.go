package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkaGo "github.com/segmentio/kafka-go"
)

const (
	// MainTopicRetention is 7 days (168 hours) aligned with FDA 7-day expedited safety reporting (§2.4).
	MainTopicRetention   = 7 * 24 * time.Hour
	MainTopicRetentionMs = int64(7 * 24 * time.Hour / time.Millisecond) // 604,800,000 ms

	// DLQTopicRetention is 14 days (336 hours) providing operational runway for clinical trial site data managers (§2.3).
	DLQTopicRetention   = 14 * 24 * time.Hour
	DLQTopicRetentionMs = int64(14 * 24 * time.Hour / time.Millisecond) // 1,209,600,000 ms

	// MainTopicMaxBytes per partition is 10 GB.
	MainTopicMaxBytes = int64(10 * 1024 * 1024 * 1024)

	// DLQTopicMaxBytes per partition is 5 GB.
	DLQTopicMaxBytes = int64(5 * 1024 * 1024 * 1024)
)

// TopicConfig defines retention and partition configuration for Pharos Kafka topics (§4).
type TopicConfig struct {
	Name        string
	Partitions  int
	Replication int
	Retention   time.Duration
	RetentionMs int64
	MaxBytes    int64
}

// DefaultTopicConfigs returns the explicit retention configuration for Pharos topics.
func DefaultTopicConfigs() []TopicConfig {
	return []TopicConfig{
		{
			Name:        MainTopic,
			Partitions:  3,
			Replication: 3, // RF=3 across 3-broker cluster (Slice 7, §2.4)
			Retention:   MainTopicRetention,
			RetentionMs: MainTopicRetentionMs,
			MaxBytes:    MainTopicMaxBytes,
		},
		{
			Name:        DLQTopic,
			Partitions:  3,
			Replication: 3, // RF=3 across 3-broker cluster (Slice 7, §2.3)
			Retention:   DLQTopicRetention,
			RetentionMs: DLQTopicRetentionMs,
			MaxBytes:    DLQTopicMaxBytes,
		},
	}
}

// EnsureTopics creates or configures Kafka topics with explicit retention policies (§4).
// Follows the same bootstrap-on-connect pattern established for Cassandra (EnsureSchema).
func EnsureTopics(ctx context.Context, brokers []string, configs []TopicConfig) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}

	addr := kafkaGo.TCP(brokers...)
	client := &kafkaGo.Client{
		Addr:    addr,
		Timeout: 15 * time.Second,
	}

	// 1. Create topics if they don't exist yet
	var createConfigs []kafkaGo.TopicConfig
	for _, tc := range configs {
		createConfigs = append(createConfigs, kafkaGo.TopicConfig{
			Topic:             tc.Name,
			NumPartitions:     tc.Partitions,
			ReplicationFactor: tc.Replication,
			ConfigEntries: []kafkaGo.ConfigEntry{
				{ConfigName: "retention.ms", ConfigValue: strconv.FormatInt(tc.RetentionMs, 10)},
				{ConfigName: "retention.bytes", ConfigValue: strconv.FormatInt(tc.MaxBytes, 10)},
			},
		})
	}

	_, _ = client.CreateTopics(ctx, &kafkaGo.CreateTopicsRequest{
		Addr:   addr,
		Topics: createConfigs,
	})

	// 2. Ensure dynamic configurations (retention.ms, retention.bytes) on existing topics
	var resources []kafkaGo.AlterConfigRequestResource
	for _, tc := range configs {
		resources = append(resources, kafkaGo.AlterConfigRequestResource{
			ResourceType: kafkaGo.ResourceTypeTopic,
			ResourceName: tc.Name,
			Configs: []kafkaGo.AlterConfigRequestConfig{
				{Name: "retention.ms", Value: strconv.FormatInt(tc.RetentionMs, 10)},
				{Name: "retention.bytes", Value: strconv.FormatInt(tc.MaxBytes, 10)},
			},
		})
	}

	alterResp, err := client.AlterConfigs(ctx, &kafkaGo.AlterConfigsRequest{
		Addr:      addr,
		Resources: resources,
	})
	if err != nil {
		return fmt.Errorf("failed to alter kafka topic configs: %w", err)
	}

	for res, rErr := range alterResp.Errors {
		if rErr != nil {
			return fmt.Errorf("failed to configure retention for topic %s: %w", res.Name, rErr)
		}
	}

	return nil
}
