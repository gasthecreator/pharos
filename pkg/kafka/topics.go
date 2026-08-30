package kafka

import (
	"time"
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
			Replication: 1, // Single-node local Kafka dev default; 3 in clustered production
			Retention:   MainTopicRetention,
			RetentionMs: MainTopicRetentionMs,
			MaxBytes:    MainTopicMaxBytes,
		},
		{
			Name:        DLQTopic,
			Partitions:  3,
			Replication: 1,
			Retention:   DLQTopicRetention,
			RetentionMs: DLQTopicRetentionMs,
			MaxBytes:    DLQTopicMaxBytes,
		},
	}
}
