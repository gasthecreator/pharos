// Package faultinjection contains dedicated fault-injection tests exercising the
// full pipeline (edge -> Central Ingestion -> Kafka -> consumer -> Cassandra)
// against real infrastructure. These prove PLAN.md's core distributed-systems
// claims under the specific failure modes the project exists to handle, rather
// than component-level correctness already covered by each package's own tests.
package faultinjection

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gasthecreator/pharos/pkg/kafka"
	"github.com/gasthecreator/pharos/pkg/model"
)

func isPortOpen(host string, port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 1*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// newFaultInjectionEvent builds a valid AdverseEvent for a given site and local
// sequence number, ready to have an idempotency key stamped onto it.
func newFaultInjectionEvent(siteID string, localSeq uint64) *model.AdverseEvent {
	now := time.Now().UTC()
	return &model.AdverseEvent{
		ResourceType: model.ResourceTypeAdverseEvent,
		Actuality:    model.ActualityActual,
		Subject:      model.Reference{Reference: "Patient/FI-SUBJECT"},
		Event: model.CodeableConcept{
			Coding: []model.Coding{
				{System: model.MedDRASystem, Code: "10012345", Display: "Fault Injection Test Event"},
			},
			Text: "Fault Injection Test Event",
		},
		Date:         now.Add(-time.Duration(localSeq) * time.Minute),
		RecordedDate: now,
		Severity: model.CodeableConcept{
			Coding: []model.Coding{{Code: "moderate"}},
		},
		Study: []model.Reference{
			{Reference: "ResearchStudy/FAULT-INJECTION"},
		},
		Location: model.Reference{Reference: "Location/" + siteID},
	}
}

// countingProducer wraps a real kafka.Producer and counts successful publishes
// per idempotency_key, giving tests a direct, unambiguous way to assert "exactly
// one Kafka publish occurred" rather than inferring it indirectly from Cassandra
// state alone.
type countingProducer struct {
	inner kafka.Producer
	mu    sync.Mutex
	count map[string]int
}

func newCountingProducer(inner kafka.Producer) *countingProducer {
	return &countingProducer{inner: inner, count: make(map[string]int)}
}

func (c *countingProducer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) (kafka.KafkaMetadata, error) {
	meta, err := c.inner.Publish(ctx, topic, key, value, headers)
	if err == nil {
		c.mu.Lock()
		c.count[headers["idempotency_key"]]++
		c.mu.Unlock()
	}
	return meta, err
}

func (c *countingProducer) CountFor(idempotencyKey string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count[idempotencyKey]
}

func (c *countingProducer) Close() error {
	return c.inner.Close()
}
