package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestMockProducer_ConcurrentPublishes(t *testing.T) {
	mp := NewMockProducer()
	ctx := context.Background()

	const numGoroutines = 50
	const msgsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		siteID := fmt.Sprintf("SITE-%02d", i%5)
		go func(site string) {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				_, err := mp.Publish(ctx, MainTopic, []byte(site), []byte(fmt.Sprintf(`{"val":%d}`, j)), map[string]string{
					"site_id": site,
				})
				if err != nil {
					t.Errorf("publish failed: %v", err)
				}
			}
		}(siteID)
	}

	wg.Wait()

	expectedTotal := numGoroutines * msgsPerGoroutine
	if mp.TotalPublishes() != expectedTotal {
		t.Fatalf("expected total publishes %d, got %d", expectedTotal, mp.TotalPublishes())
	}
	if mp.TopicCount(MainTopic) != expectedTotal {
		t.Fatalf("expected topic count %d, got %d", expectedTotal, mp.TopicCount(MainTopic))
	}
	if mp.TopicCount(DLQTopic) != 0 {
		t.Fatalf("expected DLQ count 0, got %d", mp.TopicCount(DLQTopic))
	}
}

func TestMockProducer_FailureInjection(t *testing.T) {
	mp := NewMockProducer()
	ctx := context.Background()

	// Inject 2 failures
	mp.FailNext(2)

	_, err1 := mp.Publish(ctx, MainTopic, []byte("SITE-1"), []byte("{}"), nil)
	if err1 == nil {
		t.Errorf("expected failure on 1st call")
	}

	_, err2 := mp.Publish(ctx, MainTopic, []byte("SITE-1"), []byte("{}"), nil)
	if err2 == nil {
		t.Errorf("expected failure on 2nd call")
	}

	// 3rd call should succeed
	meta, err3 := mp.Publish(ctx, MainTopic, []byte("SITE-1"), []byte("{}"), nil)
	if err3 != nil {
		t.Fatalf("expected 3rd call to succeed, got: %v", err3)
	}
	if meta.Topic != MainTopic {
		t.Errorf("expected topic %s, got %s", MainTopic, meta.Topic)
	}
	if mp.TotalPublishes() != 1 {
		t.Errorf("expected total successful publishes = 1, got %d", mp.TotalPublishes())
	}
}
