package ingestion

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gasthecreator/pharos/internal/dedup"
	"github.com/gasthecreator/pharos/internal/kafka"
	"github.com/gasthecreator/pharos/internal/model"
	"github.com/gasthecreator/pharos/internal/ratelimit"
)

// BenchmarkHandleEvents_Batch50 measures the whole intake path for one
// edge-sized batch (50 events: decode, validate, claim, mock publish,
// finalize) against the in-memory outbox and a mock producer. It excludes
// Cassandra and Kafka, so it bounds the application's own cost, not the
// end-to-end latency (see docs/benchmark-results.md for that).
func BenchmarkHandleEvents_Batch50(b *testing.B) {
	const batch = 50
	h := NewHandlerWithOutbox(ratelimit.NewTokenBucketLimiter(1e12, 1e12), dedup.NewMemoryOutboxStore(), kafka.NewMockProducer(), time.Second)

	seq := uint64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		events := make([]model.AdverseEvent, batch)
		for j := range events {
			seq++
			events[j] = validEvent("SITE-BENCH", seq)
		}
		body, _ := json.Marshal(BatchRequest{SiteID: "SITE-BENCH", Events: toRaw(events...)})
		b.StartTimer()

		rec := httptest.NewRecorder()
		h.HandleEvents(rec, httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
	}
	b.ReportMetric(float64(batch), "events/op")
}
