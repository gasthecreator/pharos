package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const samplePrometheusText = `# HELP pharos_ingestion_requests_total Total HTTP requests
# TYPE pharos_ingestion_requests_total counter
pharos_ingestion_requests_total{status_code="200"} 42
pharos_ingestion_requests_total{status_code="422"} 7
# TYPE pharos_ingestion_dedup_outcomes_total counter
pharos_ingestion_dedup_outcomes_total{outcome="new_claim"} 30
pharos_ingestion_dedup_outcomes_total{outcome="duplicate_hit"} 5
pharos_ingestion_dlq_writes_total 3
`

func TestScrapeMetrics_ParsesLabeledAndUnlabeledLines(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(samplePrometheusText))
	}))
	defer server.Close()

	samples, err := scrapeMetrics(context.Background(), &http.Client{}, server.URL)
	if err != nil {
		t.Fatalf("scrapeMetrics failed: %v", err)
	}

	if got := sumMetric(samples["pharos_ingestion_requests_total"]); got != 49 {
		t.Errorf("expected requests_total sum 49, got %v", got)
	}
	if got := metricByLabel(samples["pharos_ingestion_dedup_outcomes_total"], "outcome", "new_claim"); got != 30 {
		t.Errorf("expected new_claim 30, got %v", got)
	}
	if got := metricByLabel(samples["pharos_ingestion_dedup_outcomes_total"], "outcome", "duplicate_hit"); got != 5 {
		t.Errorf("expected duplicate_hit 5, got %v", got)
	}
	if got := sumMetric(samples["pharos_ingestion_dlq_writes_total"]); got != 3 {
		t.Errorf("expected dlq_writes_total 3, got %v", got)
	}
}

func TestFetchLedger_BothReachable(t *testing.T) {
	ingestion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(samplePrometheusText))
	}))
	defer ingestion.Close()

	now := time.Now().UTC().Add(-30 * time.Second)
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pharos_consumer_events_consumed_total 100\n" +
			"pharos_consumer_late_arrivals_total 4\n" +
			"pharos_consumer_errors_total 1\n" +
			"pharos_consumer_kafka_lag 0\n" +
			"pharos_consumer_watermark_unix_seconds " + timeToUnixString(now) + "\n"))
	}))
	defer consumer.Close()

	ledger := FetchLedger(context.Background(), &http.Client{}, ingestion.URL, consumer.URL)

	if !ledger.IngestionReachable || !ledger.ConsumerReachable {
		t.Fatalf("expected both reachable, got ingestion=%v (%s) consumer=%v (%s)",
			ledger.IngestionReachable, ledger.IngestionError, ledger.ConsumerReachable, ledger.ConsumerError)
	}
	if ledger.NewClaims != 30 || ledger.DuplicateHits != 5 {
		t.Errorf("expected NewClaims=30 DuplicateHits=5, got %v/%v", ledger.NewClaims, ledger.DuplicateHits)
	}
	if ledger.ConsumedTotal != 100 || ledger.LateArrivals != 4 {
		t.Errorf("expected ConsumedTotal=100 LateArrivals=4, got %v/%v", ledger.ConsumedTotal, ledger.LateArrivals)
	}
	if !ledger.HasWatermark {
		t.Fatalf("expected HasWatermark true")
	}
	if ledger.WatermarkAge < 25*time.Second || ledger.WatermarkAge > 35*time.Second {
		t.Errorf("expected watermark age ~30s, got %v", ledger.WatermarkAge)
	}
}

func TestFetchLedger_UnreachableEndpointsReported(t *testing.T) {
	ledger := FetchLedger(context.Background(), &http.Client{Timeout: time.Second}, "http://127.0.0.1:1/metrics", "http://127.0.0.1:2/metrics")
	if ledger.IngestionReachable || ledger.ConsumerReachable {
		t.Fatalf("expected both unreachable")
	}
	if ledger.IngestionError == "" || ledger.ConsumerError == "" {
		t.Errorf("expected error messages recorded for both unreachable endpoints")
	}
}

func timeToUnixString(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}
