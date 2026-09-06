package dashboard

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// metricSample is one Prometheus exposition-format line's value plus its
// label set.
type metricSample struct {
	labels map[string]string
	value  float64
}

var (
	metricLineRe = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$`)
	labelPairRe  = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"`)
)

// scrapeMetrics fetches and minimally parses a Prometheus exposition-format
// endpoint -- deliberately not a full parser (no HELP/TYPE handling beyond
// skipping comment lines, no exemplar/timestamp support), since the
// Correctness Ledger (§2.4, PLAN.md Slice 23) only needs a handful of known,
// simple counter/gauge lines this project's own /metrics endpoints already
// expose (Slice 6) -- reading the real thing directly rather than adding a
// new data source or a full metrics-client dependency.
func scrapeMetrics(ctx context.Context, client *http.Client, url string) (map[string][]metricSample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	result := make(map[string][]metricSample)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := metricLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name, labelStr, valStr := m[1], m[2], m[3]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		labels := map[string]string{}
		if labelStr != "" {
			for _, lm := range labelPairRe.FindAllStringSubmatch(labelStr, -1) {
				labels[lm[1]] = lm[2]
			}
		}
		result[name] = append(result[name], metricSample{labels: labels, value: val})
	}
	return result, scanner.Err()
}

func sumMetric(samples []metricSample) float64 {
	total := 0.0
	for _, s := range samples {
		total += s.value
	}
	return total
}

func metricByLabel(samples []metricSample, labelKey, labelVal string) float64 {
	for _, s := range samples {
		if s.labels[labelKey] == labelVal {
			return s.value
		}
	}
	return 0
}

// Ledger is the "Correctness Ledger" panel's data (§2.4, PLAN.md Slice 23):
// read live from Central Ingestion's and pharos-consumer's real /metrics
// endpoints (Slice 6), not a new data source.
type Ledger struct {
	IngestionReachable bool
	IngestionError     string
	ConsumerReachable  bool
	ConsumerError      string

	RequestsTotal  float64
	NewClaims      float64 // dedup_outcomes_total{outcome="new_claim"} -- first-time accepted events
	DuplicateHits  float64 // dedup_outcomes_total{outcome="duplicate_hit"} -- resubmissions correctly suppressed
	DLQWrites      float64
	ConsumedTotal  float64
	LateArrivals   float64
	ConsumerErrors float64
	KafkaLag       float64
	WatermarkUnix  float64
	WatermarkTime  time.Time
	WatermarkAge   time.Duration
	HasWatermark   bool
}

// FetchLedger scrapes both endpoints. A fetch failure for either endpoint
// is recorded on the result (Reachable=false, Error set), not returned as a
// hard error -- an unreachable ingestion or consumer process is itself
// meaningful information for an operator watching the panel while a chaos
// action might have taken something down.
func FetchLedger(ctx context.Context, client *http.Client, ingestionMetricsURL, consumerMetricsURL string) *Ledger {
	l := &Ledger{}

	if ingestionMetricsURL != "" {
		samples, err := scrapeMetrics(ctx, client, ingestionMetricsURL)
		if err != nil {
			l.IngestionError = err.Error()
		} else {
			l.IngestionReachable = true
			l.RequestsTotal = sumMetric(samples["pharos_ingestion_requests_total"])
			l.NewClaims = metricByLabel(samples["pharos_ingestion_dedup_outcomes_total"], "outcome", "new_claim")
			l.DuplicateHits = metricByLabel(samples["pharos_ingestion_dedup_outcomes_total"], "outcome", "duplicate_hit")
			l.DLQWrites = sumMetric(samples["pharos_ingestion_dlq_writes_total"])
		}
	}

	if consumerMetricsURL != "" {
		samples, err := scrapeMetrics(ctx, client, consumerMetricsURL)
		if err != nil {
			l.ConsumerError = err.Error()
		} else {
			l.ConsumerReachable = true
			l.ConsumedTotal = sumMetric(samples["pharos_consumer_events_consumed_total"])
			l.LateArrivals = sumMetric(samples["pharos_consumer_late_arrivals_total"])
			l.ConsumerErrors = sumMetric(samples["pharos_consumer_errors_total"])
			l.KafkaLag = sumMetric(samples["pharos_consumer_kafka_lag"])
			if wmSamples := samples["pharos_consumer_watermark_unix_seconds"]; len(wmSamples) > 0 {
				l.WatermarkUnix = wmSamples[0].value
				if l.WatermarkUnix > 0 {
					l.HasWatermark = true
					l.WatermarkTime = time.Unix(int64(l.WatermarkUnix), 0).UTC()
					l.WatermarkAge = time.Since(l.WatermarkTime)
				}
			}
		}
	}

	return l
}
