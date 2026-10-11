# Service level objectives (proposed)

**Status: proposal.** Nothing here has been measured over a production window
(the project has not been operated in production). Targets are starting points
anchored on the one real measurement available, the Slice 16 load test in
[`benchmark-results.md`](benchmark-results.md) (p50 ~50ms, p90 ~85ms, p95
~101ms, max 232ms on one machine; **p99 was not recorded**). Each objective
lists the metric that would measure it today, or says what instrumentation is
missing. Alert rules that implement the ones that can be alerted on are in
[`../observability/prometheus/alerts.yml`](../observability/prometheus/alerts.yml),
each linking to a runbook in [`runbooks/`](runbooks/).

Window for all objectives: rolling 30 days. Failures during a declared
maintenance window or a *site-side* outage (the edge cannot reach Central
Ingestion because of the site's own network) do not count against Central
Ingestion's objectives; the edge queue absorbs them by design.

| # | SLI | Proposed SLO | Measured by today | Missing instrumentation |
|---|---|---|---|---|
| 1 | **Ingestion availability**: share of batch requests not answered 5xx | 99.9% | `1 - sum(rate(pharos_ingestion_requests_total{status_code=~"5.."}[5m])) / sum(rate(pharos_ingestion_requests_total[5m]))`. 4xx (including 401/403/429) are client outcomes and are excluded | none |
| 2 | **Ingestion latency**: p99 end-to-end batch request time | p99 < 500ms | `histogram_quantile(0.99, sum by (le) (rate(pharos_ingestion_request_duration_seconds_bucket[5m])))`. The benchmark's p95 of ~101ms leaves headroom; the known dominant cost is the Cassandra LWT outbox insert | a load run that records p99 to validate the 500ms target |
| 3 | **Exactly-once correctness**: duplicate records in the canonical store, and events accepted but never consumed | 0 duplicates; 0 lost | *Proxy only*: over a long window `increase(pharos_consumer_events_consumed_total)` should equal `increase(pharos_ingestion_dedup_outcomes_total{outcome="new_claim"})` (offset by consumer lag). `duplicate_hit` counts *suppressed* retries and is healthy, not a failure. The canonical tables are keyed by idempotency key, so duplicate rows cannot exist; the real risk is a duplicate Kafka publish | a reconciliation job counting Kafka messages and canonical rows per idempotency key, exported as a gauge (for example `pharos_reconcile_duplicate_keys`) |
| 4 | **Consumer end-to-end freshness**: how far canonical tables trail ingestion | lag < 1000 messages for 99% of the window; time-based target once measurable (for example 99% of events queryable within 60s of publish) | `pharos_consumer_kafka_lag` (aggregate group lag). Alert `PharosConsumerLagHigh` | a freshness histogram (`consumed_at` minus the outbox `published_at`; both are already stored per event, neither is exported as a metric) |
| 5 | **Dead-letter rate**: share of submitted events rejected | informational: investigate when above 1% of events for 15 minutes; the rate depends on site data quality, not service health | `sum(rate(pharos_ingestion_dlq_writes_total[15m])) / (sum(rate(pharos_ingestion_dlq_writes_total[15m])) + sum(rate(pharos_ingestion_dedup_outcomes_total{outcome="new_claim"}[15m])))`. Alert `PharosDLQWriteSpike` uses an absolute count instead | none |
| 6 | **Edge drain time**: how long a site's oldest unforwarded record waits | oldest pending age < 15 minutes for 99% of non-outage time; after a connectivity outage ends, queue empty within one hour per day of outage (starting guess, to be validated) | `pharos_edge_queue_oldest_pending_age_seconds` and `pharos_edge_queue_pending_records`. Alert `PharosEdgeQueueBacklog` | a drain-duration measure (time from reconnect to empty queue); today it must be read off the two gauges |

## Error budgets

At 99.9% availability the 30-day budget is about 43 minutes of 5xx-equivalent
time. Edge retries mean most budget burn is delayed delivery, not data loss, so
the practical priority order when budgets conflict is: never lose or duplicate
an event (objective 3) before keeping the pipeline fresh (4, 6) before shaving
latency (2).

## What is intentionally not an SLO

Cassandra node and Kafka broker health: no exporter is deployed, so they are
tracked via healthchecks and the runbooks (`cassandra-node-down-or-dc-partition`,
`kafka-broker-down`), not as service objectives. Query-side availability of
`pharos-cli` and the dashboard has no metrics today.
