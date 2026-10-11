# Runbook: Central Ingestion returning 5xx or stalled

Alerts: `PharosIngestionDown`, `PharosIngestion5xxRatioHigh`, `PharosIngestionLatencyP99High`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

`PharosIngestionDown`, `PharosIngestion5xxRatioHigh` or `PharosIngestionLatencyP99High` fires; edges report `server_error` or `network_error` outcomes; `curl --cacert certs/ca-cert.pem https://localhost:8091/healthz` fails or is slow.

## Impact

Nothing new is accepted. **No data is lost**: each edge keeps events in its local SQLite WAL queue and retries with exponential backoff and full jitter (500ms base, 30s cap), and a retry of an already-handled event is a no-op (idempotency key plus the Cassandra outbox). A bare 503 means every event in the batch hit an infrastructure failure; 207 means a mix.

## Diagnose

1. Is the process up? Compose: the host binary's terminal output; K8s: `kubectl -n pharos get pods -l app=pharos-ingestion` and `kubectl -n pharos logs deploy/pharos-ingestion --tail=100`.
2. Which status codes? `curl -s --cacert certs/ca-cert.pem https://localhost:8091/metrics | grep pharos_ingestion_requests_total` (labels `status_code`). 401/403/429 are client-side (see the rate-limiter runbook); 5xx are infrastructure.
3. 503s almost always mean Cassandra or Kafka is unreachable from ingestion. Check both: `docker compose ps` (healthchecks), `docker exec pharos-cassandra-1 nodetool status` (all `UN`), and `docker exec pharos-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client-ssl.properties --describe --topic pharos.events.adverse` (every partition has a leader and `Isr` of at least 2). See the Cassandra and Kafka runbooks.
4. Latency only: compare `pharos_ingestion_request_duration_seconds` with `pharos_ingestion_outbox_publish_duration_seconds` and `pharos_consumer_cassandra_write_duration_seconds`; the benchmarked dominant cost is the Cassandra LWT outbox insert (`docs/benchmark-results.md`).
5. Rows stuck mid-publish: `docker exec -i -e SSL_CERTFILE=/tls/ca-cert.pem pharos-cassandra-1 cqlsh 127.0.0.1 9042 --ssl` then `SELECT idempotency_key, status, claimed_at FROM pharos.event_outbox WHERE status='PUBLISHING' ALLOW FILTERING;` is acceptable for a one-off look; the background sweeper (every 10s, 30s lease) reclaims stale claims on its own.

## Mitigate

- Restore whichever dependency is down (see the linked runbooks); ingestion recovers by itself and the sweeper republishes abandoned claims.
- If the process is wedged, restart it (`kubectl -n pharos rollout restart deploy/pharos-ingestion`, or restart the host binary). In-flight claims are safe: the lease expires and is reclaimed.
- If auth lookups fail with Cassandra healthy, check `site_api_keys` is readable; as a last resort for a local/demo instance only, `--enable-auth=false` disables key checks (never do this on a reachable deployment).

## Escalate

Escalate if 5xx persists more than 15 minutes after Cassandra and Kafka are confirmed healthy, or if duplicate Kafka messages are suspected (compare `new_claim` and `duplicate_hit` in `pharos_ingestion_dedup_outcomes_total` with the consumer's consumed count).

## Verify

5xx ratio returns to ~0, `/healthz` returns 200, edges' `pharos_edge_queue_pending_records` drains to ~0, and `pharos_consumer_events_consumed_total` advances.
