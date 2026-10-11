# Runbook: Consumer lag growing or consumer not consuming

Alerts: `PharosConsumerLagHigh`, `PharosConsumerDown`, `PharosConsumerErrors`, `PharosConsumerNotConsuming`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

`PharosConsumerLagHigh`, `PharosConsumerDown`, `PharosConsumerErrors` or `PharosConsumerNotConsuming` fires; `pharos_consumer_kafka_lag` keeps rising; dashboard/CLI queries show recent events missing.

## Impact

Events are safe in Kafka (retention 7 days on `pharos.events.adverse`, 10 GB per partition) but not yet in `canonical_events`/`events_by_study`/`events_by_site`/`events_recent`, so queries are stale. The consumer commits an offset only after all canonical writes succeed, so a stuck consumer does not lose or skip anything; the watermark simply stops advancing.

## Diagnose

1. Process: `kubectl -n pharos logs deploy/pharos-consumer --tail=100` or the host binary's output; `curl -s localhost:9091/healthz`.
2. Errors: `curl -s localhost:9091/metrics | grep -E 'pharos_consumer_(errors_total|kafka_lag|events_consumed_total)'`. Rising `pharos_consumer_errors_total` points at Cassandra writes or Kafka fetch/commit.
3. Group state: `docker exec pharos-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client-ssl.properties --describe --group pharos-canonical-sink` (CURRENT-OFFSET vs LOG-END-OFFSET per partition; a rebalance loop shows members changing).
4. Cassandra write latency: `pharos_consumer_cassandra_write_duration_seconds` (labels `outcome=success|error`).
5. Watermark: `pharos_consumer_watermark_unix_seconds` and per-partition `pharos_consumer_partition_active`; a partition idle longer than `--idle-timeout` (10m) is excluded from the watermark by design, not a fault.

## Mitigate

- Fix the failing dependency (Cassandra/Kafka runbooks); the consumer resumes from its last committed offset and rewrites any row idempotently.
- Restart the consumer if wedged. The watermark checkpoint (`consumer_watermark_checkpoints`, saved every 10s and on shutdown) is restored on startup, so the exported watermark does not regress; at most about 10 seconds of watermark progress can be lost.
- To catch up faster, run a second instance in the same group (`--kafka-group pharos-canonical-sink`); partitions rebalance and checkpoints merge per partition.

## Escalate

Escalate if lag approaches the 7-day retention window (data would age out before being consumed), or if `pharos_consumer_errors_total` keeps rising with Cassandra healthy.

## Verify

`pharos_consumer_kafka_lag` falls toward 0, `pharos_consumer_events_consumed_total` rises, and `pharos-cli --operator <you> query site <site>` shows the latest events.
