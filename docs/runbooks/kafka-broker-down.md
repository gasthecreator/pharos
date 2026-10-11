# Runbook: Kafka broker down

Alerts: `PharosIngestion5xxRatioHigh`, `PharosConsumerLagHigh`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

A Kafka container is unhealthy/exited; `kafka-topics.sh --describe` shows partitions with a reduced `Isr` or no leader; ingestion publish step fails; `PharosIngestion5xxRatioHigh`/`PharosConsumerLagHigh` fire. (No Kafka exporter is deployed, so there is no broker-down alert.)

## Impact

Cluster A has three brokers with replication factor 3 and `min.insync.replicas=2`, so one broker down is tolerated. Two down makes writes fail: ingestion returns 503 and the claim stays `PUBLISHING` until the sweeper republishes after the 30s lease. The consumer stalls on affected partitions. No event is lost.

## Diagnose

1. `docker compose ps`; `docker logs pharos-kafka-N --tail=100`.
2. `docker exec pharos-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /etc/kafka/secrets/client-ssl.properties --describe` (leaders, `Isr`).
3. A Kafka JVM heap `OutOfMemoryError` can occur while the container still reports `healthy` and `OOMKilled` is `false`; read the broker log, not just the container state. `KAFKA_HEAP_OPTS` is 320M in `docker-compose.yml`.
4. Plaintext/TLS mismatch (for example MirrorMaker 2 or a client without SSL properties against an SSL listener) can present as `OutOfMemoryError: Java heap space`; check the client's `security.protocol`.

## Mitigate

- Restart the broker (`docker compose up -d kafka-N`); wait for `Isr` to be full again before restarting another.
- Start MirrorMaker 2 only after topics exist and the cluster is settled; starting it concurrently caused repeated OOM kills.
- `./scripts/create_topics.sh` re-applies the topics and their retention settings if they are missing.

## Escalate

Escalate if two cluster-A brokers are down, or if `Isr` does not recover after restarts.

## Verify

All partitions show full `Isr`, ingestion publishes succeed, and consumer lag drains.
