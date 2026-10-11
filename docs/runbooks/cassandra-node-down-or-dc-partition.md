# Runbook: Cassandra node down or datacenter partition

Alerts: `PharosCassandraWriteP99High` (and `PharosIngestion5xxRatioHigh`)

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

A Cassandra container is unhealthy/exited or `nodetool status` shows `DN`; ingestion returns 503s or slow responses; `PharosCassandraWriteP99High` fires. (There is no Cassandra exporter, so there is no node-down alert.)

## Impact

The keyspace uses `NetworkTopologyStrategy` (`dc-us` RF 3, `dc-eu` RF 1) and the services use `LOCAL_QUORUM` against `dc-us`: one `dc-us` node down is tolerated; two down, or a partition from `dc-us`, stalls writes. `dc-eu` (one node) is not on the write quorum path. During any outage ingestion answers 503 and the edge retries, so no event is dropped.

## Diagnose

1. `docker compose ps`; `docker exec pharos-cassandra-1 nodetool status` (look for `DN`).
2. Container logs: `docker logs pharos-cassandra-N --tail=100`. Real incidents seen on this project: Docker VM memory exhaustion (`docker inspect pharos-cassandra-N --format '{{.State.OOMKilled}}'`) and, after a `docker compose down` without `-v`, a node failing to rejoin with "A node with address ... already exists".
3. Region partition (chaos panel or `tc netem`): `nodetool status` shows the other DC `DN` from each side; `internal/faultinjection/regional_partition_test.go` documents the expected recovery (hinted handoff after heal).
4. Consistency of what is queryable: `docker exec -i -e SSL_CERTFILE=/tls/ca-cert.pem pharos-cassandra-1 cqlsh 127.0.0.1 9042 --ssl`, `SELECT count(*) FROM pharos.event_outbox;` is not cheap on a large table, prefer `pharos-cli` queries for spot checks.

## Mitigate

- Restart the failed node (`docker compose up -d cassandra-N`); it replays hints and catches up. Start nodes one at a time and wait for `UN`.
- Heal a simulated partition from the dashboard chaos panel (region partitions auto-heal after 60 seconds) or remove the `tc` rules.
- Data loss/corruption of the keyspace itself (not a node): `./scripts/restore_cassandra.sh <backup-timestamp-dir>` restores from a `./scripts/backup_cassandra.sh` snapshot into the live nodes via `nodetool refresh`. It does not rebuild a destroyed node: use Cassandra's node-replacement procedure for that.

## Escalate

Escalate immediately if two or more `dc-us` nodes are down for more than 10 minutes, or if a node reports data-directory corruption.

## Verify

`nodetool status` all `UN`, ingestion 5xx back to ~0, `pharos_consumer_cassandra_write_duration_seconds` p99 normal, and `pharos_consumer_kafka_lag` draining.
