# Runbook: Edge collector queue backlog after an outage

Alerts: `PharosEdgeQueueBacklog`, `PharosEdgeDown`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

`PharosEdgeQueueBacklog` or `PharosEdgeDown` fires; `pharos_edge_queue_pending_records` is high and `pharos_edge_queue_oldest_pending_age_seconds` is large for a site; `pharos_edge_forwarder_outcomes_total{outcome="network_error"|"rate_limited"|"server_error"}` is increasing.

## Impact

The site is effectively partitioned from Central Ingestion. **Nothing is lost**: events stay `PENDING` in the edge's SQLite WAL queue and are forwarded when connectivity returns. Delivery to the central store is delayed by the outage plus the backoff (capped at 30s) and by the central rate limit on the drain.

## Diagnose

1. Edge side: `curl -s localhost:8080/api/v1/stats` (queue counts) and `curl -s localhost:8080/metrics | grep pharos_edge_`. `pharos_edge_forwarder_last_backoff_seconds` shows the current retry delay.
2. Outcome mix: `network_error` means the central URL is unreachable/TLS fails (check `--central-url`, `--ca-cert`); `rate_limited` means the drain is hitting the per-site bucket (see the rate-limiter runbook); `server_error` means ingestion is unhealthy (see its runbook). A 401 means the site's API key was revoked or rotated.
3. Chaos check: if the dashboard's chaos panel simulated a partition, `curl localhost:8080/admin/chaos/status` (only with `--enable-chaos`) shows `partitioned`.
4. Disk: the queue file is `--db-path`; confirm the volume is mounted and writable. If the disk was replaced, restore-on-missing-primary uses `--backup-path` automatically when configured (5-minute default backups).

## Mitigate

- Restore connectivity or fix the central side; the forwarder drains automatically.
- If the key was rotated, restart the edge with the new `--api-key` (see the API-key section of the audit runbook; `site create-key` replaces the old key immediately).
- Do not delete the SQLite file to "clear" the backlog: it holds the only copy of undelivered events.

## Escalate

Escalate if the oldest pending age keeps growing while the central side is healthy and `network_error` is zero (a stuck forwarder); capture the edge logs and the queue stats first.

## Verify

`pharos_edge_queue_pending_records` returns to ~0, `oldest_pending_age` resets, `forwarder_outcomes_total{outcome="success"}` increases, and the events appear via `pharos-cli query site <site>`.
