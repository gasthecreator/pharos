# Runbook: Dead-letter queue growth spike

Alerts: `PharosDLQWriteSpike`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

`PharosDLQWriteSpike` fires; `pharos_ingestion_dlq_writes_total` and `pharos_ingestion_validation_failures_total` climb; edges' forwarder logs show per-event rejections.

## Impact

Rejected events are **not lost**: each is written durably to Cassandra `dead_letter_events` (and `dead_letter_events_by_site`) and published to Kafka `pharos.events.dlq` (14-day retention) before the edge is told it was rejected. They are simply not in the canonical tables until corrected and replayed.

## Diagnose

1. Who and why: `pharos-cli --operator <you> --ca-cert certs/ca-cert.pem dlq list --site <site> --limit 50` and `dlq get <idempotency_key>` for the `rejection_reason`/`validation_errors`.
2. Single site vs everyone: `dlq list` per site, or the dashboard's DLQ view. A single site points at a client/bug; all sites at the same time points at a Central Ingestion deploy (a changed validator) or an unsupported `schemaVersion`.
3. Unsupported schema version rejections mean a site's edge is newer than ingestion (see `docs/upgrade-and-compat-policy.md`).

## Mitigate

- Contact the site / fix the producer; if the payload itself was corrected out-of-band, replay: `pharos-cli --site-id <site> --api-key <key> --ca-cert certs/ca-cert.pem dlq replay <idempotency_key>` or `dlq replay --all --site <site>` (the authenticated site must own the record; every attempt is audited).
- If a Central Ingestion release caused it, roll that release back, then replay the affected records.
- A replay of a still-invalid payload leaves the original record untouched and returns the same rejection.

## Escalate

Escalate to the data owner if records carry apparently valid clinical content that fails validation (possible profile bug), or if `dead_letter_events` growth threatens the retention plan (consider `pharos-cli archive run`).

## Verify

New `dlq_writes` rate returns to baseline and replayed records move to `REPLAYED` (`dlq get <key>`).
