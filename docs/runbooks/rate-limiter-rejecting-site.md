# Runbook: Rate limiter rejecting a legitimate site

Alerts: `PharosSiteRateLimited`

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

`PharosSiteRateLimited` fires; a site's edge shows `rate_limited` forwarder outcomes and a growing queue; clients see HTTP 429 with `Retry-After`.

## Impact

Throttling is per site (default burst 100 batch requests, refill 10/s; each request carries up to 50 events). The limiter isolates sites: other sites are unaffected. The throttled site's events wait in its local queue and drain at the refill rate; nothing is dropped.

## Diagnose

1. `curl -s --cacert certs/ca-cert.pem https://localhost:8091/metrics | grep pharos_ingestion_rate_limit_rejections_total` (label `site_id`).
2. Is it a burst after an outage (a large backlog draining) or sustained volume? Compare `pharos_edge_queue_pending_records` for that site and the request rate.
3. Multiple ingestion instances without `--redis-addr` each keep a separate in-process bucket, so effective limits differ per instance; with Redis they are shared (ADR 0024).

## Mitigate

- Backlog drain: usually just wait; the edge honors `Retry-After` and backs off.
- Sustained legitimate volume: raise `--rate-limit-capacity` and `--rate-limit-refill` on ingestion (flags apply to all sites) and restart; there is no per-site override today.
- Misbehaving client: contact the site; revoke its key with `pharos-cli site revoke-key <site>` if it must be cut off.

## Escalate

Escalate if a site needs a permanently different limit (a per-site configuration change needs an ADR).

## Verify

`pharos_ingestion_rate_limit_rejections_total` for the site stops increasing and the edge queue drains.
