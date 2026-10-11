# Runbook: Dashboard and CLI audit-log review (and API-key rotation)

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

A periodic compliance review, an investigation into who viewed or replayed records, or a suspected key leak.

## Impact

Every real `pharos-cli query/dlq` call and every dashboard view of adverse-event data is recorded in `pharos.access_audit_log` under the operator's self-declared name; DLQ replays and event submissions are recorded by Central Ingestion under the site. Identity is **self-declared, not verified** (see `docs/security-threat-model.md`).

## Diagnose

1. An operator's history: `pharos-cli --ca-cert certs/ca-cert.pem audit list --operator <name> --limit 100` (for `audit list`, `--operator` names the trail being inspected, not the inspector). For a site's own replay attempts use the site ID as the operator.
2. Late-arrival audit for a consumer group: `pharos-cli --ca-cert certs/ca-cert.pem audit late-arrivals --group pharos-canonical-sink --limit 100`.
3. Failed lookups are recorded too (outcome `ERROR: ...`), so repeated errors for one operator are worth a look.

## Mitigate

**Rotating or revoking a site's API key**
- Rotate: `pharos-cli --ca-cert certs/ca-cert.pem site create-key <site>`. This replaces the stored hash immediately (the old key stops working at once) and prints the new key exactly once. Update the edge's `--api-key` (K8s: the `pharos-site-k8s-demo-key` Secret) and restart the edge; its queue holds events meanwhile.
- Revoke: `pharos-cli ... site revoke-key <site>` disables the key without issuing another.

## Escalate

Escalate to the security contact in `SECURITY.md` for any unexplained access, or if a key may have been disclosed.

## Verify

`audit list` shows the expected entries, the rotated site authenticates (no 401s in `pharos_ingestion_requests_total`), and the old key is rejected.
