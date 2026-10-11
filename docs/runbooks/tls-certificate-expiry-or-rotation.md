# Runbook: TLS certificate expiry or rotation

Commands assume the Docker Compose topology (container names `pharos-cassandra-N`, `pharos-kafka-N`); for the Kubernetes deployment use `kubectl -n pharos logs|exec` against `deploy/pharos-ingestion`, `deploy/pharos-consumer`, `sts/cassandra-us`, `sts/kafka-a` instead. Metric names are the ones the services register (see [`../configuration.md`](../configuration.md) for flags).

## Symptom

TLS handshake errors (`certificate has expired`, `x509: certificate signed by unknown authority`, `SSL handshake failed` in broker logs) between edge/ingestion, services and Cassandra/Kafka, or a planned rotation.

## Impact

All client and (in Compose) internode traffic uses certificates issued by one project-owned CA from `scripts/generate_certs.sh`. Certificates are valid for `PHAROS_TLS_DAYS` (default 3650). An expired or replaced CA stops every dependent connection.

## Diagnose

1. Dates: `openssl x509 -in certs/ca-cert.pem -noout -enddate`, and the same for `certs/ingestion-cert.pem`.
2. Trust: `echo | openssl s_client -connect localhost:9042 -CAfile certs/ca-cert.pem 2>&1 | grep 'Verify return code'` must be `0 (ok)` (use port 9092 for Kafka, 8091 for ingestion). A `19 (self-signed certificate in certificate chain)` result means the CA on disk is not the one the running servers loaded.
3. Kubernetes: `kubectl -n pharos get secret pharos-tls`; `deploy/k8s/deploy.sh` reuses `deploy/k8s/certs/` unless `PHAROS_FORCE_REGEN_CERTS=1`.

## Mitigate

**`generate_certs.sh` deletes and recreates the whole certs directory and issues a brand-new CA.** Running it against a live stack breaks trust for already-running containers (they keep the old CA in memory). Rotate with the stack stopped:
1. `docker compose down` (keep volumes if you want the data).
2. `./scripts/generate_certs.sh`
3. Bring the stack up again in order (`make up`), rebuild/restart the host services, and re-run `pharos-cli site create-key` only if you also want new site keys (keys are independent of the CA).
4. K8s: `PHAROS_FORCE_REGEN_CERTS=1 ./deploy/k8s/deploy.sh` regenerates and re-applies the Secret; pods must be restarted to load it.
There is no zero-downtime or per-certificate rotation path today (see `SECURITY.md` known gaps).

## Escalate

Escalate if production-like data must stay online during rotation: that needs a design change (an ADR), not an improvised procedure.

## Verify

`openssl s_client` returns `Verify return code: 0 (ok)` for 9042, 9092 and 8091, `/healthz` is 200 over HTTPS, and an edge event reaches `PUBLISHED`.
