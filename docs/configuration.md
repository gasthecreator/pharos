# Configuration reference

Every setting of every Pharos binary, taken from the flag definitions in
`cmd/*/main.go` (and `internal/tlsutil` for the CA default). **None of the
five binaries reads an environment variable** (`os.Getenv`/`os.LookupEnv` do not
appear in `cmd/` or `internal/`); everything is a command-line flag. The only
environment variables in the repository are read by helper scripts and are
listed at the end, mirrored in [`.env.example`](../.env.example). Secrets are
marked in the last column; none has a default.

"Default CA" below means: when no CA flag is given, library code falls back to
`certs/ca-cert.pem` found by walking up from the working directory to the
`go.mod` root (`tlsutil.DefaultCACertPath`), and uses plaintext if it does not
exist.

## pharos-ingestion (Central Ingestion)

| Flag | Default | Purpose | Secret |
|---|---|---|---|
| `--port` | `8081` | HTTP(S) listen port (also serves `/healthz` and `/metrics`) | no |
| `--rate-limit-capacity` | `100` | Per-site token-bucket burst capacity (tokens = HTTP batch requests) | no |
| `--rate-limit-refill` | `10` | Per-site refill rate, tokens per second | no |
| `--redis-addr` | empty | `host:port` of Redis for the distributed rate limiter; empty uses the in-process limiter (single instance only) | no |
| `--cassandra-hosts` | `127.0.0.1` | Comma-separated Cassandra contact points | no |
| `--cassandra-port` | `9042` | Cassandra native port | no |
| `--cassandra-keyspace` | `pharos` | Keyspace | no |
| `--kafka-brokers` | `127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9095` | Comma-separated Kafka brokers | no |
| `--lease-timeout` | `30s` | Outbox `PUBLISHING` claim lease before another worker may steal it | no |
| `--sweeper-interval` | `10s` | How often the background outbox sweeper runs | no |
| `--use-memory-store` | `false` | In-memory outbox store and mock producer for tests; also forces auth off | no |
| `--tls-cert` | empty | TLS certificate for the listener (needs `--tls-key`) | path to cert |
| `--tls-key` | empty | TLS private key for the listener (needs `--tls-cert`) | **yes** (path to a private key) |
| `--ca-cert` | default CA | CA used to verify Cassandra/Kafka TLS | no |
| `--enable-auth` | `true` | Require per-site API keys (`X-Site-ID`/`X-API-Key`) on event submission and DLQ replay; always off with `--use-memory-store` | no |

## pharos-consumer

| Flag | Default | Purpose | Secret |
|---|---|---|---|
| `--kafka-brokers` | `127.0.0.1:9092,127.0.0.1:9094,127.0.0.1:9095` | Comma-separated Kafka brokers | no |
| `--kafka-topic` | `pharos.events.adverse` | Topic to consume | no |
| `--kafka-group` | `pharos-canonical-sink` | Consumer group ID (also the watermark checkpoint key) | no |
| `--metrics-port` | `9091` | Port serving `/metrics` and `/healthz` | no |
| `--cassandra-hosts` | `127.0.0.1` | Comma-separated Cassandra contact points | no |
| `--cassandra-port` | `9042` | Cassandra native port | no |
| `--cassandra-keyspace` | `pharos` | Keyspace | no |
| `--ca-cert` | default CA | CA used to verify Cassandra/Kafka TLS | no |
| `--lateness-tolerance` | `15m` | Event-time bounded lateness `L` in the watermark | no |
| `--idle-timeout` | `10m` | A partition with no messages for this long is excluded from the watermark | no |
| `--use-memory-store` | `false` | In-memory canonical store for standalone testing | no |

## pharos-edge (per-site collector)

| Flag | Default | Purpose | Secret |
|---|---|---|---|
| `--site-id` | `SITE-NG-01` | Site identifier stamped into idempotency keys | no |
| `--db-path` | `pharos-edge.db` | SQLite WAL queue file (mount a persistent volume in containers) | no |
| `--port` | `8080` | Local HTTP capture port (`/api/v1/adverse-events`, `/healthz`, `/metrics`) | no |
| `--central-url` | `http://localhost:8081/api/v1/events` | Central Ingestion events URL | no |
| `--batch-size` | `50` | Events per forwarded batch | no |
| `--poll-interval` | `1s` | Forwarder poll interval when the queue is empty | no |
| `--backup-path` | empty | Periodic `VACUUM INTO` backup file; empty disables backup/restore | no |
| `--backup-interval` | `5m` | Interval between backups | no |
| `--api-key` | empty | This site's API key (`phk_...`); required unless ingestion runs with auth off | **yes** |
| `--ca-cert` | empty | CA for verifying Central Ingestion's certificate; required for `https://` URLs | no |
| `--enable-chaos` | `false` | Expose `/admin/chaos/*` so the dashboard can simulate a forwarder partition; local/demo only | no |

## pharos-dashboard

| Flag | Default | Purpose | Secret |
|---|---|---|---|
| `--port` | `8092` | HTTP listen port | no |
| `--cassandra-hosts` | `127.0.0.1` | Comma-separated Cassandra contact points | no |
| `--cassandra-port` | `9042` | Cassandra native port | no |
| `--cassandra-keyspace` | `pharos` | Keyspace | no |
| `--ca-cert` | default CA | CA used to verify Cassandra and Central Ingestion TLS | no |
| `--central-url` | `http://localhost:8091` | Central Ingestion base URL for DLQ replay and test-event submission | no |
| `--grafana-url` | `http://localhost:3000` | Grafana link-out; empty hides the link | no |
| `--ingestion-metrics-url` | `http://localhost:8091/metrics` | Source for the Correctness Ledger | no |
| `--consumer-metrics-url` | `http://localhost:9091/metrics` | Source for the Correctness Ledger | no |
| `--memory` | `false` | In-memory sample data, no Cassandra | no |
| `--enable-chaos` | `false` | Enable the Chaos Control Panel's infrastructure-mutating actions; local/demo only | no |

## pharos-cli

`pharos-cli` parses global flags by hand, so they may appear anywhere on the
command line (`--flag value` or `--flag=value`).

| Flag | Default | Purpose | Secret |
|---|---|---|---|
| `--hosts` | `127.0.0.1` | Comma-separated Cassandra hosts | no |
| `--port` | `9042` | Cassandra port | no |
| `--keyspace` | `pharos` | Keyspace | no |
| `--central-url` | `http://localhost:8091` | Central Ingestion base URL, for `dlq replay` | no |
| `--ca-cert` | empty (library default CA) | CA for Cassandra TLS and, for `dlq replay`, Central Ingestion | no |
| `--operator` | none | Identity recorded in the access-audit trail; required for `query`/`dlq` unless `--memory` | no |
| `--site-id` | none | Site to authenticate as for `dlq replay` | no |
| `--api-key` | none | API key for `dlq replay` | **yes** |
| `--json` | `false` | JSON output | no |
| `--memory` | `false` | Built-in sample data, no Cassandra | no |

Subcommand flags: `query study --from --to` (RFC 3339), `query site --min-seq`
(default `1`), `dlq list --site --limit` (default `50`), `dlq replay --all --site --limit`
(default `500`), `archive run --older-than` (default `2160h`) `--archive-dir`
(default `./archive`) `--dry-run`, `audit list --limit` (default `50`),
`audit late-arrivals --group` (default `pharos-canonical-sink`) `--limit`
(default `50`).

## Environment variables read by helper scripts

None of these affect the Go binaries.

| Variable | Default | Read by | Purpose | Secret |
|---|---|---|---|---|
| `CERT_DIR` | `<repo>/certs` | `scripts/generate_certs.sh` | Output directory for TLS materials (the script **deletes and recreates it**) | no |
| `PHAROS_TLS_DAYS` | `3650` | `scripts/generate_certs.sh` | Certificate validity in days | no |
| `PHAROS_TLS_STORE_PASS` | `pharos-dev-tls-pass` | `scripts/generate_certs.sh` | Password for the generated JKS/PKCS12 stores. Note `docker-compose.yml` hardcodes the default, so changing it requires editing the compose file too | **yes** (dev-only default) |
| `EXTRA_CASSANDRA_SANS`, `EXTRA_KAFKA_SANS`, `EXTRA_INGESTION_SANS` | empty | `scripts/generate_certs.sh` | Extra certificate SANs (set by `deploy/k8s/generate_k8s_certs.sh`) | no |
| `PHAROS_K8S_NAMESPACE` | `pharos` | `deploy/k8s/generate_k8s_certs.sh` | Namespace used in the K8s SANs | no |
| `PHAROS_FORCE_REGEN_CERTS` | unset | `deploy/k8s/deploy.sh` | `1` regenerates K8s certs even if present | no |
| `KAFKA_BOOTSTRAP_SERVER` | `localhost:9092` | `scripts/create_topics.sh` | Bootstrap server for topic provisioning | no |
| `PHAROS_BACKUP_DIR`, `PHAROS_BACKUP_KEYSPACE`, `PHAROS_BACKUP_NODES` | `<repo>/backup`, `pharos`, the four Cassandra containers | `scripts/backup_cassandra.sh`, `scripts/restore_cassandra.sh` | Backup location, keyspace and node list | no |
| `LOADTEST_CASSANDRA_HOSTS`, `LOADTEST_CA_CERT`, `LOADTEST_NUM_SITES` | `127.0.0.1`, `<repo>/certs/ca-cert.pem`, `10` | `scripts/loadtest_setup.sh` | Load-test site provisioning | no |

`docker-compose.yml` itself reads no variables from the environment or an
`.env` file; its service settings (including `GF_SECURITY_ADMIN_PASSWORD`,
the Grafana demo password, and the JKS passwords) are literal values meant for
the local development topology only.
