# Load testing (PLAN.md Slice 16)

Establishes real baseline throughput/latency numbers for Central Ingestion
under realistic multi-site volume, and observes what actually happens when
one site bursts far past its rate limit while every other site keeps
submitting normally -- against real Cassandra, real Kafka, and the real
built `pharos-ingestion`/`pharos-consumer` binaries, not mocks.

## Setup

```bash
docker compose up -d                       # Cassandra + Kafka + MirrorMaker2
make build                                 # bin/pharos-ingestion, pharos-consumer, pharos-cli
./scripts/generate_certs.sh                # if certs/ doesn't exist yet
./scripts/loadtest_setup.sh                # provisions 10 fresh site API keys -> loadtest/sites.json

./bin/pharos-ingestion --port 8443 \
  --ca-cert ./certs/ca-cert.pem \
  --tls-cert ./certs/ingestion-cert.pem \
  --tls-key ./certs/ingestion-key.pem \
  --enable-auth=true &

./bin/pharos-consumer \
  --ca-cert ./certs/ca-cert.pem \
  --kafka-group pharos-loadtest-sink \
  --metrics-port 9091 &
```

## Run

```bash
k6 run loadtest/pharos_load_test.js -e PHAROS_URL=https://localhost:8443/api/v1/events
```

Takes ~80-90s: 9 sites submit steadily (`steady_state` scenario) for the
whole run; a 10th, separate site stays idle for the first 30s, then for 15s
receives a genuine 30 req/s arrival rate (`burst_site` scenario, via k6's
`constant-arrival-rate` executor -- a sequential single-VU loop doesn't
actually stress the rate limiter, since real backend latency paces it
below the refill rate on its own; see WORKLOG.md for that finding), well
past its own 100-token bucket + 10/s refill.

## What to look at

- k6's own summary: `http_req_duration` percentiles and `pharos_accepted_duration`
  (tagged `scenario:steady_state` vs `scenario:burst_site`) -- did the
  burst change *steady_state*'s latency at all, or stay isolated to the
  bursting site?
- `pharos_rate_limit_rejections` -- this is a threshold (`count==0`): any
  steady-state site getting a 429 because of an unrelated site's burst
  would mean the per-site token bucket isn't actually isolating sites.
- `pharos_burst_rate_limit_rejections` -- expected to be > 0 partway through
  the burst; this is the rate limiter doing its job, not a failure.
- `curl --cacert ./certs/ca-cert.pem https://localhost:8443/metrics` and
  `curl http://localhost:9091/metrics` before/after the run, diffing
  `pharos_cassandra_write_duration_seconds` / `pharos_outbox_publish_duration_seconds`
  histograms -- this is how to find the actual bottleneck rather than
  assuming one (PLAN.md's own framing for this slice).

Real numbers from an actual run are recorded in WORKLOG.md and PLAN.md's
Slice 16 entry, not reproduced here -- this file documents how to
reproduce them, not what they were on one particular machine on one
particular day.
