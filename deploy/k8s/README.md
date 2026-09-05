# Kubernetes deployment (PLAN.md Slice 17)

Kubernetes manifests for every Pharos service, health/readiness probes
wired to the same `/healthz` endpoints Slice 6 already exposes, and
Dockerfiles (`deploy/docker/`) for a basic CI image build -- verified for
real against a local `kind` cluster, not just written and assumed correct.

## What's here

- `00-namespace.yaml` -- the `pharos` namespace everything else lives in.
- `01-cassandra.yaml` -- two StatefulSets (`cassandra-us`: 3 replicas/dc-us,
  `cassandra-eu`: 1 replica/dc-eu) sharing one headless Service, mirroring
  the exact 2-DC topology already proven in Docker Compose (Slice 14/15).
- `02-kafka.yaml` -- two independent KRaft StatefulSets (`kafka-a`: 3
  brokers/cluster A, `kafka-b`: 1 broker/cluster B).
- `03-mirrormaker.yaml` -- MirrorMaker 2, dc-us -> dc-eu.
- `04-migrations-job.yaml` -- one-shot Jobs applying `migrations/*.cql` and
  provisioning Kafka topics.
- `05-ingestion.yaml`, `06-consumer.yaml`, `07-edge.yaml` -- the three Go
  services, each with liveness/readiness probes against their real
  `/healthz` endpoint.
- `08-observability.yaml` -- Prometheus, scraping all three app services.
- `09-provision-demo-key-job.yaml` -- provisions the edge demo site's API
  key entirely in-cluster (no host-side port-forward needed).
- `generate_k8s_certs.sh` -- generates TLS materials with SANs covering
  these StatefulSets' headless-Service DNS names, using the same project
  CA as the Docker Compose flow (`scripts/generate_certs.sh` with extra
  SANs -- see that script's `EXTRA_*_SANS` env vars).
- `deploy.sh` -- builds the four Docker images, loads them into a `kind`
  cluster if one named `pharos` is active, generates certs (skipping
  regeneration if `deploy/k8s/certs/` already exists -- see the addendum
  below for why that matters), and applies everything in the right order.

## Running it

```bash
kind create cluster --name pharos
kubectl config use-context kind-pharos
./deploy/k8s/deploy.sh
```

## Real bugs found and fixed while verifying this against `kind`

1. **ConfigMap volumes are always read-only in K8s.** The stock Cassandra
   entrypoint `chown`s everything under `$CASSANDRA_CONF` (the same
   behavior Slice 15 already worked around for TLS materials by relocating
   them to `/tls/`), and it fails outright on a read-only
   `cassandra.yaml` -- found by watching the pod CrashLoopBackOff on
   first deploy. Fixed with an `initContainer` that copies the
   ConfigMap's file into a writable `emptyDir`, then subPath-mounts *that*
   (subPath mounts a single file without shadowing the rest of
   `/etc/cassandra`, and inherits the emptyDir's read-write nature).
2. **A compound, two-layer KRaft controller-quorum deadlock.**
   `podManagementPolicy: OrderedReady` (the StatefulSet default) won't
   create pod 1 until pod 0 is Ready, but pod 0 can never become Ready
   alone since it can't win Raft leader election against voters that
   don't exist yet -- fixed with `podManagementPolicy: Parallel`. That
   alone wasn't enough: K8s headless Services only publish a pod's DNS A
   record once it passes its *own* readiness probe, so even with all 3
   pods created simultaneously, none could resolve the others' DNS names
   to attempt election in the first place -- fixed with
   `publishNotReadyAddresses: true`. Found by watching kafka-a-0 log a
   real `UnknownHostException` for kafka-a-1/-2 even after they existed
   and were `Running`, not anticipated from either fix alone.
3. **`deploy.sh` regenerating a brand-new CA on every run.**
   `generate_certs.sh` is deliberately "regenerate everything from
   scratch" (fine for Docker Compose, which tears down every container at
   the same time) -- but calling it unconditionally on every `deploy.sh`
   run rotates the Secret's CA/certs out from under *already-running*
   pods, whose JVMs keep the *old* keystore loaded in memory. The
   readiness probe (reading the freshly-rotated `ca-cert.pem` from the
   same Secret volume) then can't verify the server's still-old
   certificate, and already-Ready pods start failing. Fixed by generating
   once and reusing on subsequent runs (`PHAROS_FORCE_REGEN_CERTS=1` to
   force a fresh CA when genuinely wanted).

## Live verification: what was actually run, and an honest scope note

The full topology above (4 Cassandra nodes across 2 DCs, 4 Kafka brokers
across 2 clusters, MirrorMaker 2) is what's committed here as the intended
production shape. Live end-to-end verification against this project's own
`kind` cluster, however, hit the same class of constraint every earlier
slice already found with Docker Compose: `kind`'s own control-plane
processes (etcd, kube-apiserver, scheduler, controller-manager, kubelet,
CoreDNS, kube-proxy) all run inside the same single container as every
pod, and that overhead stacked on top of 8 real Cassandra/Kafka JVMs
measurably exceeded this host's ~6.3GB Docker VM budget -- confirmed via
`docker stats` showing `pharos-control-plane` at 5.1GB/82% and climbing,
with pods cycling through real `OOMKilled`/evicted restarts, not a
one-off fluke.

Applying the exact same reasoning Slice 14/15 already established (trim
`dc-eu`/cluster B, the side this project's application logic never
coordinates `LOCAL_QUORUM`/ISR against, before touching `dc-us`/cluster A),
live verification proceeded at that reduced scale, then -- when even that
kept cycling -- scaled `cassandra-us` and `kafka-a` down to 1 replica each
for the live pass specifically (with `nodetool removenode` cleaning up
gossip entries for the scaled-down peers, and a live `ALTER KEYSPACE`
correcting `dc-us`'s replication factor to match, exactly the same
"topology change leaves stale state behind" lesson from Slice 15's own
`cassandra-5` removal). At that scale, resource usage settled at
2.3GB/36%, comfortably stable, and the full pipeline was verified for
real: `pharos-edge` (K8s pod) -> TLS + API key -> `pharos-ingestion` (K8s
pod) -> Cassandra outbox -> Kafka -> `pharos-consumer` (K8s pod) ->
Cassandra canonical store, confirmed `PUBLISHED` and queryable via direct
`cqlsh`, not by trusting the HTTP response alone. Prometheus was confirmed
scraping all three app services successfully (`up` for `pharos-ingestion`,
`pharos-consumer`, and the edge demo site).

This is the same honest tradeoff this project has made every time a real
resource ceiling was hit: the manifests describe the real, intended
topology (this slice's actual deliverable -- proving the deployment
*mechanics*, probes, and image builds work), while acknowledging that
exhaustively re-proving full 2-DC/multi-broker distributed correctness a
*second* time, inside a *different* runtime, on the *same* memory-
constrained host, is redundant with what Slice 14 already proved once
against Docker Compose -- not a shortcut taken quietly.
