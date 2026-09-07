#!/usr/bin/env bash
set -euo pipefail

# Deploys Pharos to the current kubectl context (§2.4, PLAN.md Slice 17:
# Deployment automation). Written for and verified against a local `kind`
# cluster (see deploy/k8s/README.md) -- no cloud spend, matching this
# project's own zero-cloud-spend constraint (PLAN.md §5.1), the same
# reasoning behind every other slice's "simulate real infra locally, don't
# skip the problem" approach (Slice 14's simulated multi-region, this
# slice's simulated production deployment).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
K8S_DIR="${ROOT_DIR}/deploy/k8s"
NAMESPACE="pharos"

log() { printf '\n\033[1;36m>>> %s\033[0m\n' "$1"; }

log "Building application images"
docker build -f "${ROOT_DIR}/deploy/docker/Dockerfile.ingestion" -t pharos-ingestion:local "${ROOT_DIR}"
docker build -f "${ROOT_DIR}/deploy/docker/Dockerfile.consumer" -t pharos-consumer:local "${ROOT_DIR}"
docker build -f "${ROOT_DIR}/deploy/docker/Dockerfile.edge" -t pharos-edge:local "${ROOT_DIR}"
docker build -f "${ROOT_DIR}/deploy/docker/Dockerfile.cli" -t pharos-cli:local "${ROOT_DIR}"
docker build -f "${ROOT_DIR}/deploy/docker/Dockerfile.dashboard" -t pharos-dashboard:local "${ROOT_DIR}"

if command -v kind &>/dev/null && kind get clusters 2>/dev/null | grep -q "^pharos$"; then
  log "Loading images into kind cluster 'pharos'"
  kind load docker-image pharos-ingestion:local pharos-consumer:local pharos-edge:local pharos-cli:local pharos-dashboard:local --name pharos
fi

CERT_DIR="${K8S_DIR}/certs"
if [ -f "${CERT_DIR}/ca-cert.pem" ] && [ "${PHAROS_FORCE_REGEN_CERTS:-}" != "1" ]; then
  log "Reusing existing K8s TLS materials in ${CERT_DIR} (set PHAROS_FORCE_REGEN_CERTS=1 to regenerate)"
  # generate_certs.sh regenerates a brand-new CA and every cert on each
  # run (by design -- see its own header comment) -- fine for a from-scratch
  # Docker Compose bring-up, which tears down every container at the same
  # time, but genuinely breaks a re-run of this script against an
  # ALREADY-DEPLOYED cluster: the Secret rotates to a new CA/cert while
  # the running pods keep their OLD keystore loaded in the JVM, so the
  # readiness probe's cqlsh (reading the freshly-rotated ca-cert.pem from
  # the same Secret volume) can no longer verify the server's still-old
  # certificate. Found by watching already-Ready Cassandra pods go
  # NotReady after a second deploy.sh run, not anticipated up front.
else
  log "Generating K8s-targeted TLS materials"
  "${K8S_DIR}/generate_k8s_certs.sh"
fi

log "Applying namespace"
kubectl apply -f "${K8S_DIR}/00-namespace.yaml"

log "Creating pharos-tls Secret from generated certs"
kubectl create secret generic pharos-tls -n "${NAMESPACE}" \
  --from-file=ca-cert.pem="${CERT_DIR}/ca-cert.pem" \
  --from-file=cassandra-keystore.jks="${CERT_DIR}/cassandra-keystore.jks" \
  --from-file=cassandra-truststore.jks="${CERT_DIR}/cassandra-truststore.jks" \
  --from-file=kafka-keystore.jks="${CERT_DIR}/kafka-keystore.jks" \
  --from-file=kafka-truststore.jks="${CERT_DIR}/kafka-truststore.jks" \
  --from-file=kafka-client-ssl.properties="${CERT_DIR}/kafka-client-ssl.properties" \
  --from-file=ingestion-cert.pem="${CERT_DIR}/ingestion-cert.pem" \
  --from-file=ingestion-key.pem="${CERT_DIR}/ingestion-key.pem" \
  --dry-run=client -o yaml | kubectl apply -f -

log "Creating cassandra.yaml ConfigMap"
kubectl create configmap cassandra-yaml -n "${NAMESPACE}" \
  --from-file=cassandra.yaml="${CERT_DIR}/cassandra-1.yaml" \
  --dry-run=client -o yaml | kubectl apply -f -

log "Creating mm2-config ConfigMap"
kubectl create configmap mm2-config -n "${NAMESPACE}" \
  --from-file=mm2.properties="${K8S_DIR}/mm2.properties" \
  --dry-run=client -o yaml | kubectl apply -f -

log "Creating pharos-migrations ConfigMap"
kubectl create configmap pharos-migrations -n "${NAMESPACE}" \
  --from-file="${ROOT_DIR}/migrations" \
  --dry-run=client -o yaml | kubectl apply -f -

log "Deploying Cassandra (dc-us: 3, dc-eu: 1)"
kubectl apply -f "${K8S_DIR}/01-cassandra.yaml"
kubectl rollout status statefulset/cassandra-us -n "${NAMESPACE}" --timeout=300s
kubectl rollout status statefulset/cassandra-eu -n "${NAMESPACE}" --timeout=300s

log "Deploying Kafka (cluster A: 3, cluster B: 1)"
kubectl apply -f "${K8S_DIR}/02-kafka.yaml"
kubectl rollout status statefulset/kafka-a -n "${NAMESPACE}" --timeout=300s
kubectl rollout status statefulset/kafka-b -n "${NAMESPACE}" --timeout=300s

log "Applying migrations and provisioning Kafka topics"
kubectl delete job pharos-migrations pharos-kafka-topics -n "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${K8S_DIR}/04-migrations-job.yaml"
kubectl wait --for=condition=complete job/pharos-migrations -n "${NAMESPACE}" --timeout=120s
kubectl wait --for=condition=complete job/pharos-kafka-topics -n "${NAMESPACE}" --timeout=120s

log "Deploying MirrorMaker 2 (after topics exist, per Slice 14's own sequencing lesson)"
kubectl apply -f "${K8S_DIR}/03-mirrormaker.yaml"

log "Provisioning the edge demo site's API key (in-cluster Job)"
kubectl delete job pharos-provision-demo-key -n "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${K8S_DIR}/09-provision-demo-key-job.yaml"
kubectl wait --for=condition=complete job/pharos-provision-demo-key -n "${NAMESPACE}" --timeout=60s
DEMO_KEY=$(kubectl logs job/pharos-provision-demo-key -n "${NAMESPACE}" | grep -oE 'phk_[A-Za-z0-9_-]+' || echo "")
if [ -z "$DEMO_KEY" ]; then
  echo "WARNING: could not parse SITE-K8S-DEMO's key from the provisioning Job's logs. Skipping edge deploy."
else
  kubectl create secret generic pharos-site-k8s-demo-key -n "${NAMESPACE}" \
    --from-literal=api-key="${DEMO_KEY}" --dry-run=client -o yaml | kubectl apply -f -
  log "Deploying pharos-ingestion, pharos-consumer, pharos-edge"
  kubectl apply -f "${K8S_DIR}/05-ingestion.yaml" -f "${K8S_DIR}/06-consumer.yaml" -f "${K8S_DIR}/07-edge.yaml"
  kubectl rollout status deployment/pharos-ingestion -n "${NAMESPACE}" --timeout=120s
  kubectl rollout status deployment/pharos-consumer -n "${NAMESPACE}" --timeout=120s
  kubectl rollout status deployment/pharos-edge-site-k8s-demo -n "${NAMESPACE}" --timeout=120s
fi

log "Deploying Prometheus"
kubectl apply -f "${K8S_DIR}/08-observability.yaml"
kubectl rollout status deployment/prometheus -n "${NAMESPACE}" --timeout=120s

log "Deploying pharos-dashboard"
kubectl apply -f "${K8S_DIR}/10-dashboard.yaml"
kubectl rollout status deployment/pharos-dashboard -n "${NAMESPACE}" --timeout=120s

log "Done. kubectl get pods -n ${NAMESPACE}"
kubectl get pods -n "${NAMESPACE}"
