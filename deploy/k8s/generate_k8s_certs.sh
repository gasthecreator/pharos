#!/usr/bin/env bash
set -euo pipefail

# Generates this project's TLS materials with SANs covering K8s headless
# Service DNS names (§2.4, PLAN.md Slice 17: Deployment automation), in
# addition to the existing Docker Compose names -- one shared CA either
# way, so certs/ca-cert.pem trusted by a host-run pharos-cli also verifies
# a pod running inside the kind cluster. Output goes to deploy/k8s/certs/
# (gitignored, same reasoning as the top-level certs/: private keys).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
K8S_CERT_DIR="${ROOT_DIR}/deploy/k8s/certs"
NAMESPACE="${PHAROS_K8S_NAMESPACE:-pharos}"

export CERT_DIR="${K8S_CERT_DIR}"
export EXTRA_CASSANDRA_SANS="DNS:*.cassandra.${NAMESPACE}.svc.cluster.local,DNS:cassandra.${NAMESPACE}.svc.cluster.local"
export EXTRA_KAFKA_SANS="DNS:*.kafka-a.${NAMESPACE}.svc.cluster.local,DNS:kafka-a.${NAMESPACE}.svc.cluster.local,DNS:*.kafka-b.${NAMESPACE}.svc.cluster.local,DNS:kafka-b.${NAMESPACE}.svc.cluster.local"
export EXTRA_INGESTION_SANS="DNS:pharos-ingestion.${NAMESPACE}.svc.cluster.local,DNS:pharos-ingestion"

"${ROOT_DIR}/scripts/generate_certs.sh"

echo "K8s-ready TLS materials written to ${K8S_CERT_DIR}"
