#!/usr/bin/env bash
set -euo pipefail

# Pharos TLS certificate generation (§2.1, §2.2, §2.4, Slice 15: Auth & TLS)
#
# Generates one project-owned CA and issues every certificate this project
# uses -- Cassandra's client + inter-node listeners, Kafka's client +
# inter-broker listeners, and Central Ingestion's HTTP listener. No cloud
# spend and no real domain means no path to a publicly-trusted certificate
# anyway, and none is needed for traffic that never leaves Docker-simulated
# infrastructure this project itself controls end to end.
#
# One shared keystore per service family (all 5 Cassandra nodes share one
# cert+keystore, all 5 Kafka brokers share another) rather than a uniquely
# issued identity cert per node: this is a private cluster of trusted,
# project-owned nodes, not a zero-trust mesh of independently-operated ones.
#
# Re-running this script regenerates everything from scratch -- it's meant
# to be idempotent for local/CI use, not a long-lived PKI with rotation
# history to preserve.

CERT_DIR="${CERT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/certs}"
STORE_PASS="${PHAROS_TLS_STORE_PASS:-pharos-dev-tls-pass}"
DAYS="${PHAROS_TLS_DAYS:-3650}"

echo "Generating Pharos TLS materials in ${CERT_DIR} (validity: ${DAYS} days)..."
rm -rf "${CERT_DIR}"
mkdir -p "${CERT_DIR}"
cd "${CERT_DIR}"

# 1. Project CA -- self-signed root, signs everything else.
openssl genrsa -out ca-key.pem 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca-key.pem -sha256 -days "${DAYS}" \
  -out ca-cert.pem -subj "/O=Pharos/CN=Pharos Development CA" 2>/dev/null

issue_cert() {
  local name="$1" cn="$2" sans="$3"
  openssl genrsa -out "${name}-key.pem" 2048 2>/dev/null
  openssl req -new -key "${name}-key.pem" -out "${name}.csr" \
    -subj "/O=Pharos/CN=${cn}" 2>/dev/null
  cat > "${name}-ext.cnf" <<EOF
subjectAltName = ${sans}
extendedKeyUsage = serverAuth, clientAuth
EOF
  openssl x509 -req -in "${name}.csr" -CA ca-cert.pem -CAkey ca-key.pem \
    -CAcreateserial -out "${name}-cert.pem" -days "${DAYS}" -sha256 \
    -extfile "${name}-ext.cnf" 2>/dev/null
  rm -f "${name}.csr" "${name}-ext.cnf"
}

# 2. Cassandra: shared cert across all 5 nodes (dc-us: 1-3, dc-eu: 4-5),
# covering both container-internal names (inter-node + in-container client
# access) and localhost/127.0.0.1 (host-side Go clients via the exposed port).
# EXTRA_CASSANDRA_SANS/EXTRA_KAFKA_SANS (empty by default, unused by the
# Docker Compose flow) let deploy/k8s's own cert-generation step (§2.4,
# PLAN.md Slice 17: Deployment automation) append K8s headless-Service DNS
# names to the same shared cert/CA rather than duplicating this whole
# script for a second SAN list.
CASSANDRA_SANS="DNS:localhost,DNS:pharos-cassandra-1,DNS:pharos-cassandra-2,DNS:pharos-cassandra-3,DNS:pharos-cassandra-4,DNS:pharos-cassandra-5,IP:127.0.0.1"
if [ -n "${EXTRA_CASSANDRA_SANS:-}" ]; then
  CASSANDRA_SANS="${CASSANDRA_SANS},${EXTRA_CASSANDRA_SANS}"
fi
issue_cert "cassandra" "Pharos Cassandra Cluster" "${CASSANDRA_SANS}"

# 3. Kafka: shared cert across all 5 brokers (cluster A: 1-3, cluster B: 4-5).
KAFKA_SANS="DNS:localhost,DNS:pharos-kafka-1,DNS:pharos-kafka-2,DNS:pharos-kafka-3,DNS:pharos-kafka-4,DNS:pharos-kafka-5,IP:127.0.0.1"
if [ -n "${EXTRA_KAFKA_SANS:-}" ]; then
  KAFKA_SANS="${KAFKA_SANS},${EXTRA_KAFKA_SANS}"
fi
issue_cert "kafka" "Pharos Kafka Clusters" "${KAFKA_SANS}"

# 4. Central Ingestion: host-process HTTP listener, not containerized
# (except under deploy/k8s, which appends its own Service DNS name here).
INGESTION_SANS="DNS:localhost,IP:127.0.0.1"
if [ -n "${EXTRA_INGESTION_SANS:-}" ]; then
  INGESTION_SANS="${INGESTION_SANS},${EXTRA_INGESTION_SANS}"
fi
issue_cert "ingestion" "Pharos Central Ingestion" "${INGESTION_SANS}"

# 5. Cassandra/Kafka need Java keystores (JKS), not raw PEM -- both images
# bundle a JVM with keytool, but building the keystore here (on the host,
# once) is simpler than doing it inside a running container.
build_jks() {
  local name="$1"
  openssl pkcs12 -export -in "${name}-cert.pem" -inkey "${name}-key.pem" \
    -certfile ca-cert.pem -name "${name}" -out "${name}.p12" \
    -passout "pass:${STORE_PASS}" 2>/dev/null
  keytool -importkeystore -destkeystore "${name}-keystore.jks" \
    -srckeystore "${name}.p12" -srcstoretype PKCS12 \
    -alias "${name}" -deststorepass "${STORE_PASS}" -srcstorepass "${STORE_PASS}" \
    -noprompt >/dev/null 2>&1
  keytool -import -file ca-cert.pem -alias pharos-ca \
    -keystore "${name}-truststore.jks" -storepass "${STORE_PASS}" \
    -noprompt >/dev/null 2>&1
  rm -f "${name}.p12"
}

build_jks "cassandra"
build_jks "kafka"

# 6. A plain Java client properties file for anything that talks to Kafka's
# SSL listener via the kafka-topics.sh/kafka-configs.sh CLI (health checks,
# scripts/create_topics.sh) rather than a Go client -- security.protocol=SSL,
# trust the same CA, no client cert (ssl.client.auth=none on the broker side,
# so no keystore needed here).
cat > kafka-client-ssl.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/kafka-truststore.jks
ssl.truststore.password=${STORE_PASS}
EOF

chmod 600 ./*-key.pem

# 7. A complete cassandra.yaml with TLS enabled, built from the *actual*
# image's own stock file rather than hand-written from scratch -- the stock
# file already handles dozens of settings the official entrypoint script
# still needs to env-var-substitute at container startup (broadcast address
# autodetection, seeds, cluster_name, ...), all of which must survive
# untouched. Only the two encryption blocks are edited, each scoped strictly
# to its own block boundaries (server_encryption_options /
# client_encryption_options) -- a blind global find/replace on a key like
# "enabled: false" would also flip transparent_data_encryption_options and
# audit_logging_options, which happen to use the identical line text
# elsewhere in the same file. Caught by hand by actually reading the stock
# file before writing this, not assumed safe.
#
# Keystore path is /tls/... , not somewhere under /etc/cassandra -- the
# entrypoint script also does `find "$CASSANDRA_CONF" ... -exec chown
# cassandra`, and a read-only bind-mounted file inside that tree makes the
# chown (and the whole entrypoint, since it runs under `set -e`) fail
# outright. Found by watching the container actually exit on first attempt,
# not anticipated up front.
echo "Building TLS-enabled cassandra.yaml from the stock image config..."
docker run --rm --entrypoint sh cassandra:5.0 -c "cat /etc/cassandra/cassandra.yaml" > cassandra.yaml

python3 - "cassandra.yaml" "/tls/cassandra-keystore.jks" "${STORE_PASS}" "/tls/cassandra-truststore.jks" <<'PYEOF'
import sys

def patch(lines, block_header, keystore_path, keystore_pass, set_enabled=None, set_internode=None, truststore_path=None):
    start = None
    for i, line in enumerate(lines):
        if line.startswith(block_header):
            start = i
            break
    if start is None:
        raise SystemExit(f"block {block_header!r} not found in stock cassandra.yaml -- image contents changed?")
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i]
        if stripped.strip() == "":
            continue
        if not stripped.startswith(" ") and not stripped.startswith("#"):
            end = i
            break
    for i in range(start, end):
        line = lines[i]
        if set_enabled is not None and line.strip() == "enabled: false":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}enabled: {set_enabled}\n"
        elif set_internode is not None and line.strip() == "internode_encryption: none":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}internode_encryption: {set_internode}\n"
        elif line.strip() == "keystore: conf/.keystore":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}keystore: {keystore_path}\n"
        elif line.strip() == "#keystore_password: cassandra":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}keystore_password: {keystore_pass}\n"
        elif truststore_path is not None and line.strip() == "truststore: conf/.truststore":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}truststore: {truststore_path}\n"
        elif truststore_path is not None and line.strip() == "#truststore_password: cassandra":
            indent = line[:len(line) - len(line.lstrip())]
            lines[i] = f"{indent}truststore_password: {keystore_pass}\n"
    return lines

path, keystore_path, keystore_pass, truststore_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
with open(path) as f:
    lines = f.readlines()

# Client-to-server encryption (client_encryption_options.enabled=true).
lines = patch(lines, "client_encryption_options:", keystore_path, keystore_pass, set_enabled="true")

# Internode encryption (audit remediation, revisiting the "client-only"
# narrowing this file previously documented here): internode_encryption=all,
# reusing the exact same already-issued keystore/truststore -- the SANs
# already cover every pharos-cassandra-N container name, no new certs
# needed. require_client_auth stays false (mirrors client_encryption_options'
# own choice not to require mTLS); the truststore is still required on this
# block regardless, since the *outbound* side of an internode connection
# validates the peer's presented certificate against it even without
# demanding one back.
lines = patch(lines, "server_encryption_options:", keystore_path, keystore_pass, set_internode="all", truststore_path=truststore_path)

with open(path, "w") as f:
    f.writelines(lines)
PYEOF

# Each Cassandra node needs its *own* copy of this file, not five containers
# bind-mounting the same host file: the official entrypoint script edits
# cassandra.yaml in place (sed -i) at container startup to substitute each
# node's own broadcast address/DC/rack, and it needs to write, so the mount
# can't be read-only. Five containers sharing one writable host file would
# race to overwrite each other's node-specific substitutions -- caught by
# thinking through what "writable + shared across containers" actually means
# before wiring it into docker-compose.yml, not discovered by watching it
# corrupt a running cluster.
for i in 1 2 3 4 5; do
  cp cassandra.yaml "cassandra-${i}.yaml"
done

echo "Done. CA: ca-cert.pem / ca-key.pem"
echo "Cassandra: cassandra-cert.pem, cassandra-key.pem, cassandra-keystore.jks, cassandra-truststore.jks, cassandra-{1..5}.yaml"
echo "Kafka:     kafka-cert.pem, kafka-key.pem, kafka-keystore.jks, kafka-truststore.jks"
echo "Ingestion: ingestion-cert.pem, ingestion-key.pem"
echo "Keystore/truststore password: ${STORE_PASS} (override with PHAROS_TLS_STORE_PASS)"
