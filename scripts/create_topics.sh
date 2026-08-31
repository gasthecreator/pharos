#!/usr/bin/env bash
set -euo pipefail

# Pharos Kafka Topic Provisioning & Retention Policy Configuration (§4)
# Configures:
#   pharos.events.adverse: 7 days retention (604800000 ms), 10GB max bytes (FDA 21 CFR 312.32(c)(2))
#   pharos.events.dlq:    14 days retention (1209600000 ms), 5GB max bytes (clinical data management buffer)

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"

echo "Configuring Pharos Kafka topics on ${BOOTSTRAP_SERVER}..."

# In Docker or local environment, locate kafka-topics and kafka-configs binaries
if command -v kafka-topics &> /dev/null; then
    KAFKA_TOPICS="kafka-topics"
    KAFKA_CONFIGS="kafka-configs"
elif [ -f "/opt/kafka/bin/kafka-topics.sh" ]; then
    KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
    KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"
elif docker ps --format '{{.Names}}' | grep -q "^pharos-kafka-1$"; then
    KAFKA_TOPICS="docker exec pharos-kafka-1 /opt/kafka/bin/kafka-topics.sh"
    KAFKA_CONFIGS="docker exec pharos-kafka-1 /opt/kafka/bin/kafka-configs.sh"
elif docker ps --format '{{.Names}}' | grep -q "^pharos-kafka$"; then
    KAFKA_TOPICS="docker exec pharos-kafka /opt/kafka/bin/kafka-topics.sh"
    KAFKA_CONFIGS="docker exec pharos-kafka /opt/kafka/bin/kafka-configs.sh"
else
    echo "Warning: kafka binaries not found locally or in docker. Relying on Go EnsureTopics()."
    exit 0
fi

# 1. Main Adverse Events Topic
echo "Ensuring topic pharos.events.adverse..."
$KAFKA_TOPICS --bootstrap-server "${BOOTSTRAP_SERVER}" --create --if-not-exists \
    --topic pharos.events.adverse \
    --partitions 3 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --config retention.bytes=10737418240

$KAFKA_CONFIGS --bootstrap-server "${BOOTSTRAP_SERVER}" --entity-type topics --entity-name pharos.events.adverse --alter \
    --add-config retention.ms=604800000,retention.bytes=10737418240

# 2. Dead-Letter Events Topic
echo "Ensuring topic pharos.events.dlq..."
$KAFKA_TOPICS --bootstrap-server "${BOOTSTRAP_SERVER}" --create --if-not-exists \
    --topic pharos.events.dlq \
    --partitions 3 \
    --replication-factor 3 \
    --config retention.ms=1209600000 \
    --config retention.bytes=5368709120

$KAFKA_CONFIGS --bootstrap-server "${BOOTSTRAP_SERVER}" --entity-type topics --entity-name pharos.events.dlq --alter \
    --add-config retention.ms=1209600000,retention.bytes=5368709120

echo "Pharos Kafka topics successfully provisioned with explicit retention policies."
