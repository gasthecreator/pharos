-- Pharos Cassandra Canonical Query Schema (§2.4, §5)
-- Defines the final queryable tables populated by the downstream Kafka consumer:
-- 1. canonical_events (point lookup & audit by idempotency key)
-- 2. events_by_study (clinical safety range queries by study and event time)
-- 3. events_by_site (operational sequence verification by site and local sequence)

-- 1. Canonical Events Table
-- Point lookups by client idempotency key for full payload audit and Kafka lineage.
CREATE TABLE IF NOT EXISTS pharos.canonical_events (
    idempotency_key text,
    site_id text,
    study_id text,
    local_seq bigint,
    event_time timestamp,
    recorded_time timestamp,
    ingestion_time timestamp,
    severity text,
    event_code text,
    subject text,
    payload text,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    consumed_at timestamp,
    is_late boolean,
    PRIMARY KEY (idempotency_key)
);

-- 2. Events By Study Table
-- Serves DSMB / FDA regulatory safety queries: "all events for trial X in date range Y"
-- Partition key = study_id, Clustering = event_time DESC, idempotency_key ASC
CREATE TABLE IF NOT EXISTS pharos.events_by_study (
    study_id text,
    event_time timestamp,
    idempotency_key text,
    site_id text,
    local_seq bigint,
    recorded_time timestamp,
    ingestion_time timestamp,
    severity text,
    event_code text,
    subject text,
    payload text,
    is_late boolean,
    PRIMARY KEY ((study_id), event_time, idempotency_key)
) WITH CLUSTERING ORDER BY (event_time DESC, idempotency_key ASC);

-- 3. Events By Site Table
-- Serves site operations & auditing queries: "all events from site Z"
-- Partition key = site_id, Clustering = local_seq DESC, idempotency_key ASC
CREATE TABLE IF NOT EXISTS pharos.events_by_site (
    site_id text,
    local_seq bigint,
    idempotency_key text,
    study_id text,
    event_time timestamp,
    recorded_time timestamp,
    ingestion_time timestamp,
    severity text,
    event_code text,
    subject text,
    payload text,
    is_late boolean,
    PRIMARY KEY ((site_id), local_seq, idempotency_key)
) WITH CLUSTERING ORDER BY (local_seq DESC, idempotency_key ASC);
