-- Pharos Cassandra Dead-Letter Query Table (§2.3, §4)
-- Models rejected events by trial site_id with partition-key-first design,
-- avoiding multi-node scatter-gather queries caused by secondary indexes.

CREATE TABLE IF NOT EXISTS pharos.dead_letter_events_by_site (
    site_id text,
    rejected_at timestamp,
    idempotency_key text,
    payload text,
    rejection_reason text,
    validation_errors text,
    status text,
    claimed_at timestamp,
    published_at timestamp,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    PRIMARY KEY ((site_id), rejected_at, idempotency_key)
) WITH CLUSTERING ORDER BY (rejected_at DESC, idempotency_key ASC);
