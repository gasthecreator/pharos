-- Pharos Cassandra Schema Initialization (§2.2, §2.3)
-- Defines the keyspace and tables for the transactional outbox, dead-letter pipeline, and sweeper index.

CREATE KEYSPACE IF NOT EXISTS pharos
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 1. Dedup & Transactional Outbox
-- Primary key idempotency_key (<site_id>:<local_seq>) guarantees strict uniqueness via LWT.
-- status: 'PUBLISHING' | 'PUBLISHED'
CREATE TABLE IF NOT EXISTS pharos.event_outbox (
    idempotency_key text,
    site_id text,
    local_seq bigint,
    payload text,
    status text,
    claimed_at timestamp,
    created_at timestamp,
    published_at timestamp,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    PRIMARY KEY (idempotency_key)
);

-- 2. Dead-Letter Audit Store & Outbox
-- Mirrors the same claim/lease pattern as event_outbox for symmetric crash protection on rejections.
-- status: 'PUBLISHING' | 'PUBLISHED'
CREATE TABLE IF NOT EXISTS pharos.dead_letter_events (
    idempotency_key text,
    site_id text,
    payload text,
    rejection_reason text,
    validation_errors text,
    rejected_at timestamp,
    status text,
    claimed_at timestamp,
    published_at timestamp,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    PRIMARY KEY (idempotency_key)
);

-- 3. Time-Bucketed Sweeper Index
-- Provides efficient lookups for pending/stale outbox items without scanning main tables.
CREATE TABLE IF NOT EXISTS pharos.pending_outbox (
    bucket text,
    idempotency_key text,
    created_at timestamp,
    PRIMARY KEY (bucket, idempotency_key)
);
