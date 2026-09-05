-- Pharos Cassandra Schema Initialization (§2.2, §2.3)
-- Defines the keyspace and tables for the transactional outbox, dead-letter pipeline, and sweeper index.

-- NetworkTopologyStrategy across two simulated datacenters (§2.4, Slice 14:
-- Multi-Region Cassandra + Kafka) -- matches the replication map the Go
-- bootstrap code (internal/dedup, internal/consumer) builds at runtime, so
-- CI applying this file first (before any Go code runs) doesn't leave the
-- keyspace on the old SimpleStrategy that a later "IF NOT EXISTS" would then
-- silently leave in place. dc-eu is RF=2 (not 3) matching its 2-node
-- topology in docker-compose.yml -- see that file's Cassandra section
-- comment for why dc-eu specifically was trimmed from the originally-planned
-- 3 nodes to fit this project's 8GB host.
CREATE KEYSPACE IF NOT EXISTS pharos
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc-us': 3, 'dc-eu': 2};

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
