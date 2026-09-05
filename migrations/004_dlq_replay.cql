-- Pharos DLQ Replay Support (§2.3, Slice 10)
-- Adds replayed_at to both DLQ tables so a REPLAYED record's history is
-- fully queryable without needing a second lookup against event_outbox.
-- The row is never deleted on replay -- only status and replayed_at change,
-- keeping the original rejection permanently part of the audit trail.

ALTER TABLE pharos.dead_letter_events ADD replayed_at timestamp;
ALTER TABLE pharos.dead_letter_events_by_site ADD replayed_at timestamp;
