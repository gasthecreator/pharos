-- Pharos Cassandra DLQ Secondary Index (§2.3, §4)
-- Enables efficient inspection of rejected events by clinical trial site_id
-- without requiring a full table scan over dead_letter_events.

CREATE INDEX IF NOT EXISTS dead_letter_site_idx ON pharos.dead_letter_events (site_id);
