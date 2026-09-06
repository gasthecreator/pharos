-- Pharos Event Outbox Pruning Index (§2.2, audit remediation)
--
-- event_outbox/pending_outbox were deliberately excluded from the Slice 11
-- archive.RunCanonical/RunDLQ cold-storage pipeline as "operational
-- bookkeeping, not the data-of-record" (see internal/archive/archive.go's
-- package doc). That reasoning is still correct, but it only argues against
-- *archiving* these rows to cold storage for later query -- it says nothing
-- about whether PUBLISHED rows should eventually be deleted once they're no
-- longer needed for their operational purpose (crash-safe claim/lease +
-- fast-path dedup). Nobody ever added that deletion, so event_outbox grew
-- without bound. pending_outbox already self-cleans (MarkPublished and
-- MarkDLQPublished both delete their row from it), so it needs no pruning
-- support here.
--
-- event_outbox has no existing range-query index the way canonical
-- (events_by_study's event_time clustering key) and DLQ
-- (dead_letter_events_by_site's rejected_at clustering key) archival already
-- have, so this table is that same partition-key-first index, scoped to
-- site_id exactly like dead_letter_events_by_site, populated at
-- MarkPublished time since published_at is only known then.
--
-- Pruning a row here is safe even for a very-late resubmission: this table
-- is only ever the *fast-path* dedup check -- canonical_events' own
-- idempotent upsert (keyed by idempotency_key) is a permanent second
-- backstop against real duplication regardless of whether event_outbox
-- still has a record of that key. A resubmission after its outbox row was
-- pruned causes at most one extra Kafka publish attempt for an event
-- already durably recorded; it never produces duplicate canonical data.
CREATE TABLE IF NOT EXISTS pharos.event_outbox_by_site (
    site_id text,
    published_at timestamp,
    idempotency_key text,
    PRIMARY KEY ((site_id), published_at, idempotency_key)
) WITH CLUSTERING ORDER BY (published_at ASC, idempotency_key ASC);
