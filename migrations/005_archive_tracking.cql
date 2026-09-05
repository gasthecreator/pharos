-- Pharos Archive Tracking Tables (§2.4, Slice 11)
-- Cassandra has no cheap way to list "all partition keys that exist" for a
-- table without a secondary index or ALLOW FILTERING -- both explicitly
-- avoided elsewhere in this project. These two small tables are the
-- partition-key-first alternative: idempotent upserts alongside existing
-- writes, giving the archival job an efficient way to discover which
-- studies/sites to scan for aged-out rows, without scattering a scan
-- across the whole cluster.

CREATE TABLE IF NOT EXISTS pharos.known_studies (
    study_id text,
    PRIMARY KEY (study_id)
);

CREATE TABLE IF NOT EXISTS pharos.known_sites (
    site_id text,
    PRIMARY KEY (site_id)
);
