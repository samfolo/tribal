-- Add system fingerprint hash to jobs for eval provenance.
--
-- Tables are empty — NOT NULL applied directly without backfill.

ALTER TABLE jobs
    ADD COLUMN system_fingerprint_hash VARCHAR(64) NOT NULL
    REFERENCES system_fingerprints(content_hash);

CREATE INDEX idx_jobs_system_fingerprint ON jobs(system_fingerprint_hash);
