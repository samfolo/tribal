-- Durable ingest idempotency and the principal-scoped recent listing.
--
-- The key is producer-supplied, reused only for retries of one logical
-- ingest operation, and unique per principal so the same UUID under two
-- principals stays independent. The job row itself carries the comparison
-- payload (immutable project_id and raw_input), so no second fingerprint
-- is stored. The recent-listing index serves the principal-scoped
-- (created_at DESC, id DESC) page order.

ALTER TABLE jobs
    ADD COLUMN ingest_idempotency_key UUID;

CREATE UNIQUE INDEX jobs_principal_ingest_idempotency_key_uq
    ON jobs (principal_id, ingest_idempotency_key)
    WHERE ingest_idempotency_key IS NOT NULL;

CREATE INDEX idx_jobs_principal_recent
    ON jobs (principal_id, created_at DESC, id DESC);
