-- Attribute embedding spend to a reindex run: backfill and catch-up batches
-- record a token_usage row carrying the run id, mirroring the job and task
-- attribution the ingest path already records. Nullable and additive; ingest
-- and read-path rows leave it null. The foreign key lives in its own migration
-- because token_usage precedes reindex_runs in the initial schema, so the
-- reference can only be declared once reindex_runs exists.
ALTER TABLE token_usage
    ADD COLUMN reindex_run_id UUID REFERENCES reindex_runs(id);
