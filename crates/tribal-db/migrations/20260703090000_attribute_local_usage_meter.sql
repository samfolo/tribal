-- Make the local usage meter principal-attributable and locus-aware — the
-- commercial-OSS ledger a self-hoster keeps for their own cost tracking, even
-- if they never buy the managed service.
--
-- principal_id names the contributor a row's spend belongs to, joinable to the
-- platform (user, account) through the principal's linkage columns. It is
-- nullable: system work and calls made before a principal binds attribute to
-- no one.
--
-- execution_locus separates the work the edge runtime ran locally from the
-- managed enrichment the platform ran elsewhere on the account's behalf. Rows
-- written before this milestone predate the managed runtime, so they are edge
-- by backfill.

ALTER TABLE token_usage
    ADD COLUMN principal_id UUID REFERENCES principals(id),
    ADD COLUMN execution_locus TEXT NOT NULL DEFAULT 'edge'
        CHECK (execution_locus IN ('edge', 'managed'));

-- Per-user and per-account aggregation walks token_usage -> principals through
-- principal_id; attributed rows are the minority, so the index is partial.
CREATE INDEX idx_token_usage_principal
    ON token_usage(principal_id) WHERE principal_id IS NOT NULL;
