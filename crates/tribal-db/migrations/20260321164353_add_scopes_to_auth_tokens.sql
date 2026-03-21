-- Add permission scopes to auth tokens for scope-based authorisation.
-- Existing tokens default to full access (tribal:read + tribal:write).
-- The default is dropped after backfill so future inserts must supply
-- scopes explicitly — omitting them fails with a NOT NULL violation.
ALTER TABLE auth_tokens
    ADD COLUMN scopes TEXT[] NOT NULL DEFAULT '{tribal:read,tribal:write}';

ALTER TABLE auth_tokens
    ALTER COLUMN scopes DROP DEFAULT;
