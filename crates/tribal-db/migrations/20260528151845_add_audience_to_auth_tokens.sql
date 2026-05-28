-- Add the audience column to auth_tokens for RFC 8707 audience binding.
-- Every issued bearer token records the canonical resource URL it was
-- minted for. The bearer middleware refuses tokens whose audience does
-- not match the running server's resource URL.
--
-- Existing rows backfill to the empty string sentinel; the default is
-- then dropped so future inserts must supply audience explicitly.
ALTER TABLE auth_tokens
    ADD COLUMN audience TEXT NOT NULL DEFAULT '';

ALTER TABLE auth_tokens
    ALTER COLUMN audience DROP DEFAULT;
