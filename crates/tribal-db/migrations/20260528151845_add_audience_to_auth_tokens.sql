-- Add the audience column to auth_tokens for RFC 8707 audience binding.
-- Every issued bearer token records the canonical resource URL it was
-- minted for. The bearer middleware refuses tokens whose audience does
-- not match the running server's resource URL. The column has no
-- default, so every insert must supply an audience explicitly.
ALTER TABLE auth_tokens
    ADD COLUMN audience TEXT NOT NULL;
