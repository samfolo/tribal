-- OAuth authorisation codes per RFC 6749 §4.1.
--
-- Codes are single-use and short-lived. The raw code is delivered once
-- via the redirect; only its SHA-256 hex digest is stored. The
-- consume operation is an atomic UPDATE … RETURNING that sets
-- consumed_at, so a second exchange of the same code returns no row
-- and the /token endpoint surfaces invalid_grant.
--
-- client_id is TEXT (not a foreign key) so it can hold any client
-- identifier, including one that has no stored registration row in
-- oauth_clients; validation against the registry happens in the
-- application layer.
--
-- code_challenge_method is locked to S256 at the database level. OAuth
-- 2.1 forbids plain; advertising it in the AS metadata would be a
-- contract violation, and writing a row with anything other than S256
-- here would be a regression.
CREATE TABLE oauth_authorization_codes (
    code_hash             VARCHAR(64) PRIMARY KEY
                              CHECK (code_hash ~ '^[0-9a-f]{64}$'),
    client_id             TEXT NOT NULL,
    redirect_uri          TEXT NOT NULL,
    code_challenge        TEXT NOT NULL,
    code_challenge_method TEXT NOT NULL CHECK (code_challenge_method = 'S256'),
    scope                 TEXT,
    resource              TEXT,
    principal_id          UUID NOT NULL REFERENCES principals(id) ON DELETE RESTRICT,
    issued_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at            TIMESTAMPTZ NOT NULL,
    consumed_at           TIMESTAMPTZ
);

CREATE INDEX idx_oauth_codes_expires ON oauth_authorization_codes(expires_at);
