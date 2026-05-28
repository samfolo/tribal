-- DCR-registered OAuth clients per RFC 7591.
--
-- client_id is the opaque identifier returned to the client at
-- registration. It is TEXT (not a foreign key target for the
-- authorisation-code table) so the system can also accept client
-- identifiers that have no stored registration row here; validation
-- against the registry happens in the application layer.
--
-- client_secret_hash holds the SHA-256 hex digest of the raw secret;
-- the raw secret is returned once at registration and never persisted.
CREATE TABLE oauth_clients (
    client_id                  TEXT PRIMARY KEY,
    client_secret_hash         VARCHAR(64) CHECK (
        client_secret_hash IS NULL
        OR client_secret_hash ~ '^[0-9a-f]{64}$'
    ),
    client_name                TEXT,
    redirect_uris              TEXT[] NOT NULL,
    grant_types                TEXT[] NOT NULL DEFAULT '{authorization_code}',
    response_types             TEXT[] NOT NULL DEFAULT '{code}',
    token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none'
                                   CHECK (token_endpoint_auth_method IN (
                                       'none', 'client_secret_basic', 'client_secret_post'
                                   )),
    scope                      TEXT,
    application_type           TEXT CHECK (
        application_type IS NULL
        OR application_type IN ('web', 'native')
    ),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    client_secret_expires_at   TIMESTAMPTZ
);
