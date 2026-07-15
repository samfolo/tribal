CREATE TABLE local_default_credentials (
    authority_namespace VARCHAR(24) PRIMARY KEY
        CHECK (authority_namespace ~ '^[0-9a-f]{24}$'),
    generation_id UUID NOT NULL UNIQUE,
    token_id UUID NOT NULL UNIQUE REFERENCES auth_tokens(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
