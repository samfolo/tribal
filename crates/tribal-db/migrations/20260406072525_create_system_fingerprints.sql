-- System fingerprints: content-addressed system configuration snapshots.
--
-- Each row captures the full set of inference-affecting configuration values
-- at the time a job or feedback record was created. The content_hash column
-- is the SHA-256 of all fingerprint inputs, computed in memory.

CREATE TABLE system_fingerprints (
    id                                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_hash                          VARCHAR(64) NOT NULL UNIQUE
                                              CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    build_version                         TEXT NOT NULL,

    -- Prompt versions: FK for referential integrity
    extraction_system_prompt_version_id   UUID NOT NULL REFERENCES prompt_versions(id),
    extraction_user_prompt_version_id     UUID NOT NULL REFERENCES prompt_versions(id),
    triage_system_prompt_version_id       UUID NOT NULL REFERENCES prompt_versions(id),
    triage_user_prompt_version_id         UUID NOT NULL REFERENCES prompt_versions(id),
    relation_system_prompt_version_id     UUID NOT NULL REFERENCES prompt_versions(id),
    relation_user_prompt_version_id       UUID NOT NULL REFERENCES prompt_versions(id),

    -- Model identifiers: explicit columns for queryability
    extraction_inference_provider         TEXT NOT NULL,
    extraction_inference_model            TEXT NOT NULL,
    triage_inference_provider             TEXT NOT NULL,
    triage_inference_model                TEXT NOT NULL,
    relation_inference_provider           TEXT NOT NULL,
    relation_inference_model              TEXT NOT NULL,
    embedding_provider                    TEXT NOT NULL,
    embedding_model                       TEXT NOT NULL,

    -- Full inference-affecting parameters
    inference_parameters                  JSONB NOT NULL,

    created_at                            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Model filtering: "all fingerprints using this model"
CREATE INDEX idx_fingerprints_extraction_model
    ON system_fingerprints(extraction_inference_provider, extraction_inference_model);
CREATE INDEX idx_fingerprints_triage_model
    ON system_fingerprints(triage_inference_provider, triage_inference_model);
CREATE INDEX idx_fingerprints_relation_model
    ON system_fingerprints(relation_inference_provider, relation_inference_model);
CREATE INDEX idx_fingerprints_embedding_model
    ON system_fingerprints(embedding_provider, embedding_model);
