-- Restructure system_fingerprints around stage binding versions.
--
-- Per-stage configuration (the six prompt versions, per-stage provider,
-- model, and sampling parameters) is subsumed by each stage's
-- content-addressed binding version; the fingerprint composes the three
-- binding hashes with what no stage binding carries: the build version,
-- the job-level pipeline parameters, and the embedding identity with its
-- dimensions.
--
-- The table is recreated rather than altered: existing rows cannot name
-- binding versions that did not exist when they were written, and the
-- fingerprint has carried no downstream analysis yet. Jobs and feedback
-- rows keep their historic hash strings; the re-added foreign keys are
-- NOT VALID so those dangling references are tolerated while every new
-- write is enforced.

ALTER TABLE jobs
    DROP CONSTRAINT jobs_system_fingerprint_hash_fkey;
ALTER TABLE retrieval_feedback
    DROP CONSTRAINT fk_feedback_system_fingerprint;

DROP TABLE system_fingerprints;

CREATE TABLE system_fingerprints (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_hash                VARCHAR(64) NOT NULL UNIQUE
                                    CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    build_version               TEXT NOT NULL,

    -- Stage binding versions: content addresses over each stage's
    -- canonically serialised agent definition.
    extraction_binding_hash     VARCHAR(64) NOT NULL
                                    CHECK (extraction_binding_hash ~ '^[0-9a-f]{64}$'),
    triage_binding_hash         VARCHAR(64) NOT NULL
                                    CHECK (triage_binding_hash ~ '^[0-9a-f]{64}$'),
    relation_binding_hash       VARCHAR(64) NOT NULL
                                    CHECK (relation_binding_hash ~ '^[0-9a-f]{64}$'),

    -- Embedding identity: explicit columns for queryability.
    embedding_provider          TEXT NOT NULL,
    embedding_model             TEXT NOT NULL,
    embedding_dimensions        INTEGER NOT NULL CHECK (embedding_dimensions > 0),

    -- Job-level parameters subsumed by no stage binding.
    pipeline_parameters         JSONB NOT NULL,

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Binding filtering: "all fingerprints running this binding version".
CREATE INDEX idx_system_fingerprints_extraction_binding
    ON system_fingerprints(extraction_binding_hash);
CREATE INDEX idx_system_fingerprints_triage_binding
    ON system_fingerprints(triage_binding_hash);
CREATE INDEX idx_system_fingerprints_relation_binding
    ON system_fingerprints(relation_binding_hash);

ALTER TABLE jobs
    ADD CONSTRAINT jobs_system_fingerprint_hash_fkey
    FOREIGN KEY (system_fingerprint_hash)
    REFERENCES system_fingerprints(content_hash)
    NOT VALID;
ALTER TABLE retrieval_feedback
    ADD CONSTRAINT fk_feedback_system_fingerprint
    FOREIGN KEY (system_fingerprint_hash)
    REFERENCES system_fingerprints(content_hash)
    NOT VALID;
