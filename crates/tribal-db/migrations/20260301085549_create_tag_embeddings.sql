-- Tag embeddings for semantic tag resolution via pgvector.
-- Bare halfvec keyed by (tag, embedding_profile_id); model is denormalised
-- lineage. The per-profile partial HNSW index is built at runtime by first-boot
-- provisioning and the reindex worker, so only the profile-only btree index is
-- created here. ON CONFLICT (tag, embedding_profile_id) DO NOTHING gives
-- concurrent-safe upsert semantics.

CREATE TABLE tag_embeddings (
    tag                   TEXT NOT NULL REFERENCES tag_registry(tag),
    embedding_profile_id  UUID NOT NULL REFERENCES embedding_profiles(id),
    model                 TEXT NOT NULL,
    embedding             halfvec NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT tag_embeddings_pkey PRIMARY KEY (tag, embedding_profile_id),
    CONSTRAINT chk_nonzero_vector CHECK (l2_norm(embedding) > 0)
);

CREATE INDEX idx_tag_embeddings_profile ON tag_embeddings (embedding_profile_id);

-- Reuse the embeddings dimension guard (see the initial schema): reject a tag
-- embedding whose width does not match its profile's declared dimensions.
CREATE TRIGGER trg_tag_embeddings_dimension
    BEFORE INSERT ON tag_embeddings
    FOR EACH ROW EXECUTE FUNCTION assert_embedding_matches_profile_dimension();
