-- Tag embeddings for semantic tag resolution via pgvector.
-- The (tag, model) primary key supports model switching and provides
-- concurrent-safe upsert semantics via ON CONFLICT DO NOTHING.

CREATE TABLE tag_embeddings (
    tag        TEXT NOT NULL REFERENCES tag_registry(tag),
    model      TEXT NOT NULL,
    dimensions INT NOT NULL,
    embedding  vector(768) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tag_embeddings_pkey PRIMARY KEY (tag, model)
);

CREATE INDEX idx_tag_embeddings_hnsw
    ON tag_embeddings
    USING hnsw (embedding vector_cosine_ops);
