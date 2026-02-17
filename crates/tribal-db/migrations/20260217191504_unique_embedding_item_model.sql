-- Replace the non-unique index with a unique index so that only one
-- embedding per (knowledge_item_id, model) pair is stored.

DROP INDEX idx_embeddings_item_model;

CREATE UNIQUE INDEX idx_embeddings_item_model
    ON embeddings (knowledge_item_id, model);
