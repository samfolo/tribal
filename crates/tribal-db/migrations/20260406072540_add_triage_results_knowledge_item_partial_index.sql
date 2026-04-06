-- Partial index for provenance chain: item → triage result → job → fingerprint.
-- Only rows where knowledge_item_id IS NOT NULL are indexed.

CREATE INDEX idx_triage_results_knowledge_item
    ON triage_results(knowledge_item_id)
    WHERE knowledge_item_id IS NOT NULL;
