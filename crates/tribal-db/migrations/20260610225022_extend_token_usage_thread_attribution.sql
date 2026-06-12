-- Attribute inference spend to an agent thread and the record it produced,
-- mirroring the job, task, and reindex-run attribution the ledger already
-- carries. Nullable and additive; non-thread callers leave both null.
-- ON DELETE SET NULL deviates from the reindex-run precedent deliberately:
-- thread prune deletes thread and record rows, and the spend ledger is a
-- financial record that must outlive them.
ALTER TABLE token_usage
    ADD COLUMN agent_thread_id UUID REFERENCES agent_threads(id) ON DELETE SET NULL;
ALTER TABLE token_usage
    ADD COLUMN agent_thread_record_id UUID REFERENCES agent_thread_records(id) ON DELETE SET NULL;
