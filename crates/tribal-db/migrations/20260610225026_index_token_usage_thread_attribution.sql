-- Partial indexes for the thread-attribution columns. The ON DELETE SET
-- NULL enforcement scans the ledger once per pruned record row, and the
-- terminal commit's record-linking UPDATE scopes by thread — both need
-- index support on the system's fastest-growing table, and partial
-- indexes keep the unattributed majority of rows out of them.
CREATE INDEX idx_token_usage_agent_thread
    ON token_usage(agent_thread_id)
    WHERE agent_thread_id IS NOT NULL;
CREATE INDEX idx_token_usage_agent_thread_record
    ON token_usage(agent_thread_record_id)
    WHERE agent_thread_record_id IS NOT NULL;
