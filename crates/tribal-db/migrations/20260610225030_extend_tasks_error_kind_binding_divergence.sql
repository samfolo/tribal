-- Extend the tasks.error_kind CHECK constraint to include
-- 'binding_divergence': a durable thread resumed under a binding that no
-- longer matches the current stage configuration in an aspect execution
-- takes live (the route or the tool surface), which no retry can reconcile,
-- terminal on the error taxonomy's outcome axis.
ALTER TABLE tasks DROP CONSTRAINT tasks_error_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_error_kind_check CHECK (error_kind IN (
    'provider_error', 'semaphore_timeout', 'parse_error', 'heartbeat_expired',
    'startup_reclaim', 'ownership_lost', 'timeout', 'database_error',
    'internal_error', 'context_overflow', 'budget_exhausted', 'binding_divergence'
));
