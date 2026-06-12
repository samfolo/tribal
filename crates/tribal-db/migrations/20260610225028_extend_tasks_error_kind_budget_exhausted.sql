-- Extend the tasks.error_kind CHECK constraint to include
-- 'budget_exhausted': an execution budget exhausted in a way no retry
-- can change (the turn cap, the execution deadline, or the bounded
-- budget re-checks running dry), terminal on the error taxonomy's
-- outcome axis.
ALTER TABLE tasks DROP CONSTRAINT tasks_error_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_error_kind_check CHECK (error_kind IN (
    'provider_error', 'semaphore_timeout', 'parse_error', 'heartbeat_expired',
    'startup_reclaim', 'ownership_lost', 'timeout', 'database_error',
    'internal_error', 'context_overflow', 'budget_exhausted'
));
