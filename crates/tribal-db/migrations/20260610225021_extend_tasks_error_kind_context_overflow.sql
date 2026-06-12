-- Extend the tasks.error_kind CHECK constraint to include
-- 'context_overflow': a request that exceeded the model's context window,
-- terminal on the error taxonomy's outcome axis because retrying the same
-- input cannot succeed.
ALTER TABLE tasks DROP CONSTRAINT tasks_error_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_error_kind_check CHECK (error_kind IN (
    'provider_error', 'semaphore_timeout', 'parse_error', 'heartbeat_expired',
    'startup_reclaim', 'ownership_lost', 'timeout', 'database_error',
    'internal_error', 'context_overflow'
));
