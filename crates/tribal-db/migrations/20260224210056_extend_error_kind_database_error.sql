-- Extend the tasks.error_kind CHECK constraint to include 'database_error'.

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_error_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_error_kind_check CHECK (error_kind IN (
    'provider_error', 'semaphore_timeout', 'parse_error',
    'heartbeat_expired', 'startup_reclaim', 'ownership_lost', 'timeout',
    'database_error'
));
