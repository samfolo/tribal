-- The forward-only blocked status: a task driving a suspended agent
-- thread, unclaimable until the suspension resolves, holding no lease and
-- no worker slot. Binaries predating this status must not run against a
-- database where blocked rows exist; the release notes say so.
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (status IN (
    'queued', 'claimed', 'blocked', 'completed', 'dead_letter'
));

-- Blocked rows are unclaimed by construction: suspension releases the
-- lease in the same transaction that blocks the task.
ALTER TABLE tasks ADD CONSTRAINT blocked_requires_no_token CHECK (
    status <> 'blocked' OR claim_token IS NULL
);
