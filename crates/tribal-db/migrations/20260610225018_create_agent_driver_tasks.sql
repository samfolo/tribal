-- The agent driver queue family: rows that drive threads with no stage
-- task (delegation children, verifiers) and execute deferred tool work.
-- Same lease mechanics as the launched task families: claim token,
-- heartbeat, availability, FOR UPDATE SKIP LOCKED claiming, reclaim,
-- retry, dead-letter. The reindex family is the precedent.
CREATE TABLE agent_driver_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id UUID NOT NULL REFERENCES agent_threads(id),
    kind TEXT NOT NULL CHECK (kind IN ('drive', 'deferred_tool')),
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN (
        'pending', 'claimed', 'completed', 'dead_letter'
    )),
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_token UUID,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT claimed_requires_token CHECK (state <> 'claimed' OR claim_token IS NOT NULL),
    CONSTRAINT claimed_requires_owner CHECK (state <> 'claimed' OR claimed_by IS NOT NULL),
    CONSTRAINT claimed_requires_heartbeat CHECK (state <> 'claimed' OR heartbeat_at IS NOT NULL),
    CONSTRAINT chk_terminal_completed CHECK (
        (state IN ('completed', 'dead_letter')) = (completed_at IS NOT NULL)
    )
);

-- The claim scan: pending rows whose availability has elapsed.
CREATE INDEX idx_agent_driver_tasks_pickup ON agent_driver_tasks (available_at)
    WHERE state = 'pending';
CREATE INDEX idx_agent_driver_tasks_thread ON agent_driver_tasks (thread_id);

-- Close the deliberate circular reference: threads point at their driving
-- driver row, driver rows point at their thread.
ALTER TABLE agent_threads
    ADD CONSTRAINT fk_agent_threads_driver_task
    FOREIGN KEY (driver_task_id) REFERENCES agent_driver_tasks(id);
