-- The durable agent run: the header row over an append-only record log.
-- Status, suspension, and spend are projections of the log, committed in
-- the same transaction as the record that implies them. Exactly one
-- driving task per thread (stage family or driver family, XOR), so every
-- guard has a real row to target. The driver-task foreign key is added in
-- the driver-family migration, which closes the deliberate circular
-- reference between the two tables. The cancellation intent columns are
-- seq-free: writing them can never be lost to a racing record commit.
CREATE TABLE agent_threads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_thread_id UUID REFERENCES agent_threads(id),
    pipeline_stage TEXT NOT NULL CHECK (pipeline_stage IN ('extraction', 'triage', 'relation')),
    binding_version_id UUID NOT NULL,
    stage_task_id UUID REFERENCES tasks(id),
    driver_task_id UUID,
    principal_id UUID NOT NULL REFERENCES principals(id),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN (
        'queued', 'running', 'suspended', 'completed', 'failed', 'cancelled', 'dead_letter'
    )),
    suspension JSONB,
    cancel_requested_at TIMESTAMPTZ,
    cancel_requested_by TEXT,
    recovery_attempts INTEGER NOT NULL DEFAULT 0,
    format_version INTEGER NOT NULL,
    wake_at TIMESTAMPTZ,
    fidelity JSONB,
    execution_spend JSONB,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A thread's stage always matches its binding's stage: the composite
    -- reference makes the cross-stage pairing unrepresentable.
    CONSTRAINT fk_agent_threads_binding_stage FOREIGN KEY (binding_version_id, pipeline_stage)
        REFERENCES agent_binding_versions (id, pipeline_stage),
    -- Exactly one driving task: the guard's target is always a real row.
    CONSTRAINT chk_one_driving_task CHECK ((stage_task_id IS NULL) <> (driver_task_id IS NULL)),
    -- A suspension payload exists exactly while suspended.
    CONSTRAINT chk_suspended_has_cause CHECK ((status = 'suspended') = (suspension IS NOT NULL)),
    -- A terminal status pairs with its completion instant.
    CONSTRAINT chk_terminal_completed CHECK (
        (status IN ('completed', 'failed', 'cancelled', 'dead_letter')) = (completed_at IS NOT NULL)
    ),
    -- One thread per stage task, ever: the same task row blocks and
    -- re-queues across its thread's whole lifetime.
    CONSTRAINT uq_agent_threads_stage_task UNIQUE (stage_task_id)
);

CREATE INDEX idx_agent_threads_parent ON agent_threads (parent_thread_id)
    WHERE parent_thread_id IS NOT NULL;
-- The availability sweep's timer-wake scan.
CREATE INDEX idx_agent_threads_wake ON agent_threads (wake_at) WHERE status = 'suspended';
-- The cancel-fallback and janitor scan: live threads carrying an intent.
CREATE INDEX idx_agent_threads_cancel_intent ON agent_threads (cancel_requested_at)
    WHERE cancel_requested_at IS NOT NULL
      AND status NOT IN ('completed', 'failed', 'cancelled', 'dead_letter');
