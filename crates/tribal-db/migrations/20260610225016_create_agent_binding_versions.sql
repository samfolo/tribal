-- One row per content-addressed agent binding version: what a thread ran,
-- pinned. The hash covers the canonically serialised definition including
-- the serialised tool descriptors, so a tool-surface change is a new
-- version even when the definition text is unchanged. Rows are append-only
-- and immutable; threads reference the version they were admitted under,
-- and budgets at resume re-resolve from the version current at that moment
-- without touching the thread's original reference.
CREATE TABLE agent_binding_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hash TEXT NOT NULL CHECK (hash ~ '^[0-9a-f]{64}$'),
    pipeline_stage TEXT NOT NULL CHECK (pipeline_stage IN ('extraction', 'triage', 'relation')),
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_agent_binding_versions_hash UNIQUE (hash),
    -- The target for the composite foreign key from agent_threads, which
    -- holds a thread's stage to its binding's stage.
    CONSTRAINT uq_agent_binding_versions_id_stage UNIQUE (id, pipeline_stage)
);
