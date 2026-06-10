-- The append-only record log: one row per assistant message, one per tool
-- result, one per injected input. Merging granular records into coarser
-- views is trivial; splitting coarse rows later is not. The partial unique
-- fence makes executed tool results exactly-once per requested call;
-- external observations use the observed_tool_event kind, which is what
-- keeps the same-row CHECK expressible without reaching the thread row.
-- NULLS NOT DISTINCT is deliberately not used. Records are immutable: no
-- updated_at, and nothing updates a committed row.
CREATE TABLE agent_thread_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    thread_id UUID NOT NULL REFERENCES agent_threads(id),
    seq BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'assistant_message', 'tool_result', 'input', 'suspension',
        'cancellation', 'submission', 'observed_tool_event'
    )),
    requesting_seq BIGINT,
    tool_call_id TEXT,
    content JSONB NOT NULL,
    usage JSONB,
    trace_id TEXT,
    span_id TEXT,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_agent_thread_records_seq UNIQUE (thread_id, seq),
    -- An executed tool result always names what it answers.
    CONSTRAINT chk_tool_result_fence_columns CHECK (
        kind <> 'tool_result' OR (requesting_seq IS NOT NULL AND tool_call_id IS NOT NULL)
    )
);

-- Executed results are exactly-once per requested call (the fence).
CREATE UNIQUE INDEX uq_agent_thread_records_tool_result
    ON agent_thread_records (thread_id, requesting_seq, tool_call_id)
    WHERE kind = 'tool_result';
