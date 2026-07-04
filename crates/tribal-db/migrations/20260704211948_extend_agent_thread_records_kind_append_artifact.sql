-- Admit append_artifact: a job's typed product, committed to the record log as
-- a durable output that never projects into the model-facing conversation.
ALTER TABLE agent_thread_records DROP CONSTRAINT agent_thread_records_kind_check;
ALTER TABLE agent_thread_records ADD CONSTRAINT agent_thread_records_kind_check CHECK (kind IN (
    'assistant_message', 'tool_result', 'input', 'suspension',
    'cancellation', 'submission', 'observed_tool_event', 'append_artifact'
));
