-- One thread per driver task, mirroring the stage-task constraint: every
-- lease guards exactly one thread's log. No producer exists yet (the
-- driver family's first writer is the agentic loop), but later tickets
-- read this migration set, so the invariant lands with the tables.
ALTER TABLE agent_threads
    ADD CONSTRAINT uq_agent_threads_driver_task UNIQUE (driver_task_id);
