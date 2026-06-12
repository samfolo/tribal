-- One thread per driver task, mirroring the stage-task constraint: every
-- lease guards exactly one thread's log. No writer exists yet; the
-- invariant lands with the tables so the first one inherits it.
ALTER TABLE agent_threads
    ADD CONSTRAINT uq_agent_threads_driver_task UNIQUE (driver_task_id);
