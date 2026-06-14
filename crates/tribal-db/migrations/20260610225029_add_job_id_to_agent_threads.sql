-- The job a thread's execution is metered to, captured at launch so a
-- reclaimed driver attributes a child's spend without walking the lineage
-- to a parent that may be terminal. Nullable: a thread launched outside a
-- pipeline job (none today) carries none, and SET NULL keeps the thread
-- readable if its job is ever removed.
ALTER TABLE agent_threads
    ADD COLUMN job_id UUID REFERENCES jobs(id) ON DELETE SET NULL;
