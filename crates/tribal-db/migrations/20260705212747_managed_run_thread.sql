-- The managed run: an agent thread with no product stage, anchored to a
-- run_job by an opaque cross-database key and fenced by that plane's claim
-- token. A managed thread carries no binding and no principal, so both
-- columns become nullable; their foreign keys go unchecked under MATCH
-- SIMPLE while the referencing column is null, so 'managed' never needs a
-- row in agent_binding_versions.
ALTER TABLE agent_threads
    ADD COLUMN run_key         TEXT,
    ADD COLUMN run_claim_token UUID,
    ALTER COLUMN binding_version_id DROP NOT NULL,
    ALTER COLUMN principal_id       DROP NOT NULL;

-- The managed stage joins the product stages.
ALTER TABLE agent_threads DROP CONSTRAINT agent_threads_pipeline_stage_check;
ALTER TABLE agent_threads ADD CONSTRAINT agent_threads_pipeline_stage_check
    CHECK (pipeline_stage IN ('extraction', 'triage', 'relation', 'managed'));

-- Exactly one driving anchor: a stage task, a driver task, or a run key.
-- num_nonnulls, not a chained three-way XOR, whose naive extension a triple
-- of non-nulls would also satisfy.
ALTER TABLE agent_threads DROP CONSTRAINT chk_one_driving_task;
ALTER TABLE agent_threads ADD CONSTRAINT chk_one_driving_anchor
    CHECK (num_nonnulls(stage_task_id, driver_task_id, run_key) = 1);

-- The managed stage and the run-key anchor imply each other.
ALTER TABLE agent_threads ADD CONSTRAINT chk_managed_has_run_key
    CHECK ((pipeline_stage = 'managed') = (run_key IS NOT NULL));

-- A managed thread has no binding; a product thread always has one.
ALTER TABLE agent_threads ADD CONSTRAINT chk_managed_has_no_binding
    CHECK ((pipeline_stage = 'managed') = (binding_version_id IS NULL));

-- Only a managed thread may omit its principal (account-level automation);
-- the one-way check leaves room for a future operator-initiated managed run
-- to name one without a migration.
ALTER TABLE agent_threads ADD CONSTRAINT chk_product_has_principal
    CHECK (pipeline_stage = 'managed' OR principal_id IS NOT NULL);

-- The run-key anchor and the pipeline job reference never co-occur.
ALTER TABLE agent_threads ADD CONSTRAINT chk_run_key_excludes_job_id
    CHECK (run_key IS NULL OR job_id IS NULL);

-- One thread per run, ever: the managed analogue of uq_agent_threads_stage_task.
CREATE UNIQUE INDEX uq_agent_threads_run_key
    ON agent_threads (run_key) WHERE run_key IS NOT NULL;
