-- Prompt versions gain the executor-class axis: the launched one-shot
-- pair, the agentic loop's pair, and the verifier child's pair are the
-- same two roles under different classes. Launched rows are the one-shot
-- class by definition; the default backfills them and then drops so
-- every future insert names its class explicitly.
ALTER TABLE prompt_versions
    ADD COLUMN class TEXT NOT NULL DEFAULT 'one_shot'
    CHECK (class IN ('one_shot', 'loop', 'verifier'));

ALTER TABLE prompt_versions ALTER COLUMN class DROP DEFAULT;

-- The class joins the content-address identity: identical content in
-- the same stage and role is one row per class, so an active-slot
-- lookup never crosses executor surfaces.
ALTER TABLE prompt_versions
    DROP CONSTRAINT prompt_versions_stage_role_content_hash_key;
ALTER TABLE prompt_versions
    ADD CONSTRAINT prompt_versions_stage_class_role_content_hash_key
    UNIQUE (stage, class, role, content_hash);
