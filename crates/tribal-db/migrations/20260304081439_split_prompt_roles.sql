-- Split prompt templates into system/user roles.
--
-- Adds a `role` column to `prompt_versions`, renames and adds prompt
-- version columns on `jobs` and `token_usage`.  The CHECK constraint
-- on `token_usage` requires the table to be empty (existing rows would
-- have non-NULL system_prompt_version_id but NULL user_prompt_version_id).

-- ---------------------------------------------------------------------------
-- prompt_versions
-- ---------------------------------------------------------------------------

ALTER TABLE prompt_versions
    ADD COLUMN role TEXT NOT NULL DEFAULT 'system'
    CHECK (role IN ('system', 'user'));

ALTER TABLE prompt_versions ALTER COLUMN role DROP DEFAULT;

ALTER TABLE prompt_versions
    DROP CONSTRAINT prompt_versions_stage_content_hash_key;
ALTER TABLE prompt_versions
    ADD CONSTRAINT prompt_versions_stage_role_content_hash_key
    UNIQUE (stage, role, content_hash);

-- ---------------------------------------------------------------------------
-- jobs — requires empty jobs table
-- ---------------------------------------------------------------------------

ALTER TABLE jobs RENAME COLUMN extraction_prompt_version_id
    TO extraction_system_prompt_version_id;
ALTER TABLE jobs RENAME COLUMN triage_prompt_version_id
    TO triage_system_prompt_version_id;
ALTER TABLE jobs RENAME COLUMN relation_prompt_version_id
    TO relation_system_prompt_version_id;

ALTER TABLE jobs ADD COLUMN extraction_user_prompt_version_id
    UUID NOT NULL REFERENCES prompt_versions(id);
ALTER TABLE jobs ADD COLUMN triage_user_prompt_version_id
    UUID NOT NULL REFERENCES prompt_versions(id);
ALTER TABLE jobs ADD COLUMN relation_user_prompt_version_id
    UUID NOT NULL REFERENCES prompt_versions(id);

-- ---------------------------------------------------------------------------
-- token_usage — requires empty token_usage table
-- ---------------------------------------------------------------------------

ALTER TABLE token_usage RENAME COLUMN prompt_version_id
    TO system_prompt_version_id;

ALTER TABLE token_usage ADD COLUMN user_prompt_version_id
    UUID REFERENCES prompt_versions(id);

ALTER TABLE token_usage ADD CONSTRAINT prompt_version_columns_both_or_neither
    CHECK (
        (system_prompt_version_id IS NULL AND user_prompt_version_id IS NULL)
        OR (system_prompt_version_id IS NOT NULL AND user_prompt_version_id IS NOT NULL)
    );
