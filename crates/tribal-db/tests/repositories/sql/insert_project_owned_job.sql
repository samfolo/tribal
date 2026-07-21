INSERT INTO jobs
    (id, project_id, principal_id, source_context, status,
     extraction_system_prompt_version_id, extraction_user_prompt_version_id,
     triage_system_prompt_version_id, triage_user_prompt_version_id,
     relation_system_prompt_version_id, relation_user_prompt_version_id,
     raw_input, system_fingerprint_hash)
VALUES ($1, $2, $3, '{}', 'queued', $4, $5, $6, $7, $8, $9, 'owned', $10)
