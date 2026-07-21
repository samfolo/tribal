INSERT INTO prompt_versions (id, stage, class, role, content_hash, content)
VALUES ($1, $2, 'one_shot', $3, $4, 'migration prompt')
