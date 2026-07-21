INSERT INTO knowledge_items
    (id, project_id, principal_id, kind, content, tags, confidence, source_context)
VALUES ($1, $2, $3, 'fact', 'owned', '{}', 'verified', '{}'::jsonb)
