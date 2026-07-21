-- The origin arrives as a bind so malformed shapes can probe the CHECK.
INSERT INTO projects (origin, name, schema_version, settings)
VALUES ($1, 'invalid', 1, '{}'::jsonb)
