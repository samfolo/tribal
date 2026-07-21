-- A project in the flat-remote shape the origin migration rewrites:
-- remote and branch as scalar columns rather than a jsonb origin.
INSERT INTO projects (id, git_remote, name, default_branch, schema_version, settings)
VALUES ($1, $2, $3, $4, 1, '{}'::jsonb)
