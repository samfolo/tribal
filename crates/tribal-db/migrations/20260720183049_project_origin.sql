ALTER TABLE projects ADD COLUMN origin JSONB;

UPDATE projects
SET origin = jsonb_build_object(
    'kind', 'git',
    'remote', git_remote,
    'default_branch', default_branch
);

ALTER TABLE projects ALTER COLUMN origin SET NOT NULL;

ALTER TABLE projects ADD CONSTRAINT projects_origin_shape CHECK (
    origin = '{"kind":"system"}'::jsonb
    OR (
        origin->>'kind' = 'git'
        AND jsonb_typeof(origin->'remote') = 'string'
        AND length(origin->>'remote') > 0
        AND jsonb_typeof(origin->'default_branch') = 'string'
        AND origin = jsonb_build_object(
            'kind', 'git',
            'remote', origin->'remote',
            'default_branch', origin->>'default_branch'
        )
    )
);

ALTER TABLE projects DROP CONSTRAINT projects_git_remote_key;
ALTER TABLE projects DROP COLUMN git_remote;
ALTER TABLE projects DROP COLUMN default_branch;

CREATE UNIQUE INDEX projects_one_system_origin
    ON projects ((origin->>'kind'))
    WHERE origin->>'kind' = 'system';

CREATE UNIQUE INDEX projects_one_git_remote
    ON projects ((origin->>'remote'))
    WHERE origin->>'kind' = 'git';

INSERT INTO projects (origin, name, project_type, schema_version, settings)
VALUES ('{"kind":"system"}'::jsonb, 'General', NULL, 1, '{}'::jsonb)
ON CONFLICT DO NOTHING;
