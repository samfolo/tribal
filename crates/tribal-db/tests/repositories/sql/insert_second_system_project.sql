-- A second system-origin project, to prove the singleton index refuses it.
INSERT INTO projects (origin, name, schema_version, settings)
VALUES ('{"kind":"system"}'::jsonb, 'Duplicate', 1, '{}'::jsonb)
