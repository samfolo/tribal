ALTER TABLE projects
    RENAME CONSTRAINT projects_origin_shape TO chk_projects_origin_shape;

ALTER INDEX projects_one_system_origin
    RENAME TO uq_projects_system_origin;

ALTER INDEX projects_one_git_remote
    RENAME TO uq_projects_git_remote;
