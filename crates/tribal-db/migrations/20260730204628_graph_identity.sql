-- The graph names itself: one immutable identity row per database, created
-- here and never by a caller. The single-row constraint makes a second
-- identity impossible; the seed insert covers databases migrating from
-- earlier heads as well as fresh ones.
CREATE TABLE graph_identity (
    only_row   BOOLEAN NOT NULL PRIMARY KEY DEFAULT TRUE CHECK (only_row),
    graph_id   UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO graph_identity (only_row) VALUES (TRUE);
