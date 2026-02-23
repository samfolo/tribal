-- Amendment: add raw_input to jobs table and create extraction_results table.
--
-- Context: raw_input was omitted from the original schema due to a circular
-- deletion during document consolidation.
-- extraction_results stores the structured output of the extraction stage for
-- consumption by triage and relation stages.

ALTER TABLE jobs ADD COLUMN raw_input TEXT NOT NULL DEFAULT '';
-- DEFAULT '' is a migration convenience for any pre-existing rows.
-- In practice, the application always provides the content from tribal_ingest.
-- The default can be removed in a future migration once the schema is stable.

CREATE TABLE extraction_results (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID NOT NULL UNIQUE REFERENCES jobs(id),
    candidates     JSONB NOT NULL,
    relation_hints JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
