-- Extend the token_usage stage and purpose CHECK constraints to admit
-- provider probe calls. Probes are real, billable wire calls (a minimal
-- completion or a canonical-input embedding) and are ledgered like any
-- other call: inference probes as stage 'probe' with no purpose, embedding
-- probes as stage 'embedding' with purpose 'probe'. The existing
-- purpose_stage_check already admits both shapes.

ALTER TABLE token_usage DROP CONSTRAINT token_usage_stage_check;
ALTER TABLE token_usage ADD CONSTRAINT token_usage_stage_check
    CHECK (stage IN ('extraction', 'triage', 'relation', 'embedding', 'probe'));

ALTER TABLE token_usage DROP CONSTRAINT token_usage_purpose_check;
ALTER TABLE token_usage ADD CONSTRAINT token_usage_purpose_check
    CHECK (purpose IN ('candidate', 'query', 'tag', 'probe'));
