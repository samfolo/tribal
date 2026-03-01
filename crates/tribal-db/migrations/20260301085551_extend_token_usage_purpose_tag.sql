-- Extend the token_usage purpose CHECK constraint to include 'tag'
-- for tag embedding calls during resolution and startup backfill.

ALTER TABLE token_usage DROP CONSTRAINT token_usage_purpose_check;
ALTER TABLE token_usage ADD CONSTRAINT token_usage_purpose_check
    CHECK (purpose IN ('candidate', 'query', 'tag'));
