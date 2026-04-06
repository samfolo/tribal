-- Rename policy_version to system_fingerprint_hash on retrieval_feedback
-- and tighten the constraint to match the system_fingerprints table.
--
-- Tables are empty — NOT NULL applied directly without backfill.

ALTER TABLE retrieval_feedback
    RENAME COLUMN policy_version TO system_fingerprint_hash;

ALTER TABLE retrieval_feedback
    ALTER COLUMN system_fingerprint_hash SET NOT NULL,
    ALTER COLUMN system_fingerprint_hash TYPE VARCHAR(64);

ALTER TABLE retrieval_feedback
    ADD CONSTRAINT fk_feedback_system_fingerprint
    FOREIGN KEY (system_fingerprint_hash)
    REFERENCES system_fingerprints(content_hash);
