-- Add last_seen_at and usage_count to tag_registry for tie-breaking in
-- semantic tag resolution and longitudinal analytics on tag adoption.

ALTER TABLE tag_registry ADD COLUMN last_seen_at TIMESTAMPTZ;
ALTER TABLE tag_registry ADD COLUMN usage_count INT NOT NULL DEFAULT 0;
