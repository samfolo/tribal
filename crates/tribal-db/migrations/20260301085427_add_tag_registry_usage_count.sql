-- Add usage_count to tag_registry for tie-breaking in semantic tag resolution
-- and future analytics on tag adoption.

ALTER TABLE tag_registry ADD COLUMN usage_count INT NOT NULL DEFAULT 0;
