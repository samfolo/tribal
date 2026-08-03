-- Add migration script here
CREATE INDEX tag_registry_tag_c_collation_idx
ON tag_registry (tag COLLATE "C");
