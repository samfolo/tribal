-- Bind a local principal to its platform (user, account), or leave it local-only.
--
-- account_reference is the platform account id; platform_user_id is the opaque
-- platform user id — the linkage key. external_subject is never stored here: that
-- IdP-sourced PII stays platform-side, so a platform erase reaches every copy of
-- it. A platform-bound principal derives its display name at render time and
-- stores none; a local-only principal keeps its own. Both linkage columns are
-- cross-database pointers with no enforceable foreign key.

ALTER TABLE principals
    ADD COLUMN account_reference TEXT,
    ADD COLUMN platform_user_id  TEXT;

-- A principal is either fully platform-bound (both linkage columns set) or fully
-- local (both null).
ALTER TABLE principals
    ADD CONSTRAINT principals_platform_binding_paired
        CHECK ((account_reference IS NULL) = (platform_user_id IS NULL));

-- A platform-bound principal derives its display name and stores none.
ALTER TABLE principals
    ADD CONSTRAINT principals_platform_bound_has_no_name
        CHECK (platform_user_id IS NULL OR display_name IS NULL);
