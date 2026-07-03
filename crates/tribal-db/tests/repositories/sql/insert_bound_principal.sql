-- Insert a platform-bound principal directly with a caller-chosen id and key,
-- for a test that must place a bound principal the key-derived resolver would
-- not mint. A bound principal stores no display name, per the paired CHECKs.
INSERT INTO principals (id, principal_key, platform_user_id, account_reference)
VALUES ($1, $2, $3, $4)
