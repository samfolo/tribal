-- Insert a platform-bound principal directly, for meter-aggregation tests that
-- attribute spend to a platform (user, account) before C5's repository writer
-- exists. A bound principal stores no display name, per the M1 CHECK.
INSERT INTO principals (id, principal_key, platform_user_id, account_reference)
VALUES ($1, $2, $3, $4)
