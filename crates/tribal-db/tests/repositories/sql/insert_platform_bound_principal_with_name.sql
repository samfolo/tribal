-- A platform-bound principal must not store a display name — it derives one.
INSERT INTO principals (principal_key, display_name, account_reference, platform_user_id)
VALUES ('user:bad-bound', 'Stored Name', 'account_1', 'user_1');
