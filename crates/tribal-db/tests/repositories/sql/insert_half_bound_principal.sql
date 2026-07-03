-- An account reference without the opaque user id is neither bound nor local.
INSERT INTO principals (principal_key, account_reference)
VALUES ('user:half-bound', 'account_1');
