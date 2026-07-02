SELECT account_reference, platform_user_id, display_name
FROM principals
WHERE principal_key = $1;
